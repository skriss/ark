/*
Copyright 2018 the Heptio Ark contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	corev1informers "k8s.io/client-go/informers/core/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	arkv1api "github.com/heptio/ark/pkg/apis/ark/v1"
	arkv1client "github.com/heptio/ark/pkg/generated/clientset/versioned/typed/ark/v1"
	informers "github.com/heptio/ark/pkg/generated/informers/externalversions/ark/v1"
	listers "github.com/heptio/ark/pkg/generated/listers/ark/v1"
	"github.com/heptio/ark/pkg/restic"
	"github.com/heptio/ark/pkg/util/boolptr"
	"github.com/heptio/ark/pkg/util/kube"
)

type podVolumeRestoreController struct {
	*genericController

	podVolumeRestoreClient arkv1client.PodVolumeRestoresGetter
	podVolumeRestoreLister listers.PodVolumeRestoreLister
	secretLister           corev1listers.SecretLister
	podLister              corev1listers.PodLister
	pvcLister              corev1listers.PersistentVolumeClaimLister
	nodeName               string

	processRestoreFunc func(*arkv1api.PodVolumeRestore) error
}

// NewPodVolumeRestoreController creates a new pod volume restore controller.
func NewPodVolumeRestoreController(
	logger logrus.FieldLogger,
	podVolumeRestoreInformer informers.PodVolumeRestoreInformer,
	podVolumeRestoreClient arkv1client.PodVolumeRestoresGetter,
	secretInformer corev1informers.SecretInformer,
	podInformer corev1informers.PodInformer,
	pvcInformer corev1informers.PersistentVolumeClaimInformer,
	nodeName string,
) Interface {
	c := &podVolumeRestoreController{
		genericController:      newGenericController("pod-volume-restore", logger),
		podVolumeRestoreClient: podVolumeRestoreClient,
		podVolumeRestoreLister: podVolumeRestoreInformer.Lister(),
		secretLister:           secretInformer.Lister(),
		podLister:              podInformer.Lister(),
		pvcLister:              pvcInformer.Lister(),
		nodeName:               nodeName,
	}

	c.syncHandler = c.processQueueItem
	c.cacheSyncWaiters = append(
		c.cacheSyncWaiters,
		podVolumeRestoreInformer.Informer().HasSynced,
		secretInformer.Informer().HasSynced,
		podInformer.Informer().HasSynced,
		pvcInformer.Informer().HasSynced,
	)
	c.processRestoreFunc = c.processRestore

	podVolumeRestoreInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.enqueue,
			UpdateFunc: func(_, obj interface{}) { c.enqueue(obj) },
		},
	)

	c.resyncPeriod = time.Hour
	// c.resyncFunc = c.deleteExpiredRequests

	return c
}

func (c *podVolumeRestoreController) processQueueItem(key string) error {
	log := c.logger.WithField("key", key)
	log.Debug("Running processItem")

	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return errors.Wrap(err, "error splitting queue key")
	}

	req, err := c.podVolumeRestoreLister.PodVolumeRestores(ns).Get(name)
	if apierrors.IsNotFound(err) {
		log.Debug("Unable to find PodVolumeRestore")
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "error getting PodVolumeRestore")
	}

	// only process new items
	if req.Status.Phase != "" && req.Status.Phase != arkv1api.PodVolumeRestorePhaseNew {
		return nil
	}

	pod, err := c.podLister.Pods(req.Spec.Pod.Namespace).Get(req.Spec.Pod.Name)
	if err != nil {
		log.WithError(err).Errorf("Unable to get pod %s/%s", req.Spec.Pod.Namespace, req.Spec.Pod.Name)
		return errors.WithStack(err)
	}

	// return unscheduled pods to the queue
	if pod.Spec.NodeName == "" {
		log.Warnf("Pod has not been assigned a node yet, returning item to queue")
		return errors.New("pod has not been assigned a node")
	}

	// only process items for pods on this node
	if pod.Spec.NodeName != c.nodeName {
		return nil
	}

	// only process items for pods that have their first initContainer running
	if statuses := pod.Status.InitContainerStatuses; len(statuses) == 0 || statuses[0].State.Running == nil {
		log.Warn("Pod's first initContainer is not running, returning item to queue")
		return errors.New("pod's first initContainer is not running")
	}

	// Don't mutate the shared cache
	reqCopy := req.DeepCopy()
	return c.processRestoreFunc(reqCopy)
}

func (c *podVolumeRestoreController) processRestore(req *arkv1api.PodVolumeRestore) error {
	log := c.logger.WithFields(logrus.Fields{
		"namespace": req.Namespace,
		"name":      req.Name,
	})

	var err error

	// update status to InProgress
	req, err = c.patchPodVolumeRestore(req, updatePodVolumeRestorePhaseFunc(arkv1api.PodVolumeRestorePhaseInProgress))
	if err != nil {
		log.WithError(err).Error("Error setting phase to InProgress")
		return errors.WithStack(err)
	}

	pod, err := c.podLister.Pods(req.Spec.Pod.Namespace).Get(req.Spec.Pod.Name)
	if err != nil {
		log.WithError(err).Errorf("Error getting pod %s/%s", req.Spec.Pod.Namespace, req.Spec.Pod.Name)
		return c.fail(req, errors.Wrap(err, "error getting pod").Error(), log)
	}

	volumeDir, err := kube.GetVolumeDirectory(pod, req.Spec.Volume, c.pvcLister)
	if err != nil {
		log.WithError(err).Error("Error getting volume directory name")
		return c.fail(req, errors.Wrap(err, "error getting volume directory name").Error(), log)
	}

	// TODO creds should move into the pod's namespace

	// temp creds
	file, err := restic.TempCredentialsFile(c.secretLister, "restic-credentials", req.Namespace, req.Spec.Pod.Namespace)
	if err != nil {
		log.WithError(err).Error("Error creating temp restic credentials file")
		return c.fail(req, errors.Wrap(err, "error creating temp restic credentials file").Error(), log)
	}

	defer file.Close()
	defer os.Remove(file.Name())

	resticCmd := restic.RestoreCommand(
		req.Spec.RepoPrefix,
		req.Spec.Pod.Namespace,
		file.Name(),
		string(req.Spec.Pod.UID),
		req.Spec.SnapshotID,
	)

	output, err := resticCmd.Cmd().Output()
	log.Debugf("Ran command=%s, stdout=%s", resticCmd.String(), output)
	if err != nil {
		var stderr string
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = string(exitErr.Stderr)
		}
		log.WithError(err).Errorf("Error running command=%s, stdout=%s, stderr=%s", resticCmd.String(), output, stderr)

		return c.fail(req, fmt.Sprintf("error running restic restore, stderr=%s: %s", stderr, err.Error()), log)
	}

	var restoreUID types.UID
	for _, owner := range req.OwnerReferences {
		if boolptr.IsSetToTrue(owner.Controller) {
			restoreUID = owner.UID
			break
		}
	}

	cmd := exec.Command("/bin/sh", "-c", strings.Join([]string{"/complete-restore.sh", string(req.Spec.Pod.UID), volumeDir, string(restoreUID)}, " "))
	output, err = cmd.Output()
	log.Debugf("Ran command=%s, stdout=%s", cmd.Args, output)
	if err != nil {
		var stderr string
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = string(exitErr.Stderr)
		}
		log.WithError(err).Errorf("Error running command=%s, stdout=%s, stderr=%s", cmd.Args, output, stderr)

		return c.fail(req, fmt.Sprintf("error running restic restore: %s: stderr=%s", err.Error(), stderr), log)
	}

	// update status to Completed
	if _, err = c.patchPodVolumeRestore(req, updatePodVolumeRestorePhaseFunc(arkv1api.PodVolumeRestorePhaseCompleted)); err != nil {
		log.WithError(err).Error("Error setting phase to Completed")
		return err
	}

	return nil
}

func (c *podVolumeRestoreController) patchPodVolumeRestore(req *arkv1api.PodVolumeRestore, mutate func(*arkv1api.PodVolumeRestore)) (*arkv1api.PodVolumeRestore, error) {
	// Record original json
	oldData, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling original PodVolumeRestore")
	}

	// Mutate
	mutate(req)

	// Record new json
	newData, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling updated PodVolumeRestore")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(oldData, newData)
	if err != nil {
		return nil, errors.Wrap(err, "error creating json merge patch for PodVolumeRestore")
	}

	req, err = c.podVolumeRestoreClient.PodVolumeRestores(req.Namespace).Patch(req.Name, types.MergePatchType, patchBytes)
	if err != nil {
		return nil, errors.Wrap(err, "error patching PodVolumeRestore")
	}

	return req, nil
}

func (c *podVolumeRestoreController) fail(req *arkv1api.PodVolumeRestore, msg string, log logrus.FieldLogger) error {
	if _, err := c.patchPodVolumeRestore(req, func(pvr *arkv1api.PodVolumeRestore) {
		pvr.Status.Phase = arkv1api.PodVolumeRestorePhaseFailed
		pvr.Status.Message = msg
	}); err != nil {
		log.WithError(err).Error("Error setting phase to Failed")
		return err
	}
	return nil
}

func updatePodVolumeRestorePhaseFunc(phase arkv1api.PodVolumeRestorePhase) func(r *arkv1api.PodVolumeRestore) {
	return func(r *arkv1api.PodVolumeRestore) {
		r.Status.Phase = phase
	}
}
