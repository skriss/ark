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
	switch req.Status.Phase {
	case "", arkv1api.PodVolumeRestorePhaseNew:
	default:
		return nil
	}

	// only process items for this node
	if req.Spec.Node != c.nodeName {
		return nil
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
		return c.fail(req, log)
	}

	volumeDir, err := kube.GetVolumeDirectory(pod, req.Spec.Volume, c.pvcLister)
	if err != nil {
		log.WithError(err).Error("Error getting volume directory name")
		return c.fail(req, log)
	}

	// matches, err := filepath.Glob(fmt.Sprintf("/host_pods/%s/volumes/*/%s/", string(req.Spec.Pod.UID), volumeDir))
	// if err != nil {
	// 	log.WithError(err).Error("Error matching volume path")
	// 	return errors.WithStack(err)
	// }
	// if len(matches) != 1 {
	// 	log.Errorf("Found %d matches for volume path", len(matches))
	// 	return errors.New("cannot uniquely identify volume path")
	// }

	// req.Status.Path = matches[0]

	// NOTE temp-creds creation is all copy-pasta from repositoryManager.exec()
	// TODO creds should move into the pod's namespace

	// temp creds
	file, err := restic.TempCredentialsFile(c.secretLister, "restic-credentials", req.Namespace, req.Spec.Pod.Namespace)
	if err != nil {
		log.WithError(err).Error("Error creating temp restic credentials file")
		return c.fail(req, log)
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

	// exec command
	res, err := resticCmd.Cmd().CombinedOutput()
	log.Infof("Ran restic command=%s, output=%s", resticCmd.String(), res)
	if err != nil {
		// TODO include error on CR
		// update status to Failed
		return c.fail(req, log)
	}

	var restoreUID types.UID
	for _, owner := range req.OwnerReferences {
		if boolptr.IsSetToTrue(owner.Controller) {
			restoreUID = owner.UID
			break
		}
	}
	// TODO handle error?
	cmdArgs := []string{"/complete-restore.sh", string(req.Spec.Pod.UID), volumeDir, string(restoreUID)}
	cmd := exec.Command("/bin/sh", "-c", strings.Join(cmdArgs, " "))
	res, err = cmd.CombinedOutput()
	log.Infof("Ran command=%v, output=%s", cmd.Args, res)
	if err != nil {
		return c.fail(req, log)
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

func (c *podVolumeRestoreController) fail(req *arkv1api.PodVolumeRestore, log logrus.FieldLogger) error {
	if _, err := c.patchPodVolumeRestore(req, updatePodVolumeRestorePhaseFunc(arkv1api.PodVolumeRestorePhaseFailed)); err != nil {
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
