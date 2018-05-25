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

package restic

import (
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kerrs "k8s.io/apimachinery/pkg/util/errors"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"

	arkv1api "github.com/heptio/ark/pkg/apis/ark/v1"
	clientset "github.com/heptio/ark/pkg/generated/clientset/versioned"
	informers "github.com/heptio/ark/pkg/generated/informers/externalversions/ark/v1"
	"github.com/heptio/ark/pkg/util/boolptr"
	"github.com/heptio/ark/pkg/util/kube"
)

type backupperRestorer struct {
	metadataManager         RepositoryManager
	daemonSetExecutor       DaemonSetExecutor
	pvcGetter               corev1client.PersistentVolumeClaimsGetter
	client                  clientset.Interface
	podVolumeBackupInformer cache.SharedIndexInformer
	backupResults           map[types.UID]chan *arkv1api.PodVolumeBackup
	namespace               string
}

func NewBackupperRestorer(
	metadataManager RepositoryManager,
	daemonSetExecutor DaemonSetExecutor,
	pvcGetter corev1client.PersistentVolumeClaimsGetter,
	client clientset.Interface,
	namespace string,
) (BackupperRestorer, error) {
	br := &backupperRestorer{
		metadataManager:   metadataManager,
		daemonSetExecutor: daemonSetExecutor,
		pvcGetter:         pvcGetter,
		client:            client,
		backupResults:     make(map[types.UID]chan *arkv1api.PodVolumeBackup),
		namespace:         namespace,
	}

	br.podVolumeBackupInformer = informers.NewPodVolumeBackupInformer(client, namespace, 0, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	br.podVolumeBackupInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(_, obj interface{}) {
				pvb := obj.(*arkv1api.PodVolumeBackup)

				if pvb.Status.Phase != arkv1api.PodVolumeBackupPhaseCompleted &&
					pvb.Status.Phase != arkv1api.PodVolumeBackupPhaseFailed {
					return
				}

				var backupUID types.UID
				for _, owner := range pvb.OwnerReferences {
					if boolptr.IsSetToTrue(owner.Controller) {
						backupUID = owner.UID
						break
					}
				}

				if c, ok := br.backupResults[backupUID]; ok {
					c <- pvb
				}
			},
		},
	)

	// TODO do I need to use an actual context here?
	go br.podVolumeBackupInformer.Run(make(chan struct{}))
	if !cache.WaitForCacheSync(make(chan struct{}), br.podVolumeBackupInformer.HasSynced) {
		return nil, errors.New("timed out waiting for cache to sync")
	}

	return br, nil
}

type BackupperRestorer interface {
	Backupper
	Restorer
}

// Backupper can execute restic backups of volumes in a pod.
type Backupper interface {
	BackupPodVolumes(backup *arkv1api.Backup, pod *corev1api.Pod, log logrus.FieldLogger) error
}

// Restorer can execute restic restores of volumes in a pod.
type Restorer interface {
	RestorePodVolumes(restore *arkv1api.Restore, pod *corev1api.Pod, log logrus.FieldLogger) error
}

func (br *backupperRestorer) BackupPodVolumes(backup *arkv1api.Backup, pod *corev1api.Pod, log logrus.FieldLogger) error {
	// get volumes to backup from pod's annotations
	volumesToBackup := GetVolumesToBackup(pod)
	if len(volumesToBackup) == 0 {
		return nil
	}

	// ensure a repo exists for the pod's namespace
	exists, err := br.metadataManager.RepositoryExists(pod.Namespace)
	if err != nil {
		return err
	}
	if !exists {
		if err := br.metadataManager.InitRepo(pod.Namespace); err != nil {
			return err
		}
	}

	// add a channel for this Ark backup to receive PVB results on
	br.backupResults[backup.UID] = make(chan *arkv1api.PodVolumeBackup)
	defer delete(br.backupResults, backup.UID)

	for _, volumeName := range volumesToBackup {
		br.metadataManager.RLock(pod.Namespace)
		defer br.metadataManager.RUnlock(pod.Namespace)

		// TODO should we return here, or continue with what we can?
		if err := br.createPodVolumeBackup(backup, pod, volumeName, log); err != nil {
			return err
		}
	}

	var (
		errs []error
		// TODO configurable
		timeout = time.After(10 * time.Minute)
	)

ForLoop:
	for i := 0; i < len(volumesToBackup); i++ {
		select {
		case <-timeout:
			errs = append(errs, errors.New("timed out waiting for all PodVolumeBackups to complete"))
			break ForLoop
		case res := <-br.backupResults[backup.UID]:
			switch {
			case res.Status.Phase == arkv1api.PodVolumeBackupPhaseFailed:
				errs = append(errs, errors.New("PodVolumeBackup failed"))
			default:
				SetPodSnapshotAnnotation(pod, res.Spec.Volume, res.Status.SnapshotID)
			}
		}
	}

	return kerrs.NewAggregate(errs)
}

func (br *backupperRestorer) createPodVolumeBackup(backup *arkv1api.Backup, pod *corev1api.Pod, volumeName string, log logrus.FieldLogger) error {
	volumeBackup := &arkv1api.PodVolumeBackup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    br.namespace,
			GenerateName: backup.Name + "-",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: arkv1api.SchemeGroupVersion.String(),
					Kind:       "Backup",
					Name:       backup.Name,
					UID:        backup.UID,
					Controller: boolptr.True(),
				},
			},
			Labels: map[string]string{
				"ark.heptio.com/backup": backup.Name,
			},
		},
		Spec: arkv1api.PodVolumeBackupSpec{
			Node: pod.Spec.NodeName,
			Pod: corev1api.ObjectReference{
				Kind:      "Pod",
				Namespace: pod.Namespace,
				Name:      pod.Name,
				UID:       pod.UID,
			},
			Volume: volumeName,
			Tags: map[string]string{
				"backup":     backup.Name,
				"backup-uid": string(backup.UID),
				"pod":        pod.Name,
				"pod-uid":    string(pod.UID),
				"ns":         pod.Namespace,
				"volume":     volumeName,
			},
			RepoPrefix: br.metadataManager.RepoPrefix(),
		},
	}

	return errors.WithStack(errorOnly(br.client.ArkV1().PodVolumeBackups(br.namespace).Create(volumeBackup)))
}

func (br *backupperRestorer) RestorePodVolumes(restore *arkv1api.Restore, pod *corev1api.Pod, log logrus.FieldLogger) error {
	// get volumes to restore from pod's annotations
	volumesToRestore := GetPodSnapshotAnnotations(pod)
	if len(volumesToRestore) == 0 {
		return nil
	}

	var (
		errs       []error
		resultChan = make(chan error)
	)

	// for each volume to restore:
	for volumeName, snapshotID := range volumesToRestore {
		go br.restoreVolume(restore, pod, volumeName, snapshotID, resultChan, log)
	}

	for i := 0; i < len(volumesToRestore); i++ {
		if err := <-resultChan; err != nil {
			errs = append(errs, err)
		}
	}

	return kerrs.NewAggregate(errs)
}

func (br *backupperRestorer) restoreVolume(restore *arkv1api.Restore, pod *corev1api.Pod, volumeName, snapshotID string, resultChan chan<- error, log logrus.FieldLogger) {
	// confirm it exists in the pod
	volume := getVolume(pod, volumeName)
	if volume == nil {
		resultChan <- errors.Errorf("volume %s does not exist in pod %s", volumeName, kube.NamespaceAndName(pod))
		return
	}

	// get the volume's directory name under /var/lib/kubelet/pods/... on the host
	volumeDir, err := getVolumeDirectory(volume, pod.Namespace, br.pvcGetter)
	if err != nil {
		resultChan <- err
		return
	}

	// assemble restic restore command
	restoreCmd := RestoreCommand(br.metadataManager.RepoPrefix(), pod.Namespace, string(pod.UID), snapshotID)

	if err := br.exec(pod.Spec.NodeName, pod.Namespace, restoreCmd.StringSlice(), 10*time.Minute, log); err != nil {
		resultChan <- err
		return
	}

	// exec the post-restore command (copy contents into target dir, write done file)
	completeCmd := []string{"/complete-restore.sh", string(pod.UID), volumeDir, string(restore.UID)}
	if err := br.daemonSetExecutor.Exec(pod.Spec.NodeName, completeCmd, time.Minute, log); err != nil {
		resultChan <- err
		return
	}

	resultChan <- nil
}

func (br *backupperRestorer) exec(node, namespace string, cmd []string, timeout time.Duration, log logrus.FieldLogger) error {
	br.metadataManager.RLock(namespace)
	defer br.metadataManager.RUnlock(namespace)

	return br.daemonSetExecutor.Exec(node, cmd, timeout, log)
}

// TODO get rid of once restore moves to controller
func getVolume(pod *corev1api.Pod, volumeName string) *corev1api.Volume {
	for _, item := range pod.Spec.Volumes {
		if item.Name == volumeName {
			return &item
		}
	}

	return nil
}

func getVolumeDirectory(volume *corev1api.Volume, namespace string, pvcGetter corev1client.PersistentVolumeClaimsGetter) (string, error) {
	if volume.VolumeSource.PersistentVolumeClaim == nil {
		return volume.Name, nil
	}

	pvc, err := pvcGetter.PersistentVolumeClaims(namespace).Get(volume.VolumeSource.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
	if err != nil {
		return "", errors.WithStack(err)
	}

	return pvc.Spec.VolumeName, nil
}
