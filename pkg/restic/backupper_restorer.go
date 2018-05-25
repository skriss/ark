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
	"k8s.io/client-go/tools/cache"

	arkv1api "github.com/heptio/ark/pkg/apis/ark/v1"
	clientset "github.com/heptio/ark/pkg/generated/clientset/versioned"
	informers "github.com/heptio/ark/pkg/generated/informers/externalversions/ark/v1"
	"github.com/heptio/ark/pkg/util/boolptr"
)

type backupperRestorer struct {
	metadataManager          RepositoryManager
	client                   clientset.Interface
	podVolumeBackupInformer  cache.SharedIndexInformer
	podVolumeRestoreInformer cache.SharedIndexInformer
	backupResults            map[types.UID]chan *arkv1api.PodVolumeBackup
	restoreResults           map[types.UID]chan *arkv1api.PodVolumeRestore
	namespace                string
}

func NewBackupperRestorer(
	metadataManager RepositoryManager,
	client clientset.Interface,
	namespace string,
) (BackupperRestorer, error) {
	br := &backupperRestorer{
		metadataManager: metadataManager,
		client:          client,
		backupResults:   make(map[types.UID]chan *arkv1api.PodVolumeBackup),
		restoreResults:  make(map[types.UID]chan *arkv1api.PodVolumeRestore),
		namespace:       namespace,
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

	br.podVolumeRestoreInformer = informers.NewPodVolumeRestoreInformer(client, namespace, 0, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	br.podVolumeRestoreInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(_, obj interface{}) {
				pvr := obj.(*arkv1api.PodVolumeRestore)

				if pvr.Status.Phase != arkv1api.PodVolumeRestorePhaseCompleted &&
					pvr.Status.Phase != arkv1api.PodVolumeRestorePhaseFailed {
					return
				}

				var restoreUID types.UID
				for _, owner := range pvr.OwnerReferences {
					if boolptr.IsSetToTrue(owner.Controller) {
						restoreUID = owner.UID
						break
					}
				}

				if c, ok := br.restoreResults[restoreUID]; ok {
					c <- pvr
				}
			},
		},
	)

	// TODO do I need to use an actual context here?
	go br.podVolumeBackupInformer.Run(make(chan struct{}))
	go br.podVolumeRestoreInformer.Run(make(chan struct{}))
	if !cache.WaitForCacheSync(make(chan struct{}), br.podVolumeBackupInformer.HasSynced, br.podVolumeRestoreInformer.HasSynced) {
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

	// add a channel for this Ark restore to receive PVR results on
	br.restoreResults[restore.UID] = make(chan *arkv1api.PodVolumeRestore)
	defer delete(br.restoreResults, restore.UID)

	for volume, snapshot := range volumesToRestore {
		br.metadataManager.RLock(pod.Namespace)
		defer br.metadataManager.RUnlock(pod.Namespace)

		// TODO should we return here, or continue with what we can?
		if err := br.createPodVolumeRestore(restore, pod, volume, snapshot, log); err != nil {
			return err
		}
	}

	var (
		errs []error
		// TODO configurable
		timeout = time.After(10 * time.Minute)
	)

ForLoop:
	for i := 0; i < len(volumesToRestore); i++ {
		select {
		case <-timeout:
			errs = append(errs, errors.New("timed out waiting for all PodVolumeRestores to complete"))
			break ForLoop
		case res := <-br.restoreResults[restore.UID]:
			if res.Status.Phase == arkv1api.PodVolumeRestorePhaseFailed {
				errs = append(errs, errors.New("PodVolumeRestore failed"))
			}
		}
	}

	return kerrs.NewAggregate(errs)
}

func (br *backupperRestorer) createPodVolumeRestore(restore *arkv1api.Restore, pod *corev1api.Pod, volume, snapshot string, log logrus.FieldLogger) error {
	volumeRestore := &arkv1api.PodVolumeRestore{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    br.namespace,
			GenerateName: restore.Name + "-",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: arkv1api.SchemeGroupVersion.String(),
					Kind:       "Restore",
					Name:       restore.Name,
					UID:        restore.UID,
					Controller: boolptr.True(),
				},
			},
			Labels: map[string]string{
				"ark.heptio.com/restore": restore.Name,
			},
		},
		Spec: arkv1api.PodVolumeRestoreSpec{
			Pod: corev1api.ObjectReference{
				Kind:      "Pod",
				Namespace: pod.Namespace,
				Name:      pod.Name,
				UID:       pod.UID,
			},
			Volume:     volume,
			SnapshotID: snapshot,
			RepoPrefix: br.metadataManager.RepoPrefix(),
		},
	}

	return errors.WithStack(errorOnly(br.client.ArkV1().PodVolumeRestores(br.namespace).Create(volumeRestore)))
}
