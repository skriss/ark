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
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kerrs "k8s.io/apimachinery/pkg/util/errors"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	arkv1api "github.com/heptio/ark/pkg/apis/ark/v1"
	"github.com/heptio/ark/pkg/util/kube"
)

type backupperRestorer struct {
	metadataManager   RepositoryManager
	daemonSetExecutor DaemonSetExecutor
	pvcGetter         corev1client.PersistentVolumeClaimsGetter
}

func NewBackupperRestorer(
	metadataManager RepositoryManager,
	daemonSetExecutor DaemonSetExecutor,
	pvcGetter corev1client.PersistentVolumeClaimsGetter,
) BackupperRestorer {
	return &backupperRestorer{
		metadataManager:   metadataManager,
		daemonSetExecutor: daemonSetExecutor,
		pvcGetter:         pvcGetter,
	}
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

	// Get existing snapshots annotated on backup, and fail-fast if
	// we can't. We'll append new snapshots to this at the end of
	// this func.
	backupSnapshots, err := GetSnapshotsInBackup(backup)
	if err != nil {
		return err
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

	var errs []error
	// for each volume to backup:
	for _, volumeName := range volumesToBackup {
		volume := getVolume(pod, volumeName)
		if volume == nil {
			errs = append(errs, errors.Errorf("volume %s does not exist in pod %s", volumeName, kube.NamespaceAndName(pod)))
			continue
		}

		// get the volume's directory name under /var/lib/kubelet/pods/... on the host
		volumeDir, err := getVolumeDirectory(volume, pod.Namespace, br.pvcGetter)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// assemble restic backup command
		snapshotTags := map[string]string{
			"backup":     backup.Name,
			"backup-uid": string(backup.UID),
			"pod":        pod.Name,
			"pod-uid":    string(pod.UID),
			"ns":         pod.Namespace,
			"volume":     volumeName,
		}

		cmd := backupCommand(br.metadataManager.RepoPrefix(), pod.Namespace, string(pod.UID), volumeDir, snapshotTags)

		// exec it in the daemonset pod
		if err := br.daemonSetExecutor.Exec(pod.Spec.NodeName, cmd, time.Minute, log); err != nil {
			errs = append(errs, err)
			continue
		}

		// get the snapshot's ID
		snapshotID, err := br.metadataManager.GetSnapshotID(pod.Namespace, string(backup.UID), string(pod.UID), volumeName)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// save the snapshot's ID in the pod annotation
		SetPodSnapshotAnnotation(pod, volumeName, snapshotID)
		backupSnapshots = append(backupSnapshots, fmt.Sprintf("%s/%s", pod.Namespace, snapshotID))
	}

	// only write the backup annotation if we had at least one successful snapshot
	if len(backupSnapshots) > 0 {
		// update backup's annotations with all snapshot IDs
		if err := SetSnapshotsInBackup(backup, backupSnapshots); err != nil {
			errs = append(errs, err)
		}
	}

	return kerrs.NewAggregate(errs)
}

func (br *backupperRestorer) RestorePodVolumes(restore *arkv1api.Restore, pod *corev1api.Pod, log logrus.FieldLogger) error {
	// get volumes to restore from pod's annotations
	volumesToRestore := GetPodSnapshotAnnotations(pod)
	if len(volumesToRestore) == 0 {
		return nil
	}

	var errs []error
	// for each volume to restore:
	for volumeName, snapshotID := range volumesToRestore {
		// confirm it exists in the pod
		volume := getVolume(pod, volumeName)
		if volume == nil {
			errs = append(errs, errors.Errorf("volume %s does not exist in pod %s", volumeName, kube.NamespaceAndName(pod)))
			continue
		}

		// get the volume's directory name under /var/lib/kubelet/pods/... on the host
		volumeDir, err := getVolumeDirectory(volume, pod.Namespace, br.pvcGetter)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// assemble restic restore command
		cmd := restoreCommand(br.metadataManager.RepoPrefix(), pod.Namespace, string(pod.UID), snapshotID)

		// exec it in the daemonset pod
		if err := br.daemonSetExecutor.Exec(pod.Spec.NodeName, cmd, time.Minute, log); err != nil {
			errs = append(errs, err)
			continue
		}

		// exec the post-restore command (copy contents into target dir, write done file)
		cmd = []string{"/complete-restore.sh", string(pod.UID), volumeDir, string(restore.UID)}
		if err := br.daemonSetExecutor.Exec(pod.Spec.NodeName, cmd, time.Minute, log); err != nil {
			errs = append(errs, err)
			continue
		}
	}

	return kerrs.NewAggregate(errs)
}

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

func backupCommand(repoPrefix, namespace, podUID, volumeDir string, tags map[string]string) []string {
	cmd := &command{
		baseName:   "/restic-wrapper",
		command:    "backup",
		repoPrefix: repoPrefix,
		repo:       namespace,
		args:       []string{fmt.Sprintf("/host_pods/%s/volumes/*/%s", podUID, volumeDir)},
		extraFlags: backupTagFlags(tags),
	}

	// needs to be executed within a shell because we're using a path wildcard
	// that needs to be expanded
	return []string{"/bin/sh", "-c", cmd.String()}
}

func restoreCommand(repoPrefix, namespace, podUID, snapshotID string) []string {
	cmd := &command{
		baseName:   "/restic-wrapper",
		command:    "restore",
		repoPrefix: repoPrefix,
		repo:       namespace,
		args:       []string{snapshotID},
		extraFlags: []string{restoreTargetFlag(podUID)},
	}

	return cmd.StringSlice()
}
