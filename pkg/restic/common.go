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
	"encoding/json"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/heptio/ark/pkg/apis/ark/v1"
	"github.com/pkg/errors"
)

const (
	podAnnotationPrefix         = "snapshot.ark.heptio.com/"
	volumesToBackupAnnotation   = "backup.ark.heptio.com/backup-volumes"
	snapshotsInBackupAnnotation = "backup.ark.heptio.com/restic-snapshots"
)

func PodHasSnapshotAnnotation(obj metav1.Object) bool {
	for key := range obj.GetAnnotations() {
		if strings.HasPrefix(key, podAnnotationPrefix) {
			return true
		}
	}

	return false
}

func GetPodSnapshotAnnotations(obj metav1.Object) map[string]string {
	var res map[string]string

	for k, v := range obj.GetAnnotations() {
		if strings.HasPrefix(k, podAnnotationPrefix) {
			if res == nil {
				res = make(map[string]string)
			}

			res[k[len(podAnnotationPrefix):]] = v
		}
	}

	return res
}

func SetPodSnapshotAnnotation(obj metav1.Object, volumeName, snapshotID string) {
	annotations := obj.GetAnnotations()

	if annotations == nil {
		annotations = make(map[string]string)
	}

	annotations[podAnnotationPrefix+volumeName] = snapshotID

	obj.SetAnnotations(annotations)
}

func GetVolumesToBackup(obj metav1.Object) []string {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		return nil
	}

	backupsValue := annotations[volumesToBackupAnnotation]
	if backupsValue == "" {
		return nil
	}

	var backups []string
	// check for json array
	if backupsValue[0] == '[' {
		if err := json.Unmarshal([]byte(backupsValue), &backups); err != nil {
			backups = []string{backupsValue}
		}
	} else {
		backups = append(backups, backupsValue)
	}

	return backups
}

func GetSnapshotsInBackup(backup *v1.Backup) ([]string, error) {
	if backup.Annotations == nil {
		return nil, nil
	}

	snapshotsValue := backup.Annotations[snapshotsInBackupAnnotation]
	if snapshotsValue == "" {
		return nil, nil
	}

	var snapshots []string
	if err := json.Unmarshal([]byte(snapshotsValue), &snapshots); err != nil {
		return nil, errors.WithStack(err)
	}

	return snapshots, nil
}

func SetSnapshotsInBackup(backup *v1.Backup, snapshots []string) error {
	jsonBytes, err := json.Marshal(snapshots)
	if err != nil {
		return errors.WithStack(err)
	}

	if backup.Annotations == nil {
		backup.Annotations = make(map[string]string)
	}

	backup.Annotations[snapshotsInBackupAnnotation] = string(jsonBytes)

	return nil
}
