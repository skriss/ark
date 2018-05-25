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
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/pkg/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1listers "k8s.io/client-go/listers/core/v1"

	arkv1api "github.com/heptio/ark/pkg/apis/ark/v1"
	arkv1listers "github.com/heptio/ark/pkg/generated/listers/ark/v1"
)

const (
	podAnnotationPrefix       = "snapshot.ark.heptio.com/"
	volumesToBackupAnnotation = "backup.ark.heptio.com/backup-volumes"
)

// TODO audit functions, make private (?)

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

func GetSnapshotsInBackup(backup *arkv1api.Backup, podVolumeBackupLister arkv1listers.PodVolumeBackupLister) ([]string, error) {
	selector, err := labels.Parse(fmt.Sprintf("%s=%s", arkv1api.BackupNameLabel, backup.Name))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	podVolumeBackups, err := podVolumeBackupLister.List(selector)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var res []string
	for _, item := range podVolumeBackups {
		if item.Status.SnapshotID == "" {
			continue
		}
		res = append(res, fmt.Sprintf("%s/%s", item.Spec.Pod.Namespace, item.Status.SnapshotID))
	}

	return res, nil
}

func TempCredentialsFile(secretLister corev1listers.SecretLister, secretName, secretNamespace, repoName string) (*os.File, error) {
	secret, err := secretLister.Secrets(secretNamespace).Get(secretName)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	repoKey, found := secret.Data[repoName]
	if !found {
		return nil, errors.Errorf("key %s not found in restic-credentials secret", repoName)
	}

	file, err := ioutil.TempFile("", fmt.Sprintf("restic-credentials-%s", repoName))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if _, err := file.Write(repoKey); err != nil {
		return nil, errors.WithStack(err)
	}

	return file, nil
}
