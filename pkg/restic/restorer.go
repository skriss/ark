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
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kerrs "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/cache"

	arkv1api "github.com/heptio/ark/pkg/apis/ark/v1"
	"github.com/heptio/ark/pkg/util/boolptr"
)

// Restorer can execute restic restores of volumes in a pod.
type Restorer interface {
	// RestorePodVolumes restores all annotated volumes in a pod.
	RestorePodVolumes(ctx context.Context, restore *arkv1api.Restore, pod *corev1api.Pod, log logrus.FieldLogger) error
}

type restorer struct {
	repoManager *repositoryManager
	informer    cache.SharedIndexInformer
	results     map[string]chan *arkv1api.PodVolumeRestore
	resultsLock sync.Mutex
	ctx         context.Context
}

func (r *restorer) RestorePodVolumes(ctx context.Context, restore *arkv1api.Restore, pod *corev1api.Pod, log logrus.FieldLogger) error {
	// get volumes to restore from pod's annotations
	volumesToRestore := GetPodSnapshotAnnotations(pod)
	if len(volumesToRestore) == 0 {
		return nil
	}

	r.resultsLock.Lock()
	r.results[resultsKey(pod.Namespace, pod.Name)] = make(chan *arkv1api.PodVolumeRestore)
	r.resultsLock.Unlock()

	for volume, snapshot := range volumesToRestore {
		r.repoManager.repoLocker.Lock(pod.Namespace, false)
		defer r.repoManager.repoLocker.Unlock(pod.Namespace, false)

		// TODO should we return here, or continue with what we can?
		volumeRestore := &arkv1api.PodVolumeRestore{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:    restore.Namespace,
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
					arkv1api.RestoreNameLabel: restore.Name,
					arkv1api.RestoreUIDLabel:  string(restore.UID),
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
				RepoPrefix: r.repoManager.config.repoPrefix,
			},
		}

		// TODO should we return here, or continue with what we can?
		if err := errorOnly(r.repoManager.arkClient.ArkV1().PodVolumeRestores(volumeRestore.Namespace).Create(volumeRestore)); err != nil {
			return errors.WithStack(err)
		}
	}

	var errs []error

ForEachVolume:
	for i := 0; i < len(volumesToRestore); i++ {
		select {
		case <-ctx.Done():
			errs = append(errs, errors.New("timed out waiting for all PodVolumeRestores to complete"))
			break ForEachVolume
		case res := <-r.results[resultsKey(pod.Namespace, pod.Name)]:
			if res.Status.Phase == arkv1api.PodVolumeRestorePhaseFailed {
				errs = append(errs, errors.Errorf("pod volume restore failed: %s", res.Status.Message))
			}
		}
	}

	r.resultsLock.Lock()
	delete(r.results, resultsKey(pod.Namespace, pod.Name))
	r.resultsLock.Unlock()

	return kerrs.NewAggregate(errs)
}
