/*
Copyright 2017 Heptio Inc.

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

package restore

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/heptio/ark/pkg/apis/ark/v1"
)

type addResticInitContainerAction struct {
	log logrus.FieldLogger
}

// NewaddResticInitContainerAction creates a new ItemAction for pods.
func NewAddResticInitContainerAction(log logrus.FieldLogger) ItemAction {
	return &addResticInitContainerAction{log: log}
}

// AppliesTo returns a ResourceSelector that applies only to pods.
func (a *addResticInitContainerAction) AppliesTo() (ResourceSelector, error) {
	return ResourceSelector{
		IncludedResources: []string{"pods"},
	}, nil
}

func (a *addResticInitContainerAction) Execute(item runtime.Unstructured, restore *v1.Restore) (runtime.Unstructured, error, error) {
	a.log.Info("Executing addResticInitContainerAction")
	defer a.log.Info("Done executing addResticInitContainerAction")

	var pod corev1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.UnstructuredContent(), &pod); err != nil {
		return nil, nil, errors.WithStack(err)
	}

	if len(pod.Spec.Volumes) == 0 {
		a.log.Info("pod has no volumes")
		return item, nil, nil
	}

	for _, volume := range pod.Spec.Volumes {
		// is this a volume that we have a snapshot for?
		snapshotID, found := pod.Annotations["snapshot.ark.heptio.com/"+volume.Name]
		if !found {
			a.log.Infof("Couldn't find snapshot annotation for volume %s", volume.Name)
			continue
		}
		a.log.Infof("Found snapshotID=%s", snapshotID)

		// if so, construct the init container to restore it
		initContainer := corev1.Container{
			Name:            "restic-init-" + volume.Name,
			Image:           "gcr.io/steve-heptio/restic:v0.8.2",
			ImagePullPolicy: corev1.PullAlways,
			Command:         []string{"/restic"},
			Args:            []string{"restore", "--target=/restores/", snapshotID},
			Env: []corev1.EnvVar{
				{
					Name: "RESTIC_REPOSITORY",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "restic-credentials",
							},
							Key: "RESTIC_REPOSITORY",
						},
					},
				},
				{
					Name: "RESTIC_PASSWORD",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "restic-credentials",
							},
							Key: "RESTIC_PASSWORD",
						},
					},
				},
				{
					Name: "AWS_ACCESS_KEY_ID",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "restic-credentials",
							},
							Key: "AWS_ACCESS_KEY_ID",
						},
					},
				},
				{
					Name: "AWS_SECRET_ACCESS_KEY",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "restic-credentials",
							},
							Key: "AWS_SECRET_ACCESS_KEY",
						},
					},
				},
			},
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      volume.Name,
					MountPath: "/restores/" + volume.Name,
				},
			},
		}

		idx := -1
		for i, container := range pod.Spec.InitContainers {
			if container.Name == initContainer.Name {
				idx = i
				break
			}
		}

		if idx == -1 {
			pod.Spec.InitContainers = append(pod.Spec.InitContainers, initContainer)
		} else {
			pod.Spec.InitContainers[idx] = initContainer
		}
	}

	result, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pod)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	return &unstructured.Unstructured{Object: result}, nil, nil
}
