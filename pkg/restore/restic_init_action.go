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
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/heptio/ark/pkg/apis/ark/v1"
	"github.com/heptio/ark/pkg/util/collections"
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

	pod := item.UnstructuredContent()
	if !collections.Exists(pod, "spec.volumes") {
		a.log.Info("pod has no volumes")
		return item, nil, nil
	}

	metadata, err := meta.Accessor(item)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to access pod metadata")
	}

	volumes, err := collections.GetSlice(pod, "spec.volumes")
	if err != nil {
		return nil, nil, errors.WithMessage(err, "error getting spec.volumes")
	}

	for i := range volumes {
		volume, ok := volumes[i].(map[string]interface{})
		if !ok {
			continue
		}

		volumeName, err := collections.GetString(volume, "name")
		if err != nil {
			continue
		}

		// is this a volume that we have a snapshot for?
		snapshotID, found := metadata.GetAnnotations()["snapshot.ark.heptio.com/"+volumeName]
		if !found {
			a.log.Infof("Couldn't find snapshot annotation for volume %s", volumeName)
			continue
		}
		a.log.Infof("Found snapshotID=%s", snapshotID)

		// if so, construct the init container to restore it
		initContainer := &corev1.Container{
			Image:   "gcr.io/steve-heptio/restic:v0.8.2",
			Name:    "restic-init-" + volumeName,
			Command: []string{"/restic"},
			Args:    []string{"restore", "--target=/restores/", snapshotID},
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
				corev1.VolumeMount{
					Name:      volumeName,
					MountPath: "/restores/" + volumeName,
				},
			},
		}

		// get restic config/creds off the sidecar (which we assume to be there since a backup was taken)
		// resticEnv := pod["spec"].(map[string]interface{})["containers"].([]interface{})[1].(map[string]interface{})["env"].([]interface{})
		// for _, obj := range resticEnv {
		// 	sourceVar := obj.(map[string]interface{})

		// 	envVar := corev1.EnvVar{
		// 		Name: sourceVar["name"].(string),
		// 		ValueFrom: &corev1.EnvVarSource{
		// 			SecretKeyRef: &corev1.SecretKeySelector{
		// 				LocalObjectReference: corev1.LocalObjectReference{
		// 					Name: sourceVar["valueFrom"].(map[string]interface{})["secretKeyRef"].(map[string]interface{})["name"].(string),
		// 				},
		// 				Key: sourceVar["valueFrom"].(map[string]interface{})["secretKeyRef"].(map[string]interface{})["key"].(string),
		// 			},
		// 		},
		// 	}

		// 	initContainer.Env = append(initContainer.Env, envVar)
		// }

		initContainerJSON, err := json.Marshal(initContainer)
		if err != nil {
			a.log.WithError(err).Error("error marshaing initContainer to JSON")
		}

		a.log.Infof("initContainer JSON: %s", initContainerJSON)

		var initContainerMap map[string]interface{}

		if err := json.Unmarshal(initContainerJSON, &initContainerMap); err != nil {
			a.log.WithError(err).Error("error unmarshalling initContainer from JSON")
		}

		a.log.Infof("initContainer map: %#v", initContainerMap)

		if initContainers, err := collections.GetSlice(pod, "spec.initContainers"); err == nil {
			// is there already an init container on this pod for the volume (from a prev restore)?
			existing := -1
			for x, init := range initContainers {
				container := init.(map[string]interface{})

				if container["name"].(string) == initContainer.Name {
					existing = x
					break
				}
			}

			// if there is an existing one, replace it
			if existing >= 0 {
				initContainers[existing] = initContainerMap
			} else {
				initContainers = append(initContainers, initContainerMap)
			}
		} else {
			pod["spec"].(map[string]interface{})["initContainers"] = []interface{}{initContainerMap}
		}

		a.log.Infof("Pod map: %#v", pod)
	}

	return item, nil, nil
}
