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

package restore

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	api "github.com/heptio/ark/pkg/apis/ark/v1"
	"github.com/heptio/ark/pkg/util/kube"
)

type resticRestoreAction struct {
	logger logrus.FieldLogger
}

func NewResticRestoreAction(logger logrus.FieldLogger) ItemAction {
	return &resticRestoreAction{
		logger: logger,
	}
}

func (a *resticRestoreAction) AppliesTo() (ResourceSelector, error) {
	return ResourceSelector{
		IncludedResources: []string{"pods"},
	}, nil
}

func (a *resticRestoreAction) Execute(obj runtime.Unstructured, restore *api.Restore) (runtime.Unstructured, error, error) {
	a.logger.Info("Executing resticRestoreAction")
	defer a.logger.Info("Done executing resticRestoreAction")

	var pod corev1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), &pod); err != nil {
		return nil, nil, errors.Wrap(err, "unable to convert pod from runtime.Unstructured")
	}

	log := a.logger.WithField("pod", kube.NamespaceAndName(&pod))

	// nil annotations: no restic snapshots, so return early
	if pod.Annotations == nil {
		log.Debug("No annotations found")
		return obj, nil, nil
	}

	for _, volume := range pod.Spec.Volumes {
		log := log.WithField("volume", volume.Name)

		var snapshotID string
		if snapshotID = pod.Annotations["snapshot.ark.heptio.com/"+volume.Name]; snapshotID == "" {
			log.Debug("No restic snapshot ID annotation found")
			continue
		}

		log.Infof("Restic snapshot ID %s found", snapshotID)

		// TODO init container
	}

	return obj, nil, nil
}
