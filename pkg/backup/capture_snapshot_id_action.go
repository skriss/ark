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

package backup

import (
	"encoding/json"
	"os/exec"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/heptio/ark/pkg/apis/ark/v1"
	"github.com/heptio/ark/pkg/util/collections"
)

type captureSnapshotIDAction struct {
	log logrus.FieldLogger
}

// NewCaptureSnapshotIDAction creates a new ItemAction for pods.
func NewCaptureSnapshotIDAction(log logrus.FieldLogger) ItemAction {
	return &captureSnapshotIDAction{log: log}
}

// AppliesTo returns a ResourceSelector that applies only to pods.
func (a *captureSnapshotIDAction) AppliesTo() (ResourceSelector, error) {
	return ResourceSelector{
		IncludedResources: []string{"pods"},
	}, nil
}

func (a *captureSnapshotIDAction) Execute(item runtime.Unstructured, backup *v1.Backup) (runtime.Unstructured, []ResourceIdentifier, error) {
	a.log.Info("Executing captureSnapshotIDAction")
	defer a.log.Info("Done executing captureSnapshotIDAction")

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
			// errs = append(errs, errors.Errorf("unexpected type %T", volumes[i]))
			continue
		}

		volumeName, err := collections.GetString(volume, "name")
		if err != nil {
			continue
		}

		// snapshot is created without a backup name tag, so don't filter on it yet
		snapshotID := a.getSnapshotID("", metadata.GetNamespace(), metadata.GetName(), volumeName)
		if snapshotID == "" {
			return item, nil, nil
		}

		// add the backup name tag then re-fetch the snapshotID since it changes
		cmd := exec.Command("/restic", "tag", "--add", "backup="+backup.Name, snapshotID)
		if err := cmd.Run(); err != nil {
			a.log.WithError(err).Error("error tagging snapshot with backup name")
		}

		snapshotID = a.getSnapshotID(backup.Name, metadata.GetNamespace(), metadata.GetName(), volumeName)
		if snapshotID == "" {
			return item, nil, nil
		}

		annotations := metadata.GetAnnotations()

		annotations["snapshot.ark.heptio.com/"+volumeName] = snapshotID

		metadata.SetAnnotations(annotations)
	}

	return item, nil, nil
}

func (a *captureSnapshotIDAction) getSnapshotID(backup, namespace, pod, volume string) string {
	cmd := exec.Command("/restic", "snapshots", "--json", "--last")

	tagFilters := []string{"ns=" + namespace, "pod=" + pod, "volume=" + volume}
	if backup != "" {
		tagFilters = append(tagFilters, "backup="+backup)
	}

	cmd.Args = append(cmd.Args, "--tag="+strings.Join(tagFilters, ","))

	a.log.Infof("Running cmd=%v", cmd.Args)

	res, err := cmd.Output()
	if err != nil {
		a.log.Errorf("error running restic snapshots cmd: %s", err)
		return ""
	}

	a.log.Warnf("restic snapshots result: %s", res)

	type jsonArray []map[string]interface{}

	var snapshots jsonArray

	if err := json.Unmarshal(res, &snapshots); err != nil {
		a.log.Errorf("error unmarshalling restic snapshots result: %s", err)
		return ""
	}

	if len(snapshots) > 1 {
		a.log.Errorf("more than one matching snapshot found.")
		return ""
	} else if len(snapshots) == 0 {
		a.log.Errorf("no matching snapshot found.")
		return ""
	}

	return snapshots[0]["short_id"].(string)
}
