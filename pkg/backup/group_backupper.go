/*
Copyright 2017 the Velero contributors.

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

// import (
// 	"sort"
// 	"strings"

// 	"github.com/pkg/errors"
// 	"github.com/sirupsen/logrus"
// 	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
// 	"k8s.io/apimachinery/pkg/runtime/schema"

// 	"github.com/vmware-tanzu/velero/pkg/client"
// 	"github.com/vmware-tanzu/velero/pkg/discovery"
// 	"github.com/vmware-tanzu/velero/pkg/podexec"
// 	"github.com/vmware-tanzu/velero/pkg/restic"
// )

// type groupBackupperFactory interface {
// 	newGroupBackupper(
// 		log logrus.FieldLogger,
// 		backupRequest *Request,
// 		dynamicFactory client.DynamicFactory,
// 		discoveryHelper discovery.Helper,
// 		cohabitatingResources map[string]*cohabitatingResource,
// 		podCommandExecutor podexec.PodCommandExecutor,
// 		tarWriter tarWriter,
// 		resticBackupper restic.Backupper,
// 		resticSnapshotTracker *pvcSnapshotTracker,
// 		volumeSnapshotterGetter VolumeSnapshotterGetter,
// 	) groupBackupper
// }

// type defaultGroupBackupperFactory struct{}

// func (f *defaultGroupBackupperFactory) newGroupBackupper(
// 	log logrus.FieldLogger,
// 	backupRequest *Request,
// 	dynamicFactory client.DynamicFactory,
// 	discoveryHelper discovery.Helper,
// 	cohabitatingResources map[string]*cohabitatingResource,
// 	podCommandExecutor podexec.PodCommandExecutor,
// 	tarWriter tarWriter,
// 	resticBackupper restic.Backupper,
// 	resticSnapshotTracker *pvcSnapshotTracker,
// 	volumeSnapshotterGetter VolumeSnapshotterGetter,
// ) groupBackupper {
// 	return &defaultGroupBackupper{
// 		log:                     log,
// 		backupRequest:           backupRequest,
// 		dynamicFactory:          dynamicFactory,
// 		discoveryHelper:         discoveryHelper,
// 		cohabitatingResources:   cohabitatingResources,
// 		podCommandExecutor:      podCommandExecutor,
// 		tarWriter:               tarWriter,
// 		resticBackupper:         resticBackupper,
// 		resticSnapshotTracker:   resticSnapshotTracker,
// 		volumeSnapshotterGetter: volumeSnapshotterGetter,

// 		resourceBackupperFactory: &defaultResourceBackupperFactory{},
// 	}
// }

// type groupBackupper interface {
// 	backupGroup(group *metav1.APIResourceList) error
// }

// type defaultGroupBackupper struct {
// 	log                      logrus.FieldLogger
// 	backupRequest            *Request
// 	dynamicFactory           client.DynamicFactory
// 	discoveryHelper          discovery.Helper
// 	cohabitatingResources    map[string]*cohabitatingResource
// 	podCommandExecutor       podexec.PodCommandExecutor
// 	tarWriter                tarWriter
// 	resticBackupper          restic.Backupper
// 	resticSnapshotTracker    *pvcSnapshotTracker
// 	resourceBackupperFactory resourceBackupperFactory
// 	volumeSnapshotterGetter  VolumeSnapshotterGetter
// }

// // backupGroup backs up a single API group.
// func (gb *defaultGroupBackupper) backupGroup(group *metav1.APIResourceList) error {
// 	log := gb.log.WithField("group", group.GroupVersion)

// 	log.Infof("Backing up group")

// 	// Parse so we can check if this is the core group
// 	gv, err := schema.ParseGroupVersion(group.GroupVersion)
// 	if err != nil {
// 		return errors.Wrapf(err, "error parsing GroupVersion %q", group.GroupVersion)
// 	}
// 	if gv.Group == "" {
// 		// This is the core group, so make sure we process in the following order: pods, pvcs, pvs,
// 		// everything else.
// 		sortCoreGroup(group)
// 	}

// 	rb := gb.resourceBackupperFactory.newResourceBackupper(
// 		log,
// 		gb.backupRequest,
// 		gb.dynamicFactory,
// 		gb.discoveryHelper,
// 		gb.cohabitatingResources,
// 		gb.podCommandExecutor,
// 		gb.tarWriter,
// 		gb.resticBackupper,
// 		gb.resticSnapshotTracker,
// 		gb.volumeSnapshotterGetter,
// 	)

// 	for _, resource := range group.APIResources {
// 		if err := rb.backupResource(group, resource); err != nil {
// 			log.WithError(err).WithField("resource", resource.String()).Error("Error backing up API resource")
// 		}
// 	}

// 	return nil
// }
