/*
Copyright 2020 the Velero contributors.

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
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/vmware-tanzu/velero/pkg/client"
	"github.com/vmware-tanzu/velero/pkg/discovery"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/util/collections"
)

type resourceCollector struct {
	log                   logrus.FieldLogger
	discoveryHelper       discovery.Helper
	backupRequest         *Request
	cohabitatingResources map[string]*cohabitatingResource
	dynamicFactory        client.DynamicFactory
	items                 []*apiResource
}

type apiResource struct {
	*unstructured.Unstructured

	groupResource schema.GroupResource
}

// collectResources returns a partially-ordered list of items to back up.
func (rc *resourceCollector) collectResources() []*apiResource {
	for _, group := range rc.discoveryHelper.Resources() {
		if err := rc.backupGroup(rc.log, group); err != nil {
			rc.log.WithError(err).WithField("apiGroup", group.String()).Error("Error backing up API group")
		}
	}

	return rc.items
}

func (rc *resourceCollector) backupGroup(log logrus.FieldLogger, group *metav1.APIResourceList) error {
	log = log.WithField("group", group.GroupVersion)

	log.Infof("Backing up group")

	// Parse so we can check if this is the core group
	gv, err := schema.ParseGroupVersion(group.GroupVersion)
	if err != nil {
		return errors.Wrapf(err, "error parsing GroupVersion %q", group.GroupVersion)
	}
	if gv.Group == "" {
		// This is the core group, so make sure we process in the following order: pods, pvcs, pvs,
		// everything else.
		sortCoreGroup(group)
	}

	for _, resource := range group.APIResources {
		if err := rc.backupResource(group, resource); err != nil {
			log.WithError(err).WithField("resource", resource.String()).Error("Error backing up API resource")
		}
	}

	return nil
}

func (rc *resourceCollector) backupResource(group *metav1.APIResourceList, resource metav1.APIResource) error {
	log := rc.log.WithField("resource", resource.Name)

	log.Info("Backing up resource")

	gv, err := schema.ParseGroupVersion(group.GroupVersion)
	if err != nil {
		return errors.Wrapf(err, "error parsing GroupVersion %s", group.GroupVersion)
	}
	gr := schema.GroupResource{Group: gv.Group, Resource: resource.Name}

	clusterScoped := !resource.Namespaced

	// If the resource we are backing up is NOT namespaces, and it is cluster-scoped, check to see if
	// we should include it based on the IncludeClusterResources setting.
	if gr != kuberesource.Namespaces && clusterScoped {
		if rc.backupRequest.Spec.IncludeClusterResources == nil {
			if !rc.backupRequest.NamespaceIncludesExcludes.IncludeEverything() {
				// when IncludeClusterResources == nil (auto), only directly
				// back up cluster-scoped resources if we're doing a full-cluster
				// (all namespaces) backup. Note that in the case of a subset of
				// namespaces being backed up, some related cluster-scoped resources
				// may still be backed up if triggered by a custom action (e.g. PVC->PV).
				// If we're processing namespaces themselves, we will not skip here, they may be
				// filtered out later.
				log.Info("Skipping resource because it's cluster-scoped and only specific namespaces are included in the backup")
				return nil
			}
		} else if !*rc.backupRequest.Spec.IncludeClusterResources {
			log.Info("Skipping resource because it's cluster-scoped")
			return nil
		}
	}

	if !rc.backupRequest.ResourceIncludesExcludes.ShouldInclude(gr.String()) {
		log.Infof("Skipping resource because it's excluded")
		return nil
	}

	if cohabitator, found := rc.cohabitatingResources[resource.Name]; found {
		if cohabitator.seen {
			log.WithFields(
				logrus.Fields{
					"cohabitatingResource1": cohabitator.groupResource1.String(),
					"cohabitatingResource2": cohabitator.groupResource2.String(),
				},
			).Infof("Skipping resource because it cohabitates and we've already processed it")
			return nil
		}
		cohabitator.seen = true
	}

	namespacesToList := getNamespacesToList(rc.backupRequest.NamespaceIncludesExcludes)

	// Check if we're backing up namespaces, and only certain ones
	if gr == kuberesource.Namespaces && namespacesToList[0] != "" {
		resourceClient, err := rc.dynamicFactory.ClientForGroupVersionResource(gv, resource, "")
		if err != nil {
			log.WithError(err).Error("Error getting dynamic client")
		} else {
			var labelSelector labels.Selector
			if rc.backupRequest.Spec.LabelSelector != nil {
				labelSelector, err = metav1.LabelSelectorAsSelector(rc.backupRequest.Spec.LabelSelector)
				if err != nil {
					// This should never happen...
					return errors.Wrap(err, "invalid label selector")
				}
			}

			for _, ns := range namespacesToList {
				log = log.WithField("namespace", ns)
				log.Info("Getting namespace")
				unstructured, err := resourceClient.Get(ns, metav1.GetOptions{})
				if err != nil {
					log.WithError(errors.WithStack(err)).Error("Error getting namespace")
					continue
				}

				labels := labels.Set(unstructured.GetLabels())
				if labelSelector != nil && !labelSelector.Matches(labels) {
					log.Info("Skipping namespace because it does not match the backup's label selector")
					continue
				}

				// NEW collect item
				rc.items = append(rc.items, &apiResource{Unstructured: unstructured, groupResource: gr})
			}

			return nil
		}
	}

	// If we get here, we're backing up something other than namespaces
	if clusterScoped {
		namespacesToList = []string{""}
	}

	backedUpItem := false
	for _, namespace := range namespacesToList {
		log = log.WithField("namespace", namespace)

		resourceClient, err := rc.dynamicFactory.ClientForGroupVersionResource(gv, resource, namespace)
		if err != nil {
			log.WithError(err).Error("Error getting dynamic client")
			continue
		}

		var labelSelector string
		if selector := rc.backupRequest.Spec.LabelSelector; selector != nil {
			labelSelector = metav1.FormatLabelSelector(selector)
		}

		resourceClient.List(metav1.ListOptions{})

		log.Info("Listing items")
		unstructuredList, err := resourceClient.List(metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			log.WithError(errors.WithStack(err)).Error("Error listing items")
			continue
		}

		// do the backup
		items, err := meta.ExtractList(unstructuredList)
		if err != nil {
			log.WithError(errors.WithStack(err)).Error("Error extracting list")
			continue
		}

		log.Infof("Retrieved %d items", len(items))

		for _, item := range items {
			unstructured, ok := item.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("Unexpected type %T", item)
				continue
			}

			if gr == kuberesource.Namespaces && !rc.backupRequest.NamespaceIncludesExcludes.ShouldInclude(unstructured.GetName()) {
				log.WithField("name", unstructured.GetName()).Info("Skipping namespace because it's excluded")
				continue
			}

			// NEW collect item
			rc.items = append(rc.items, &apiResource{Unstructured: unstructured, groupResource: gr})

			// TODO this isn't quite right since we don't know at this point if it's actually
			// being backed up, can we move this into the backup code?
			backedUpItem = true
		}
	}

	// back up CRD for resource if found. We should only need to do this if we've backed up at least
	// one item and IncludeClusterResources is nil. If IncludeClusterResources is false
	// we don't want to back it up, and if it's true it will already be included.
	if backedUpItem && rc.backupRequest.Spec.IncludeClusterResources == nil {
		rc.backupCRD(log, gr)
	}

	return nil
}

// backupCRD checks if the resource is a custom resource, and if so, backs up the custom resource definition
// associated with it.
func (rc *resourceCollector) backupCRD(log logrus.FieldLogger, gr schema.GroupResource) {
	crdGroupResource := kuberesource.CustomResourceDefinitions

	log.Debugf("Getting server preferred API version for %s", crdGroupResource)
	gvr, resource, err := rc.discoveryHelper.ResourceFor(crdGroupResource.WithVersion(""))
	if err != nil {
		log.WithError(errors.WithStack(err)).Errorf("Error getting resolved resource for %s", crdGroupResource)
		return
	}
	log.Debugf("Got server preferred API version %s for %s", gvr.Version, crdGroupResource)

	log.Debugf("Getting dynamic client for %s", gvr.String())
	crdClient, err := rc.dynamicFactory.ClientForGroupVersionResource(gvr.GroupVersion(), resource, "")
	if err != nil {
		log.WithError(errors.WithStack(err)).Errorf("Error getting dynamic client for %s", crdGroupResource)
		return
	}
	log.Debugf("Got dynamic client for %s", gvr.String())

	// try to get a CRD whose name matches the provided GroupResource
	unstructured, err := crdClient.Get(gr.String(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		// not found: this means the GroupResource provided was not a
		// custom resource, so there's no CRD to back up.
		log.Debugf("No CRD found for GroupResource %s", gr.String())
		return
	}
	if err != nil {
		log.WithError(errors.WithStack(err)).Errorf("Error getting CRD %s", gr.String())
		return
	}
	log.Infof("Found associated CRD %s to add to backup", gr.String())

	// NEW collect item
	rc.items = append(rc.items, &apiResource{Unstructured: unstructured, groupResource: crdGroupResource})
}

// sortCoreGroup sorts group as a coreGroup.
func sortCoreGroup(group *metav1.APIResourceList) {
	sort.Stable(coreGroup(group.APIResources))
}

// coreGroup is used to sort APIResources in the core API group. The sort order is pods, pvcs, pvs,
// then everything else.
type coreGroup []metav1.APIResource

func (c coreGroup) Len() int {
	return len(c)
}

func (c coreGroup) Less(i, j int) bool {
	return coreGroupResourcePriority(c[i].Name) < coreGroupResourcePriority(c[j].Name)
}

func (c coreGroup) Swap(i, j int) {
	c[j], c[i] = c[i], c[j]
}

// These constants represent the relative priorities for resources in the core API group. We want to
// ensure that we process pods, then pvcs, then pvs, then anything else. This ensures that when a
// pod is backed up, we can perform a pre hook, then process pvcs and pvs (including taking a
// snapshot), then perform a post hook on the pod.
const (
	pod = iota
	pvc
	pv
	other
)

// coreGroupResourcePriority returns the relative priority of the resource, in the following order:
// pods, pvcs, pvs, everything else.
func coreGroupResourcePriority(resource string) int {
	switch strings.ToLower(resource) {
	case "pods":
		return pod
	case "persistentvolumeclaims":
		return pvc
	case "persistentvolumes":
		return pv
	}

	return other
}

// getNamespacesToList examines ie and resolves the includes and excludes to a full list of
// namespaces to list. If ie is nil or it includes *, the result is just "" (list across all
// namespaces). Otherwise, the result is a list of every included namespace minus all excluded ones.
func getNamespacesToList(ie *collections.IncludesExcludes) []string {
	if ie == nil {
		return []string{""}
	}

	if ie.ShouldInclude("*") {
		// "" means all namespaces
		return []string{""}
	}

	var list []string
	for _, i := range ie.GetIncludes() {
		if ie.ShouldInclude(i) {
			list = append(list, i)
		}
	}

	return list
}

type cohabitatingResource struct {
	resource       string
	groupResource1 schema.GroupResource
	groupResource2 schema.GroupResource
	seen           bool
}

func newCohabitatingResource(resource, group1, group2 string) *cohabitatingResource {
	return &cohabitatingResource{
		resource:       resource,
		groupResource1: schema.GroupResource{Group: group1, Resource: resource},
		groupResource2: schema.GroupResource{Group: group2, Resource: resource},
		seen:           false,
	}
}
