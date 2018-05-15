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
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	arkv1api "github.com/heptio/ark/pkg/apis/ark/v1"
	"github.com/heptio/ark/pkg/podexec"
)

// DaemonSetExecutor can execute commands within the pods of the ark
// restic daemonset.
type DaemonSetExecutor interface {
	Exec(node string, cmd []string, timeout time.Duration, log logrus.FieldLogger) error
}

func NewDaemonSetExecutor(executor podexec.PodCommandExecutor, podClient corev1client.PodInterface) DaemonSetExecutor {
	return &defaultDaemonSetExecutor{
		executor:  executor,
		podClient: podClient,
	}
}

type defaultDaemonSetExecutor struct {
	executor  podexec.PodCommandExecutor
	podClient corev1client.PodInterface
}

func (dse *defaultDaemonSetExecutor) Exec(nodeName string, cmd []string, timeout time.Duration, log logrus.FieldLogger) error {
	dsPod, err := dse.getDaemonSetPod(nodeName)
	if err != nil {
		return err
	}

	dsCmd := &arkv1api.ExecHook{
		Container: "restic",
		Command:   cmd,
		OnError:   arkv1api.HookErrorModeFail,
		Timeout:   metav1.Duration{Duration: timeout},
	}

	unstructuredPod, err := runtime.DefaultUnstructuredConverter.ToUnstructured(dsPod)
	if err != nil {
		return errors.WithStack(err)
	}

	return dse.executor.ExecutePodCommand(
		log,
		unstructuredPod,
		dsPod.Namespace,
		dsPod.Name,
		"restic-backup",
		dsCmd)
}

func (dse *defaultDaemonSetExecutor) getDaemonSetPod(node string) (*corev1api.Pod, error) {
	// TODO we may want to cache this list
	dsPods, err := dse.podClient.List(metav1.ListOptions{LabelSelector: "name=restic-daemon"})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	for _, itm := range dsPods.Items {
		if itm.Spec.NodeName == node {
			return &itm, nil
		}
	}

	return nil, errors.Errorf("unable to find ark daemonset pod for node %q", node)
}
