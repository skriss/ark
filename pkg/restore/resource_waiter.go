/*
Copyright 2017 the Heptio Ark contributors.

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
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/heptio/ark/pkg/util/kube"
)

// resourceWaiter knows how to wait for a set of registered items to become "ready" (according
// to a provided readyFunc) based on listening to a channel of Events. The correct usage
// of this struct is to construct it, register all of the desired items to wait for via
// RegisterItem, and then to Wait() for them to become ready or the timeout to be exceeded.
type resourceWaiter struct {
	itemWatch  watch.Interface
	watchChan  <-chan watch.Event
	items      sets.String
	readyFunc  func(runtime.Unstructured) bool
	actionFunc func(runtime.Unstructured)
	waitGroup  sync.WaitGroup
	log        logrus.FieldLogger
}

func newResourceWaiter(itemWatch watch.Interface, readyFunc func(runtime.Unstructured) bool, actionFunc func(runtime.Unstructured), log logrus.FieldLogger) *resourceWaiter {
	return &resourceWaiter{
		itemWatch:  itemWatch,
		watchChan:  itemWatch.ResultChan(),
		items:      sets.NewString(),
		readyFunc:  readyFunc,
		actionFunc: actionFunc,
		log:        log,
	}
}

// RegisterItem adds the specified key to a list of items to listen for events for.
func (rw *resourceWaiter) RegisterItem(item *unstructured.Unstructured) {
	key := kube.NamespaceAndName(item)
	rw.log.Debugf("Registering item %s", key)
	rw.items.Insert(key)
	rw.waitGroup.Add(1)
	rw.log.Debugf("Registered item %s", key)
}

// Wait listens for events on the watchChan related to items that have been registered,
// and returns when either all of them have become ready according to readyFunc, or when
// the timeout has been exceeded.
func (rw *resourceWaiter) Wait() error {
	var (
		timeoutDuration = 30 * time.Second
		timeout         = time.After(timeoutDuration)
	)

	for {
		if rw.items.Len() <= 0 {
			rw.log.Debug("All items are ready")
			break
		} else {
			rw.log.Debug("%d items are unready", rw.items.Len())
		}

		select {
		case event := <-rw.watchChan:
			if event.Type != watch.Added && event.Type != watch.Modified {
				break
			}

			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				return errors.Errorf("Unexpected type %T", event.Object)
			}
			key := kube.NamespaceAndName(obj)

			if !rw.items.Has(key) {
				break
			}

			if rw.readyFunc(obj) {
				rw.log.Debugf("Item %s is ready", key)
				rw.items.Delete(key)
				go func() {
					defer rw.waitGroup.Done()

					rw.log.Debugf("Executing actionFunc for item %s", key)
					rw.actionFunc(obj)
					rw.log.Debugf("Done executing actionFunc for item %s", key)
				}()
			} else {
				rw.log.Debugf("Item %s is not ready yet", key)
			}
		case <-timeout:
			return errors.New("failed to observe all items becoming ready within the timeout")
		}
	}

	rw.log.Debug("Waiting for all items' actions to be executed")
	done := make(chan struct{}, 1)
	go func() {
		rw.waitGroup.Wait()
		close(done)
	}()

	select {
	case <-done:
		rw.log.Debug("All items' actions have been executed")
		return nil
	case <-time.After(timeoutDuration):
		rw.log.Debug("timed out waiting for all items' actions to execute")
		return errors.New("Timed out waiting for actions to execute")
	}
}

func (rw *resourceWaiter) Stop() {
	rw.itemWatch.Stop()
}
