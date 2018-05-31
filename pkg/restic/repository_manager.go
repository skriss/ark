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
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kerrs "k8s.io/apimachinery/pkg/util/errors"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	arkv1api "github.com/heptio/ark/pkg/apis/ark/v1"
	"github.com/heptio/ark/pkg/cloudprovider"
	clientset "github.com/heptio/ark/pkg/generated/clientset/versioned"
	arkv1informers "github.com/heptio/ark/pkg/generated/informers/externalversions/ark/v1"
	"github.com/heptio/ark/pkg/util/sync"
)

// RepositoryManager executes commands against restic repositories.
type RepositoryManager interface {
	// CheckRepo checks the specified repo for errors.
	CheckRepo(name string) error

	// CheckAllRepos checks all repos for errors.
	CheckAllRepos() error

	// PruneRepo deletes unused data from a repo.
	PruneRepo(name string) error

	// PruneAllRepos deletes unused data from all
	// repos.
	PruneAllRepos() error

	// Forget removes a snapshot from the list of
	// availabel snapshots in a repo.
	Forget(repo, snapshotID string) error

	BackupperFactory

	RestorerFactory
}

// BackupperFactory can construct restic backuppers.
type BackupperFactory interface {
	// Backupper returns a restic backupper for use during a single
	// Ark backup.
	Backupper(context.Context, *arkv1api.Backup) (Backupper, error)
}

// RestorerFactory can construct restic restorers.
type RestorerFactory interface {
	// Restorer returns a restic restorer for use during a single
	// Ark restore.
	Restorer(context.Context, *arkv1api.Restore) (Restorer, error)
}

type BackendType string

const (
	AWSBackend   BackendType = "aws"
	AzureBackend BackendType = "azure"
	GCPBackend   BackendType = "gcp"
)

type repositoryManager struct {
	objectStore   cloudprovider.ObjectStore
	config        config
	arkClient     clientset.Interface
	secretsLister corev1listers.SecretLister
	secretsClient corev1client.SecretsGetter
	log           logrus.FieldLogger
	repoLocker    *repoLocker
}

type config struct {
	repoPrefix string
	bucket     string
	path       string
}

func getConfig(objectStorageConfig arkv1api.ObjectStorageProviderConfig) config {
	var (
		c     = config{}
		parts = strings.SplitN(objectStorageConfig.ResticLocation, "/", 2)
	)

	switch len(parts) {
	case 0:
	case 1:
		c.bucket = parts[0]
	default:
		c.bucket = parts[0]
		c.path = parts[1]
	}

	switch BackendType(objectStorageConfig.Name) {
	case AWSBackend:
		var url string
		switch {
		// non-AWS, S3-compatible object store
		case objectStorageConfig.Config != nil && objectStorageConfig.Config["s3Url"] != "":
			url = objectStorageConfig.Config["s3Url"]
		default:
			url = "s3.amazonaws.com"
		}

		c.repoPrefix = fmt.Sprintf("s3:%s/%s", url, c.bucket)
		if c.path != "" {
			c.repoPrefix += "/" + c.path
		}
	case AzureBackend:
		c.repoPrefix = fmt.Sprintf("azure:%s:/%s", c.bucket, c.path)
	case GCPBackend:
		c.repoPrefix = fmt.Sprintf("gs:%s:/%s", c.bucket, c.path)
	}

	return c
}

// NewRepositoryManager constructs a RepositoryManager.
func NewRepositoryManager(
	ctx context.Context,
	objectStore cloudprovider.ObjectStore,
	config arkv1api.ObjectStorageProviderConfig,
	arkClient clientset.Interface,
	secretsInformer cache.SharedIndexInformer,
	secretsClient corev1client.SecretsGetter,
	log logrus.FieldLogger,
) (RepositoryManager, error) {
	rm := &repositoryManager{
		objectStore:   objectStore,
		config:        getConfig(config),
		arkClient:     arkClient,
		secretsLister: corev1listers.NewSecretLister(secretsInformer.GetIndexer()),
		secretsClient: secretsClient,
		log:           log,
		repoLocker:    newRepoLocker(),
	}

	if !cache.WaitForCacheSync(ctx.Done(), secretsInformer.HasSynced) {
		return nil, errors.New("timed out waiting for cache to sync")
	}

	return rm, nil
}

func (rm *repositoryManager) Backupper(ctx context.Context, backup *arkv1api.Backup) (Backupper, error) {
	b := &backupper{
		repoManager: rm,
		informer: arkv1informers.NewFilteredPodVolumeBackupInformer(
			rm.arkClient,
			backup.Namespace,
			0,
			cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			func(opts *metav1.ListOptions) {
				opts.LabelSelector = fmt.Sprintf("%s=%s", arkv1api.BackupUIDLabel, backup.UID)
			},
		),
		results: make(map[string]chan *arkv1api.PodVolumeBackup),
		ctx:     ctx,
	}

	b.informer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(_, obj interface{}) {
				pvb := obj.(*arkv1api.PodVolumeBackup)

				if pvb.Status.Phase == arkv1api.PodVolumeBackupPhaseCompleted || pvb.Status.Phase == arkv1api.PodVolumeBackupPhaseFailed {
					b.results[resultsKey(pvb.Spec.Pod.Namespace, pvb.Spec.Pod.Name)] <- pvb
				}
			},
		},
	)

	go b.informer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), b.informer.HasSynced) {
		return nil, errors.New("timed out waiting for cache to sync")
	}

	return b, nil
}

func (rm *repositoryManager) Restorer(ctx context.Context, restore *arkv1api.Restore) (Restorer, error) {
	r := &restorer{
		repoManager: rm,
		informer: arkv1informers.NewFilteredPodVolumeRestoreInformer(
			rm.arkClient,
			restore.Namespace,
			0,
			cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			func(opts *metav1.ListOptions) {
				opts.LabelSelector = fmt.Sprintf("%s=%s", arkv1api.RestoreUIDLabel, restore.UID)
			},
		),
		results: make(map[string]chan *arkv1api.PodVolumeRestore),
		ctx:     ctx,
	}

	r.informer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(_, obj interface{}) {
				pvr := obj.(*arkv1api.PodVolumeRestore)

				if pvr.Status.Phase == arkv1api.PodVolumeRestorePhaseCompleted || pvr.Status.Phase == arkv1api.PodVolumeRestorePhaseFailed {
					r.results[resultsKey(pvr.Spec.Pod.Namespace, pvr.Spec.Pod.Name)] <- pvr
				}
			},
		},
	)

	go r.informer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), r.informer.HasSynced) {
		return nil, errors.New("timed out waiting for cache to sync")
	}

	return r, nil
}

func (rm *repositoryManager) ensureRepo(name string) error {
	repos, err := rm.getAllRepos()
	if err != nil {
		return err
	}

	for _, repo := range repos {
		if repo == name {
			return nil
		}
	}

	rm.repoLocker.Lock(name, true)
	defer rm.repoLocker.Unlock(name, true)

	// init the repo
	cmd := &Command{
		Command:    "init",
		RepoPrefix: rm.config.repoPrefix,
		Repo:       name,
	}

	return errorOnly(rm.exec(cmd))
}

func (rm *repositoryManager) getAllRepos() ([]string, error) {
	// TODO support rm.config.path
	prefixes, err := rm.objectStore.ListCommonPrefixes(rm.config.bucket, "/")
	if err != nil {
		return nil, err
	}

	var repos []string
	for _, prefix := range prefixes {
		if len(prefix) <= 1 {
			continue
		}

		// strip the trailing '/' if it exists
		repos = append(repos, strings.TrimSuffix(prefix, "/"))
	}

	return repos, nil
}

func (rm *repositoryManager) CheckAllRepos() error {
	repos, err := rm.getAllRepos()
	if err != nil {
		return err
	}

	var eg sync.ErrorGroup
	for _, repo := range repos {
		this := repo
		eg.Go(func() error {
			rm.log.WithField("repo", this).Debugf("Checking repo %s", this)
			return rm.CheckRepo(this)
		})
	}

	return kerrs.NewAggregate(eg.Wait())
}

func (rm *repositoryManager) PruneAllRepos() error {
	repos, err := rm.getAllRepos()
	if err != nil {
		return err
	}

	var eg sync.ErrorGroup
	for _, repo := range repos {
		this := repo
		eg.Go(func() error {
			rm.log.WithField("repo", this).Debugf("Pre-prune checking repo %s", this)
			if err := rm.CheckRepo(this); err != nil {
				return err
			}

			rm.log.WithField("repo", this).Debugf("Pruning repo %s", this)
			if err := rm.PruneRepo(this); err != nil {
				return err
			}

			rm.log.WithField("repo", this).Debugf("Post-prune checking repo %s", this)
			return rm.CheckRepo(this)
		})
	}

	return kerrs.NewAggregate(eg.Wait())
}

func (rm *repositoryManager) CheckRepo(name string) error {
	rm.repoLocker.Lock(name, true)
	defer rm.repoLocker.Unlock(name, true)

	cmd := &Command{
		Command:    "check",
		RepoPrefix: rm.config.repoPrefix,
		Repo:       name,
	}

	return errorOnly(rm.exec(cmd))
}

func (rm *repositoryManager) PruneRepo(name string) error {
	rm.repoLocker.Lock(name, true)
	defer rm.repoLocker.Unlock(name, true)

	cmd := &Command{
		Command:    "prune",
		RepoPrefix: rm.config.repoPrefix,
		Repo:       name,
	}

	return errorOnly(rm.exec(cmd))
}

func (rm *repositoryManager) Forget(repo, snapshotID string) error {
	rm.repoLocker.Lock(repo, true)
	defer rm.repoLocker.Unlock(repo, true)

	cmd := &Command{
		Command:    "forget",
		RepoPrefix: rm.config.repoPrefix,
		Repo:       repo,
		Args:       []string{snapshotID},
	}

	return errorOnly(rm.exec(cmd))
}

func (rm *repositoryManager) exec(cmd *Command) ([]byte, error) {
	file, err := TempCredentialsFile(rm.secretsLister, cmd.Repo)
	if err != nil {
		return nil, err
	}
	// ignore error since there's nothing we can do and it's a temp file.
	defer os.Remove(file)

	cmd.PasswordFile = file

	output, err := cmd.Cmd().Output()
	rm.log.WithField("repository", cmd.Repo).Debugf("Ran restic command=%q, output=%s", cmd.String(), output)
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, errors.Wrapf(err, "error running command, stderr=%s", exitErr.Stderr)
		}
		return nil, errors.Wrap(err, "error running command")
	}

	return output, nil
}

func errorOnly(_ interface{}, err error) error {
	return err
}
