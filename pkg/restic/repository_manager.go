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
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kerrs "k8s.io/apimachinery/pkg/util/errors"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
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

	credsSecret = "restic-credentials"
)

type repositoryManager struct {
	objectStore   cloudprovider.ObjectStore
	config        config
	arkClient     clientset.Interface
	secretsClient corev1client.SecretInterface
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
	objectStore cloudprovider.ObjectStore,
	config arkv1api.ObjectStorageProviderConfig,
	arkClient clientset.Interface,
	secretsClient corev1client.SecretInterface,
	log logrus.FieldLogger,
) RepositoryManager {
	return &repositoryManager{
		objectStore:   objectStore,
		config:        getConfig(config),
		arkClient:     arkClient,
		secretsClient: secretsClient,
		log:           log,
		repoLocker:    newRepoLocker(),
	}
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

	resticCreds, err := rm.secretsClient.Get(credsSecret, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		secret := &corev1api.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: credsSecret,
			},
			Type: corev1api.SecretTypeOpaque,
		}
		if resticCreds, err = rm.secretsClient.Create(secret); err != nil {
			return errors.WithStack(err)
		}
	} else if err != nil {
		return errors.WithStack(err)
	}

	// do we already have a key for this repo? we shouldn't
	if _, exists := resticCreds.Data[name]; exists {
		return errors.New("restic-credentials secret already contains an encryption key for this repo")
	}

	// generate an encryption key for the repo
	key := make([]byte, 256)
	if _, err := rand.Read(key); err != nil {
		return errors.Wrap(err, "unable to generate encryption key")
	}

	// capture current state for patch generation
	preBytes, err := json.Marshal(resticCreds)
	if err != nil {
		return errors.WithStack(err)
	}

	if resticCreds.Data == nil {
		resticCreds.Data = make(map[string][]byte)
	}

	// TODO dev code only
	resticCreds.Data[name] = []byte("passw0rd")

	// get the modified state and generate a patch
	postBytes, err := json.Marshal(resticCreds)
	if err != nil {
		return errors.WithStack(err)
	}

	patch, err := jsonpatch.CreateMergePatch(preBytes, postBytes)
	if err != nil {
		return errors.WithStack(err)
	}

	// patch the secret
	if _, err := rm.secretsClient.Patch(resticCreds.Name, types.MergePatchType, patch); err != nil {
		return errors.Wrap(err, "unable to patch restic-credentials secret")
	}

	// init the repo
	cmd := &Command{
		Command:    "init",
		RepoPrefix: rm.config.repoPrefix,
		Repo:       name,
	}

	return errorOnly(rm.exec(cmd))
}

func (rm *repositoryManager) getAllRepos() ([]string, error) {
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
	// TODO use a SecretLister instead of a client and switch to using restic.TempCredentialsFile func

	// get the encryption key for this repo from the secret
	secret, err := rm.secretsClient.Get(credsSecret, metav1.GetOptions{})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	repoKey, found := secret.Data[cmd.Repo]
	if !found {
		return nil, errors.Errorf("key %s not found in restic-credentials secret", cmd.Repo)
	}

	// write it to a temp file
	file, err := ioutil.TempFile("", fmt.Sprintf("restic-credentials-%s", cmd.Repo))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	defer func() {
		file.Close()
		os.Remove(file.Name())
	}()

	if _, err := file.Write(repoKey); err != nil {
		return nil, errors.WithStack(err)
	}

	// use the temp creds file for running the command
	cmd.PasswordFile = file.Name()

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
