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
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kerrs "k8s.io/apimachinery/pkg/util/errors"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	arkv1api "github.com/heptio/ark/pkg/apis/ark/v1"
	"github.com/heptio/ark/pkg/cloudprovider"
)

// TODO this is more like a metadata manager
type RepositoryManager interface {
	RepoPrefix() string
	RepositoryExists(name string) (bool, error)
	InitRepo(name string) error
	CheckRepo(name string) error
	CheckAllRepos() error
	PruneRepo(name string) error
	PruneAllRepos() error

	GetSnapshotID(repo, backupUID, podUID, volume string) (string, error)
	Forget(repo, snapshotID string) error
}

type repositoryManager struct {
	objectStore   cloudprovider.ObjectStore
	bucket        string
	secretsClient corev1client.SecretInterface
	log           logrus.FieldLogger
	repoPrefix    string
}

type BackendType string

const (
	AWSBackend   BackendType = "aws"
	AzureBackend BackendType = "azure"
	GCPBackend   BackendType = "gcp"
)

const (
	credsSecret   = "restic-credentials"
	credsFilePath = "/restic-credentials/%s"
)

// NewRepositoryManager constructs a RepositoryManager.
func NewRepositoryManager(objectStore cloudprovider.ObjectStore, config arkv1api.ObjectStorageProviderConfig, secretsClient corev1client.SecretInterface, log logrus.FieldLogger) RepositoryManager {
	rm := &repositoryManager{
		objectStore:   objectStore,
		bucket:        config.ResticLocation,
		secretsClient: secretsClient,
		log:           log,
	}

	switch BackendType(config.Name) {
	case AWSBackend:
		url := "s3.amazonaws.com"

		// non-AWS, S3-compatible object store
		if config.Config != nil && config.Config["s3Url"] != "" {
			url = config.Config["s3Url"]
		}

		rm.repoPrefix = fmt.Sprintf("s3:%s/%s", url, rm.bucket)
	case AzureBackend:
		rm.repoPrefix = "azure:" + rm.bucket + ":"
	case GCPBackend:
		rm.repoPrefix = "gs:" + rm.bucket + ":"
	}

	return rm
}

func (rm *repositoryManager) RepoPrefix() string {
	return rm.repoPrefix
}

func (rm *repositoryManager) RepositoryExists(name string) (bool, error) {
	repos, err := rm.getAllRepos()
	if err != nil {
		return false, err
	}

	for _, repo := range repos {
		if repo == name {
			return true, nil
		}
	}

	return false, nil
}

func (rm *repositoryManager) InitRepo(name string) error {
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
	cmd := &command{
		baseName:   "/restic",
		command:    "init",
		repoPrefix: rm.repoPrefix,
		repo:       name,
	}

	return errorOnly(rm.exec(cmd))
}

func (rm *repositoryManager) getAllRepos() ([]string, error) {
	prefixes, err := rm.objectStore.ListCommonPrefixes(rm.bucket, "/")
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

	var (
		errors  []error
		wg      sync.WaitGroup
		errLock sync.Mutex
	)

	for _, repo := range repos {
		this := repo

		wg.Add(1)
		go func() {
			defer wg.Done()

			rm.log.WithField("repo", this).Debugf("Checking repo %s", this)
			err := rm.CheckRepo(this)

			errLock.Lock()
			errors = append(errors, err)
			errLock.Unlock()
		}()
	}

	wg.Wait()

	return kerrs.NewAggregate(errors)
}

func (rm *repositoryManager) PruneAllRepos() error {
	repos, err := rm.getAllRepos()
	if err != nil {
		return err
	}

	var (
		errors  []error
		wg      sync.WaitGroup
		errLock sync.Mutex
	)

	for _, repo := range repos {
		this := repo

		wg.Add(1)
		go func() {
			defer wg.Done()

			rm.log.WithField("repo", this).Debugf("Pre-prune checking repo %s", this)
			if err := rm.CheckRepo(this); err != nil {
				errLock.Lock()
				errors = append(errors, err)
				errLock.Unlock()

				return
			}

			rm.log.WithField("repo", this).Debugf("Pruning repo %s", this)
			if err := rm.PruneRepo(this); err != nil {
				errLock.Lock()
				errors = append(errors, err)
				errLock.Unlock()
			}

			rm.log.WithField("repo", this).Debugf("Post-prune checking repo %s", this)
			if err := rm.CheckRepo(this); err != nil {
				errLock.Lock()
				errors = append(errors, err)
				errLock.Unlock()
			}
		}()
	}

	wg.Wait()

	return kerrs.NewAggregate(errors)
}

func (rm *repositoryManager) CheckRepo(name string) error {
	cmd := &command{
		baseName:   "/restic",
		command:    "check",
		repoPrefix: rm.repoPrefix,
		repo:       name,
	}

	return errorOnly(rm.exec(cmd))
}

func (rm *repositoryManager) PruneRepo(name string) error {
	cmd := &command{
		baseName:   "/restic",
		command:    "prune",
		repoPrefix: rm.repoPrefix,
		repo:       name,
	}

	return errorOnly(rm.exec(cmd))
}

func (rm *repositoryManager) GetSnapshotID(repo, backupUID, podUID, volume string) (string, error) {
	tagFilters := []string{
		"ns=" + repo,
		"pod-uid=" + podUID,
		"volume=" + volume,
		"backup-uid=" + backupUID,
	}

	cmd := &command{
		baseName:   "/restic",
		command:    "snapshots",
		repoPrefix: rm.repoPrefix,
		repo:       repo,
		extraFlags: []string{"--json", "--last", fmt.Sprintf("--tag=%s", strings.Join(tagFilters, ","))},
	}

	res, err := rm.exec(cmd)
	if err != nil {
		return "", errors.Wrap(err, "unable to run restic snapshots command")
	}

	type jsonArray []map[string]interface{}

	var snapshots jsonArray

	if err := json.Unmarshal(res, &snapshots); err != nil {
		return "", errors.Wrap(err, "error unmarshalling restic snapshots result")
	}

	if len(snapshots) != 1 {
		return "", errors.Errorf("expected one matching snapshot, got %d", len(snapshots))
	}

	return snapshots[0]["short_id"].(string), nil
}

func (rm *repositoryManager) Forget(repo, snapshotID string) error {
	cmd := &command{
		baseName:   "/restic",
		command:    "forget",
		repoPrefix: rm.repoPrefix,
		repo:       repo,
		args:       []string{snapshotID},
	}

	return errorOnly(rm.exec(cmd))
}

func (rm *repositoryManager) exec(cmd *command) ([]byte, error) {
	// get the encryption key for this repo from the secret
	secret, err := rm.secretsClient.Get(credsSecret, metav1.GetOptions{})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	repoKey, found := secret.Data[cmd.repo]
	if !found {
		return nil, errors.Errorf("key %s not found in restic-credentials secret", cmd.repo)
	}

	// write it to a temp file
	file, err := ioutil.TempFile("", fmt.Sprintf("restic-credentials-%s", cmd.repo))
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
	cmd.passwordFile = file.Name()

	res, err := cmd.Command().Output()
	rm.log.WithField("repository", cmd.repo).Debugf("Ran restic command=%q, output=%s", cmd.String(), res)

	return res, errors.WithStack(err)
}

func errorOnly(_ interface{}, err error) error {
	return err
}
