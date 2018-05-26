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
	"github.com/heptio/ark/pkg/util/boolptr"
	"github.com/heptio/ark/pkg/util/sync"
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
	Forget(repo, snapshotID string) error

	Backupper
	Restorer
}

// Backupper can execute restic backups of volumes in a pod.
type Backupper interface {
	BackupPodVolumes(ctx context.Context, backup *arkv1api.Backup, pod *corev1api.Pod, log logrus.FieldLogger) error
}

// Restorer can execute restic restores of volumes in a pod.
type Restorer interface {
	RestorePodVolumes(ctx context.Context, restore *arkv1api.Restore, pod *corev1api.Pod, log logrus.FieldLogger) error
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
	bucket        string
	arkClient     clientset.Interface
	secretsClient corev1client.SecretInterface
	log           logrus.FieldLogger
	repoPrefix    string
	repoLocker    *repoLocker
}

// NewRepositoryManager constructs a RepositoryManager.
func NewRepositoryManager(
	objectStore cloudprovider.ObjectStore,
	config arkv1api.ObjectStorageProviderConfig,
	arkClient clientset.Interface,
	secretsClient corev1client.SecretInterface,
	log logrus.FieldLogger,
) RepositoryManager {
	rm := &repositoryManager{
		objectStore:   objectStore,
		bucket:        config.ResticLocation,
		arkClient:     arkClient,
		secretsClient: secretsClient,
		log:           log,
		repoLocker:    newRepoLocker(),
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

func (rm *repositoryManager) BackupPodVolumes(ctx context.Context, backup *arkv1api.Backup, pod *corev1api.Pod, log logrus.FieldLogger) error {
	// get volumes to backup from pod's annotations
	volumesToBackup := GetVolumesToBackup(pod)
	if len(volumesToBackup) == 0 {
		return nil
	}

	// ensure a repo exists for the pod's namespace
	exists, err := rm.RepositoryExists(pod.Namespace)
	if err != nil {
		return err
	}
	if !exists {
		if err := rm.InitRepo(pod.Namespace); err != nil {
			return err
		}
	}

	resultChan := make(chan *arkv1api.PodVolumeBackup)

	informer := arkv1informers.NewFilteredPodVolumeBackupInformer(
		rm.arkClient,
		backup.Namespace,
		0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		func(opts *metav1.ListOptions) {
			opts.LabelSelector = fmt.Sprintf("%s=%s", arkv1api.BackupNameLabel, backup.Name)
		},
	)
	informer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(_, obj interface{}) {
				pvb := obj.(*arkv1api.PodVolumeBackup)

				if pvb.Status.Phase == arkv1api.PodVolumeBackupPhaseCompleted || pvb.Status.Phase == arkv1api.PodVolumeBackupPhaseFailed {
					resultChan <- pvb
				}
			},
		},
	)
	go informer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return errors.New("timed out waiting for cache to sync")
	}

	for _, volumeName := range volumesToBackup {
		rm.repoLocker.Lock(pod.Namespace, false)
		defer rm.repoLocker.Unlock(pod.Namespace, false)

		volumeBackup := &arkv1api.PodVolumeBackup{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:    backup.Namespace,
				GenerateName: backup.Name + "-",
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: arkv1api.SchemeGroupVersion.String(),
						Kind:       "Backup",
						Name:       backup.Name,
						UID:        backup.UID,
						Controller: boolptr.True(),
					},
				},
				Labels: map[string]string{
					arkv1api.BackupNameLabel: backup.Name,
					arkv1api.BackupUIDLabel:  string(backup.UID),
				},
			},
			Spec: arkv1api.PodVolumeBackupSpec{
				Node: pod.Spec.NodeName,
				Pod: corev1api.ObjectReference{
					Kind:      "Pod",
					Namespace: pod.Namespace,
					Name:      pod.Name,
					UID:       pod.UID,
				},
				Volume: volumeName,
				Tags: map[string]string{
					"backup":     backup.Name,
					"backup-uid": string(backup.UID),
					"pod":        pod.Name,
					"pod-uid":    string(pod.UID),
					"ns":         pod.Namespace,
					"volume":     volumeName,
				},
				RepoPrefix: rm.repoPrefix,
			},
		}

		// TODO should we return here, or continue with what we can?
		if err := errorOnly(rm.arkClient.ArkV1().PodVolumeBackups(volumeBackup.Namespace).Create(volumeBackup)); err != nil {
			return errors.WithStack(err)
		}
	}

	var errs []error

ForLoop:
	for i := 0; i < len(volumesToBackup); i++ {
		select {
		case <-ctx.Done():
			errs = append(errs, errors.New("timed out waiting for all PodVolumeBackups to complete"))
			break ForLoop
		case res := <-resultChan:
			switch res.Status.Phase {
			case arkv1api.PodVolumeBackupPhaseCompleted:
				SetPodSnapshotAnnotation(pod, res.Spec.Volume, res.Status.SnapshotID)
			case arkv1api.PodVolumeBackupPhaseFailed:
				errs = append(errs, errors.Errorf("pod volume backup failed: %s", res.Status.Message))
			}
		}
	}

	return kerrs.NewAggregate(errs)
}

func (rm *repositoryManager) RestorePodVolumes(ctx context.Context, restore *arkv1api.Restore, pod *corev1api.Pod, log logrus.FieldLogger) error {
	// get volumes to restore from pod's annotations
	volumesToRestore := GetPodSnapshotAnnotations(pod)
	if len(volumesToRestore) == 0 {
		return nil
	}

	resultChan := make(chan *arkv1api.PodVolumeRestore)

	informer := arkv1informers.NewFilteredPodVolumeRestoreInformer(
		rm.arkClient,
		restore.Namespace,
		0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		func(opts *metav1.ListOptions) {
			opts.LabelSelector = fmt.Sprintf("%s=%s", arkv1api.RestoreNameLabel, restore.Name)
		},
	)
	informer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(_, obj interface{}) {
				pvr := obj.(*arkv1api.PodVolumeRestore)

				if pvr.Status.Phase == arkv1api.PodVolumeRestorePhaseCompleted || pvr.Status.Phase == arkv1api.PodVolumeRestorePhaseFailed {
					resultChan <- pvr
				}
			},
		},
	)
	go informer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return errors.New("timed out waiting for cache to sync")
	}

	for volume, snapshot := range volumesToRestore {
		rm.repoLocker.Lock(pod.Namespace, false)
		defer rm.repoLocker.Unlock(pod.Namespace, false)

		// TODO should we return here, or continue with what we can?
		volumeRestore := &arkv1api.PodVolumeRestore{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:    restore.Namespace,
				GenerateName: restore.Name + "-",
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: arkv1api.SchemeGroupVersion.String(),
						Kind:       "Restore",
						Name:       restore.Name,
						UID:        restore.UID,
						Controller: boolptr.True(),
					},
				},
				Labels: map[string]string{
					arkv1api.RestoreNameLabel: restore.Name,
					arkv1api.RestoreUIDLabel:  string(restore.UID),
				},
			},
			Spec: arkv1api.PodVolumeRestoreSpec{
				Pod: corev1api.ObjectReference{
					Kind:      "Pod",
					Namespace: pod.Namespace,
					Name:      pod.Name,
					UID:       pod.UID,
				},
				Volume:     volume,
				SnapshotID: snapshot,
				RepoPrefix: rm.repoPrefix,
			},
		}

		// TODO should we return here, or continue with what we can?
		if err := errorOnly(rm.arkClient.ArkV1().PodVolumeRestores(volumeRestore.Namespace).Create(volumeRestore)); err != nil {
			return errors.WithStack(err)
		}
	}

	var errs []error

ForLoop:
	for i := 0; i < len(volumesToRestore); i++ {
		select {
		case <-ctx.Done():
			errs = append(errs, errors.New("timed out waiting for all PodVolumeRestores to complete"))
			break ForLoop
		case res := <-resultChan:
			if res.Status.Phase == arkv1api.PodVolumeRestorePhaseFailed {
				errs = append(errs, errors.Errorf("pod volume restore failed: %s", res.Status.Message))
			}
		}
	}

	return kerrs.NewAggregate(errs)
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
		RepoPrefix: rm.repoPrefix,
		Repo:       name,
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
		RepoPrefix: rm.repoPrefix,
		Repo:       name,
	}

	return errorOnly(rm.exec(cmd))
}

func (rm *repositoryManager) PruneRepo(name string) error {
	rm.repoLocker.Lock(name, true)
	defer rm.repoLocker.Unlock(name, true)

	cmd := &Command{
		Command:    "prune",
		RepoPrefix: rm.repoPrefix,
		Repo:       name,
	}

	return errorOnly(rm.exec(cmd))
}

func (rm *repositoryManager) Forget(repo, snapshotID string) error {
	rm.repoLocker.Lock(repo, true)
	defer rm.repoLocker.Unlock(repo, true)

	cmd := &Command{
		Command:    "forget",
		RepoPrefix: rm.repoPrefix,
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
