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
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1"
)

type commandBuilder struct {
	baseName     string
	command      string
	repo         string
	passwordFile string
	args         []string

	secretInterface  v1.SecretInterface
	repoFormatString string

	err error
	cmd *exec.Cmd
}

func newCommandBuilder(repoFormatString string, secretInterface v1.SecretInterface) *commandBuilder {
	return &commandBuilder{
		baseName:         "/restic",
		repoFormatString: repoFormatString,
		secretInterface:  secretInterface,
	}
}

func (cb *commandBuilder) WithBaseName(name string) *commandBuilder {
	cb.baseName = name
	return cb
}

func (cb *commandBuilder) WithCommand(name string) *commandBuilder {
	cb.command = name
	return cb
}

func (cb *commandBuilder) WithRepo(repo string) *commandBuilder {
	cb.repo = repo
	return cb
}

func (cb *commandBuilder) WithPasswordFile(path string) *commandBuilder {
	cb.passwordFile = path
	return cb
}

func (cb *commandBuilder) WithArgs(args ...string) *commandBuilder {
	cb.args = append(cb.args, args...)
	return cb
}

func (cb *commandBuilder) Command() *commandBuilder {
	passwordFile := cb.passwordFile
	var err error

	if passwordFile == "" {
		passwordFile, err = ensureCredsFile(cb.repo, cb.secretInterface)
		if err != nil {
			cb.err = err
			return nil
		}
	}

	args := append([]string{
		cb.command,
		"-r=" + fmt.Sprintf(cb.repoFormatString, cb.repo),
		"-p=" + passwordFile,
	}, cb.args...)

	cb.cmd = exec.Command(cb.baseName, args...)
	return cb
}

func (cb *commandBuilder) Output() ([]byte, error) {
	if cb.err != nil {
		return nil, cb.err
	}

	if cb.cmd == nil {
		cb = cb.Command()
	}

	return cb.cmd.Output()
}

func (cb *commandBuilder) RunAndLog(log logrus.FieldLogger) ([]byte, error) {
	res, err := cb.Output()

	log.WithField("repository", cb.repo).Debugf("command=%v; output=%s", cb.cmd.Args, res)

	return res, err
}

// TODO I should probably put this exclusively in the restic-wrapper binary, and
// use that within the ark server pod as well. That way the server doesn't have
// to worry about this.
func ensureCredsFile(repo string, secrets v1.SecretInterface) (string, error) {
	_, err := os.Stat(fmt.Sprintf(credsFilePath, repo))
	switch {
	case err == nil:
		return fmt.Sprintf(credsFilePath, repo), nil
	case !os.IsNotExist(err):
		return "", errors.WithStack(err)
	}

	secret, err := secrets.Get(credsSecret, metav1.GetOptions{})
	if err != nil {
		return "", errors.WithStack(err)
	}

	repoKey, found := secret.Data[repo]
	if !found {
		return "", errors.Errorf("key %s not found in restic-credentials secret", repo)
	}

	file, err := ioutil.TempFile("", fmt.Sprintf("restic-credentials-%s", repo))
	if err != nil {
		return "", errors.WithStack(err)
	}
	defer file.Close()

	if _, err := file.Write(repoKey); err != nil {
		return "", errors.WithStack(err)
	}

	return file.Name(), nil
}
