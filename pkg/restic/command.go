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
	"os/exec"
	"strings"
)

type command struct {
	baseName     string
	command      string
	repoPrefix   string
	repo         string
	passwordFile string
	args         []string
	extraFlags   []string
}

func (c *command) StringSlice() []string {
	res := []string{c.baseName, c.command, repoFlag(c.repoPrefix, c.repo)}
	if c.passwordFile != "" {
		res = append(res, passwordFlag(c.passwordFile))
	}
	res = append(res, c.args...)
	res = append(res, c.extraFlags...)

	return res
}

func (c *command) String() string {
	return strings.Join(c.StringSlice(), " ")
}

func (c *command) Command() *exec.Cmd {
	parts := c.StringSlice()
	return exec.Command(parts[0], parts[1:]...)
}

func repoFlag(prefix, repo string) string {
	return fmt.Sprintf("--repo=%s/%s", prefix, repo)
}

func passwordFlag(file string) string {
	return fmt.Sprintf("--password-file=%s", file)
}

func backupTagFlags(vals map[string]string) []string {
	var flags []string
	for k, v := range vals {
		flags = append(flags, fmt.Sprintf("--tag=%s=%s", k, v))
	}
	return flags
}

func restoreTargetFlag(podUID string) string {
	return fmt.Sprintf("--target=/restores/%s", podUID)
}
