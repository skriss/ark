/*
Copyright 2017 Heptio Inc.

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
	"errors"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/heptio/ark/pkg/apis/ark/v1"
	"github.com/heptio/ark/pkg/client"
	"github.com/heptio/ark/pkg/cmd"
	"github.com/heptio/ark/pkg/cmd/util/downloadrequest"
	"github.com/heptio/ark/pkg/cmd/util/flag"
)

func NewDiffCommand(f client.Factory) *cobra.Command {
	o := NewDiffOptions()

	c := &cobra.Command{
		Use:   "diff BACKUP",
		Short: "Compare backup to cluster",
		Run: func(c *cobra.Command, args []string) {
			cmd.CheckError(o.Validate(c, args))
			cmd.CheckError(o.Complete(args))
			cmd.CheckError(o.Run(c, f))
		},
	}

	o.BindFlags(c.Flags())

	return c
}

type DiffOptions struct {
	BackupName        string
	IncludeNamespaces flag.StringArray
	ExcludeNamespaces flag.StringArray
	IncludeResources  flag.StringArray
	ExcludeResources  flag.StringArray
	Selector          flag.LabelSelector
	NamespaceMappings flag.Map
	Timeout           time.Duration
}

func NewDiffOptions() *DiffOptions {
	return &DiffOptions{
		IncludeNamespaces: flag.NewStringArray("*"),
		NamespaceMappings: flag.NewMap().WithEntryDelimiter(",").WithKeyValueDelimiter(":"),
		Timeout:           time.Minute,
	}
}

func (o *DiffOptions) BindFlags(flags *pflag.FlagSet) {
	flags.Var(&o.IncludeNamespaces, "include-namespaces", "namespaces to include in the backup (use '*' for all namespaces)")
	flags.Var(&o.ExcludeNamespaces, "exclude-namespaces", "namespaces to exclude from the backup")
	flags.Var(&o.IncludeResources, "include-resources", "resources to include in the backup, formatted as resource.group, such as storageclasses.storage.k8s.io (use '*' for all resources)")
	flags.Var(&o.ExcludeResources, "exclude-resources", "resources to exclude from the backup, formatted as resource.group, such as storageclasses.storage.k8s.io")
	flags.VarP(&o.Selector, "selector", "l", "only back up resources matching this label selector")
	flags.Var(&o.NamespaceMappings, "namespace-mappings", "namespace mappings from name in the backup to desired restored name in the form src1:dst1,src2:dst2,...")
	flags.DurationVar(&o.Timeout, "timeout", o.Timeout, "how long to wait to receive backup")
}

func (o *DiffOptions) Validate(c *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("you must specify only one argument, the backup's name")
	}

	return nil
}

func (o *DiffOptions) Complete(args []string) error {
	o.BackupName = args[0]
	return nil
}

func (o *DiffOptions) Run(c *cobra.Command, f client.Factory) error {
	arkClient, err := f.Client()
	cmd.CheckError(err)

	// TODO stream the download to something that can extract the backup

	err = downloadrequest.Stream(arkClient.ArkV1(), o.BackupName, v1.DownloadTargetKindBackup, os.Stdout, o.Timeout)
	cmd.CheckError(err)

	// TODO do diff

	return nil
}
