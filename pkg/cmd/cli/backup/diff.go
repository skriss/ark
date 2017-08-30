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
	"archive/tar"
	"bytes"
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
	"github.com/heptio/ark/pkg/diff"
	"github.com/heptio/ark/pkg/diff/source"
	"github.com/heptio/ark/pkg/util/filesystem"
	arktar "github.com/heptio/ark/pkg/util/tar"
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
	flags.Var(&o.IncludeNamespaces, "include-namespaces", "namespaces to include in the diff (use '*' for all namespaces)")
	flags.Var(&o.ExcludeNamespaces, "exclude-namespaces", "namespaces to exclude from the diff")
	flags.Var(&o.IncludeResources, "include-resources", "resources to include in the diff, formatted as resource.group, such as storageclasses.storage.k8s.io (use '*' for all resources)")
	flags.Var(&o.ExcludeResources, "exclude-resources", "resources to exclude from the diff, formatted as resource.group, such as storageclasses.storage.k8s.io")
	flags.VarP(&o.Selector, "selector", "l", "only diff resources matching this label selector")
	flags.Var(&o.NamespaceMappings, "namespace-mappings", "namespace mappings from name in the backup to desired namespace for compare in the form src1:dst1,src2:dst2,...")
	flags.DurationVar(&o.Timeout, "timeout", o.Timeout, "how long to wait to receive backup from object storage")
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

	var (
		buf       = &bytes.Buffer{}
		tarReader = tar.NewReader(buf)
		fs        = &filesystem.OSFileSystem{}
	)

	err = downloadrequest.Stream(arkClient.ArkV1(), o.BackupName, v1.DownloadTargetKindBackup, buf, o.Timeout)
	cmd.CheckError(err)

	dir, err := arktar.ExtractToTempDir(tarReader, fs)
	cmd.CheckError(err)
	defer fs.RemoveAll(dir)

	left, err := source.Directory(dir)
	cmd.CheckError(err)

	// TODO finding config should be more robust than this
	right, err := source.Cluster(os.Getenv("KUBECONFIG"))
	cmd.CheckError(err)

	options := &diff.Options{
		Left:  left,
		Right: right,
	}
	report, err := diff.Generate(options)
	cmd.CheckError(err)

	diff.PrintDeltas(report.LeftOnly, os.Stdout, true)
	diff.PrintDeltas(report.Both, os.Stdout, true)

	return nil
}
