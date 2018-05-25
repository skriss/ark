package daemonset

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	k8sinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"

	"github.com/heptio/ark/pkg/client"
	"github.com/heptio/ark/pkg/cmd"
	"github.com/heptio/ark/pkg/controller"
	clientset "github.com/heptio/ark/pkg/generated/clientset/versioned"
	informers "github.com/heptio/ark/pkg/generated/informers/externalversions"
)

func NewCommand(f client.Factory) *cobra.Command {
	var command = &cobra.Command{
		Use:   "daemonset",
		Short: "Run the ark daemonset",
		Long:  "Run the ark daemonset",
		Run: func(c *cobra.Command, args []string) {
			clientConfig, err := client.Config("", "", fmt.Sprintf("%s-%s", c.Parent().Name(), c.Name()))
			cmd.CheckError(err)

			kubeClient, err := kubernetes.NewForConfig(clientConfig)
			cmd.CheckError(err)

			arkClient, err := clientset.NewForConfig(clientConfig)
			cmd.CheckError(err)

			arkInformerFactory := informers.NewFilteredSharedInformerFactory(arkClient, 0, os.Getenv("HEPTIO_ARK_NAMESPACE"), nil)
			k8sInformerFactory := k8sinformers.NewSharedInformerFactory(kubeClient, 0)

			ctx := context.Background()

			controller := controller.NewPodVolumeBackupController(
				logrus.New(),
				arkInformerFactory.Ark().V1().PodVolumeBackups(),
				arkClient.ArkV1(),
				k8sInformerFactory.Core().V1().Secrets(),
				k8sInformerFactory.Core().V1().Pods(),
				k8sInformerFactory.Core().V1().PersistentVolumeClaims(),
				os.Getenv("NODE_NAME"),
			)

			var wg sync.WaitGroup

			wg.Add(1)
			go func() {
				controller.Run(ctx, 1)
				wg.Done()
			}()

			go arkInformerFactory.Start(ctx.Done())
			go k8sInformerFactory.Start(ctx.Done())

			<-ctx.Done()

			wg.Wait()
		},
	}

	return command
}
