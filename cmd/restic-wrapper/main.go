package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

func main() {
	// get all the args except the program name
	var args []string
	if len(os.Args) > 1 {
		args = os.Args[1:]
	}

	var (
		passwordFileArg      string
		passwordFileArgIndex int
	)

	// find the -p flag
	for i, arg := range args {
		if strings.HasPrefix(arg, "-p=") {
			passwordFileArg = arg
			passwordFileArgIndex = i
			break
		}
	}

	if passwordFileArg != "" {
		passwordFile := strings.Replace(passwordFileArg, "-p=", "", 1)

		_, err := os.Stat(passwordFile)
		switch {
		case os.IsNotExist(err):
			client, err := coreV1Client()
			checkError(err, "ERROR getting corev1 client")

			ns := os.Getenv("HEPTIO_ARK_NAMESPACE")
			if ns == "" {
				checkError(errors.New("HEPTIO_ARK_NAMESPACE environment variable not found"), "unable to determine namespace")
			}

			secret, err := client.Secrets(ns).Get("restic-credentials", metav1.GetOptions{})
			checkError(err, "ERROR getting heptio-ark/restic-credentials secret")

			// inline func to scope the defer-close of the file
			func() {
				_, repo := filepath.Split(passwordFile)

				file, err := ioutil.TempFile("", fmt.Sprintf("restic-credentials-%s", repo))
				checkError(err, "ERROR creating temp file")
				defer file.Close()

				_, err = file.Write(secret.Data[repo])
				checkError(err, "ERROR writing to temp file")

				args[passwordFileArgIndex] = "-p=" + file.Name()
			}()
		}
	}

	resticCmd := exec.Command("/restic", args...)
	resticCmd.Stdout = os.Stdout
	resticCmd.Stderr = os.Stderr

	checkError(resticCmd.Run(), "ERROR running /restic command")
}

func coreV1Client() (*v1client.CoreV1Client, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	client, err := v1client.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// checkError prints an error message and the error's text to stderr and
// exits with a status code of 1 if the error is not nil, or is a no-op
// otherwise.
func checkError(err error, msg string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, msg+": %s\n", err)
		os.Exit(1)
	}
}
