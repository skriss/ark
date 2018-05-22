package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

func main() {
	repo := repoName(os.Args[1:])
	if repo == "" {
		checkError("ERROR getting repository name", errors.New("repository flag (--repo) not found"))
	}

	ns := os.Getenv("HEPTIO_ARK_NAMESPACE")
	if ns == "" {
		checkError("ERROR getting heptio ark namespace", errors.New("HEPTIO_ARK_NAMESPACE environment variable not found"))
	}

	secrets, err := secretInterface(ns)
	checkError("ERROR getting secrets interface", err)

	secret, err := secrets.Get("restic-credentials", metav1.GetOptions{})
	checkError("ERROR getting heptio-ark/restic-credentials secret", err)

	passwordFile, err := createPasswordFile(secret, repo)
	checkError("ERROR creating password temp file", err)
	defer os.Remove(passwordFile)

	resticArgs := append([]string{"--password-file=" + passwordFile}, os.Args[1:]...)
	resticCmd := exec.Command("/restic", resticArgs...)
	resticCmd.Stdout = os.Stdout
	resticCmd.Stderr = os.Stderr

	checkError("ERROR running restic command", resticCmd.Run())
}

// repoName parses the restic repo name from a standard restic --repo
// flag, e.g.:
//		s3:s3.amazonaws.com/<bucket>/<repo>
//		azure:<container>:/<repo>
//		gs:<bucket>:/<repo>
func repoName(args []string) string {
	for _, arg := range args {
		if strings.HasPrefix(arg, "--repo=") {
			return arg[strings.LastIndex(arg, "/")+1:]
		}
	}

	return ""
}

func secretInterface(namespace string) (corev1client.SecretInterface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	client, err := corev1client.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	return client.Secrets(namespace), nil
}

// checkError prints an error message and the error's text to stderr and
// exits with a status code of 1 if the error is not nil, or is a no-op
// otherwise.
func checkError(msg string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, msg+": %v\n", err)
		os.Exit(1)
	}
}

func createPasswordFile(secret *corev1api.Secret, repo string) (string, error) {
	file, err := ioutil.TempFile("", fmt.Sprintf("restic-credentials-%s", repo))
	if err != nil {
		return "", err
	}

	_, err = file.Write(secret.Data[repo])
	if err != nil {
		file.Close()
		return "", err
	}

	return file.Name(), file.Close()
}
