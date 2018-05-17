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
		checkError(errors.New("repository flag (-r) not found"), "ERROR getting repository name")
	}

	ns := os.Getenv("HEPTIO_ARK_NAMESPACE")
	if ns == "" {
		checkError(errors.New("HEPTIO_ARK_NAMESPACE environment variable not found"), "ERROR getting heptio ark namespace")
	}

	secrets, err := secretInterface(ns)
	checkError(err, "ERROR getting secrets interface")

	secret, err := secrets.Get("restic-credentials", metav1.GetOptions{})
	checkError(err, "ERROR getting heptio-ark/restic-credentials secret")

	passwordFile, err := createPasswordFile(secret, repo)
	checkError(err, "ERROR creating password temp file")
	defer os.Remove(passwordFile)

	// set the env vars that restic uses for azure credentials
	os.Setenv("AZURE_ACCOUNT_NAME", os.Getenv("AZURE_STORAGE_ACCOUNT_ID"))
	os.Setenv("AZURE_ACCOUNT_KEY", os.Getenv("AZURE_STORAGE_KEY"))

	resticArgs := append([]string{"-p=" + passwordFile}, os.Args[1:]...)
	resticCmd := exec.Command("/restic", resticArgs...)
	resticCmd.Stdout = os.Stdout
	resticCmd.Stderr = os.Stderr

	checkError(resticCmd.Run(), "ERROR running restic command")
}

func repoName(args []string) string {
	for _, arg := range args {
		if strings.HasPrefix(arg, "-r=") {
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
func checkError(err error, msg string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, msg+": %s\n", err)
		os.Exit(1)
	}
}

func createPasswordFile(secret *corev1api.Secret, repo string) (string, error) {
	file, err := ioutil.TempFile("", fmt.Sprintf("restic-credentials-%s", repo))
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Write(secret.Data[repo])
	if err != nil {
		return "", err
	}

	return file.Name(), nil
}
