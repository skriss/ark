package tar

import (
	"archive/tar"
	"io"
	"path"

	"github.com/golang/glog"

	"github.com/heptio/ark/pkg/util/filesystem"
)

// ExtractToTempDir extracts a tar reader to a local directory/file tree within a
// temp directory.
func ExtractToTempDir(tarRdr *tar.Reader, fs filesystem.FileSystem) (string, error) {
	dir, err := fs.TempDir("", "")
	if err != nil {
		glog.Errorf("error creating temp dir: %v", err)
		return "", err
	}

	for {
		header, err := tarRdr.Next()

		if err == io.EOF {
			glog.Infof("end of tar")
			break
		}
		if err != nil {
			glog.Errorf("error reading tar: %v", err)
			return "", err
		}

		target := path.Join(dir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			err := fs.MkdirAll(target, header.FileInfo().Mode())
			if err != nil {
				glog.Errorf("mkdirall error: %v", err)
				return "", err
			}

		case tar.TypeReg:
			// make sure we have the directory created
			err := fs.MkdirAll(path.Dir(target), header.FileInfo().Mode())
			if err != nil {
				glog.Errorf("mkdirall error: %v", err)
				return "", err
			}

			// create the file
			file, err := fs.Create(target)
			if err != nil {
				return "", err
			}
			defer file.Close()

			if _, err := io.Copy(file, tarRdr); err != nil {
				glog.Errorf("error copying: %v", err)
				return "", err
			}
		}
	}

	return dir, nil
}
