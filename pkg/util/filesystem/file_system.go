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

package filesystem

import (
	"io"
	"io/ioutil"
	"os"
)

// FileSystem defines methods for interacting with an
// underlying file system.
type FileSystem interface {
	TempDir(dir, prefix string) (string, error)
	MkdirAll(path string, perm os.FileMode) error
	Create(name string) (io.WriteCloser, error)
	RemoveAll(path string) error
	ReadDir(dirname string) ([]os.FileInfo, error)
	ReadFile(filename string) ([]byte, error)
	DirExists(path string) (bool, error)
}

var _ FileSystem = &OSFileSystem{}

type OSFileSystem struct {
}

func (fs *OSFileSystem) TempDir(dir, prefix string) (string, error) {
	return ioutil.TempDir(dir, prefix)
}

func (fs *OSFileSystem) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (fs *OSFileSystem) Create(name string) (io.WriteCloser, error) {
	return os.Create(name)
}

func (fs *OSFileSystem) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (fs *OSFileSystem) ReadDir(dirname string) ([]os.FileInfo, error) {
	return ioutil.ReadDir(dirname)
}

func (fs *OSFileSystem) ReadFile(filename string) ([]byte, error) {
	return ioutil.ReadFile(filename)
}

func (fs *OSFileSystem) DirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
