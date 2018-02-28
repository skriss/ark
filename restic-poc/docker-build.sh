#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

docker build -t gcr.io/steve-heptio/restic:v0.8.2 .