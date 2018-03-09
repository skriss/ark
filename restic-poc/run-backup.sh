#!/bin/sh

set -o errexit
set -o nounset
set -o pipefail

PID=$(./docker inspect -f '{{.State.Pid}}' "$1")

./restic backup /host_proc/${PID}/root/$2 $3