#!/bin/sh

set -o errexit
set -o nounset
set -o pipefail

RESTORE_DIR=/restores/$1/host_pods/*/volumes/*/$2
VOLUME_DIR=$(cd /host_pods/$1/volumes/*/$2 && echo $PWD)

echo $RESTORE_DIR
echo $VOLUME_DIR

mv $RESTORE_DIR/* $VOLUME_DIR/

rm -rf /restores/$1

mkdir "$VOLUME_DIR"/.ark && touch "$VOLUME_DIR"/.ark/$3
