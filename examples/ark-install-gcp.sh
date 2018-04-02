#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

### Required Inputs:
#
#   BUCKET: name of the GCS bucket to store Ark backups in.

### Optional Inputs:
#
#   ARK_ROOT: location of the base directory of the Ark codebase. Default: https://raw.githubusercontent.com/heptio/ark/master
#
#   ARK_USER: name of the service account Ark will use. Default: heptio-ark
#
#   NAMESPACE: namespace where Ark server will be installed. Default: heptio-ark
#
#   IMAGE: Docker image to use for Ark server. Default: gcr.io/heptio-images/ark:master
#
#   IMAGE_PULL_KEY: credentials to use for creating an image pull secret on the Ark service account. Optional.

### Examples:
#
#   BUCKET=my-ark-backups ./ark-install-gcp.sh
#
#   BUCKET=my-ark-backups NAMESPACE=my-ark-ns ./ark-install-gcp.sh

ARK_ROOT=${ARK_ROOT:-https://raw.githubusercontent.com/heptio/ark/master}
ARK_USER=${ARK_USER:-heptio-ark}
NAMESPACE=${NAMESPACE:-heptio-ark}
IMAGE=${IMAGE:-gcr.io/heptio-images/ark:master}
IMAGE_PULL_KEY=${IMAGE_PULL_KEY:-}

echo "BUCKET: $BUCKET"
printf "\\n"
echo "ARK ROOT: $ARK_ROOT"
echo "ARK USER: $ARK_USER"
echo "NAMESPACE: $NAMESPACE"
echo "IMAGE: $IMAGE"
echo "IMAGE PULL KEY: $IMAGE_PULL_KEY"
printf "\\n"

getFileContent () {
    PREFIX=$(echo $1 | cut -c -4)
    if [[ $PREFIX == "http" ]]; then
        curl -s $1
    else
        cat $1
    fi
}

# 1. create GCS bucket if it does not exist
BUCKET_EXISTS=$(gsutil ls | grep "gs://$BUCKET/")

if [[ -z $BUCKET_EXISTS ]]; then
    gsutil mb gs://$BUCKET/
else
    echo "bucket $BUCKET already exists."
fi

# 2. create IAM user if it does not exist
DISPLAY_NAME="Heptio Ark service account"
USER_EXISTS=$(gcloud iam service-accounts list --format json | jq -r --arg DISPLAY_NAME "$DISPLAY_NAME" -c '.[] | select(.displayName == $DISPLAY_NAME)')

if [[ -z $USER_EXISTS ]]; then
    PROJECT_ID=$(gcloud config configurations list --filter IS_ACTIVE=True --format json | jq -r -c '.[] | .properties.core.project')
    if [[ -z $PROJECT_ID ]]; then
        echo "cannot determine active project name"
        exit
    fi
    
    gcloud iam service-accounts create $ARK_USER \
        --display-name "$DISPLAY_NAME"
else
    echo "IAM user $ARK_USER already exists."
fi

SERVICE_ACCOUNT_EMAIL=$(gcloud iam service-accounts list --format json | jq -r --arg DISPLAY_NAME "$DISPLAY_NAME" -c '.[] | select(.displayName == $DISPLAY_NAME) | .email')    

ROLE_PERMISSIONS=(
    compute.disks.get
    compute.disks.create
    compute.disks.createSnapshot
    compute.snapshots.get
    compute.snapshots.create
    compute.snapshots.useReadOnly
    compute.snapshots.delete
    compute.projects.get
)

# 3. create or update IAM role, and assign IAM user to relevant roles for compute & storage
ROLE_EXISTS=$(gcloud iam roles list --project $PROJECT_ID --format json | jq -r --arg ROLE_NAME "projects/$PROJECT_ID/roles/heptio_ark.server" -c '.[] | select(.name == $ROLE_NAME) | .name')

if [[ -z $ROLE_EXISTS ]]; then
    echo "Creating IAM role"
    gcloud iam roles create heptio_ark.server \
        --project $PROJECT_ID \
        --title "Heptio Ark Server" \
        --permissions "$(IFS=","; echo "${ROLE_PERMISSIONS[*]}")"
else
    echo "Updating IAM role"
    gcloud iam roles update heptio_ark.server \
        --project $PROJECT_ID \
        --title "Heptio Ark Server" \
        --permissions "$(IFS=","; echo "${ROLE_PERMISSIONS[*]}")"
fi

echo "Assigning role to IAM user $ARK_USER"
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member serviceAccount:$SERVICE_ACCOUNT_EMAIL \
    --role projects/$PROJECT_ID/roles/heptio_ark.server

echo "Granting IAM user $ARK_USER access to bucket $BUCKET"
gsutil iam ch serviceAccount:$SERVICE_ACCOUNT_EMAIL:objectAdmin gs://${BUCKET}

# 3. create access key & credentials-ark file
gcloud iam service-accounts keys create credentials-ark \
    --iam-account $SERVICE_ACCOUNT_EMAIL

# 4. install ark prereqs
getFileContent $ARK_ROOT/examples/common/00-prereqs.yaml \
    | sed "s/name: heptio-ark/name: $NAMESPACE/" \
    | sed "s/namespace: heptio-ark/namespace: $NAMESPACE/" \
    | kubectl apply -f -

# 5. create cloud-credentials secret
kubectl create secret generic cloud-credentials \
    --namespace $NAMESPACE \
    --from-file cloud=credentials-ark

# 6. create ark config
getFileContent $ARK_ROOT/examples/gcp/00-ark-config.yaml \
    | sed "s/<YOUR_BUCKET>/$BUCKET/" \
    | kubectl apply -f -

# 7. create image-pull secret if provided
if [[ ! -z $IMAGE_PULL_KEY ]]; then
    kubectl create secret docker-registry ark-image-pull-secret \
         --namespace $NAMESPACE \
         --docker-server https://gcr.io \
         --docker-username=_json_key \
         --docker-email=user@example.com \
         --docker-password "$(cat $IMAGE_PULL_KEY)"

    kubectl patch serviceaccount ark \
        --namespace $NAMESPACE \
        --patch '{"imagePullSecrets": [{"name": "ark-image-pull-secret"}]}'
fi

# 8. create ark deployment 
getFileContent $ARK_ROOT/examples/gcp/10-deployment.yaml \
    | sed "s|gcr.io/heptio-images/ark:latest|$IMAGE|" \
    | sed "s/namespace: heptio-ark/namespace: $NAMESPACE/" \
    | kubectl apply -f -
