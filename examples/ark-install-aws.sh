#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

### Required Inputs:
#
#   BUCKET: name of the S3 bucket to store Ark backups in.
#
#   REGION: AWS region where S3 bucket should be located.

### Optional Inputs:
#
#   ARK_ROOT: location of the base directory of the Ark codebase. Default: https://raw.githubusercontent.com/heptio/ark/master
#
#   ARK_USER: name of the IAM user Ark will use. Default: heptio-ark
#
#   NAMESPACE: namespace where Ark server will be installed. Default: heptio-ark
#
#   IMAGE: Docker image to use for Ark server. Default: gcr.io/heptio-images/ark:master
#
#   IMAGE_PULL_KEY: credentials to use for creating an image pull secret on the Ark service account. Optional.

### Examples:
#
#   BUCKET=my-ark-backups REGION=us-east-1 ./ark-install-aws.sh
#
#   BUCKET=my-ark-backups REGION=us-east-1 NAMESPACE=my-ark-ns ./ark-install-aws.sh


ARK_ROOT=${ARK_ROOT:-https://raw.githubusercontent.com/heptio/ark/master}
ARK_USER=${ARK_USER:-heptio-ark}
NAMESPACE=${NAMESPACE:-heptio-ark}
IMAGE=${IMAGE:-gcr.io/heptio-images/ark:master}
IMAGE_PULL_KEY=${IMAGE_PULL_KEY:-}

echo "BUCKET: $BUCKET"
echo "REGION: $REGION"
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

# 1. create S3 bucket if it does not exist
BUCKET_EXISTS=$(aws s3api list-buckets --query 'Buckets[].Name' | jq -r --arg BUCKET $BUCKET -c '.[] | select(. == $BUCKET)')

if [[ -z $BUCKET_EXISTS ]]; then
    if [[ $REGION -eq "us-east-1" ]]; then
        aws s3api create-bucket \
            --bucket $BUCKET \
            --region $REGION
    else
        aws s3api create-bucket \
            --bucket $BUCKET \
            --region $REGION \
            --create-bucket-configuration LocationConstraint=$REGION
    fi
else
    echo "bucket $BUCKET already exists."
fi

# 2. create IAM user if it does not exist
USER_EXISTS=$(aws iam list-users --query 'Users[].UserName' | jq -r --arg ARK_USER $ARK_USER -c '.[] | select(. == $ARK_USER)')

if [[ -z $USER_EXISTS ]]; then
    aws iam create-user --user-name $ARK_USER

    cat > heptio-ark-policy.json <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "ec2:DescribeVolumes",
                    "ec2:DescribeSnapshots",
                    "ec2:CreateTags",
                    "ec2:CreateVolume",
                    "ec2:CreateSnapshot",
                    "ec2:DeleteSnapshot"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:DeleteObject",
                    "s3:PutObject"
                ],
                "Resource": [
                    "arn:aws:s3:::${BUCKET}/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:ListBucket",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts"
                ],
                "Resource": [
                    "arn:aws:s3:::${BUCKET}"
                ]
            }
        ]
    }
EOF

    aws iam put-user-policy \
        --user-name $ARK_USER \
        --policy-name $ARK_USER \
        --policy-document file://heptio-ark-policy.json
else
    echo "IAM user $ARK_USER already exists."
fi

# 3. create access key & credentials-ark file
ACCESS_KEY=$(aws iam create-access-key --user-name $ARK_USER | jq -c '.AccessKey')
ARK_AWS_ACCESS_KEY_ID=$(echo $ACCESS_KEY | jq -r -c '.AccessKeyId')
ARK_AWS_SECRET_ACCESS_KEY=$(echo $ACCESS_KEY | jq -r -c '.SecretAccessKey')

cat > credentials-ark <<EOF
[default]
aws_access_key_id=$ARK_AWS_ACCESS_KEY_ID
aws_secret_access_key=$ARK_AWS_SECRET_ACCESS_KEY
EOF

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
getFileContent $ARK_ROOT/examples/aws/00-ark-config.yaml \
    | sed "s/<YOUR_BUCKET>/$BUCKET/" \
    | sed "s/<YOUR_REGION>/$REGION/" \
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
getFileContent $ARK_ROOT/examples/aws/10-deployment.yaml \
    | sed "s|gcr.io/heptio-images/ark:latest|$IMAGE|" \
    | sed "s/namespace: heptio-ark/namespace: $NAMESPACE/" \
    | kubectl apply -f -
