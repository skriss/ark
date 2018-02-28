### CAVEATS
- this demo expects that your source cluster & your restic repo are in AWS
- this demo is only expected to work for bare pods (i.e. no replicasets/deployments/etc.)
- if you're doing a cross-cloud demo, this will only work for emptyDir volume types since it doesn't convert PV specs from EBS to GCE PD.

### SETUP
0. Build your images:
```
# Ark image
REGISTRY=gcr.io/steve-heptio VERSION=restic-demo make container && docker push gcr.io/steve-heptio/ark:restic-demo

# Restic image
cd restic 
docker build -t gcr.io/steve-heptio/restic:latest .
docker push gcr.io/steve-heptio/restic:latest
```

1. Set up your restic repository env vars:
```
export RESTIC_REPOSITORY=s3:s3.amazonaws.com/<your-restic-bucket>
export RESTIC_PASSWORD=passw0rd
```

2. Set up an AWS IAM account with access to your restic bucket:
```
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
```

3. Initialize your restic repository (when prompted, use the same password from Step 1):
```
restic init
```

4. Create the restic-credentials secret in your heptio-ark namespace:
```
kubecl create namespace heptio-ark 

kubectl create secret generic restic-credentials -n heptio-ark \
    --from-literal RESTIC_REPOSITORY=$RESTIC_REPOSITORY \
    --from-literal RESTIC_PASSWORD=$RESTIC_PASSWORD \
    --from-literal AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    --from-literal AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
```

5. Install Ark following the normal instructions for AWS, with the following exceptions:
    - remove the `persistentVolumeProvider` section from the Ark config - it's not needed
    - use the `examples/aws/10-deployment.yaml` file in this branch to source the restic-credentials data
    - within `examples/aws/10-deployment.yaml`, use the Ark image you built/pushed in Step #0.

6. Install the ark-restic daemonset:
    - in `restic/daemonset.yaml`, use the Restic image you built/pushed in Step #0.
    - run `kubectl apply -f restic/daemonset.yaml`

7. Deploy your sample workload:
    - this prototype code is only expected to work for bare pods (i.e. no replicasets/deployments/etc.)
    - you should take the secret you created in Step #4 and also create it in your sample workload's namespace (needed for restore, later)
    - if you plan to do cross-cloud, use an emptyDir volume type since this prototype won't translate an EBS PV to a GCE PD PV
    - an example is `restic-demo/nginx.yaml`
    - make sure your pod has the `backup.ark.heptio.com/backup-volumes` annotation, where the value is either a single volume name or a JSON array of volume names to back up using restic

8. Prepare your restore target cluster:
    - Run Step #4 from above in the target cluster
    - Run Step #5 from above in the target cluster, with the following notes:
        - your Ark config should have its `objectStorageProvider` config pointed to the S3 bucket being populated by the source cluster
        - set the `backupSyncPeriod` to something low, e.g. 30s
        - remove the `persistentVolumeProvider` section from the Ark config - it's not needed
    
