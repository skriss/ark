#!/bin/bash

. demo-magic.sh

clear

export DEMO_PROMPT=$GREEN"heptio"$PURPLE"@ark"$COLOR_RESET" $ "
export PROMPT_TIMEOUT=2

# 1. Show sample workload
p  "# we have nginx deployed in our cluster:"
pe "kubectl -n nginx-example get pods"
wait

p  "# the nginx pod is using a persistent volume to store its logs:"
pe "kubectl get persistentvolumes"
wait

POD=$(kc get po -n nginx-example -o json | jq -r '.items | .[0] | .metadata.name')
CMD="kubectl -n nginx-example exec "$POD" cat /var/log/nginx/access.log"

p "# there's some data in the access log:"
pe "$CMD"
wait

# 2. Take an Ark backup
p "# we have Heptio Ark installed in our cluster:"
pe "kubectl -n heptio-ark get pods"
wait

p "# let's take an Ark backup of our nginx namespace:" 
pe "ark backup create nginx-backup --include-namespaces nginx-example"
PROMPT_TIMEOUT=15
wait
PROMPT_TIMEOUT=1

# 3. Show Ark backup
# TODO wait a few extra seconds so backup can complete
p "# our Ark backup has completed:"
pe "ark backup get nginx-backup"
wait

# 4. Simulate disaster
p "# oops! we accidentally deleted our nginx namespace:"
pe "kubectl delete namespace nginx-example"
kubectl delete pv --all >/dev/null 2>&1
PROMPT_TIMEOUT=30
wait
PROMPT_TIMEOUT=1

p "# our nginx namespace is definitely gone:"
pe "kubectl get namespace nginx-example"
wait

p "# our persistent volume containing our nginx logs is also gone:"
pe "kubectl get persistentvolumes"
wait

# 5. Execute a restore
p "# let's execute an Ark restore from the backup we created:"
pe "ark restore create nginx-restore --from-backup nginx-backup"
PROMPT_TIMEOUT=180
wait
PROMPT_TIMEOUT=1

p "# our Ark restore has completed:"
pe "ark restore get nginx-restore"
wait

# 6. Show recovery of workload
p "# phew! our nginx pod has been restored:"
pe "kubectl -n nginx-example get pods"
wait

p "# and so has our persistent volume:"
pe "kubectl get persistentvolumes"
wait

POD=$(kc get po -n nginx-example -o json | jq -r '.items | .[0] | .metadata.name')
CMD="kubectl -n nginx-example exec "$POD" cat /var/log/nginx/access.log"

p "# our log data has been recovered:"
pe "$CMD"

wait