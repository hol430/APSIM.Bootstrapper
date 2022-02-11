# CLI Instructions

In order to use the openstack CLI, you will need to authenticate with nectar. See the instructions on [this page](https://tutorials.rc.nectar.org.au/openstack-cli/04-credentials).
To use a GUI, see the [Nectar Dashboard](https://dashboard.rc.nectar.org.au).

Example commands:

```
openstack coe cluster list
openstack coe cluster resize $cluster_name --nodegroup default-worker $num_nodes
openstack coe nodegroup list $cluster_name
```

In order to use kubernetes (kubectl or other API), you need to generate a kubernetes config file for the cluster.
This can be done via the openstack CLI:

```
openstack coe cluster config $cluster_name
```

This will generate a file in the current directory called `config`. Copy this into the `~/.kube/` directory and
then run a kubectl command to test. e.g.

```
kubectl get namespaces
```

## Nodegroup Maintenance

```
# List node groups
openstack coe nodegroup list $cluster_name

# List available instance flavours
openstack flavor list

# Create a node group
openstack coe nodegroup create --node-count 1 --role job-manager-node --flavor m3.small --min-nodes 1 --max-nodes 1 --image fedora-coreos-32 apsim-cluster job-managers

# Delete a node group
openstack coe nodegroup delete $cluster_name $nodegroup_name

# Resize a node group (e.g. to reduce costs)
openstack coe cluster resize $cluster_name --nodegroup $nodegroup_name $desired_size
```

## Notes

After resizing the cluster, it is best to restart the flannel pods. Flannel is used by nectar/opensatck to allow
for inter-node communication on the cluster, but resizing the cluster appears to reveal a bug in flannel, preventing
this communication from working. The easiest solution is to restart flannel after resizing the cluster:

```
kubectl delete pod -n kube-system -l app=flannel
```
