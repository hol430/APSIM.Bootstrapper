# NECTAR

## Background

Nectar uses the OpenStack API/CLI for cluster management. In the OpenStack world, the cluster is made up of nodes (aka VMs/instances). The nodes are grouped into nodegroups; to create a node, one must create a nodegroup, and nodes will be automatically provisioned to fill a nodegroup.

A nodegroup is defined by a node type/flavour, and a node count. Nodegroups can be assigned roles, which will be translated into kubernetes labels on each node in the nodegroup, and which may then be used as criteria for kubernetes' allocation of pods to nodes. By default, the bootstrapper will only schedule worker pods to run on nodes with the `role` (magnum.openstack.org/role) label set to `workers`. This means that when creating a nodegroup to be used as worker nodes for a job, you must set the role to `workers`. Otherwise the bootstrapper will be unable to allocate pods to nodes.

The job manager pod will prefer to run on nodes with a role of `job-manager-node`. If no such pods exist, it will run on any worker node (ie a node with the `workers` role). This may be useful for saving some CPU time, as the job manager doesn't really need a large number of CPUs.

# CLI

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

## Troubleshooting

After resizing the cluster, it is best to restart the flannel pods. Flannel is used by nectar/opensatck to allow
for inter-node communication on the cluster, but resizing the cluster appears to reveal a bug in flannel, preventing
this communication from working. The easiest solution is to restart flannel after resizing the cluster:

```
kubectl delete pod -n kube-system -l app=flannel
```

---

```
Service 'org.kde.klipper' does not exist.
Traceback (most recent call last):
  File "/usr/bin/openstack", line 6, in <module>
    from openstackclient.shell import main
  File "/usr/lib/python3.10/site-packages/openstackclient/shell.py", line 23, in <module>
    from osc_lib import shell
  File "/usr/lib/python3.10/site-packages/osc_lib/shell.py", line 24, in <module>
    from cliff import app
  File "/usr/lib/python3.10/site-packages/cliff/app.py", line 23, in <module>
    import cmd2
  File "/usr/lib/python3.10/site-packages/cmd2/__init__.py", line 55, in <module>
    from .cmd2 import Cmd
  File "/usr/lib/python3.10/site-packages/cmd2/cmd2.py", line 85, in <module>
    from .clipboard import (
  File "/usr/lib/python3.10/site-packages/cmd2/clipboard.py", line 19, in <module>
    _ = pyperclip.paste()
  File "/usr/lib/python3.10/site-packages/pyperclip/__init__.py", line 681, in lazy_load_stub_paste
    return paste()
  File "/usr/lib/python3.10/site-packages/pyperclip/__init__.py", line 301, in paste_klipper
    assert len(clipboardContents) > 0
AssertionError
```

See these [bug](https://github.com/asweigart/pyperclip/issues/26) [reports](https://github.com/asweigart/pyperclip/issues/202). A workaround is to start klipper (run `klipper`).
