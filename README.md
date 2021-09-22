# APSIM.Bootstrapper

This is a console application which bootstraps a cloud/cluster run of APSIM. It takes a single .apsimx file as input, and spins up many VMs (pods) in a kubernetes cluster (one per simulation in the .apsimx file). It also starts a "relay" pod which can act as an intermediary to/from the other pods in the cluster.

## Getting Started

To run a debug build of the bootstrapper on a local machine, you will need [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) (the kubernetes CLI), [minikube](https://minikube.sigs.k8s.io/docs/start/) (a kubernetes cluster which runs on a local machine) and [docker](https://docs.docker.com/get-docker/) (minikube supports multiple container runtimes, but this tool only supports docker). Once these are in place, you will need to start minikube, and will almost certainly need to start a proxy server to the kubernetes API server as well. Finally, it's also recommended to start the kubernetes dashboard in order to monitor and diagnose any jobs started with the bootstrapper.

```
minikube start
kubectl proxy &
kubectl dashboard &
```

## Building

The [ApsimX](https://github.com/APSIMInitiative/ApsimX) repository needs to be cloned as a sibling directory to this repository. Once this is done, just build APSIM.Bootstrapper.csproj with .NET CLI or your IDE of choice.

## Running

The project supports a few command-line arguments. These may be viewed with the `--help` flag. The only essential argument is the input file, which may be specified using the `-f` flag:

```
$ dotnet run -- -f /path/to/input/file.apsimx

$ dotnet /path/to/APSIM.Bootstrapper.dll -f /path/to/input/file.apsimx
```

Currently this requires code changes for use in a non-minikube kubernetes environment (e.g. a real cluster).
