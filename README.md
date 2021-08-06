# APSIM.Bootstrapper

This is a console application which bootstraps a cloud/cluster run of APSIM. It takes a single .apsimx file as input.

## Building

The [ApsimX](https://github.com/APSIMInitiative/ApsimX) repository needs to be cloned as a sibling directory to this repository. Once this is done, just build the APSIM.Bootstrapper with .NET CLI or your IDE of choice.

## Running

The project supports a few command-line arguments. These may be viewed with the `--help` flag. The only essential argument is the input file, which may be specified using the `-f` flag:

```
$ dotnet run -- -f /path/to/input/file.apsimx

$ dotnet /path/to/APSIM.Bootstrapper.dll -f /path/to/input/file.apsimx
```

Currently this requires code changes for use in a non-minikube kubernetes environment (e.g. a real cluster).
