using APSIM.Server.Commands;
using APSIM.Server.IO;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using APSIM.Bootstrapper.Extensions;
using k8s;
using k8s.Models;
using Microsoft.Rest;
using Models.Core.Run;
using System.Data;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using APSIM.Bootstrapper.Logging;
using System.Net;

namespace APSIM.Bootstrapper
{
    public class Bootstrapper : IDisposable
    {
        /// <summary>
        /// Number of simulations per pod. This should probably be equal to
        /// # of vCPUs per node.
        /// </summary>
        private const uint chunkSize = 16;

        /// <summary>
        /// Number of iterations which the demo application should run.
        /// </summary>
        private const uint numIterations = 100;

        /// <summary>
        /// API Version passed into all kubernetes API requests.
        /// </summary>
        private const string apiVersion = "v1";

        /// <summary>
        /// Value of the "app-name" label given to the created namespace.
        /// </summary>
        private const string appName = "apsim-cluster";

        /// <summary>
        /// Value of the "instance-name" label given to the created namespace.
        /// </summary>
        private const string instanceName = "apsim-cluster-job";

        /// <summary>
        /// Value of the "version" label given to the created namespace.
        /// </summary>
        private const string version = "1.0";

        /// <summary>
        /// Value of the "component" label given to the created namespace.
        /// </summary>
        private const string component = "simulation";

        /// <summary>
        /// Value of the "part-of" label given to the created namespace.
        /// </summary>
        private const string partOf = "apsim";

        /// <summary>
        /// Value of the "managed-by" label given to the created namespace.
        /// </summary>
        private const string managedBy = "ApsimClusterJobManager";

        /// <summary>
        /// Name of the docker image used by the job manager.
        /// </summary>
        private const string imageName = "apsiminitiative/apsimng-server:latest";

        /// <summary>
        /// Name of the job manager pod.
        /// </summary>
        private const string jobManagerPodName = "job-manager";

        /// <summary>
        /// Name of the job manager container.
        /// </summary>
        private const string jobManagerContainerName = "job-manager-container";

        /// <summary>
        /// Job manager container entrypoint.
        /// </summary>
        /// <remarks>
        /// Using bash for now to facilitate a hack to wait until files are
        /// copied into the container before starting the job manager.
        /// </remarks>
        private const string containerEntrypoint = "/bin/bash";//"/apsim/apsim-server";

        /// <summary>
        /// The name of the service account created for and used by the job manager.
        /// </summary>
        /// <remarks>
        /// Service account names need only be unique within a namespace.
        /// </remarks>
        private const string serviceAccountName = "apsim-account";

        /// <summary>
        /// The worker nodes expect to find their input files here.
        /// </summary>
        private const string workerInputsPath = "/inputs";

        /// <summary>
        /// The worker containers will start when they successfully stat this file.
        /// </summary>
        private const string containerStartFile = "/start";

        /// <summary>
        /// A label with this name is added to all pods created by the bootstrapper.
        /// </summary>
        private const string podTypeLabelName = "dev.apsim.info/pod-type";

        /// <summary>
        /// All worker pods have their <see cref="podTypeLabelName"/> set to this value.
        /// </summary>
        private const string workerPodType = "worker";

        /// <summary>
        /// The job manager pod has its <see cref="podTypeLabelName"/> label set to this value.
        /// </summary>
        private const string jobManagerPodType = "job-manager";

        /// <summary>
        /// The optimiser pod has it s<see cref="podTypeLabelName"/> label set to this value.
        /// </summary>
        private const string optimiserPodType = "optimiser";

        /// <summary>
        /// This is the name of the "role" label given to all nodes in an openstack cluster.
        /// </summary>
        private const string openStackNodeRoleLabelName = "magnum.openstack.org/role";

        /// <summary>
        /// Name of the 'name' label given to all kubernetes nodes.
        /// </summary>
        private const string kubernetesNodeNameLabel = "kubernetes.io/hostname";

        /// <summary>
        /// This is the role given to 'master' nodes on openstack clusters.
        /// </summary>
        private const string openStackMasterNodeRole = "master";

        /// <summary>
        /// This is the role given to 'worker' nodes on openstack clusters.
        /// </summary>
        private const string openStackWorkerNodeRoleName = "workers";

        /// <summary>
        /// This is the role which should be given to 'job manager' nodes on openstack clusters.
        /// </summary>
        /// <remarks>
        /// This is not a cluster default, and no job manager ndoes are actually required. However,
        /// it can be useful to create a job manager node which has less CPUs than the
        /// worker pods in order to improve overall CPU usage efficiency of the job.
        /// </remarks>
        private const string openStackJobManagerNodeRole = "job-manager-node";

        /// <summary>
        /// The verbs given to the job manager's service account's role.
        /// </summary>
        /// <remarks>
        /// (These are essentially the permissions granted to the job manager container.)
        /// </remarks>
        private static readonly IList<string> jobManagerPermissions = new string[6]
        {
            // todo: double check which of these the job manager actually uses
            "get",
            // "watch",
            "list",
            "create",
            "delete",
            "update",
            "post"
        };

        /// <summary>
        /// Resources to which the job manager permissions apply.
        /// </summary>
        private static readonly IList<string> jobManagerResources = new string[2]
        {
            "pods",
            "pods/exec"
        };

        /// <summary>
        /// Do not copy files with these extensions into a container.
        /// </summary>
        private static readonly IList<string> doNotCopy = new string[4]
        {
            ".db",
            ".db-wal",
            ".db-shm",
            ".bak"
        };

        /// <summary>
        /// Path at which input files will be copied into the job manager container.
        /// </summary>
        private const string jobManagerInputsPath = "/inputs";

        /// <summary>
        /// Name of the role assigned to the job manager's service account.
        /// </summary>
        /// <remarks>
        /// Role name needs only be unique within its namespace.
        /// </remarks>
        private const string roleName = "apsim-role";

        /// <summary>
        /// The "In" operator used for node scheduling affinities. This is a constant
        /// exposed by the kubernetes API.
        /// </summary>
        private const string @in = "In";

        /// <summary>
        /// The "NotIn" operator used for node scheduling affinities. This is a constant
        /// exposed by the kubernetes API.
        /// </summary>
        private const string notIn = "NotIn";

        /// <summary>
        /// Port on which the job manager listens for connections.
        /// </summary>
        /// <remarks>
        /// fixme: this could be a user parameter as it could conflict with other services???
        /// </remarks>
        private const ushort jobManagerPortNo = 27746;

        /// <summary>
        /// Port on which the worker pods listen for connections.
        /// </summary>
        private const ushort workerPodPortNo = 27746;

        /// <summary>
        /// Name of the service which is created to handle port forwarding to the job manager.
        /// </summary>
        private const string portForwardingServiceName = "job-manager-port-forwarding";

        /// <summary>
        /// This specifies behaviour for a pod spread constraint when the constraint
        /// cannot be satisfied. This behaviour is to schedule the pod anyway.
        /// </summary>
        private const string scheduleAnyway = "ScheduleAnyway";

        /// <summary>
        /// This specifies behaviour for a pod spread constraint when the constraint
        /// cannot be satisfied. This behaviour is to not scheudle the pod.
        /// </summary>
        private const string doNotSchedule = "DoNotSchedule";

        /// <summary>
        /// Describes the "if not present" image pull policy.
        /// </summary>
        private const string ifNotPresent = "IfNotPresent";

        // Stateful variables

        /// <summary>
        /// Name of the namespace created to run the job.
        /// </summary>
        private readonly string jobNamespace;

        /// <summary>
        /// The unique job ID. This is suffixed to some component names
        /// which need to be unique across the cluster (e.g. the namespace name).
        /// </summary>
        private readonly Guid jobID;

        /// <summary>
        /// Arguments passed into the container's entrypoint.
        /// </summary>
        /// <remarks>
        /// Currently part of a hack, but this is also useful for printing
        /// out to diagnostics, for debugging purposes.
        /// </remarks>
        private string[] containerArgs;

        /// <summary>
        /// The kubernetes client through which our API requests are made.
        /// </summary>
        private Kubernetes client;

        /// <summary>
        /// User options.
        /// </summary>
        private Options options;

        /// <summary>
        /// Names of the worker pods. This is used mainly for monitoring
        /// purposes, to ensure that they launched without error.
        /// </summary>
        private IEnumerable<string> workers;

        /// <summary>
        /// Create a new <see cref="Bootstrapper"/> instance.
        /// </summary>
        /// <param name="options"></param>
        public Bootstrapper(Options options)
        {
            var config = KubernetesClientConfiguration.BuildConfigFromConfigFile();
            client = new Kubernetes(config);
            this.options = options;
            jobID = Guid.NewGuid();
            jobNamespace = $"apsim-cluster-job-{jobID}";
            if (!File.Exists(options.InputFile))
                throw new FileNotFoundException($"File {options.InputFile} does not exist");
        }

        public void Initialise()
        {
            // Create the namespace in which the job manager pod will run.
            CreateNamespace();
            Console.WriteLine($"export namespace={jobNamespace}");
        }

        /// <summary>
        /// Run the bootstrapper - initialise the cluster environment.
        /// </summary>
        internal void CreateDefaultSetup()
        {
            try
            {
                // Create a service account for the job manager pod.
                CreateServiceAccount();

                // Create the Role which will be linked to the servicea account.
                // This role will give the job manager the necessary permissions
                // to create and manage its worker pods.
                CreateRole();

                // Create a RoleBinding, linking the role to the service account.
                CreateRoleBinding();

                // Create the job manager pod.
                CreateJobManager();

                // Create worker pods.
                CreateWorkers();

                // Wait for the job manager pod to finish creation/startup.
                WriteToLog("Waiting for job manager pod to start...", Verbosity.Information);
                CancellationTokenSource cts = new CancellationTokenSource(100 * 1000);
                WaitFor(jobManagerPodName, HasStarted, cts);

                // Send the start signal to the job manager pod.
                // This will start the apsim server.
                SendStartSignalToPod(jobManagerPodName);

                // Wait for the apsim server to complete its initialisation.
                WriteToLog("Waiting for job manager pod to enter ready state...", Verbosity.Information);
                WaitFor(jobManagerPodName, IsReady, cts);

                WriteToLog("Waiting for worker nodes to enter ready state...", Verbosity.Information);
                foreach (string worker in workers)
                    WaitFor(worker, IsReady, cts);
            }
            catch (HttpOperationException httpError)
            {
                // If we encounter any errors while setting up the job manager pod
                // or its resources, attempt to delete the namespace. Any created
                // resources will be inside this namespace and will be deleted along
                // with the namespace.
                Dispose();
                throw new BootstrapperException(httpError);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <summary>
        /// Get the IP Address of a pod.
        /// </summary>
        /// <param name="podName">Name of the pod.</param>
        private string GetPodIPAddress(string podName)
        {
            V1Pod pod = GetPod(podName);
            return pod.Status.PodIP;
        }

        /// <summary>
        /// Issue a RUN command to the job manager.
        /// </summary>
        /// <param name="command">Command to be sent.</param>
        public void RunWithChanges(RunCommand command)
        {
            WriteToLog($"Executing {command}", Verbosity.Information);

            var stopwatch = Stopwatch.StartNew();

            // Create a socket server for comms to the job manager pod.
            using (PortForwardingService service = new PortForwardingService(client))
            {
                // Start the socket server.
                WriteToLog($"Initiating port forwarding to job manager...", Verbosity.Information);
                int port = service.Start(jobManagerPodName, jobNamespace, (int)jobManagerPortNo);

                // fixme: IPAddress.Loopback
                string ip = "127.0.0.1";

                WriteToLog($"Initiating connection to job manager...", Verbosity.Information);
                using (var conn = new NetworkSocketClient(options.Verbose, ip, (uint)port, Protocol.Managed))
                {
                    WriteToLog($"Successfully established connection to job manager. Running command...", Verbosity.Information);

                    // Note - SendCommand will wait for the command to finish.
                    conn.SendCommand(command);

                    WriteToLog($"Command executed successfully. Disconnecting...", Verbosity.Information);
                }
            }

            stopwatch.Stop();
            Console.WriteLine($"{command} ran in {stopwatch.ElapsedMilliseconds}ms");
        }

        /// <summary>
        /// Issue a READ comand to the job manager.
        /// </summary>
        /// <param name="command">The command, detailing which parameters should be read.</param>
        public DataTable ReadOutput(ReadCommand command)
        {
            WriteToLog($"Executing {command}", Verbosity.Information);

            var stopwatch = Stopwatch.StartNew();

            // Create a socket server for comms to the job manager pod.
            using (PortForwardingService service = new PortForwardingService(client))
            {
                // Start the socket server.
                WriteToLog($"Initiating port forwarding to job manager...", Verbosity.Information);
                uint socketServerPort = (uint)service.Start(jobManagerPodName, jobNamespace, (int)jobManagerPortNo);

                string ip = "127.0.0.1"; // IPAddress.Loopback
                WriteToLog($"Initiating connection to job manager...", Verbosity.Information);
                using (var conn = new NetworkSocketClient(options.Verbose, ip, socketServerPort, Protocol.Managed))
                {
                    WriteToLog($"Successfully established connection to job manager. Running command...", Verbosity.Information);

                    // Note - SendCommand will wait for the command to finish.
                    DataTable result = conn.ReadOutput(command);

                    stopwatch.Stop();
                    WriteToLog($"Command executed successfully in {stopwatch.ElapsedMilliseconds}ms. Disconnecting...", Verbosity.Information);

                    return result;
                }
            }
        }

        /// <summary>
        /// Cleanup of all kubernetes resources created by this bootstrapper instance.
        /// I'm not implementing IDisposable here because we normally do NOT want to do
        /// this - usually, the bootstrapper will initialise its resources/workers and
        /// then leave them to their own devices. However, I'm making this public so that
        /// the caller (/owner) of the bootstrapper can perform cleanup in the event of
        /// an error.
        /// </summary>
        public void Dispose()
        {
            try
            {
                try
                {
                    client.DeleteNamespace(jobNamespace);
                }
                catch (HttpOperationException httpError)
                {
                    throw new BootstrapperException("Unable to delete job namespace", httpError);
                }
                catch (Exception otherError)
                {
                    throw new Exception("Unable to delete job namespace", otherError);
                }
            }
            catch (Exception err)
            {
                Console.Error.WriteLine(err);
            }
        }

        /// <summary>
        /// Monitor a pod until a condition is met, or until the cancellation token has expired.
        /// </summary>
        /// <param name="podName">Name of the pod to be monitored.</param>
        /// <param name="condition">Condition - when this returns true, monitoring will cease.</param>
        /// <param name="cancelToken">This is used to cancel the wait operation..</param>
        private void WaitFor(string podName, Func<V1Pod, bool> condition, CancellationTokenSource cancelToken)
        {
            try
            {
                Task waiter = WaitForAsync(podName, condition, cancelToken.Token);
                waiter.Wait(cancelToken.Token);
                if (waiter.Status == TaskStatus.Faulted)
                    throw new Exception($"Failed to wait for condition {condition.Method.Name} on pod {podName}", waiter.Exception);
            }
            catch (OperationCanceledException)
            {
                WriteToLog($"WARNING: Timeout while waiting for condition {condition.Method.Name} on pod {podName}. Continuing bootstrapper execution...", Verbosity.Information);
            }
        }

        /// <summary>
        /// Monitor a pod and wait for a condition to evaluate to true.
        /// </summary>
        /// <param name="podName">Name of the pod to be watched.</param>
        /// <param name="condition">Condition for which to wait.</param>
        private async Task WaitForAsync(string podName, Func<V1Pod, bool> condition, CancellationToken cancelToken)
        {
            bool conditionMet = false;
            Action<WatchEventType, V1Pod> onEvent = (_, pod) => { conditionMet = condition(pod); };
            Action<Exception> errorHandler = e => { if (!conditionMet) throw e; };

            // Start monitoring the pod.
            using (Watcher<V1Pod> watcher = await client.WatchNamespacedPodAsync(podName, jobNamespace, onEvent: onEvent, onError: errorHandler, cancellationToken: cancelToken))
            {
                // The callback will only be invoked when the pod's state has changed. So we
                // manually check the condition now, in case the pod has already reached a
                // state which satisfies the given condition.
                V1Pod pod = await client.ReadNamespacedPodAsync(podName, jobNamespace, cancellationToken: cancelToken);
                if (condition(pod))
                    return;

                // Wait until the condition has been met.
                SpinWait.SpinUntil(() => conditionMet || cancelToken.IsCancellationRequested);
            }

            cancelToken.ThrowIfCancellationRequested();
        }

        /// <summary>
        /// Check if a pod is "ready" to receive connections. Throw if the pod has failed.
        /// </summary>
        /// <remarks>
        /// This makes use of the custom readiness probe.
        /// </remarks>
        /// <param name="pod">The pod to check.</param>
        private bool IsReady(V1Pod pod)
        {
            string podName = pod.Name();
            string containerName = GetContainerName(podName);
            V1ContainerStatus status = client.GetContainerStatus(pod, containerName);
            if (status.State.Terminated != null)
            {
                string log = GetLog(jobNamespace, podName, containerName);
                throw new Exception($"Pod {pod.Name()} has failed:\n{log}");
            }

            // The container's readiness probe is configured to check if the pod
            // is listening for TCP connections.
            bool ready = status.Ready;
            WriteToLog($"Container {containerName} in pod {podName} is {(ready ? "" : "not ")}ready.", Verbosity.Diagnostic);
            return ready;
        }

        /// <summary>
        /// Check if a pod has completed execution.
        /// </summary>
        /// <param name="pod">The pod to check.</param>
        private bool IsFinished(V1Pod pod)
        {
            string podName = pod.Name();
            string containerName = GetContainerName(podName);
            V1ContainerStatus status = client.GetContainerStatus(pod, containerName);
            return status?.State?.Terminated != null;
        }

        /// <summary>
        /// Check if a given pod has started, and throw if the pod has failed.
        /// </summary>
        /// <remarks>
        /// This currently just checks if the container is in a "running" state.
        /// </remarks>
        private bool HasStarted(V1Pod pod)
        {
            string podName = pod.Name();
            string containerName = GetContainerName(podName);
            V1ContainerStatus status = client.GetContainerStatus(pod, containerName);
            if (status == null)
                return false;
            V1ContainerState state = status.State;

            if (state.Terminated != null)
            {
                string log = GetLog(jobNamespace, podName, containerName);
                throw new Exception($"Pod {pod.Name()} has failed:\n{log}");
            }
            bool started = state.Running != null;
            Console.WriteLine($"Container {containerName} in pod {podName} has {(started ? "" : "not ")}started.");
            return started;
        }

        /// <summary>
        /// Write a log message (ie to stdout).
        /// </summary>
        /// <remarks>
        /// Current implementation is to write to stdout iff verbose option is enabled.
        /// </remarks>
        /// <param name="message">Message to be written.</param>
        protected void WriteToLog(string message, Verbosity verbosity)
        {
            if (options.Verbose)
                Console.WriteLine(message);
        }

        /// <summary>
        /// Read console output from a particular container in a pod.
        /// </summary>
        /// <param name="podNamespace">Namespace of the pod.</param>
        /// <param name="podName">Pod name.</param>
        /// <param name="containerName">Container name.</param>
        /// <returns></returns>
        private string GetLog(string podNamespace, string podName, string containerName)
        {
            try
            {
                using (Stream logStream = client.ReadNamespacedPodLog(podName, podNamespace, containerName))
                    using (StreamReader reader = new StreamReader(logStream))
                        return reader.ReadToEnd();
            }
            catch (HttpOperationException)
            {
                throw;
            }
        }

        /// <summary>
        /// Get the name of the apsim-server container running in a given pod.
        /// </summary>
        /// <param name="podName">Name of the pod.</param>
        private string GetContainerName(string podName)
        {
            return $"{podName}-container";
        }

        /// <summary>
        /// Create the namespace in which the job will be run, and add it
        /// to the cluster.
        /// </summary>
        private void CreateNamespace()
        {
            WriteToLog($"Creating namespace {jobNamespace}...", Verbosity.Information);
            try
            {
                client.CreateNamespace(CreateNamespaceTemplate());
            }
            catch (Exception err)
            {
                throw new BootstrapperException("Unable to create namespace", err);
            }
        }

        /// <summary>
        /// This instantiates a (k8s) namespace object with our desired labels,
        /// but doesn't actually create the namespace in the cluster.
        /// </summary>
        private V1Namespace CreateNamespaceTemplate()
        {
            // https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
            Dictionary<string, string> labels = new Dictionary<string, string>();
            labels["app.kubernetes.io/name"] = appName;
            labels["app.kubernetes.io/instance"] = instanceName;
            labels["app.kubernetes.io/version"] = version;
            labels["app.kubernetes.io/component"] = component;
            labels["app.kubernetes.io/part-of"] = partOf;
            labels["app.kubernetes.io/managed-by"] = managedBy;
            labels["app.kubernetes.io/created-by"] = "drew";

            V1ObjectMeta namespaceMetadata = new V1ObjectMeta(name: jobNamespace, labels: labels);
            return new V1Namespace(apiVersion, "Namespace", namespaceMetadata);
        }

        /// <summary>
        /// Create the necessary service account for the job manager.
        /// </summary>
        private void CreateServiceAccount()
        {
            WriteToLog($"Creating service account {serviceAccountName}...", Verbosity.Information);
            try
            {
                client.CreateNamespacedServiceAccount(CreateServiceAccountTemplate(), jobNamespace);
            }
            catch (Exception err)
            {
                throw new BootstrapperException("Unable to create service account", err);
            }
        }

        /// <summary>
        /// This instantiates a service account object with our desired settings,
        /// but doesn't actually create it (with kubernetes API).
        /// </summary>
        private V1ServiceAccount CreateServiceAccountTemplate()
        {
            return new V1ServiceAccount(
                apiVersion,
                metadata: new V1ObjectMeta(name: serviceAccountName, namespaceProperty: jobNamespace)
            );
        }

        /// <summary>
        /// Create the kubernetes role to be used by the job manager's service account.
        /// </summary>
        private void CreateRole()
        {
            WriteToLog($"Creating role {roleName}...", Verbosity.Information);
            try
            {
                client.CreateNamespacedRole(CreateRoleTemplate(), jobNamespace);
            }
            catch (Exception err)
            {
                throw new BootstrapperException("Unable to create job manager role", err);
            }
        }

        /// <summary>
        /// This instantiates a role object with our desired settings, but
        /// doesn't actually add it to the cluster.
        /// </summary>
        /// <remarks>
        /// https://kubernetes.io/docs/reference/access-authn-authz/rbac/
        /// </remarks>
        private V1Role CreateRoleTemplate()
        {
            V1PolicyRule rule = new V1PolicyRule(
                jobManagerPermissions,
                apiGroups: new[] { "" }, // "" indicates the core API group.
                resources: jobManagerResources
            );
            return new V1Role(
                "rbac.authorization.k8s.io/v1",
                metadata: new V1ObjectMeta(name: roleName, namespaceProperty: jobNamespace),
                rules: new[] { rule }
            );
        }

        /// <summary>
        /// Create the necessary role binding for the job manager
        /// and add it to the cluster.
        /// </summary>
        private void CreateRoleBinding()
        {
            WriteToLog("Creating role binding...", Verbosity.Information);
            try
            {
                client.CreateNamespacedRoleBinding(CreateRoleBindingTemplate(), namespaceParameter: jobNamespace);
            }
            catch (Exception err)
            {
                throw new BootstrapperException("Unable to create role binding", err);
            }
        }

        /// <summary>
        /// This instantiates ta role binding object linking our role to our
        /// service account, but it doesn't actually add the role binding
        /// to the cluster.
        /// </summary>
        private V1RoleBinding CreateRoleBindingTemplate()
        {
            return new V1RoleBinding(
                new V1RoleRef(
                    apiGroup: "rbac.authorization.k8s.io",
                    kind: "Role",
                    name: roleName
                ),
                apiVersion: "rbac.authorization.k8s.io/v1",
                metadata: new V1ObjectMeta(name: roleName),
                subjects: new[] { new V1Subject(
                    kind: "ServiceAccount",
                    name: serviceAccountName,
                    namespaceProperty: jobNamespace
                )}
            );
        }

        /// <summary>
        /// Create and launch the job manager pod, and return the created
        /// pod object.
        /// </summary>
        private void CreateJobManager()
        {
            try
            {
                WriteToLog($"Creating job manager pod...", Verbosity.Information);
                client.CreateNamespacedPod(CreateJobManagerPodTemplate(), jobNamespace);
            }
            catch (Exception err)
            {
                throw new BootstrapperException("Unable to create job manager pod", err);
            }
        }

        /// <summary>
        /// Instantiate a pod object which will run a single container (the job manager).
        /// This function does not launch the pod with the kubernetes API.
        /// </summary>
        private V1Pod CreateJobManagerPodTemplate()
        {
            V1ObjectMeta metadata = new V1ObjectMeta(name: jobManagerPodName);
            metadata.Labels = new Dictionary<string, string>();
            metadata.Labels["name"] = jobManagerPodName;
            metadata.Labels[podTypeLabelName] = jobManagerPodType;

            string jobNamespace = Guid.NewGuid().ToString();
            // Get the effective file path of the input file in its mounted location
            // (ie as the job manager container will see it).
            string actualInputFile = Path.Combine(jobManagerInputsPath, Path.GetFileName(options.InputFile));
            V1PodSpec spec = new V1PodSpec(
                containers: new[] { CreateJobManagerContainer(actualInputFile, options.CpuCount) },
                serviceAccountName: serviceAccountName,
                affinity: CreateJobManagerAffinity(),
                restartPolicy: "Never" // If the job manager falls over for some reason, restart it.
                // In theory, it doesn't really have any longterm state so this should be fine.
            );
            return new V1Pod(apiVersion, "Pod", metadata, spec);
        }

        /// <summary>
        /// Create the job manager container template.
        /// </summary>
        /// <param name="inputFile">The path to the input file on which the job manager should be run.</param>
        /// <param name="cpuCount">The number of vCPUs to which the container should have access.</param>
        private V1Container CreateJobManagerContainer(string inputFile, uint cpuCount)
        {
            string cpuString = cpuCount.ToString(CultureInfo.InvariantCulture);
            containerArgs = new string[] { "-c", $"until [ -f /start ]; do sleep 1; done; echo File upload complete, starting job manager && /apsim/apsim-server relay --in-pod -vkrnc {cpuString} --namespace {jobNamespace} -f {inputFile} -p {jobManagerPortNo}" };
            return new V1Container(
                jobManagerContainerName,
                image: imageName,
                command: new[] { containerEntrypoint },
                args: containerArgs,
                imagePullPolicy: "Always",
                readinessProbe: CreateReadinessProbe(jobManagerPortNo)
            );
        }

        public IEnumerable<IPEndPoint> CreateWorkers(ushort numWorkers, string inputFile)
        {
            if (workerPodPortNo + numWorkers > ushort.MaxValue)
                throw new NotSupportedException("TBI: that's a lot of workers. You'll need to modify the code and pick a lower starting port number");

            if (string.IsNullOrEmpty(jobNamespace))
                throw new InvalidOperationException("Job namespace has not been created. Missing call to Initialise()?");

            string inputsPath = Path.GetDirectoryName(inputFile);

            // Get a list of support files (.met files, .xlsx, ...). These are all
            // other input files required by the .apsimx file. They are just the
            // sibling files of the input file.
            IEnumerable<string> files = Directory.EnumerateFiles(inputsPath, "*", SearchOption.AllDirectories);

            // Don't copy certain file types (.db, .bak, ...).
            files = files.Where(f => !doNotCopy.Contains(Path.GetExtension(f))).ToArray();

            IDictionary<string, ushort> workerPorts = new Dictionary<string, ushort>();
            for (ushort i = 0; i < numWorkers; i++)
            {
                ushort port = (ushort)(workerPodPortNo + i);
                string podName = $"worker-{i}";
                CreateWorkerPod(inputFile, podName, port);
                workerPorts[podName] = port;
            }

            // Wait for the pods to start.
            List<IPEndPoint> workers = new List<IPEndPoint>(numWorkers);
            ParallelOptions options = new ParallelOptions();
            options.MaxDegreeOfParallelism = Environment.ProcessorCount;
            Parallel.ForEach(workerPorts, options, pair =>
            {
                string podName = pair.Key;
                ushort port = pair.Value;

                CancellationTokenSource cts = new CancellationTokenSource(60 * 1000);
                WriteToLog($"Waiting for {podName} to start...", Verbosity.Information);
                WaitFor(podName, HasStarted, cts);

                // After each pod has started, copy the input files into that pod
                // and send the start signal so it will start listening.
                SendFilesToPod(podName, files, workerInputsPath);
                SendStartSignalToPod(podName);

                string ipAddress = GetPodIPAddress(podName);
                workers.Add(new IPEndPoint(IPAddress.Parse(ipAddress), port));
            });

            foreach ((string podName, _) in workerPorts)
            {
                CancellationTokenSource cts = new CancellationTokenSource(60 * 1000);
                WriteToLog($"Waiting for {podName} to become ready...", Verbosity.Information);
                WaitFor(podName, IsReady, cts);
            }

            return workers;
        }

        /// <summary>
        /// Initialise the worker nodes.
        /// </summary>
        /// <param name="client">The kubernetes client to be used.</param>
        private void CreateWorkers()
        {
            WriteToLog("Initialising workers...", Verbosity.Information);

            // Split apsimx file into smaller chunks.
            string inputsPath = Path.GetDirectoryName(options.InputFile);

            // Get a list of support files (.met files, .xlsx, ...). These are all
            // other input files required by the .apsimx file. They are just the
            // sibling files of the input file.
            IEnumerable<string> supportFiles = Directory.EnumerateFiles(inputsPath, "*", SearchOption.AllDirectories).Except(new[] { options.InputFile });
            supportFiles = supportFiles.Where(f => !doNotCopy.Contains(Path.GetExtension(f)));
            string tempDir = Path.Combine(Path.GetTempPath(), $"apsim-cluster-job-chunked-inputs-{jobNamespace}");
            Directory.CreateDirectory(tempDir);
            IEnumerable<string> generatedFiles = SplitApsimXFile(options.InputFile, chunkSize, tempDir); // todo : extact this to be a user parameter
            if (generatedFiles.Any())
            {
                int n = generatedFiles.Count();
                WriteToLog($"Split input file into {n} chunk{(n == 1 ? "" : "s")}.", Verbosity.Information);
            }
            else
                throw new InvalidOperationException($"Input file {options.InputFile} contains no simulations.");

            WriteToLog("Launching pods...", Verbosity.Information);

            // Create pod templates.
            uint i = 0;
            Dictionary<string, string> pods = new Dictionary<string, string>();
            foreach (string file in generatedFiles)
            {
                string podName = $"worker-{i++}";
                pods.Add(podName, file);
                CreateWorkerPod(file, podName, workerPodPortNo);
            }
            workers = pods.Select(k => k.Key).ToArray();
            WriteToLog($"Successfully created {pods.Count} pod{(pods.Count == 1 ? "" : "s")}.", Verbosity.Information);

            // Wait for worker containers to launch.
            WriteToLog($"Waiting for pods to start...", Verbosity.Information);
            CancellationTokenSource cts = new CancellationTokenSource(10 * 1000);
            foreach (string worker in workers)
                WaitFor(worker, HasStarted, cts);

            // Send input files to pods.
            // The keys here are the pod names
            // The values are the pod's input file.
            // todo: create a struct to hold this?
            WriteToLog("Sending input files to pods...", Verbosity.Information);
            foreach (KeyValuePair<string, string> pod in pods)
            {
                SendFilesToPod(pod.Key, supportFiles.Append(pod.Value), workerInputsPath);
                SendStartSignalToPod(pod.Key);
            }

            // Delete temp files.
            Directory.Delete(tempDir, true);
        }

        /// <summary>
        /// Split an .apsimx file into smaller chunks, of the given size.
        /// </summary>
        /// <param name="inputFile">The input .apsimx file.</param>
        /// <param name="simsPerFile">The number of simulations to add to each generated file.</param>
        private static IEnumerable<string> SplitApsimXFile(string inputFile, uint simsPerFile, string outputPath)
        {
            var files = GenerateApsimXFiles.SplitFile(inputFile, simsPerFile, outputPath, p => Console.Write($"Chunking input file {inputFile}: {(100.0 * p):f2}%\r"), true);
            Console.WriteLine();
            return files;
        }

        /// <summary>
        /// Create and launch a worker pod which runs on the given input file.
        /// </summary>
        /// <param name="file">Input file for the pod.</param>
        /// <param name="podName">Name of the worker pod.</param>
        private void CreateWorkerPod(string file, string podName, uint portNo)
        {
            V1Pod template = CreateWorkerPodTemplate(file, podName, portNo);
            client.CreateNamespacedPod(template, jobNamespace);
            WriteToLog($"Successfully launched pod {podName}.", Verbosity.Information);
        }

        /// <summary>
        /// Create a pod for running the given file.
        /// </summary>
        /// <param name="file">The .apsimx file which the pod should run.</param>
        /// <param name="supportFiles">Other misc input files (e.g. met file) which are required to run the main .apsimx file.</param>
        /// <param name="workerCpuCount">The number of vCPUs for the worker container in the pod. This should probably be equal to the number of simulations in the .apsimx file.</param>
        private V1Pod CreateWorkerPodTemplate(string file, string podName, uint portNo)
        {
            // todo: pod labels
            V1ObjectMeta metadata = new V1ObjectMeta(name: podName);
            metadata.Labels = new Dictionary<string, string>();
            metadata.Labels[podTypeLabelName] = workerPodType;

            V1PodSpec spec = new V1PodSpec()
            {
                Containers = new[] { CreateWorkerContainerTemplate(GetContainerName(podName), file, portNo) },
                RestartPolicy = "Never",
                Affinity = CreateWorkerAffinity(),
                TopologySpreadConstraints = new V1TopologySpreadConstraint[]
                {
                    EvenlyDistributeWorkers()
                }
            };
            return new V1Pod(apiVersion, metadata: metadata, spec: spec);
        }

        /// <summary>
        /// Create a pod topology spread constraint which ensures that worker pods
        /// scheduled as evenly as possible across the available worker nodes.
        /// 
        /// That is, that any worker node has at most 1 more or 1 less worker pod
        /// than any other worker node.
        /// </summary>
        private V1TopologySpreadConstraint EvenlyDistributeWorkers()
        {
            return new V1TopologySpreadConstraint()
            {
                MaxSkew = 1,
                TopologyKey = kubernetesNodeNameLabel,
                // If the constraint cannot be scheduled, the pod will be scheduled
                // anyway, and the scheduler will give higher priority to nodes
                // which would help reduce the skew/spread.
                WhenUnsatisfiable = scheduleAnyway,
                LabelSelector = WorkerPods()
            };
        }

        /// <summary>
        /// Create an affinity which prevents a pod from being scheduled
        /// on one of the cluster 'master' nodes (if in an openstack cluster),
        /// and also prevents the pod from being scheduled on any node which is already
        /// running a worker, job manager, or optimiser pod, to ensure that the pods
        /// aren't competing for resources.
        /// </summary>
        private V1Affinity CreateWorkerAffinity()
        {
            return new V1Affinity()
            {
                NodeAffinity = new V1NodeAffinity()
                {
                    RequiredDuringSchedulingIgnoredDuringExecution = new V1NodeSelector()
                    {
                        NodeSelectorTerms = new[]
                        {
                            AvoidMasterNodes(),
                            AvoidJobManagerNodes(true),
                            PreferWorkerNodes(true),
                        }
                    },
                },
                // Don't need this anymore - we're using a spread topology constraint instead.
                // PodAntiAffinity = CreatePodAntiAffinity(workerPodType, jobManagerPodType, optimiserPodType)
            };
        }

        /// <summary>
        /// Create an affinity to be used for a job manager pod. This affinity will
        /// prevent the pod from being scheduled on a master node, and will cause
        /// the pod scheduler to "prefer" that the pod be run on a job manager pod
        /// (if any exist).
        /// </summary>
        private V1Affinity CreateJobManagerAffinity()
        {
            return new V1Affinity()
            {
                NodeAffinity = new V1NodeAffinity()
                {
                    RequiredDuringSchedulingIgnoredDuringExecution = new V1NodeSelector()
                    {
                        NodeSelectorTerms = new[]
                        {
                            AvoidMasterNodes(),
                        }
                    },
                    PreferredDuringSchedulingIgnoredDuringExecution = new[]
                    {
                        new V1PreferredSchedulingTerm(AvoidJobManagerNodes(false), 100)
                    }
                },
                PodAntiAffinity = CreatePodAntiAffinity(workerPodType, jobManagerPodType, optimiserPodType)
            };
        }

        /// <summary>
        /// Create a node selector which prefers worker nodes (or avoids
        /// worker nodes if prefer is set to false).
        /// </summary>
        /// <param name="prefer">Prefer (T) or avoid (F) worker nodes?</param>
        private V1NodeSelectorTerm PreferWorkerNodes(bool prefer)
        {
            return new V1NodeSelectorTerm()
            {
                MatchExpressions = new[]
                {
                    new V1NodeSelectorRequirement()
                    {
                        Key = openStackNodeRoleLabelName,
                        OperatorProperty = notIn,
                        Values = new[] { openStackWorkerNodeRoleName }
                    }
                }
            };
        }

        /// <summary>
        /// Create a node affinity which will either avoid or prefer all master nodes
        /// on magnum (openstack) clusters.
        /// </summary>
        /// <remarks>
        /// This is only applicable for openstack cluters (ie Nectar), but it will
        /// have no effect (good or bad) on other clusters, as the label we check for
        /// should not exist on any nodes on other clusters.
        /// </remarks>
        private V1NodeSelectorTerm AvoidMasterNodes()
        {
            return new V1NodeSelectorTerm()
            {
                MatchExpressions = new[]
                {
                    new V1NodeSelectorRequirement()
                    {
                        Key = openStackNodeRoleLabelName,
                        OperatorProperty = notIn,
                        Values = new[] { openStackMasterNodeRole }
                    }
                }
            };
        }

        /// <summary>
        /// Create a node selector which either prefers or avoids job manager nodes.
        /// </summary>
        /// <param name="avoid">
        /// If set to true, the resultant affinity will prefer nodes which /are not/ master
        /// nodes. If set to false, the resultant affinity will prefer nodes which /are/
        /// master nodes.
        /// </param>
        private V1NodeSelectorTerm AvoidJobManagerNodes(bool avoid)
        {
            return new V1NodeSelectorTerm()
            {
                MatchExpressions = new[]
                {
                    new V1NodeSelectorRequirement()
                    {
                        Key = openStackNodeRoleLabelName,
                        OperatorProperty = avoid ? notIn : @in,
                        Values = new[] { openStackJobManagerNodeRole }
                    }
                }
            };
        }

        /// <summary>
        /// Create a pod affinity which prevents a pod from being run on a node
        /// if any pods of the given pod types are already running on that node.
        /// </summary>
        /// <param name="podTypes">The pod types to avoid.</param>
        /// <remarks>
        /// https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#inter-pod-affinity-and-anti-affinity
        /// </remarks>
        private V1PodAntiAffinity CreatePodAntiAffinity(params string[] podTypes)
        {
            return new V1PodAntiAffinity()
            {
                RequiredDuringSchedulingIgnoredDuringExecution = new[]
                {
                    new V1PodAffinityTerm()
                    {
                        LabelSelector = new V1LabelSelector()
                        {
                            MatchExpressions = new[]
                            {
                                new V1LabelSelectorRequirement()
                                {
                                    Key = podTypeLabelName,
                                    OperatorProperty = @in,
                                    Values = podTypes
                                }
                            }
                        },
                        TopologyKey = kubernetesNodeNameLabel
                    }
                }
            };
        }

        private V1LabelSelector WorkerPods()
        {
            return new V1LabelSelector()
            {
                MatchExpressions = new[]
                {
                    new V1LabelSelectorRequirement()
                    {
                        Key = podTypeLabelName,
                        OperatorProperty = @in,
                        Values = new[] { workerPodType }
                    }
                }
            };
        }

        /// <summary>
        /// Create a container template for a worker node.
        /// </summary>
        /// <param name="name">Display name for the container.</parama>
        /// <param name="file">The input file on which the container should run.</param>
        private V1Container CreateWorkerContainerTemplate(string name, string file, uint portNo)
        {
            string fileName = Path.GetFileName(file);
            // fixme - this is hacky and nasty
            string[] args = new[]
            {
                "-c",
                // todo: extract port # to user param
                // todo: use pod's internal IP address rather than 0.0.0.0
                $"mkdir -p {workerInputsPath} && until [ -f {containerStartFile} ]; do sleep 1; done; echo File upload complete, starting server && /apsim/apsim-server listen -vkrmf {workerInputsPath}/{fileName} -a 0.0.0.0 -p {portNo}"
            };

            return new V1Container(
                name,
                image: imageName,
                command: new string[] { "/bin/sh" },
                args: args,
                imagePullPolicy: ifNotPresent,
                readinessProbe: CreateReadinessProbe(portNo)
            );
        }

        /// <summary>
        /// Create a probe which will check if a container is listening
        /// for TCP connections on the given port number.
        /// </summary>
        /// <param name="port">Port number to check.</param>
        private V1Probe CreateReadinessProbe(uint port)
        {
            return new V1Probe()
            {
                // The probe succeeds once the container starts listening on the specified port.
                TcpSocket = new V1TCPSocketAction(port.ToString(CultureInfo.InvariantCulture)),

                // Use a failure threshold of one. If the container goes dark,
                // it won't be going back online.
                FailureThreshold = 1,

                // Initial delay before startup probes should be fairly low.
                InitialDelaySeconds = 1,

                // PeriodSeconds should probably be proportional to # of worker nodes.
                PeriodSeconds = 3
            };
        }

        /// <summary>
        /// Send the given files to the pod at the specified location.
        /// </summary>
        /// <param name="podName">Name of the pod into which files will be copied.</param>
        /// <param name="files">Files to be copied into the pod.</param>
        /// <param name="destinationDirectory">Directory on the pod into which the files will be copied.</param>
        private void SendFilesToPod(string podName, IEnumerable<string> files, string destinationDirectory)
        {
            foreach (string file in files)
            {
                WriteToLog($"Sending {file} to {podName}...", Verbosity.Information);
                V1Pod pod = GetPod(podName);
                string destination = $"{destinationDirectory}/{Path.GetFileName(file)}";
                client.CopyFileToPod(pod, GetContainerName(podName), file, destination);
            }
        }

        /// <summary>
        /// Get the state of the given worker pod. Can return null (e.g. if the pod is in the "Pending" phase.)
        /// </summary>
        /// <param name="podName">Name of the pod.</param>
        private V1ContainerState GetPodState(string podName)
        {
            V1Pod pod = GetPod(podName);
            string container = GetContainerName(podName);
            return pod.Status.ContainerStatuses?.FirstOrDefault(c => c.Name == container)?.State;
        }

        /// <summary>
        /// Get the worker pod with the given name.
        /// </summary>
        /// <param name="podName">Name of the worker pod.</param>
        private V1Pod GetPod(string podName)
        {
            try
            {
                return client.ReadNamespacedPod(podName, jobNamespace);
            }
            catch (System.Net.Http.HttpRequestException)
            {
                int numTries = 3;
                for (uint i = 0; i < numTries; i++)
                {
                    try
                    {
                        return client.ReadNamespacedPod(podName, jobNamespace);
                    }
                    catch
                    {
                    }
                }
                throw;
            }
        }

        /// <summary>
        /// Send the "start" signal to the pod.
        /// </summary>
        /// <remarks>
        /// When we start a worker pod, it goes into a busy wait until we
        /// copy the input files into the pod. This function tells the pod
        /// to end its busy wait (presumably because the input files have)
        /// already been copied into the pod.
        /// </remarks>
        /// <param name="podName">Name of the pod.</param>
        private void SendStartSignalToPod(string podName)
        {
            // fixme
            WriteToLog($"Sending start signal to {podName}...", Verbosity.Information);
            string file = Path.GetTempFileName();
            using (File.Create(file)) { }
            V1Pod pod = GetPod(podName);
            string container = GetContainerName(podName);
            client.CopyFileToPod(pod, container, file, containerStartFile);
            // ExecAsyncCallback action = (_, __, ___) => Task.CompletedTask;
            // string container = GetContainerName(podName);
            // string[] cmd = new[] { "touch", containerStartFile };
            // CancellationToken token = new CancellationTokenSource().Token;
            // client.NamespacedPodExecAsync(podName, podNamespace, container, cmd, false, action, token);
        }
    }
}
