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

namespace APSIM.Bootstrapper
{
    public class Bootstrapper
    {
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
        /// Port on which the job manager listens for connections.
        /// </summary>
        /// <remarks>
        /// fixme: this could be a user parameter as it could conflict with other services???
        /// </remarks>
        private const uint jobManagerPortNo = 27746;

        /// <summary>
        /// Port on which the worker nodes listen for connections.
        /// </summary>
        private const uint workerNodePortNo = 27746;

        /// <summary>
        /// Name of the service which is created to handle port forwarding to the job manager.
        /// </summary>
        private const string portForwardingServiceName = "job-manager-port-forwarding";

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

        /// <summary>
        /// Run the bootstrapper - initialise the cluster environment.
        /// </summary>
        internal void Initialise()
        {
            // Create the namespace in which the job manager pod will run.
            CreateNamespace();
#if DEBUG
            // WriteToLog($"kubectl exec --stdin --tty -n {jobNamespace} {jobManagerPodName} -- /bin/bash");
            WriteToLog($"export namespace={jobNamespace}");
#endif
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

                // Wait up to 10 seconds for the pods to start.
                WaitFor(jobManagerPodName, IsReady, 10);
                WaitForPodsToStart(workers.Append(jobManagerPodName), 10 * 1000);

                // Copy the input files into the job manager pod.
                // The job manager will wait until this is complete before it
                // launches the relay server.
                SendStartSignalToPod(jobManagerPodName);

                // Initiate port forwarding to the job manager pod.
                // InitiatePortForwarding();

                // Monitor the job manager pod for 10 seconds to ensure that it is healthy.
                int seconds = 10;
                VerifyPodHealth(jobManagerPodName, seconds * 1000);

                // Finally, verify that the pod has logged some output.
                // (It should log output as soon as it receives the start signal.)
                EnsurePodHasWrittenOutput(jobManagerPodName);
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

        private bool IsReady(V1Pod pod)
        {
            string podName = pod.Name();
            string containerName = GetContainerName(podName);
            V1ContainerState state = client.GetContainerState(pod, containerName);
            if (state.Terminated != null)
            {
                string log = GetLog(jobNamespace, podName, containerName);
                throw new Exception($"Pod {pod.Name()} has failed:\n{log}");
            }
            return state.Running != null;
        }

        /// <summary>
        /// Issue a RUN command to the job manager.
        /// </summary>
        /// <param name="command">Command to be sent.</param>
        public void RunWithChanges(RunCommand command)
        {
            WriteToLog($"Executing {command}");

            var stopwatch = Stopwatch.StartNew();

            // Create a socket server for comms to the job manager pod.
            using (PortForwardingService service = new PortForwardingService(client))
            {
                // Start the socket server.
                WriteToLog($"Initiating port forwarding to job manager...");
                int port = service.Start(jobManagerPodName, jobNamespace, (int)jobManagerPortNo);

                // fixme: IPAddress.Loopback
                string ip = "127.0.0.1";

                WriteToLog($"Initiating connection to job manager...");
                using (var conn = new NetworkSocketClient(options.Verbose, ip, (uint)port, Protocol.Managed))
                {
                    WriteToLog($"Successfully established connection to job manager. Running command...");

                    // Note - SendCommand will wait for the command to finish.
                    conn.SendCommand(command);

                    WriteToLog($"Command executed successfully. Disconnecting...");
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
            WriteToLog($"Executing {command}");

            var stopwatch = Stopwatch.StartNew();

            // Create a socket server for comms to the job manager pod.
            using (PortForwardingService service = new PortForwardingService(client))
            {
                // Start the socket server.
                WriteToLog($"Initiating port forwarding to job manager...");
                uint socketServerPort = (uint)service.Start(jobManagerPodName, jobNamespace, (int)jobManagerPortNo);

                string ip = "127.0.0.1"; // IPAddress.Loopback
                WriteToLog($"Initiating connection to job manager...");
                using (var conn = new NetworkSocketClient(options.Verbose, ip, socketServerPort, Protocol.Managed))
                {
                    WriteToLog($"Successfully established connection to job manager. Running command...");

                    // Note - SendCommand will wait for the command to finish.
                    DataTable result = conn.ReadOutput(command);

                    stopwatch.Stop();
                    WriteToLog($"Command executed successfully in {stopwatch.ElapsedMilliseconds}ms. Disconnecting...");

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
        /// Monitor the specified pods until either they start (status changes to "Running"),
        /// or the given amount of time passes.
        /// </summary>
        /// <param name="pods">The pods to monitor.</param>
        /// <param name="timeout">Timeout period in milliseconds</param>
        private void WaitForPodsToStart(IEnumerable<string> pods, int timeout)
        {
            var stopwatch = Stopwatch.StartNew();
            List<string> podsToCheck = new List<string>(workers.Append(jobManagerPodName));
            while (stopwatch.ElapsedMilliseconds < timeout && podsToCheck.Any())
            {
                foreach (string podName in podsToCheck)
                {
                    V1Pod pod = GetPod(podName);
                    V1ContainerState state = pod.Status.ContainerStatuses.FirstOrDefault(c => c.Name == GetContainerName(podName)).State;
                    if (state.Running != null || state.Terminated != null)
                    {
                        // This pod has started successfully. We can stop monitoring it.
                        podsToCheck.Remove(podName);

                        // (fixme - collection has been modified)
                        break; // (out of the for loop)
                    }

                    if (state.Waiting != null && state.Waiting.Reason != "ContainerCreating")
                        throw new Exception($"Job manager pod failed to start. Reason={state.Waiting.Reason}. Message: {state.Waiting.Message}");
                }
                
            }
        }

        /// <summary>
        /// Monitor a pod until a condition is met, or until a certain period of time has elapsed.
        /// </summary>
        /// <param name="podName">Name of the pod to be monitored.</param>
        /// <param name="condition">Condition - when this returns true, monitoring will cease.</param>
        /// <param name="maxTime">Max amount of time to wait (in seconds).</param>
        private void WaitFor(string podName, Func<V1Pod, bool> condition, int maxTime)
        {
            try
            {
                CancellationTokenSource cts = new CancellationTokenSource(maxTime * 1000);
                WaitForAsync(podName, condition, cts.Token).Wait(cts.Token);
            }
            catch (OperationCanceledException)
            {
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
            Action<Exception> errorHandler = e => throw e;
            Action closureHandler = () => throw new Exception($"Connection closed unexpectedly while watching pod {podName}");

            // Start monitoring the pod.
            using (Watcher<V1Pod> watcher = await client.WatchNamespacedPodAsync(podName, jobNamespace, onEvent: onEvent, onError: errorHandler, onClosed: closureHandler, cancellationToken: cancelToken))
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
        }

        /// <summary>
        /// Create the namespace in which the job will be run, and add it
        /// to the cluster.
        /// </summary>
        private void CreateNamespace()
        {
            WriteToLog($"Creating namespace {jobNamespace}...");
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
            WriteToLog($"Creating service account {serviceAccountName}...");
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
            WriteToLog($"Creating role {roleName}...");
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
            WriteToLog("Creating role binding...");
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
                WriteToLog($"Creating job manager pod...");
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
            string jobNamespace = Guid.NewGuid().ToString();
            // Get the effective file path of the input file in its mounted location
            // (ie as the job manager container will see it).
            string actualInputFile = Path.Combine(jobManagerInputsPath, Path.GetFileName(options.InputFile));
            V1PodSpec spec = new V1PodSpec(
                containers: new[] { CreateJobManagerContainer(actualInputFile, options.CpuCount) },
                serviceAccountName: serviceAccountName,
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
            containerArgs = new string[] { "-c", $"until [ -f /start ]; do sleep 1; done; echo File upload complete, starting job manager && /apsim/apsim-server relay --in-pod -vkrmc {cpuString} --namespace {jobNamespace} -f {inputFile} -p {jobManagerPortNo}" };
            return new V1Container(
                jobManagerContainerName,
                image: imageName,
                command: new[] { containerEntrypoint },
                args: containerArgs,
                imagePullPolicy: "Always"
            );
        }

        /// <summary>
        /// This function will monitor the a particular pod's health for the specified
        /// period of time. An exception will be thrown if the pod fails to start.
        /// </summary>
        private void VerifyPodHealth(string pod, int duration)
        {
            string containerName = GetContainerName(pod);
            WriteToLog("Verifying pod health...");
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.ElapsedMilliseconds < duration)
            {
                // Check the job manager container's state. If it's fallen over, then
                // we'll report the error immediately and clean up.
                V1ContainerState state = GetPod(pod).Status.ContainerStatuses.FirstOrDefault(c => c.Name == containerName).LastState;
                if (state.Terminated != null)
                {
                    // todo: check terminated reason
                    // Get console output from the container.
                    string containerLog = GetLog(jobNamespace, jobManagerPodName, containerName);
                    string entrypoint = $"{containerEntrypoint} {string.Join(" ", containerArgs)}";
                    throw new Exception($"Job manager pod failed to start (state = {state.Terminated.Reason}).\nContainer command:\n{entrypoint}\nContainer log:\n{containerLog}");
                }
            }

            // Once it receives the input files, the pod should print a message to stdout.
            // If this hasn't happened by now, something has gone wrong.
            // if (string.IsNullOrEmpty(GetLog(jobNamespace, jobManagerPodName, jobManagerContainerName)))
            //     throw new Exception("Job manager pod did not receive start signal");
        }

        /// <summary>
        /// Write a log message (ie to stdout).
        /// </summary>
        /// <remarks>
        /// Current implementation is to write to stdout iff verbose option is enabled.
        /// </remarks>
        /// <param name="message">Message to be written.</param>
        private void WriteToLog(string message)
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
            using (Stream logStream = client.ReadNamespacedPodLog(podName, podNamespace, containerName))
                using (StreamReader reader = new StreamReader(logStream))
                    return reader.ReadToEnd();
        }

        /// <summary>
        /// Initialise the worker nodes.
        /// </summary>
        /// <param name="client">The kubernetes client to be used.</param>
        private void CreateWorkers()
        {
            WriteToLog("Initialising workers...");

            // Split apsimx file into smaller chunks.
            string inputsPath = Path.GetDirectoryName(options.InputFile);

            // Get a list of support files (.met files, .xlsx, ...). These are all
            // other input files required by the .apsimx file. They are just the
            // sibling files of the input file.
            IEnumerable<string> supportFiles = Directory.EnumerateFiles(inputsPath, "*", SearchOption.AllDirectories).Except(new[] { options.InputFile });
            supportFiles = supportFiles.Where(f => !doNotCopy.Contains(Path.GetExtension(f)));
            string tempDir = Path.Combine(Path.GetTempPath(), $"apsim-cluster-job-chunked-inputs-{jobNamespace}");
            Directory.CreateDirectory(tempDir);
            IEnumerable<string> generatedFiles = SplitApsimXFile(options.InputFile, 1, tempDir); // todo : extact this to be a user parameter
            if (generatedFiles.Any())
            {
                int n = generatedFiles.Count();
                WriteToLog($"Split input file into {n} chunk{(n == 1 ? "" : "s")}.");
            }
            else
                throw new InvalidOperationException($"Input file {options.InputFile} contains no simulations.");

            WriteToLog("Launching pods...");

            // Create pod templates.
            uint i = 0;
            Dictionary<string, string> pods = new Dictionary<string, string>();
            foreach (string file in generatedFiles)
            {
                string podName = $"worker-{i++}";
                pods.Add(podName, file);
                CreateWorkerPod(file, podName);
            }
            workers = pods.Select(k => k.Key).ToArray();
            WriteToLog($"Successfully created {pods.Count} pod{(pods.Count == 1 ? "" : "s")}.");

            // Wait for worker containers to launch.
            WaitForWorkersToLaunch(10 * 1000);

            // Send input files to pods.
            // The keys here are the pod names
            // The values are the pod's input file.
            // todo: create a struct to hold this?
            WriteToLog("Sending input files to pods...");
            foreach (KeyValuePair<string, string> pod in pods)
            {
                SendFilesToPod(pod.Key, supportFiles.Append(pod.Value), workerInputsPath);
                SendStartSignalToPod(pod.Key);
            }

            // Monitor the pods for some time.
            WriteToLog($"Monitoring worker pods...");
            int seconds = 10;
            MonitorPods(seconds * 1000);
            EnsureWorkersHaveWrittenOutput();

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
        private void CreateWorkerPod(string file, string podName)
        {
            V1Pod template = CreateWorkerPodTemplate(file, podName);
            client.CreateNamespacedPod(template, jobNamespace);
            WriteToLog($"Successfully launched pod {podName}.");
        }

        /// <summary>
        /// Create a pod for running the given file.
        /// </summary>
        /// <param name="file">The .apsimx file which the pod should run.</param>
        /// <param name="supportFiles">Other misc input files (e.g. met file) which are required to run the main .apsimx file.</param>
        /// <param name="workerCpuCount">The number of vCPUs for the worker container in the pod. This should probably be equal to the number of simulations in the .apsimx file.</param>
        private V1Pod CreateWorkerPodTemplate(string file, string podName)
        {
            // todo: pod labels
            V1ObjectMeta metadata = new V1ObjectMeta(name: podName);
            V1PodSpec spec = new V1PodSpec()
            {
                Containers = new[] { CreateWorkerContainerTemplate(GetContainerName(podName), file) }
            };
            return new V1Pod(apiVersion, metadata: metadata, spec: spec);
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
        /// Create a container template for a worker node.
        /// </summary>
        /// <param name="name">Display name for the container.</parama>
        /// <param name="file">The input file on which the container should run.</param>
        private V1Container CreateWorkerContainerTemplate(string name, string file)
        {
            string fileName = Path.GetFileName(file);
            // fixme - this is hacky and nasty
            string[] args = new[]
            {
                "-c",
                // todo: extract port # to user param
                // todo: use pod's internal IP address rather than 0.0.0.0
                $"mkdir -p {workerInputsPath} && until [ -f {containerStartFile} ]; do sleep 1; done; echo File upload complete, starting server && /apsim/apsim-server listen -vkrmf {workerInputsPath}/{fileName} -a 0.0.0.0 -p {workerNodePortNo}"
            };

            return new V1Container(
                name,
                image: imageName,
                command: new string[] { "/bin/sh" },
                args: args
            );
        }

        private void WaitForWorkersToLaunch(int maxTimePerPod)
        {
            WriteToLog($"Waiting for pods to start...");
            CancellationTokenSource source = new CancellationTokenSource();

            foreach (string worker in workers)
            {
                source.CancelAfter(maxTimePerPod);
                VerifyPodHealth(worker, source.Token);
                WriteToLog($"Pod {worker} is now online and waiting for input files.");
            }
        }

        /// <summary>
        /// Send the given files to the pod at the specified location.
        /// </summary>
        /// <param name="podName">Name of the pod into which files will be copied.</param>
        /// <param name="files">Files to be copied into the pod.</param>
        /// <param name="destinationDirectory">Directory on the pod into which the files will be copied.</param>
        private void SendFilesToPod(string podName, IEnumerable<string> files, string destinationDirectory)
        {
            WriteToLog($"Sending inputs to pod {podName}...");
            foreach (string file in files)
            {
                WriteToLog($"Sending input file {file} to pod {podName}...");
                V1Pod pod = GetPod(podName);
                string destination = $"{destinationDirectory}/{Path.GetFileName(file)}";
                client.CopyFileToPod(pod, GetContainerName(podName), file, destination);
            }
        }

        /// <summary>
        /// This function will monitor the job manager pod's health for the specified
        /// period of time. An exception will be thrown if the pod fails to start.
        /// </summary>
        private void VerifyPodHealth(string podName, CancellationToken cancellationToken)
        {
            WriteToLog("Verifying pod health...");
            while (!cancellationToken.IsCancellationRequested)
                VerifyPodHealth(podName);
        }

        /// <summary>
        /// Check that a pod is healthy. Throw if not.
        /// </summary>
        /// <param name="podName">Name of the pod to check.</param>
        private void VerifyPodHealth(string podName)
        {
            V1ContainerState state = GetPodState(podName);
            if (state == null)
                // todo: test the implications of this. afaik, this only happens when the pod is in "Pending"
                // state, and is perfectly natural.
                return;
            if (state.Running != null)
                return;
            if (state.Terminated != null)
            {
                // Get console output from the container.
                string log = GetLog(jobNamespace, podName, GetContainerName(podName));
                throw new Exception($"Pod {podName} failed to start (Reason = {state.Terminated.Reason}, Message = {state.Terminated.Message}).\nContainer log:\n{log}");
            }
            if (state.Waiting != null && state.Waiting.Reason != "ContainerCreating")
                // todo: verify that this is correct...are there other waiting reasons???
                throw new Exception($"Worker pod {podName} failed to start. Reason={state.Waiting.Reason}. Message={state.Waiting.Message}");
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
            return client.ReadNamespacedPod(podName, jobNamespace);
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
            WriteToLog($"Sending start signal to pod {podName}...");
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

        /// <summary>
        /// Monitor the worker pods for the given duration, and throw
        /// if any of the pods have failed.
        /// </summary>
        /// <param name="duration">Time period for which to montior pods (in ms).</param>
        private void MonitorPods(int duration)
        {
            CancellationTokenSource source = new CancellationTokenSource(duration);
            while (!source.IsCancellationRequested)
                foreach (string worker in workers)
                    VerifyPodHealth(worker);
        }

        private void EnsureWorkersHaveWrittenOutput()
        {
            foreach (string worker in workers)
                EnsurePodHasWrittenOutput(worker);
        }

        private void EnsurePodHasWrittenOutput(string podName)
        {
            string container = GetContainerName(podName);
            string log = GetLog(jobNamespace, podName, container);
            if (string.IsNullOrEmpty(log))
                throw new Exception($"Pod {podName} has not written output after receiving start signal.");
        }
    }
}
