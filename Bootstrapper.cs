using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using k8s;
using k8s.Models;
using Microsoft.Rest;

namespace APSIM.Cluster
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
        private const string imageName = "apsiminitiative/apsimng-server:custom";

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
        /// The verbs given to the job manager's service account's role.
        /// </summary>
        /// <remarks>
        /// (These are essentially the permissions granted to the job manager container.)
        /// </remarks>
        private static readonly IList<string> jobManagerPermissions = new string[5]
        {
            // todo: double check which of these the job manager actually uses
            "get",
            // "watch",
            "list",
            "create",
            "delete",
            "update"
        };

        /// <summary>
        /// Path at which input files will be mounted in the job manager container.
        /// </summary>
        private const string inputsMountPath = "/inputs";

        /// <summary>
        /// Name of the input files volume mount.
        /// </summary>
        private const string inputsVolumeName = "input-files";

        /// <summary>
        /// Name of the role assigned to the job manager's service account.
        /// </summary>
        /// <remarks>
        /// Role name needs only be unique within its namespace.
        /// </remarks>
        private const string roleName = "apsim-role";

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
        /// Create a new <see cref="Bootstrapper"/> instance.
        /// </summary>
        /// <param name="options"></param>
        public Bootstrapper(Options options)
        {
            // var config = KubernetesClientConfiguration.BuildConfigFromConfigFile();
            var config = new KubernetesClientConfiguration { Host = "http://127.0.0.1:8001" };
            client = new Kubernetes(config);
            this.options = options;
            jobID = Guid.NewGuid();
            jobNamespace = $"apsim-cluster-job-{jobID}";
        }

        /// <summary>
        /// Run the bootstrapper - launch the job manager.
        /// </summary>
        internal void Run()
        {
            // Create the namespace in which the job manager pod will run.
            CreateNamespace();
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
                CreatePod();

                // Finally, verify that the pod launched successfully and is healthy.
                VerifyPodHealth();
            }
            catch
            {
                // If we encounter any errors while setting up the job manager pod
                // or its resources, attempt to delete the namespace. Any created
                // resources will be inside this namespace and will be deleted along
                // with the namespace.
                try
                {
                    client.DeleteNamespace(jobNamespace);
                }
                catch (Exception err)
                {
                    // BootstrapperException will show additional debug info.
                    Console.Error.WriteLine(new BootstrapperException("Unable to delete namespace", err).ToString());
                }

                // Rethrow the error.
                throw;
            }
        }

        /// <summary>
        /// Create the namespace in which the job will be run, and add it
        /// to the cluster.
        /// </summary>
        private void CreateNamespace()
        {
            Log($"Creating namespace {jobNamespace}...");
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
            Log($"Creating service account {serviceAccountName}...");
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
            Log($"Creating role {roleName}...");
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
                resources: new[] { "pods" }
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
            Log("Creating role binding...");
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
        private void CreatePod()
        {
            try
            {
                Log($"Creating job manager pod...");
                client.CreateNamespacedPod(CreatePodTemplate(), jobNamespace);
                
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
        private V1Pod CreatePodTemplate()
        {
            V1ObjectMeta metadata = new V1ObjectMeta(name: jobManagerPodName);
            string jobNamespace = Guid.NewGuid().ToString();
            V1Volume volume = InputFilesVolume(options.InputFile);
            // Get the effective file path of the input file in its mounted location
            // (ie as the job manager container will see it).
            string actualInputFile = Path.Combine(inputsMountPath, Path.GetFileName(options.InputFile));
            V1PodSpec spec = new V1PodSpec(
                containers: new[] { CreateJobManagerContainer(actualInputFile, options.CpuCount) },
                volumes: new[] { volume },
                serviceAccountName: serviceAccountName
            );
            return new V1Pod(apiVersion, "Pod", metadata, spec);
        }

        /// <summary>
        /// Create the volume containing the input files used by the job manager container.
        /// </summary>
        /// <param name="file">Name of the input file.</param>
        private V1Volume InputFilesVolume(string file)
        {
            V1HostPathVolumeSource hostVolume = new V1HostPathVolumeSource(
               path: Path.GetDirectoryName(file),
               type: "Directory"
            );
            return new V1Volume(
               name: $"{inputsVolumeName}",
               hostPath: hostVolume
            );
        }

        /// <summary>
        /// Create the job manager container template.
        /// </summary>
        /// <param name="inputFile">The path to the input file on which the job manager should be run.</param>
        /// <param name="cpuCount">The number of vCPUs to which the container should have access.</param>
        private V1Container CreateJobManagerContainer(string inputFile, uint cpuCount)
        {
            string cpuString = cpuCount.ToString(CultureInfo.InvariantCulture);
            containerArgs = new string[] { "-c", $"until [ -f /start ]; do sleep 1; done; echo File upload complete, starting job manager && /apsim/apsim-server relay --in-pod -vnc {cpuString} --namespace {jobNamespace} -f {inputFile}" };
            V1VolumeMount volume = new V1VolumeMount(
                mountPath: inputsMountPath,
                name: inputsVolumeName
            );
            return new V1Container(
                jobManagerContainerName,
                image: imageName,
                command: new[] { containerEntrypoint },
                args: containerArgs,
                // volumeMounts: new[] { volume },
                imagePullPolicy: "Never"
            );
        }

        /// <summary>
        /// Busy wait until the pod gets off the ground and starts running.
        /// </summary>
        private void VerifyPodHealth()
        {
            Log("Verifying pod health...");
#if DEBUG
            // Log($"kubectl exec --stdin --tty -n {jobNamespace} {jobManagerPodName} -- /bin/bash");
            Log($"export namespace={jobNamespace}");
#endif
            // todo: verify that this is the "correct" way to go about things...
            V1ContainerState state;
            V1Pod pod;
            do
            {
                pod = client.ReadNamespacedPod(jobManagerPodName, jobNamespace);

                // Check the job manager container's state. If it's fallen over, then
                // we'll report the error immediately and clean up.
                state = pod.Status.ContainerStatuses.FirstOrDefault(c => c.Name == jobManagerContainerName).LastState;
                if (state.Terminated != null)
                {
                    // Get console output from the container.
                    string containerLog = GetLog(jobNamespace, jobManagerPodName, jobManagerContainerName);
                    string entrypoint = $"{containerEntrypoint} {string.Join(" ", containerArgs)}";
                    throw new Exception($"Job manager pod failed to start (state = {state.Terminated.Reason}).\nContainer command:\n{entrypoint}\nContainer log:\n{containerLog}");
                }
            }
            while (pod.Status.Phase == "Pending" || (state.Terminated == null && state.Running == null && state.Waiting == null));
        }

        /// <summary>
        /// Write a log message (ie to stdout).
        /// </summary>
        /// <remarks>
        /// Current implementation is to write to stdout iff verbose option is enabled.
        /// </remarks>
        /// <param name="message">Message to be written.</param>
        private void Log(string message)
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
            using (Stream logStream = client.ReadNamespacedPodLog(podName, podNamespace, containerName, previous: true))
                using (StreamReader reader = new StreamReader(logStream))
                    return reader.ReadToEnd();
        }
    }
}
