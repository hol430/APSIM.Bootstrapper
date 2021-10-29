using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Tar;
using k8s;
using k8s.Models;

namespace APSIM.Bootstrapper.Extensions
{
    /// <summary>
    /// Hack to use kubectl for file copying.
    /// </summary>
    internal static class KubernetesExtensions
    {
        /*
        public static async Task CopyFileToPod(this Kubernetes client, V1Pod pod, string container, string sourceFilePath, string destinationFilePath, CancellationToken cancellationToken = default(CancellationToken))
        {
            await client.CopyFileToPodAsync(pod.Name(), pod.Namespace(), container, sourceFilePath, destinationFilePath, cancellationToken).ConfigureAwait(false);
        }

        public static async Task CopyFileToPodAsync(this Kubernetes client, string pod, string @namespace, string container, string sourceFilePath, string destinationFilePath, CancellationToken cancellationToken = default(CancellationToken))
        {
            // All other parameters are being validated by MuxedStreamNamespacedPodExecAsync called by NamespacedPodExecAsync
            ValidatePathParameters(sourceFilePath, destinationFilePath);

            string destinationFolder = GetFolderName(destinationFilePath);
            string destinationFile = Path.GetFileName(destinationFilePath);
            // The callback which processes the standard input, standard output and standard error of exec method
            var handler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) =>
            {
                try
                {
                    // If the shell command fails for some reason, we could try
                    // and read from stdout/stderr here, however if the base64 -d
                    // is still running, reading from stdout/stderr will cause a
                    // deadlock (as it's waiting for us to write to stdin).
                    // using (MemoryStream memoryStream = new MemoryStream())
                    // {
                    //     using (var cryptoStream = new CryptoStream(memoryStream, new ToBase64Transform(), CryptoStreamMode.Write, leaveOpen: true))
                    //         await CompressTo(sourceFilePath, cryptoStream, destinationFile).ConfigureAwait(false);
                    //     memoryStream.Seek(0, SeekOrigin.Begin);
                    //     await memoryStream.CopyToAsync(stdIn).ConfigureAwait(false);
                    //     // await CopyInChunks(memoryStream, stdIn, 512).ConfigureAwait(false);
                    // }

                    // using (FileStream instream = File.OpenRead(sourceFilePath))
                    // {
                    //     using (MemoryStream memoryStream = new MemoryStream())
                    //     {
                    //         using (var cryptoStream = new CryptoStream(memoryStream, new ToBase64Transform(), CryptoStreamMode.Write, leaveOpen: true))
                    //         {
                    //             await instream.CopyToAsync(cryptoStream).ConfigureAwait(false);
                    //             // await CopyInChunks(instream, cryptoStream, 1024).ConfigureAwait(false);
                    //         }
                    //         memoryStream.Seek(0, SeekOrigin.Begin);
                    //         await CopyInChunks(memoryStream, stdIn, 512);
                    //         // await memoryStream.CopyToAsync(stdIn).ConfigureAwait(false);
                    //     }
                    // }
                    // await CompressTo(sourceFilePath, stdIn, destinationFile);
                    // stdIn.Close();

                    using (StreamReader reader = new StreamReader(stdOut))
                    {
                        string stdout = reader.ReadToEnd();
                        Console.WriteLine($"stdout={stdout}");
                    }

                    using (StreamReader reader = new StreamReader(stdError))
                    {
                        string stderr = reader.ReadToEnd();
                        Console.WriteLine($"stderr={stderr}");
                    }
                }
                catch (Exception ex)
                {
                    throw new IOException($"Copy command failed: {ex.Message}", ex);
                }
            });

            await client.NamespacedPodExecAsync(
                pod,
                @namespace,
                container,
                // new string[] { "sh", "-c", $"base64 -d >{destinationFilePath}" },
                new string[] { "sh", "-c", $"tar xmf - -C {destinationFolder}" },
                true,
                handler,
                cancellationToken).ConfigureAwait(false);
        }

        private static async Task CompressTo(string source, Stream dest, string destFileName)
        {
            using (GZipOutputStream gzStream = new GZipOutputStream(dest))
            using (TarOutputStream tarStream = new TarOutputStream(gzStream))
            using (FileStream inStream = File.OpenRead(source))
            {
                // gzStream.IsStreamOwner = false;
                // tarStream.IsStreamOwner = false;
                TarEntry entry = TarEntry.CreateTarEntry(destFileName);
                // TarEntry entry = TarEntry.CreateEntryFromFile(source);
                entry.Size = inStream.Length;
                tarStream.PutNextEntry(entry);
                await inStream.CopyToAsync(tarStream).ConfigureAwait(false);
                tarStream.CloseEntry();
            }
        }

        private static async Task CopyInChunks(Stream source, Stream dest, int chunkSize)
        {
            int read;
            byte[] buffer = new byte[chunkSize];
            while ((read = await source.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false)) > 0)
                await dest.WriteAsync(buffer, 0, read).ConfigureAwait(false);
        }

        private static string GetFolderName(string filePath)
        {
            var folderName = Path.GetDirectoryName(filePath);

            return string.IsNullOrEmpty(folderName) ? "." : folderName;
        }

        private static void ValidatePathParameters(string sourcePath, string destinationPath)
        {
            if (string.IsNullOrWhiteSpace(sourcePath))
            {
                throw new ArgumentException($"{nameof(sourcePath)} cannot be null or whitespace");
            }

            if (string.IsNullOrWhiteSpace(destinationPath))
            {
                throw new ArgumentException($"{nameof(destinationPath)} cannot be null or whitespace");
            }

        }
        */

        private const string kubectlInstallLink = "https://kubernetes.io/docs/tasks/tools/";

        public static void CopyFileToPod(this Kubernetes client, V1Pod pod, string container, string sourceFilePath, string destinationFilePath, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (!IsKubectlInstalled())
                throw new Exception($"kubectl is either not installed or is not on path\n{kubectlInstallLink}");
            if (!(File.Exists(sourceFilePath) || Directory.Exists(sourceFilePath)))
                throw new FileNotFoundException($"File {sourceFilePath} does not exist");

            Process proc = new Process();
            proc.StartInfo.FileName = "kubectl";
            string sourceFile = Path.GetFileName(sourceFilePath);
            proc.StartInfo.Arguments = $"cp {sourceFile} {pod.Namespace()}/{pod.Name()}:{destinationFilePath} -c {container}";
            proc.StartInfo.WorkingDirectory = Path.GetDirectoryName(sourceFilePath);
            proc.StartInfo.RedirectStandardOutput = true;
            proc.StartInfo.RedirectStandardError = true;
            proc.Start();
            // int timeToWait = 5 * 1000; // ms

            // Time to wait based on file size. Otherwise we get a timeout on
            // long files vs wait to long for botched copies on small files.
            long fileSize = (long)(new FileInfo(sourceFilePath).Length / Math.Pow(2, 20));

            // Time to wait (in s) = file size (in MB).
            // If this overflows, the file is >2PB.
            int timeToWait = Math.Max(5, (int)fileSize) * 1000;

            bool exited = proc.WaitForExit(timeToWait);
            if (!exited)
                proc.Kill();
            if (!exited || proc.ExitCode != 0)
            {
                string stdout = proc.StandardOutput.ReadToEnd();
                string stderr = proc.StandardError.ReadToEnd();
                int numRetries = 5;
                for (int i = 0; i < numRetries; i++)
                {
                    Console.WriteLine($"Failed to copy file to {pod.Name()}. Retrying ({i + 1}/{numRetries})");
                    proc.Start();
                    exited = proc.WaitForExit(timeToWait);
                    if (exited && proc.ExitCode == 0)
                        break;
                    if (!exited)
                    {
                        proc.Kill();
                        proc.WaitForExit(1000);
                    }
                }
                if (proc.ExitCode != 0)
                    throw new Exception($"Failed to copy {sourceFilePath} to pod {pod.Name()};\nstdout:\n{stdout}\nstderr:\n{stderr}");
            }
        }

        public static void CopyDirectoryToPod(this Kubernetes client, V1Pod pod, string container, string sourceDirectoryPath, string destinationDirectoyPath, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (!Directory.Exists(sourceDirectoryPath))
                throw new DirectoryNotFoundException($"Directory {sourceDirectoryPath} does not exist");
            client.CopyFileToPod(pod, container, sourceDirectoryPath, destinationDirectoyPath, cancellationToken);
        }

        /// <summary>
        /// Get the state of a particular container in a pod.
        /// </summary>
        /// <param name="client">Kubernetes client.</param>
        /// <param name="pod">The pod.</param>
        /// <param name="containerName">Name of a container inside the pod.</param>
        public static V1ContainerState GetContainerState(this Kubernetes client, V1Pod pod, string containerName)
        {
            return client.GetContainerStatus(pod, containerName).State;
        }

        /// <summary>
        /// Get the status of a particular container in a pod.
        /// </summary>
        /// <param name="client">Kubernetes client.</param>
        /// <param name="pod">The pod.</param>
        /// <param name="containerName">Name of a container inside the pod.</param>
        public static V1ContainerStatus GetContainerStatus(this Kubernetes client, V1Pod pod, string containerName)
        {
            return pod.Status.ContainerStatuses?.FirstOrDefault(c => c.Name == containerName);
        }

        private static bool IsKubectlInstalled()
        {
            // Run "where" on windows, otherwise run "which" on 'nix systems.
            string program = IsWindows() ? "where" : "which";
            Process proc = new Process();
            proc.StartInfo.FileName = program;
            proc.StartInfo.Arguments = "kubectl";
            proc.StartInfo.RedirectStandardOutput = true;
            proc.StartInfo.RedirectStandardError = true;
            proc.Start();
            proc.WaitForExit();
            // If exit code is 1, kubectl is not installed.
            return proc.ExitCode == 0;
        }

        private static bool IsWindows()
        {
            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
        }
    }
}
