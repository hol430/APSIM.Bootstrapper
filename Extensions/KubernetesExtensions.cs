using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Tar;
using k8s;
using k8s.Models;

namespace APSIM.Cluster.Extensions
{
    internal static class KubernetesExtensions
    {
        public static async Task<int> CopyFileToPodAsync(this Kubernetes client, V1Pod pod, string container, string sourceFilePath, string destinationFilePath, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (pod == null)
            {
                throw new ArgumentNullException(nameof(pod));
            }

            return await client.CopyFileToPodAsync(pod.Metadata.Name, pod.Metadata.NamespaceProperty, container, sourceFilePath, destinationFilePath, cancellationToken).ConfigureAwait(false);
        }

        public static async Task<int> CopyFileToPodAsync(this Kubernetes client, string name, string @namespace, string container, string sourceFilePath, string destinationFilePath, CancellationToken cancellationToken = default(CancellationToken))
        {
            // All other parameters are being validated by MuxedStreamNamespacedPodExecAsync called by NamespacedPodExecAsync
            ValidatePathParameters(sourceFilePath, destinationFilePath);

            // The callback which processes the standard input, standard output and standard error of exec method
            var handler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) =>
            {
                var fileInfo = new FileInfo(destinationFilePath);

                try
                {
                    using (var outputStream = new MemoryStream())
                    {
                        using (var inputFileStream = File.OpenRead(sourceFilePath))
                        using (var gZipOutputStream = new GZipOutputStream(outputStream))
                        using (var tarOutputStream = new TarOutputStream(gZipOutputStream))
                        {
                            // To avoid gZipOutputStream to close the memoryStream
                            gZipOutputStream.IsStreamOwner = false;

                            var fileSize = inputFileStream.Length;
                            var entry = TarEntry.CreateTarEntry(fileInfo.Name);
                            entry.Size = fileSize;

                            tarOutputStream.PutNextEntry(entry);

                            // this is copied from TarArchive.WriteEntryCore
                            byte[] localBuffer = new byte[32 * 1024];
                            while (true)
                            {
                                int numRead = inputFileStream.Read(localBuffer, 0, localBuffer.Length);
                                if (numRead <= 0)
                                {
                                    break;
                                }

                                tarOutputStream.Write(localBuffer, 0, numRead);
                            }

                            tarOutputStream.CloseEntry();
                        }

                        outputStream.Position = 0;
                        using (var cryptoStream = new CryptoStream(stdIn, new ToBase64Transform(), CryptoStreamMode.Write))
                        {
                            await outputStream.CopyToAsync(cryptoStream).ConfigureAwait(false);
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new IOException($"Copy command failed: {ex.Message}");
                }

                using (var errorReader = new StreamReader(stdError))
                {
                    if (errorReader.Peek() != -1)
                    {
                        var error = await errorReader.ReadToEndAsync().ConfigureAwait(false);
                        throw new IOException($"Copy command failed: {error}");
                    }
                }
            });

            var destinationFolder = GetFolderName(destinationFilePath);

            return await client.NamespacedPodExecAsync(
                name,
                @namespace,
                container,
                new string[] { "sh", "-c", $"base64 -d | tar xzmf - -C {destinationFolder}" },
                false,
                handler,
                cancellationToken).ConfigureAwait(false);
        }

        public static async Task<int> CopyDirectoryToPodAsync(this Kubernetes client, V1Pod pod, string container, string sourceDirectoryPath, string destinationDirectoyPath, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (pod == null)
            {
                throw new ArgumentNullException(nameof(pod));
            }

            return await client.CopyFileToPodAsync(pod.Metadata.Name, pod.Metadata.NamespaceProperty, container, sourceDirectoryPath, destinationDirectoyPath, cancellationToken).ConfigureAwait(false);
        }

        public static async Task<int> CopyDirectoryToPodAsync(this Kubernetes client, string name, string @namespace, string container, string sourceDirectoryPath, string destinationDirectoryPath, CancellationToken cancellationToken = default(CancellationToken))
        {
            // All other parameters are being validated by MuxedStreamNamespacedPodExecAsync called by NamespacedPodExecAsync
            ValidatePathParameters(sourceDirectoryPath, destinationDirectoryPath);

            // The callback which processes the standard input, standard output and standard error of exec method
            var handler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) =>
            {
                try
                {
                    using (var outputStream = new MemoryStream())
                    {
                        using (var gZipOutputStream = new GZipOutputStream(outputStream))
                        using (var tarArchive = TarArchive.CreateOutputTarArchive(gZipOutputStream))
                        {
                            // To avoid gZipOutputStream to close the memoryStream
                            gZipOutputStream.IsStreamOwner = false;

                            // RootPath must be forward slashes and must not end with a slash
                            tarArchive.RootPath = sourceDirectoryPath.Replace('\\', '/');
                            if (tarArchive.RootPath.EndsWith("/", StringComparison.InvariantCulture))
                            {
                                tarArchive.RootPath = tarArchive.RootPath.Remove(tarArchive.RootPath.Length - 1);
                            }

                            client.AddDirectoryFilesToTar(tarArchive, sourceDirectoryPath);
                        }

                        outputStream.Position = 0;
                        using (var cryptoStream = new CryptoStream(stdIn, new ToBase64Transform(), CryptoStreamMode.Write))
                        {
                            await outputStream.CopyToAsync(cryptoStream).ConfigureAwait(false);
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new IOException($"Copy command failed: {ex.Message}");
                }

                using (var errorReader = new StreamReader(stdError))
                {
                    if (errorReader.Peek() != -1)
                    {
                        var error = await errorReader.ReadToEndAsync().ConfigureAwait(false);
                        throw new IOException($"Copy command failed: {error}");
                    }
                }
            });

            return await client.NamespacedPodExecAsync(
                name,
                @namespace,
                container,
                new string[] { "sh", "-c", $"base64 -d | tar xzmf - -C {destinationDirectoryPath}" },
                false,
                handler,
                cancellationToken).ConfigureAwait(false);
        }

        private static void AddDirectoryFilesToTar(this Kubernetes client, TarArchive tarArchive, string sourceDirectoryPath)
        {
            var tarEntry = TarEntry.CreateEntryFromFile(sourceDirectoryPath);
            tarArchive.WriteEntry(tarEntry, false);

            var filenames = Directory.GetFiles(sourceDirectoryPath);
            for (var i = 0; i < filenames.Length; i++)
            {
                tarEntry = TarEntry.CreateEntryFromFile(filenames[i]);
                tarArchive.WriteEntry(tarEntry, true);
            }

            var directories = Directory.GetDirectories(sourceDirectoryPath);
            for (var i = 0; i < directories.Length; i++)
            {
                client.AddDirectoryFilesToTar(tarArchive, directories[i]);
            }
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
    }
}
