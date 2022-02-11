using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using k8s;

namespace APSIM.Bootstrapper
{
    /// <summary>
    /// This class creates a socket server listening on the
    /// </summary>
    internal class PortForwardingService : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// Buffer size used by reader/writer threads.
        /// </summary>
        private const int bufferSize = 4096;

        /// <summary>
        /// Cancellation token which is registered to the socket server.
        /// Cancel this to cancel the socket server.
        /// </summary>
        private readonly CancellationTokenSource cts;

        /// <summary>
        /// Client used to make kubernetes API requests.
        /// </summary>
        private readonly IKubernetes client;

        /// <summary>
        /// The port forwarding (socket server) task.
        /// </summary>
        private Task portForwarding;

        /// <summary>
        /// Create a new <see cref="PortForwardingService"/> instance.
        /// </summary>
        /// <param name="kubernetesClient">Client object to use for kubernetes API requests.</param>
        public PortForwardingService(IKubernetes kubernetesClient)
        {
            client = kubernetesClient;
            cts = new CancellationTokenSource();
        }

        /// <summary>
        /// Initiate port forwarding from localhost to the job manager,
        /// allowing us to connect to the job manager pod. This is done
        /// by creating a socket server listening on localhost on a
        /// randomly selected free port. This socket server allows for
        /// duplex comms to/from the specified pod. The return value of
        /// this function is the port number on which the server is
        /// listening.
        /// 
        /// The port forwarding service can be stopped either by calling
        /// <see cref="Stop"/> or <see cref="Dispose"/>.
        /// </summary>
        /// <remarks>
        /// Note this will only accept a single connection. It's also
        /// single-threaded, it won't handle concurrent requests well.
        /// </remarks>
        /// <param name="podName">Name of the pod.</param>
        /// <param name="podNamespace">Namespace of the pod.</param>
        /// <param name="podPortNo">Port number on which the pod is listening.</param>
        /// <param name="portNo">Port number on which to create the socket server.</param>
        /// <param name="cancelToken">Cancellation token.</param>
        /// <returns>Port on which the socket server is listening.</returns>
        public int Start(string podName, string podNamespace, int podPortNo)
        {
            // Specify port 0 to choose any free port.
            IPEndPoint endpoint = new IPEndPoint(IPAddress.Loopback, 0);

            portForwarding = InitiatePortForwarding(podName, podNamespace, podPortNo, endpoint, cts.Token);
            return endpoint.Port;
        }

        /// <summary>
        /// Stop the port forwarding service.
        /// </summary>
        /// <remarks>
        /// This will not wait for the portForwarding task to terminate.
        /// In fact, normally this shouldn't be called manually - it will
        /// be easier to use the IDisposable pattern.
        /// </remarks>
        public void Stop()
        {
            cts.Cancel();
        }

        /// <summary>
        /// Stop the port forwarding service.
        /// </summary>
        public void Dispose()
        {
            var timer = System.Diagnostics.Stopwatch.StartNew();
            DisposeAsync().AsTask().Wait();
            Console.WriteLine($"Shutting down socket server took {timer.ElapsedMilliseconds}ms");
        }

        /// <summary>
        /// Asynchronously stop the port forwarding service.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            Stop();
            if (portForwarding != null)
                await portForwarding.ConfigureAwait(false);
        }

        /// <summary>
        /// Initiate port forwarding from localhost to a given pod,
        /// allowing us to connect to the pod. This is done by creating
        /// a socket server to which the caller can connect. This socket
        /// server implements full duplex comms to/from the given pod.
        /// 
        /// This function returns a task which probably should not be
        /// immediately awaited. Instead, perform whatever operation
        /// by connecting to the socket server, then cancel the token
        /// passed into this function, then await the result of this
        /// function.
        /// </summary>
        /// <remarks>
        /// Note this will only accept a single connection. It's also
        /// single-threaded, it won't handle concurrent requests well.
        /// 
        /// This is not cancellable while it's waiting for a client
        /// connection - need to fix this!!
        /// 
        /// The endpoint argument's port number will be modified to
        /// match that of the listening socket server.
        /// </remarks>
        /// <param name="podName">Name of the pod.</param>
        /// <param name="podNamespace">Namespace of the pod.</param>
        /// <param name="podPortNo">Port number on which the pod is listening.</param>
        /// <param name="endpoint">Endpoint on which to create the socket server.</param>
        /// <param name="cancelToken">Cancellation token. When this is cancelled, the socket servers will be cancelled.</param>
        private async Task InitiatePortForwarding(string podName, string podNamespace, int podPortNo, IPEndPoint endpoint, CancellationToken cancelToken)
        {
            // We need to wait for this task to complete before returning. Otherwise
            // the caller might proceed to attempt a connection without await-ing,
            // which will result in a connection refused error because we are still
            // waiting for this task to return before starting the socket server.
            Task<WebSocket> task = client.WebSocketNamespacedPodPortForwardAsync(podName, podNamespace, new int[] { podPortNo }, "v4.channel.k8s.io");
            task.Wait(cancelToken);
            using (StreamDemuxer demuxer = new StreamDemuxer(task.Result, StreamType.PortForward, ownsSocket: true))
            {
                demuxer.Start();

                // This is a hack to ensure that the stream demuxer closes correctly.
                // Without this, we will timeout inside ReadAsync(), despite the
                // cancellation token being passed in correctly. This appears to be
                // a bug in the kubernetes client, and one without a ready fix.
                cancelToken.Register(() => demuxer.Dispose());

                // This stream acts as a proxy between the client connected to the
                // socket, and the pod to which we've just connected. This stream
                // reads from the client and forwards data through to the pod. It
                // also reads data from the job manager pod and sends it back to
                // the client connected to the socket.
                Stream stream = demuxer.GetStream((byte?)0, (byte?)0);

                // Create a socket server listening on the specified endpoint.
                using (Socket server = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp))
                {
                    server.Bind(endpoint);  
                    server.Listen(100);

                    // fixme - this should really be done in a more explicit fashion.
                    endpoint.Port = ((IPEndPoint)server.LocalEndPoint).Port;

                    // This is the socket handle for connected clients.
                    // fixme - need to make this cancellable
                    using (Socket socketClient = await server.AcceptAsync().ConfigureAwait(false))
                    {
                        // This task relays data received from the client through to the pod.
                        Task accept = PortForwardingRelayToPod(socketClient, stream, cancelToken);

                        // This task relays data received from the pod back to the client.
                        Task copy = PortForwardingRelayToClient(socketClient, stream, cancelToken);

                        await accept.ConfigureAwait(false);
                        await copy.ConfigureAwait(false);
                    }
                }
            }
        }

        /// <summary>
        /// This task relays data from the pod to the local client.
        /// </summary>
        /// <param name="server">The socket for a client connected to the local socket server.</param>
        /// <param name="stream">The stream which is connected to the pod.</param>
        /// <param name="cancelToken">Cancellation token - use this to cancel the async requests.</param>
        private async Task PortForwardingRelayToClient(Socket server, Stream stream, CancellationToken cancelToken)
        {
            try
            {
                byte[] buff = new byte[bufferSize];
                while (true)
                {
                    int read = await stream.ReadAsync(buff, 0, bufferSize, cancelToken).ConfigureAwait(false);
                    server.Send(buff, read, 0); // fixme - use async api
                    cancelToken.ThrowIfCancellationRequested();
                }
            }
            catch (OperationCanceledException)
            {
                // This is normal.
                // WriteToLog($"Relay to client: task was cancelled");
            }
            catch (Exception)
            {
                // Cancellation of the stream demuxer can lead to a broken pipe.
                // In this situation, don't propagate such an error. Normally,
                // the exception will be an IOException or SocketException.
                if (!cancelToken.IsCancellationRequested)
                    throw;
            }
        }

        /// <summary>
        /// This task relays data from the local client to the pod.
        /// </summary>
        /// <remarks>
        /// This is essentially a single message loop which reads from the socket
        /// server, and relays all received data to the stream (the pod).
        /// </remarks>
        /// <param name="server">The socket for a client connected to the socket server.</param>
        /// <param name="stream">Stream connected to the pod.</param>
        /// <param name="cancelToken">Cancellation token - use this to cancel the async requests.</param>
        private async Task PortForwardingRelayToPod(Socket server, Stream stream, CancellationToken cancelToken)
        {
            try
            {
                byte[] bytes = new byte[bufferSize];
                while (true)
                {
                    cancelToken.ThrowIfCancellationRequested();

                    int messageLength = await server.ReceiveAsync(bytes, SocketFlags.None, cancelToken).ConfigureAwait(false);
                    stream.Write(bytes, 0, messageLength); // fixme - use async api
                    if (messageLength == 0 || Encoding.ASCII.GetString(bytes, 0, messageLength).IndexOf("<EOF>") > -1) // ahhh
                        break;
                }
            }
            catch (OperationCanceledException)
            {
                // This is normal.
                // WriteToLog("Relay to pod: task was canceled");
            }
            catch (Exception)
            {
                // Cancellation of the stream demuxer can lead to a broken pipe.
                // In this situation, don't propagate such an error. Normally,
                // the exception will be an IOException or SocketException.
                if (!cancelToken.IsCancellationRequested)
                    throw;
            }
        }
    }
}
