using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using server;
using server.Enums;

namespace tests;

public class ReplicationTests
{
    private const int HeaderSize = sizeof(int);

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters =
        {
            new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)
        }
    };

    [Fact]
    public async Task SetOnPrimary_PropagatesToBothReplicas()
    {
        var ports = GetAvailablePorts(4);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var (servers, serverTasks) = StartCluster(ports, cancellation.Token);

        try
        {
            await WaitForReplicasAsync(ports, cancellation.Token);
            await SendAsync(
                ports[0],
                new FrameRequest(CommandType.Set, "replicated-key", "replicated-value"),
                cancellation.Token);

            await WaitForValueAsync(ports[1], "replicated-key", "replicated-value", cancellation.Token);
            await WaitForValueAsync(ports[2], "replicated-key", "replicated-value", cancellation.Token);
        }
        finally
        {
            GC.KeepAlive(servers);
            await cancellation.CancelAsync();
            await Task.WhenAll(serverTasks);
        }
    }

    [Fact]
    public async Task DeleteOnPrimary_PropagatesToBothReplicas()
    {
        var ports = GetAvailablePorts(4);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var (servers, serverTasks) = StartCluster(ports, cancellation.Token);

        try
        {
            await WaitForReplicasAsync(ports, cancellation.Token);
            await SendAsync(
                ports[0],
                new FrameRequest(CommandType.Set, "deleted-key", "value"),
                cancellation.Token);
            await WaitForValueAsync(ports[1], "deleted-key", "value", cancellation.Token);
            await WaitForValueAsync(ports[2], "deleted-key", "value", cancellation.Token);

            await SendAsync(
                ports[0],
                new FrameRequest(CommandType.Delete, "deleted-key", null),
                cancellation.Token);

            await WaitForValueAsync(ports[1], "deleted-key", string.Empty, cancellation.Token);
            await WaitForValueAsync(ports[2], "deleted-key", string.Empty, cancellation.Token);
        }
        finally
        {
            GC.KeepAlive(servers);
            await cancellation.CancelAsync();
            await Task.WhenAll(serverTasks);
        }
    }

    [Fact]
    public async Task Replicas_RejectSetAndDeleteRequests()
    {
        var ports = GetAvailablePorts(4);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var (servers, serverTasks) = StartCluster(ports, cancellation.Token);

        try
        {
            var writeRequests = new[]
            {
                new FrameRequest(CommandType.Set, "blocked-key", "blocked-value"),
                new FrameRequest(CommandType.Delete, "blocked-key", null)
            };
            var expectedResponse = new FrameResponse(
                ResultType.Error,
                "Writes are not allowed on a replica.");

            foreach (var replicaPort in ports.Skip(1).Take(2))
            {
                foreach (var request in writeRequests)
                {
                    var response = await SendAsync(replicaPort, request, cancellation.Token);
                    Assert.Equal(expectedResponse, response);
                }

                var getResponse = await SendAsync(
                    replicaPort,
                    new FrameRequest(CommandType.Get, "blocked-key", null),
                    cancellation.Token);
                Assert.Equal(new FrameResponse(ResultType.Success, string.Empty), getResponse);
            }
        }
        finally
        {
            GC.KeepAlive(servers);
            await cancellation.CancelAsync();
            await Task.WhenAll(serverTasks);
        }
    }

    private static (TcpServer[] Servers, Task[] ServerTasks) StartCluster(
        IReadOnlyList<int> ports,
        CancellationToken cancellationToken)
    {
        var replicationEndPoint = new IPEndPoint(IPAddress.Loopback, ports[3]);
        var servers = new[]
        {
            CreateServer(ports[0], ServerRole.Primary, replicationEndPoint),
            CreateServer(ports[1], ServerRole.Replica, replicationEndPoint),
            CreateServer(ports[2], ServerRole.Replica, replicationEndPoint)
        };
        var serverTasks = servers.Select(server => server.Start(cancellationToken)).ToArray();
        return (servers, serverTasks);
    }

    private static TcpServer CreateServer(
        int port,
        ServerRole role,
        IPEndPoint replicationEndPoint)
    {
        return new TcpServer(
            new IPEndPoint(IPAddress.Loopback, port),
            new CacheStore(),
            NullLogger<TcpServer>.Instance,
            role: role,
            replicationEndPoint: replicationEndPoint);
    }

    private static async Task WaitForValueAsync(
        int port,
        string key,
        string expectedValue,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < TimeSpan.FromSeconds(2))
        {
            var response = await SendAsync(
                port,
                new FrameRequest(CommandType.Get, key, null),
                cancellationToken);

            if (response == new FrameResponse(ResultType.Success, expectedValue))
            {
                return;
            }

            await Task.Delay(25, cancellationToken);
        }

        Assert.Fail($"Replica on port {port} did not return '{expectedValue}' for key '{key}'.");
    }

    private static async Task WaitForReplicasAsync(
        IReadOnlyList<int> ports,
        CancellationToken cancellationToken)
    {
        const string readinessKey = "replication-ready";
        const string readinessValue = "ready";
        var stopwatch = Stopwatch.StartNew();

        while (stopwatch.Elapsed < TimeSpan.FromSeconds(2))
        {
            await SendAsync(
                ports[0],
                new FrameRequest(CommandType.Set, readinessKey, readinessValue),
                cancellationToken);

            var firstReplica = await SendAsync(
                ports[1],
                new FrameRequest(CommandType.Get, readinessKey, null),
                cancellationToken);
            var secondReplica = await SendAsync(
                ports[2],
                new FrameRequest(CommandType.Get, readinessKey, null),
                cancellationToken);

            var expectedResponse = new FrameResponse(ResultType.Success, readinessValue);
            if (firstReplica == expectedResponse && secondReplica == expectedResponse)
            {
                return;
            }

            await Task.Delay(25, cancellationToken);
        }

        Assert.Fail("Both replicas did not connect to the primary within the timeout.");
    }

    private static async Task<FrameResponse> SendAsync(
        int port,
        FrameRequest request,
        CancellationToken cancellationToken)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
        var stream = client.GetStream();
        var payload = JsonSerializer.SerializeToUtf8Bytes(request, JsonOptions);
        var header = new byte[HeaderSize];
        BinaryPrimitives.WriteInt32BigEndian(header, payload.Length);

        await stream.WriteAsync(header, cancellationToken);
        await stream.WriteAsync(payload, cancellationToken);

        await stream.ReadExactlyAsync(header, cancellationToken);
        var responseLength = BinaryPrimitives.ReadInt32BigEndian(header);
        var responsePayload = new byte[responseLength];
        await stream.ReadExactlyAsync(responsePayload, cancellationToken);

        return JsonSerializer.Deserialize<FrameResponse>(responsePayload, JsonOptions)
            ?? throw new InvalidDataException("The server returned an invalid response.");
    }

    private static int[] GetAvailablePorts(int count)
    {
        var listeners = Enumerable.Range(0, count)
            .Select(_ => new TcpListener(IPAddress.Loopback, 0))
            .ToArray();

        try
        {
            foreach (var listener in listeners)
            {
                listener.Start();
            }

            return listeners
                .Select(listener => ((IPEndPoint)listener.LocalEndpoint).Port)
                .ToArray();
        }
        finally
        {
            foreach (var listener in listeners)
            {
                listener.Stop();
            }
        }
    }
}
