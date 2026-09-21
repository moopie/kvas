using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using server;

namespace tests;

public class TcpServerTests
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
    public async Task Server_HandlesSetAndGetRequests()
    {
        var port = GetAvailablePort();
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var server = CreateServer(port);
        var serverTask = server.Start(cancellation.Token);

        try
        {
            var setResponse = await SendAsync(
                port,
                new FrameRequest(CommandType.Set, "greeting", "hello"),
                cancellation.Token);
            var getResponse = await SendAsync(
                port,
                new FrameRequest(CommandType.Get, "greeting", null),
                cancellation.Token);

            Assert.Equal(new FrameResponse(ResultType.Success, "greeting"), setResponse);
            Assert.Equal(new FrameResponse(ResultType.Success, "hello"), getResponse);
        }
        finally
        {
            await cancellation.CancelAsync();
            await serverTask;
        }
    }

    [Fact]
    public async Task Server_LimitsConcurrentConnectionsAndTimesThemOut()
    {
        var port = GetAvailablePort();
        var connectionTimeout = TimeSpan.FromMilliseconds(300);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var server = CreateServer(port, maxConnections: 1, connectionTimeout);
        var serverTask = server.Start(cancellation.Token);

        try
        {
            using var stalledClient = new TcpClient();
            await stalledClient.ConnectAsync(IPAddress.Loopback, port, cancellation.Token);
            await Task.Delay(50, cancellation.Token);

            var stopwatch = Stopwatch.StartNew();
            await Assert.ThrowsAnyAsync<IOException>(() => SendAsync(
                port,
                new FrameRequest(CommandType.Set, "rejected", "connection"),
                cancellation.Token));
            stopwatch.Stop();

            Assert.True(
                stopwatch.Elapsed < connectionTimeout,
                $"The excess connection was not rejected promptly: {stopwatch.Elapsed}.");

            var buffer = new byte[1];
            var bytesRead = await stalledClient.GetStream().ReadAsync(buffer, cancellation.Token);
            Assert.Equal(0, bytesRead);

            var response = await SendAsync(
                port,
                new FrameRequest(CommandType.Set, "limited", "connection"),
                cancellation.Token);
            Assert.Equal(new FrameResponse(ResultType.Success, "limited"), response);
        }
        finally
        {
            await cancellation.CancelAsync();
            await serverTask;
        }
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

    private static TcpServer CreateServer(
        int port,
        int maxConnections = TcpServer.DefaultMaxConnections,
        TimeSpan? connectionTimeout = null)
    {
        return new TcpServer(
            new IPEndPoint(IPAddress.Loopback, port),
            new CacheStore(),
            NullLogger<TcpServer>.Instance,
            maxConnections,
            connectionTimeout);
    }

    private static int GetAvailablePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
