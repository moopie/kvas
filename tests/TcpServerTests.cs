using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.Json.Serialization;
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
        var server = new TcpServer(new IPEndPoint(IPAddress.Loopback, port));
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

    private static int GetAvailablePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
