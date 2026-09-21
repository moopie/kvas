using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace server;

public class TcpServer(IPEndPoint endPoint)
{
    private readonly TcpListener _listener = new(endPoint);
    private readonly CacheStore _cacheStore = new();
    
    private const int HeaderSize = sizeof(int);
    private const int MaxFrameSize = 2 * 1024;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters =
        {
            new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)
        }
    };

    public async Task Start(CancellationToken stoppingToken)
    {
        _listener.Start();
        
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    //logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                    var client = await _listener.AcceptTcpClientAsync(stoppingToken);
                    _ = HandleClientAsync(client, stoppingToken);
                }
                catch (Exception e)
                {
                    if (!stoppingToken.IsCancellationRequested)
                    {
                        //logger.LogError(e, "Error");
                    }
                }
            }
        }
        finally
        {
            _listener.Stop();
        }
    }
    
    private async Task HandleClientAsync(TcpClient client, CancellationToken token)
    {
        using (client)
        {
            try
            {
                //logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                var stream = client.GetStream();
                var request = await ReadFrameAsync(stream, token);
                var response = await HandleRequest(request, token);
                //logger.LogInformation("[msg] {msg}", request);

                await WriteFrameAsync(stream, response, token);
            }
            catch (OperationCanceledException) {}
            catch (Exception e)
            {
                // exceptions don't propagate further
                // because i don't want the handler to block other connections
                if (!token.IsCancellationRequested)
                {
                    //logger.LogError(e, "listener error.");
                }
            }
        }
    }

    private async Task<FrameResponse> HandleRequest(FrameRequest request, CancellationToken token)
    {
        return request.Command switch
        {
            CommandType.Get => await HandleGet(request, token),
            CommandType.Set => await HandleSet(request, token),
            CommandType.Delete => await HandleDelete(request, token),
            _ => throw new ArgumentOutOfRangeException(nameof(request.Command), request.Command, "Unknown command")
        };
    }

    private async Task<FrameResponse> HandleDelete(FrameRequest request, CancellationToken token)
    {
        ArgumentException.ThrowIfNullOrEmpty(request.Key);
        
        await _cacheStore.RemoveAsync(request.Key);
        return new FrameResponse(ResultType.Success, request.Key);
    }

    private async Task<FrameResponse> HandleSet(FrameRequest request, CancellationToken token)
    {
        ArgumentException.ThrowIfNullOrEmpty(request.Key);
        ArgumentException.ThrowIfNullOrEmpty(request.Value);

        await _cacheStore.SetAsync(request.Key, request.Value);
        
        return new FrameResponse(ResultType.Success, request.Key);
    }

    private async Task<FrameResponse> HandleGet(FrameRequest request, CancellationToken token)
    {
        if (string.IsNullOrEmpty(request.Key))
        {
            throw new KeyNotFoundException();
        }
        var item = await _cacheStore.GetAsync(request.Key);
        
        return new FrameResponse(ResultType.Success, item);
    }


    private static async Task<FrameRequest> ReadFrameAsync(NetworkStream stream, CancellationToken token)
    {
        // Add content size header to track the length of network data
        var header = new byte[HeaderSize];
        await stream.ReadExactlyAsync(header, token);

        // check frame size
        int payloadLength = BinaryPrimitives.ReadInt32BigEndian(header);
        if (payloadLength < 0 || payloadLength > MaxFrameSize)
        {
            throw new IOException($"Invalid frame size: {payloadLength}.");
        }

        // get the data
        var buffer = new byte[payloadLength];
        await stream.ReadExactlyAsync(buffer, token);
        var msg = Encoding.UTF8.GetString(buffer);
        var payload = JsonSerializer.Deserialize<FrameRequest>(msg, JsonOptions);
        return payload ?? throw new IOException($"Invalid frame: {msg}");
    }

    private static async Task WriteFrameAsync<T>(NetworkStream stream, T message, CancellationToken token)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions);
        if (payload.Length > MaxFrameSize)
        {
            throw new InvalidDataException($"Frame size {payload.Length} exceeds the limit.");
        }
        var header = new byte[HeaderSize];
        BinaryPrimitives.WriteInt32BigEndian(header, payload.Length);

        // send content size + payload
        await stream.WriteAsync(header, token);
        await stream.WriteAsync(payload, token);
    }
}
