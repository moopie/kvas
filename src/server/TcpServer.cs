using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace server;

public sealed class TcpServer : ITcpServer
{
    public const int DefaultMaxConnections = 128;
    public static readonly TimeSpan DefaultConnectionTimeout = TimeSpan.FromSeconds(2);

    private readonly TcpListener _listener;
    private readonly ICacheStore _cacheStore;
    private readonly ILogger<TcpServer> _logger;
    private readonly SemaphoreSlim _connectionSlots;
    private readonly TimeSpan _connectionTimeout;
    private readonly ServerRole _role;
    
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

    public TcpServer(
        IPEndPoint endPoint,
        ICacheStore cacheStore,
        ILogger<TcpServer> logger,
        int maxConnections = DefaultMaxConnections,
        TimeSpan? connectionTimeout = null,
        ServerRole role = ServerRole.Primary)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConnections, 1);

        if (!Enum.IsDefined(role))
        {
            throw new ArgumentOutOfRangeException(
                nameof(role),
                role,
                "Unknown server role.");
        }

        _connectionTimeout = connectionTimeout ?? DefaultConnectionTimeout;
        if (_connectionTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(connectionTimeout),
                "Connection timeout must be greater than zero.");
        }

        _listener = new TcpListener(endPoint);
        _connectionSlots = new SemaphoreSlim(maxConnections, maxConnections);
        _role = role;
        _cacheStore = cacheStore;
        _logger = logger;
    }

    public async Task Start(CancellationToken stoppingToken)
    {
        _listener.Start();
        _logger.LogInformation("TCP server started on {EndPoint} as {Role}", _listener.LocalEndpoint, _role);
        
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var client = await _listener.AcceptTcpClientAsync(stoppingToken);
                    _ = HandleClientAsync(client, stoppingToken);
                }
                catch (Exception e)
                {
                    if (!stoppingToken.IsCancellationRequested)
                    {
                        _logger.LogError(e, "Error accepting TCP connection");
                    }
                }
            }
        }
        finally
        {
            _listener.Stop();
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken stoppingToken)
    {
        if (!_connectionSlots.Wait(0))
        {
            _logger.LogWarning("Connection rejected because the connection pool is full");
            client.Dispose();
            return;
        }

        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            timeout.CancelAfter(_connectionTimeout);
            using (client)
            {
                var stream = client.GetStream();
                var request = await ReadFrameAsync(stream, timeout.Token);
                var response = await HandleRequest(request, timeout.Token);
                _logger.LogInformation("[msg] {@Message}", request);

                await WriteFrameAsync(stream, response, timeout.Token);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception e)
        {
            // exceptions don't propagate further
            // because i don't want the handler to block other connections
            if (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogError(e, "TCP client handler failed");
            }
        }
        finally
        {
            client.Dispose();
            _connectionSlots.Release();
        }
    }

    private async Task<FrameResponse> HandleRequest(FrameRequest request, CancellationToken token)
    {
        if (_role == ServerRole.Replica && request.Command is CommandType.Set or CommandType.Delete)
        {
            return new FrameResponse(ResultType.Error, "Writes are not allowed on a replica.");
        }

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
