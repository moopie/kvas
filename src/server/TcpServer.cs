using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using server.Enums;
using server.Interfaces;
using server.Models;

namespace server;

public sealed class TcpServer : ITcpServer
{
    public const int DefaultMaxConnections = 128;

    private static readonly TimeSpan DefaultConnectionTimeout = TimeSpan.FromSeconds(3);

    private readonly TcpListener _listener;
    private readonly TcpListener? _replicationListener;
    private readonly IPEndPoint? _replicationEndPoint;
    private readonly ICacheStore _cacheStore;
    private readonly ILogger<TcpServer> _logger;
    private readonly SemaphoreSlim _connectionSlots;
    private readonly TimeSpan _connectionTimeout;
    private readonly ServerRole _role;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly ConcurrentDictionary<long, ReplicaConnection> _replicas = new();
    private readonly List<ReplicationEvent> _replicationEvents = [];
    private long _nextReplicaId;
    private long _nextReplicationEventId;
    private long _lastAppliedReplicationId;
    
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
        ServerRole role = ServerRole.Primary,
        IPEndPoint? replicationEndPoint = null)
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
        _replicationListener = role == ServerRole.Primary && replicationEndPoint is not null
            ? new TcpListener(replicationEndPoint)
            : null;
        _replicationEndPoint = replicationEndPoint;
        _connectionSlots = new SemaphoreSlim(maxConnections, maxConnections);
        _role = role;
        _cacheStore = cacheStore;
        _logger = logger;
    }

    public async Task Start(CancellationToken stoppingToken)
    {
        _listener.Start();
        _logger.LogInformation("TCP server started on {EndPoint} as {Role}", _listener.LocalEndpoint, _role);
        Task? replicationTask = null;

        if (_replicationListener is not null)
        {
            _replicationListener.Start();
            _logger.LogInformation(
                "Replication listener started on {EndPoint}",
                _replicationListener.LocalEndpoint);
            replicationTask = AcceptReplicasAsync(stoppingToken);
        }
        else if (_role == ServerRole.Replica && _replicationEndPoint is not null)
        {
            replicationTask = ConsumeReplicationEventsAsync(stoppingToken);
        }
        
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
            _replicationListener?.Stop();

            foreach (var replica in _replicas.Values)
            {
                replica.Client.Dispose();
            }

            _replicas.Clear();

            if (replicationTask is not null)
            {
                await replicationTask;
            }
        }
    }

    private async Task ConsumeReplicationEventsAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var primary = new TcpClient();
                await primary.ConnectAsync(_replicationEndPoint!, stoppingToken);
                var stream = primary.GetStream();
                await WriteFrameAsync(
                    stream,
                    new ReplicationAck(_lastAppliedReplicationId),
                    stoppingToken);
                _logger.LogInformation(
                    "Replica on {EndPoint} connected to primary replication endpoint {PrimaryEndPoint}",
                    _listener.LocalEndpoint,
                    _replicationEndPoint);

                while (!stoppingToken.IsCancellationRequested)
                {
                    var replicationEvent = await ReadFrameAsync<ReplicationEvent>(stream, stoppingToken);
                    await ApplyReplicationEventAsync(replicationEvent);
                    await WriteFrameAsync(
                        stream,
                        new ReplicationAck(_lastAppliedReplicationId),
                        stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception e)
            {
                _logger.LogWarning(
                    e,
                    "Replica on {EndPoint} lost its primary connection; reconnecting",
                    _listener.LocalEndpoint);

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    private async Task ApplyReplicationEventAsync(ReplicationEvent replicationEvent)
    {
        if (replicationEvent.Id <= _lastAppliedReplicationId)
        {
            return;
        }

        if (replicationEvent.Id != _lastAppliedReplicationId + 1)
        {
            _logger.LogWarning(
                "Replication gap detected on {EndPoint}. Expected {ExpectedId}, received {ReceivedId}",
                _listener.LocalEndpoint,
                _lastAppliedReplicationId + 1,
                replicationEvent.Id);
            return;
        }

        switch (replicationEvent.Command)
        {
            case CommandType.Set:
                ArgumentException.ThrowIfNullOrEmpty(replicationEvent.Value);
                await _cacheStore.SetAsync(replicationEvent.Key, replicationEvent.Value);
                break;
            case CommandType.Delete:
                try
                {
                    await _cacheStore.RemoveAsync(replicationEvent.Key);
                }
                catch (KeyNotFoundException)
                {
                    // The desired replicated state is already present.
                }
                break;
            default:
                throw new InvalidDataException(
                    $"Command {replicationEvent.Command} is not a replication event.");
        }

        _replicationEvents.Add(replicationEvent);
        _lastAppliedReplicationId = replicationEvent.Id;
    }

    private async Task AcceptReplicasAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var replica = await _replicationListener!.AcceptTcpClientAsync(stoppingToken);
                var replicaId = Interlocked.Increment(ref _nextReplicaId);
                _ = HandleReplicaAsync(replicaId, replica, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception e)
            {
                if (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogError(e, "Error accepting replica connection");
                }
            }
        }
    }

    private async Task HandleReplicaAsync(
        long replicaId,
        TcpClient client,
        CancellationToken stoppingToken)
    {
        var connection = new ReplicaConnection(client);

        try
        {
            var stream = client.GetStream();
            using var handshakeTimeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            handshakeTimeout.CancelAfter(_connectionTimeout);
            var initialAck = await ReadFrameAsync<ReplicationAck>(stream, handshakeTimeout.Token);

            await _writeLock.WaitAsync(stoppingToken);
            try
            {
                await ReplayMissingEventsAsync(connection, initialAck.LastAppliedId, stoppingToken);
                _replicas[replicaId] = connection;
            }
            finally
            {
                _writeLock.Release();
            }

            _logger.LogInformation(
                "Replica {ReplicaId} connected from {RemoteEndPoint} at event {LastAppliedId}",
                replicaId,
                client.Client.RemoteEndPoint,
                initialAck.LastAppliedId);

            while (!stoppingToken.IsCancellationRequested)
            {
                var ack = await ReadFrameAsync<ReplicationAck>(stream, stoppingToken);

                await _writeLock.WaitAsync(stoppingToken);
                try
                {
                    await ReplayMissingEventsAsync(connection, ack.LastAppliedId, stoppingToken);
                }
                finally
                {
                    _writeLock.Release();
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            if (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogWarning(e, "Replica {ReplicaId} disconnected", replicaId);
            }
        }
        finally
        {
            if (_replicas.TryGetValue(replicaId, out var registeredConnection)
                && ReferenceEquals(connection, registeredConnection))
            {
                _replicas.TryRemove(replicaId, out _);
            }

            client.Dispose();
        }
    }

    private async Task ReplayMissingEventsAsync(
        ReplicaConnection connection,
        long lastAppliedId,
        CancellationToken token)
    {
        if (lastAppliedId < 0 || lastAppliedId > _nextReplicationEventId)
        {
            throw new InvalidDataException($"Invalid replication ACK: {lastAppliedId}.");
        }

        connection.LastAcknowledgedId = lastAppliedId;
        foreach (var replicationEvent in _replicationEvents)
        {
            if (replicationEvent.Id > lastAppliedId)
            {
                await WriteFrameAsync(connection.Client.GetStream(), replicationEvent, token);
            }
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
                var request = await ReadFrameAsync<FrameRequest>(stream, timeout.Token);
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

        await _writeLock.WaitAsync(token);
        try
        {
            await _cacheStore.RemoveAsync(request.Key);
            var replicationEvent = RecordReplicationEvent(request);
            await ReplicateWriteAsync(replicationEvent, token);
            return new FrameResponse(ResultType.Success, request.Key);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task<FrameResponse> HandleSet(FrameRequest request, CancellationToken token)
    {
        ArgumentException.ThrowIfNullOrEmpty(request.Key);
        ArgumentException.ThrowIfNullOrEmpty(request.Value);

        await _writeLock.WaitAsync(token);
        try
        {
            await _cacheStore.SetAsync(request.Key, request.Value);
            var replicationEvent = RecordReplicationEvent(request);
            await ReplicateWriteAsync(replicationEvent, token);
            return new FrameResponse(ResultType.Success, request.Key);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private ReplicationEvent RecordReplicationEvent(FrameRequest request)
    {
        var replicationEvent = new ReplicationEvent(
            ++_nextReplicationEventId,
            request.Command,
            request.Key,
            request.Value);
        _replicationEvents.Add(replicationEvent);
        return replicationEvent;
    }

    private async Task ReplicateWriteAsync(ReplicationEvent replicationEvent, CancellationToken token)
    {
        foreach (var (replicaId, replica) in _replicas)
        {
            try
            {
                await WriteFrameAsync(replica.Client.GetStream(), replicationEvent, token);
            }
            catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException)
            {
                if (_replicas.TryRemove(replicaId, out var disconnectedReplica))
                {
                    disconnectedReplica.Client.Dispose();
                }

                _logger.LogWarning(e, "Replica {ReplicaId} disconnected", replicaId);
            }
        }
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


    private static async Task<T> ReadFrameAsync<T>(NetworkStream stream, CancellationToken token)
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
        var payload = JsonSerializer.Deserialize<T>(msg, JsonOptions);
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
