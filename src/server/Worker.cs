using System.Net.Sockets;
using System.Net;
using System.Text;
using System.Buffers.Binary;

namespace server;

public class Worker(ILogger<Worker> logger) : BackgroundService
{
    private const int _headerSize = sizeof(int);
    private const int _maxFrameSize = 2 * 1024;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var listener = new TcpListener(IPAddress.Any, 8080);
        listener.Start();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                    var client = await listener.AcceptTcpClientAsync(stoppingToken);
                    _ = HandleClientAsync(client, stoppingToken);
                }
                catch (Exception e)
                {
                    if (!stoppingToken.IsCancellationRequested)
                    {
                        logger.LogError(e, "Error");
                    }
                }
            }
        }
        finally
        {
            listener.Stop();
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken token)
    {
        using (client)
        {
            try
            {
                logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                var stream = client.GetStream();
                var request = await ReadFrameAsync(stream, token);
                logger.LogInformation("[msg] {msg}", request);

                await WriteFrameAsync(stream, $"yoooooo, {request.Name}!", token);
            }
            catch (OperationCanceledException) {}
            catch (Exception e)
            {
                if (!token.IsCancellationRequested)
                {
                    logger.LogError(e, "listener error.");
                }
            }
        }
    }

    private async Task<FrameRequest> ReadFrameAsync(NetworkStream stream, CancellationToken token)
    {
        // Add content size header to track transerred network data
        var header = new byte[_headerSize];
        await stream.ReadExactlyAsync(header, token);

        // check frame size
        int payloadLength = BinaryPrimitives.ReadInt32BigEndian(header);
        if (payloadLength < 0 || payloadLength > _maxFrameSize)
        {
            throw new IOException($"Invalid frame size: {payloadLength}.");
        }

        // get the data
        var buffer = new byte[payloadLength];
        await stream.ReadExactlyAsync(buffer, token);
        var msg = Encoding.UTF8.GetString(buffer);
        return new FrameRequest(msg);
    }

    private async Task WriteFrameAsync(NetworkStream stream, string message, CancellationToken token)
    {
        var payload = Encoding.UTF8.GetBytes(message);
        if (payload.Length > _maxFrameSize)
        {
            throw new InvalidDataException($"Frame size {payload.Length} exceeds the limit.");
        }
        var header = new byte[_headerSize];
        BinaryPrimitives.WriteInt32BigEndian(header, payload.Length);

        // send content size + payload
        await stream.WriteAsync(header, token);
        await stream.WriteAsync(payload, token);
    }
}

public record FrameRequest(string Name);
public record FrameResponse(string Name);
