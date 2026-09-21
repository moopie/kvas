using System.Net;

namespace server;

public class Worker(ILogger<Worker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var server = new TcpServer(new IPEndPoint(IPAddress.Loopback, 19000));
            await server.Start(stoppingToken);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }
}

public record FrameRequest(CommandType Command, string Key, string? Value);
public record FrameResponse(ResultType Result, string Value);

public enum CommandType
{
    Get = 1,
    Set = 2,
    Delete = 3,
}

public enum ResultType
{
    Value = 1,
    Success = 2,
    NotFound = 3,
    Error = 4,
}
