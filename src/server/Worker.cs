using server.Interfaces;

namespace server;

public sealed class Worker(IEnumerable<ITcpServer> servers, ILogger<Worker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await Task.WhenAll(servers.Select(server => server.Start(stoppingToken)));
        }
        catch (Exception e)
        {
            logger.LogCritical(e, "TCP server stopped unexpectedly");
            throw;
        }
    }
}
