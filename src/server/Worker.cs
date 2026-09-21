namespace server;

public sealed class Worker(ITcpServer server, ILogger<Worker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await server.Start(stoppingToken);
        }
        catch (Exception e)
        {
            logger.LogCritical(e, "TCP server stopped unexpectedly");
            throw;
        }
    }
}
