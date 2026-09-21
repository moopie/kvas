namespace server;

public interface ITcpServer
{
    Task Start(CancellationToken stoppingToken);
}
