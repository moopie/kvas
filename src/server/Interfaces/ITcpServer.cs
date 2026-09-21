namespace server.Interfaces;

public interface ITcpServer
{
    Task Start(CancellationToken stoppingToken);
}
