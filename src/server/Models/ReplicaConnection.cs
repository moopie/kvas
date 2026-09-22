using System.Net.Sockets;

namespace server.Models;

public sealed class ReplicaConnection(TcpClient client)
{
    public TcpClient Client { get; } = client;
    public long LastAcknowledgedId { get; set; }
}
