using System.Net;
using server;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<ICacheStore, CacheStore>();
builder.Services.AddSingleton<ITcpServer>(provider => new TcpServer(
    new IPEndPoint(IPAddress.Loopback, 19000),
    provider.GetRequiredService<ICacheStore>(),
    provider.GetRequiredService<ILogger<TcpServer>>(),
    role: ServerRole.Primary));
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
