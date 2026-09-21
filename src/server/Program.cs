using System.Net;
using server;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddKeyedSingleton<ICacheStore, CacheStore>("primary");
builder.Services.AddKeyedSingleton<ICacheStore, CacheStore>("replica-1");
builder.Services.AddKeyedSingleton<ICacheStore, CacheStore>("replica-2");
builder.Services.AddSingleton<ITcpServer>(provider => new TcpServer(
    new IPEndPoint(IPAddress.Loopback, 5757),
    provider.GetRequiredKeyedService<ICacheStore>("primary"),
    provider.GetRequiredService<ILogger<TcpServer>>(),
    replicationEndPoint: new IPEndPoint(IPAddress.Loopback, 57570),
    role: ServerRole.Primary));
builder.Services.AddSingleton<ITcpServer>(provider => new TcpServer(
    new IPEndPoint(IPAddress.Loopback, 5758),
    provider.GetRequiredKeyedService<ICacheStore>("replica-1"),
    provider.GetRequiredService<ILogger<TcpServer>>(),
    replicationEndPoint: new IPEndPoint(IPAddress.Loopback, 57570),
    role: ServerRole.Replica));
builder.Services.AddSingleton<ITcpServer>(provider => new TcpServer(
    new IPEndPoint(IPAddress.Loopback, 5759),
    provider.GetRequiredKeyedService<ICacheStore>("replica-2"),
    provider.GetRequiredService<ILogger<TcpServer>>(),
    replicationEndPoint: new IPEndPoint(IPAddress.Loopback, 57570),
    role: ServerRole.Replica));
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
