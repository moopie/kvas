using server.Enums;

namespace server.Models;

public record ReplicationEvent(long Id, CommandType Command, string Key, string? Value);
