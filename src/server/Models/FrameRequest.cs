using server.Enums;

namespace server;

public record FrameRequest(CommandType Command, string Key, string? Value);
