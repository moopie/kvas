# KVAS

A key-value store with replication capabilities.

## System requirements

1. .NET 10 SDK

## Launch the application

From the repository root, run:

   ```bash
   dotnet run --project src/server/server.csproj
   ```

The primary listens on port `5757`, and the replicas listen on ports `5758` and `5759`.
The primary replication listener uses port `57570`.

Stop the application with `Ctrl+C`.

## Test the application

From the repository root, run:

   ```bash
   dotnet test kvas.slnx
   ```
