# Windows Setup Guide for FLINK.NET Aspire

This guide helps you set up and run the FLINK.NET Aspire project on Windows systems.

## Prerequisites

### 1. Required Software
- **Windows 10/11** (64-bit)
- **.NET 8.0 SDK** or later
- **Docker Desktop for Windows** with Linux containers enabled
- **PowerShell 7+** (recommended) or Windows PowerShell 5.1
- **Visual Studio 2022** or **Visual Studio Code** with C# extension

### 2. Docker Desktop Configuration

**Important**: The Aspire project uses Linux-based containers (Kafka, Redis) which require Docker Desktop to be configured for Linux containers.

#### Docker Desktop Setup:
1. Install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/)
2. **Enable Linux containers** (this is usually the default)
3. In Docker Desktop settings:
   - **General**: Enable "Use Docker Compose V2"
   - **Resources**: Allocate at least 4GB RAM and 2 CPU cores
   - **WSL 2**: Enable WSL 2 based engine (recommended)

#### Verify Docker Setup:
```powershell
# Check Docker is running
docker --version
docker info

# Verify Linux containers are available
docker run hello-world
```

### 3. .NET Aspire Workload
```powershell
# Install .NET Aspire workload
dotnet workload install aspire

# Verify installation
dotnet workload list
```

## Running the Aspire Project

### 1. Command Line (Recommended)
```powershell
# Navigate to the Aspire AppHost directory
cd FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost

# Run the Aspire application
dotnet run
```

### 2. Visual Studio 2022
1. Open `FlinkDotNetAspire.sln` in Visual Studio
2. Set `FlinkDotNetAspire.AppHost.AppHost` as the startup project
3. Press F5 or click "Start Debugging"

### 3. Visual Studio Code
1. Open the repository folder in VS Code
2. Open integrated terminal (Ctrl+`)
3. Run:
```powershell
cd FlinkDotNetAspire/FlinkDotNetAspire.AppHost.AppHost
dotnet run
```

## Windows-Specific Optimizations

The Aspire project automatically detects Windows and applies these optimizations:

### Container Memory Optimization
- **Kafka**: Reduced heap size to 512MB (from 1GB on Linux)
- **Redis**: Memory limit set to 256MB with LRU eviction policy

### Enhanced Logging
- Reduced log levels for better Windows performance
- Optimized GC settings for containerized services

### Simplified Shell Scripts
- Uses `sh` instead of `bash` for better Windows container compatibility
- Shorter timeouts for faster startup on Windows

## Troubleshooting

### Common Issues

#### 1. "Docker not found" or container startup errors
```powershell
# Check if Docker Desktop is running
Get-Service -Name "Docker Desktop Service"

# Start Docker Desktop if not running
Start-Service -Name "Docker Desktop Service"
```

#### 2. Port conflicts
The Aspire project uses dynamic port allocation to avoid conflicts. If you see port binding errors:
```powershell
# Check what's using the ports
netstat -an | findstr "6379 9092 50051"

# Kill conflicting processes if needed
taskkill /F /PID <process_id>
```

#### 3. Container memory issues
If containers fail with out-of-memory errors:
1. Increase Docker Desktop memory allocation (Settings > Resources)
2. Close other applications to free memory
3. Restart Docker Desktop

#### 4. WSL 2 issues (if using WSL backend)
```powershell
# Update WSL
wsl --update

# Restart WSL
wsl --shutdown
```

### Performance Tips

#### 1. Optimize Docker Desktop
- **Disk Image Location**: Move to SSD if possible
- **File Sharing**: Only share necessary directories
- **CPU/Memory**: Allocate 50-70% of system resources

#### 2. Windows Defender Exclusions
Add these to Windows Defender exclusions for better performance:
- Docker Desktop installation folder
- WSL 2 distributions folder: `%LOCALAPPDATA%\Docker\wsl`
- Your project directory

#### 3. PowerShell Execution Policy
```powershell
# Allow script execution (run as Administrator)
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope LocalMachine

# Or for current user only
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## Environment Variables

The Aspire project automatically configures these environment variables for Windows:

| Variable | Purpose | Windows Value |
|----------|---------|---------------|
| `KAFKA_HEAP_OPTS` | JVM memory | `-Xmx512m -Xms256m` |
| `REDIS_ARGS` | Redis configuration | `--maxmemory 256mb --maxmemory-policy allkeys-lru` |
| `ASPIRE_USE_DYNAMIC_PORTS` | Port allocation | `true` |

## Verification

After starting the Aspire project, verify everything is working:

### 1. Check Aspire Dashboard
Open [https://localhost:15888](https://localhost:15888) (or the URL shown in console output)

### 2. Verify Services
- **Kafka**: Should show "Running" status
- **Redis**: Should show "Running" status  
- **FlinkJobSimulator**: Should show "Running" status
- **TaskManagers**: All 20 instances should show "Running" status

### 3. Test Connectivity
```powershell
# Test Redis (if port is exposed)
# telnet localhost <redis_port>

# Test Kafka (if port is exposed)  
# telnet localhost <kafka_port>
```

## Additional Resources

- [.NET Aspire Documentation](https://learn.microsoft.com/en-us/dotnet/aspire/)
- [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)
- [WSL 2 Installation Guide](https://learn.microsoft.com/en-us/windows/wsl/install)

## Support

If you continue to experience issues:

1. Check the [GitHub Issues](https://github.com/devstress/FLINK.NET/issues)
2. Review container logs in the Aspire Dashboard
3. Enable detailed logging by setting `ASPNETCORE_ENVIRONMENT=Development`

The Windows-specific optimizations should resolve most compatibility issues. If problems persist, please report them with:
- Windows version
- Docker Desktop version
- Error messages from Aspire Dashboard
- Container logs