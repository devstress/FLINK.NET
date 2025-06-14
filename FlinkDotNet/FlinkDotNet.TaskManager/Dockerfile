FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["FlinkDotNet.TaskManager/FlinkDotNet.TaskManager.csproj", "FlinkDotNet.TaskManager/"]
COPY ["FlinkDotNet.Common.Constants/FlinkDotNet.Common.Constants.csproj", "FlinkDotNet.Common.Constants/"]
COPY ["FlinkDotNet.Core.Abstractions/FlinkDotNet.Core.Abstractions.csproj", "FlinkDotNet.Core.Abstractions/"]
COPY ["FlinkDotNet.Core/FlinkDotNet.Core.csproj", "FlinkDotNet.Core/"]
COPY ["FlinkDotNet.Storage.FileSystem/FlinkDotNet.Storage.FileSystem.csproj", "FlinkDotNet.Storage.FileSystem/"]
COPY ["FlinkDotNet.Storage.RocksDB/FlinkDotNet.Storage.RocksDB.csproj", "FlinkDotNet.Storage.RocksDB/"]
RUN dotnet restore "FlinkDotNet.TaskManager/FlinkDotNet.TaskManager.csproj"
# Copy only necessary source files instead of everything
COPY FlinkDotNet.TaskManager/ FlinkDotNet.TaskManager/
COPY FlinkDotNet.Common.Constants/ FlinkDotNet.Common.Constants/
COPY FlinkDotNet.Core.Abstractions/ FlinkDotNet.Core.Abstractions/
COPY FlinkDotNet.Core/ FlinkDotNet.Core/
COPY FlinkDotNet.Storage.FileSystem/ FlinkDotNet.Storage.FileSystem/
COPY FlinkDotNet.Storage.RocksDB/ FlinkDotNet.Storage.RocksDB/
WORKDIR "/src/FlinkDotNet.TaskManager"
RUN dotnet build "FlinkDotNet.TaskManager.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "FlinkDotNet.TaskManager.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
# Create a non-root user
RUN adduser --disabled-password --gecos "" --uid 1001 flinkuser
WORKDIR /app
COPY --from=publish /app/publish .
# Change ownership of the app directory to the non-root user
RUN chown -R flinkuser:flinkuser /app
USER flinkuser

# Environment variables for TaskManager - ports will be set by Aspire
ENV ASPNETCORE_ENVIRONMENT=Development
ENV ASPNETCORE_URLS=http://+:8080

ENTRYPOINT ["dotnet", "FlinkDotNet.TaskManager.dll"]