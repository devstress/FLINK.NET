# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY . .

# Install Aspire workload so publishing succeeds
RUN dotnet workload install aspire \
    && dotnet workload restore IntegrationTestImage/IntegrationTestImage.csproj

# Publish a self-contained binary for Alpine
RUN mkdir -p /app/publish
RUN dotnet publish IntegrationTestImage/IntegrationTestImage.csproj \
    -c Release -r linux-musl-x64 \
    --self-contained true \
    -p:PublishSingleFile=true \
    -o /app/publish

# Final stage with Docker-in-Docker
FROM docker:26-dind
WORKDIR /app

# Preload Redis and Kafka images so the AppHost can start them without pulling
# This avoids lengthy network operations during health checks
RUN apk add --no-cache skopeo \
    && skopeo copy docker://redis:latest docker-archive:/redis.tar \
    && skopeo copy docker://confluentinc/cp-kafka:latest docker-archive:/kafka.tar

# No runtime installation is required because the app is self-contained
COPY --from=build /app/publish/ .
COPY IntegrationTestImage/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
