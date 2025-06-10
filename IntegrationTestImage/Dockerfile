# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY . .

# Install Aspire workload so publishing succeeds
RUN dotnet workload install aspire

# Publish a self-contained binary for Alpine
RUN dotnet publish IntegrationTestImage/IntegrationTestImage.csproj \
    -c Release -r linux-musl-x64 \
    --self-contained true \
    -p:PublishSingleFile=true \
    -o /app/publish

# Final stage with Docker-in-Docker
FROM docker:26-dind
WORKDIR /app

# No runtime installation is required because the app is self-contained

# Pre-fetch Redis and Kafka images and store as archives
RUN dockerd-entrypoint.sh & \
    sleep 5 && \
    docker pull redis:latest && \
    docker pull confluentinc/cp-kafka:latest && \
    docker save redis:latest -o /redis.tar && \
    docker save confluentinc/cp-kafka:latest -o /kafka.tar && \
    pkill dockerd

COPY --from=build /app/publish/ .
COPY IntegrationTestImage/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
