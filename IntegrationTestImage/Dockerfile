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
# Use the rootless variant so the container can run without --privileged
FROM docker:26-dind-rootless
WORKDIR /app

# No runtime installation is required because the app is self-contained
COPY --from=build /app/publish/ .
COPY IntegrationTestImage/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
