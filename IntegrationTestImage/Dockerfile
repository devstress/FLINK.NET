# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish IntegrationTestImage/IntegrationTestImage.csproj -c Release -o /app/publish

# Final stage with Docker-in-Docker
FROM docker:26-dind
WORKDIR /app

# Install bash required by dotnet-install script
RUN apk add --no-cache bash

# Install .NET 8 runtime and Aspire workload
ENV DOTNET_ROOT=/usr/share/dotnet
ENV PATH="$PATH:/usr/share/dotnet"
RUN wget https://dot.net/v1/dotnet-install.sh -O dotnet-install.sh \
    && chmod +x dotnet-install.sh \
    && ./dotnet-install.sh --version 8.0.116 --install-dir $DOTNET_ROOT \
    && ln -s $DOTNET_ROOT/dotnet /usr/bin/dotnet \
    && dotnet workload install aspire

# Pre-fetch Redis and Kafka images and store as archives
RUN dockerd-entrypoint.sh & \
    sleep 5 && \
    docker pull redis:latest && \
    docker pull confluentinc/cp-kafka:latest && \
    docker save redis:latest -o /redis.tar && \
    docker save confluentinc/cp-kafka:latest -o /kafka.tar && \
    pkill dockerd

COPY --from=build /app/publish .
COPY IntegrationTestImage/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
