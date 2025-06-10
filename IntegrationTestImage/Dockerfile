# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY . .

# Install Aspire workload so publishing succeeds
RUN dotnet workload install aspire

RUN dotnet build IntegrationTestImage/IntegrationTestImage.csproj -c Release

# Final stage with Docker-in-Docker
FROM docker:26-dind
WORKDIR /app

# Install bash required by dotnet-install script and then install .NET
RUN apk add --no-cache bash \
    && wget https://dot.net/v1/dotnet-install.sh -O dotnet-install.sh \
    && chmod +x dotnet-install.sh \
    && bash ./dotnet-install.sh --version 8.0.116 --install-dir /usr/share/dotnet \
    && ln -s /usr/share/dotnet/dotnet /usr/bin/dotnet \
    && dotnet workload install aspire

ENV DOTNET_ROOT=/usr/share/dotnet
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$DOTNET_ROOT

# Pre-fetch Redis and Kafka images and store as archives
RUN dockerd-entrypoint.sh & \
    sleep 5 && \
    docker pull redis:latest && \
    docker pull confluentinc/cp-kafka:latest && \
    docker save redis:latest -o /redis.tar && \
    docker save confluentinc/cp-kafka:latest -o /kafka.tar && \
    pkill dockerd

COPY --from=build /src/IntegrationTestImage/bin/Release/net8.0/ .
COPY IntegrationTestImage/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
