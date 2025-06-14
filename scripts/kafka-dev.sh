#!/bin/bash

# Kafka Development Environment Setup Script
# Following Apache Flink.Net best practices

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.kafka.yml"

function show_help {
    echo "Kafka Development Environment for Flink.Net"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start     Start Kafka development environment"
    echo "  stop      Stop Kafka development environment"
    echo "  restart   Restart Kafka development environment"
    echo "  status    Show status of services"
    echo "  logs      Show logs from all services"
    echo "  ui        Open Kafka UI in browser"
    echo "  topics    List all topics"
    echo "  monitor   Monitor consumer groups"
    echo "  test      Run basic connectivity test"
    echo "  cleanup   Remove all data and containers"
    echo "  help      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 start                 # Start Kafka environment"
    echo "  $0 logs kafka            # Show Kafka broker logs"
    echo "  $0 monitor               # Monitor consumer groups"
}

function start_kafka {
    echo "🚀 Starting Kafka development environment..."
    docker-compose -f "$COMPOSE_FILE" up -d
    
    echo "⏳ Waiting for services to be ready..."
    sleep 10
    
    echo "✅ Kafka environment started successfully!"
    echo ""
    echo "📊 Kafka UI: http://localhost:8080"
    echo "🔌 Kafka Bootstrap Servers: localhost:9092"
    echo ""
    echo "Topics created:"
    docker-compose -f "$COMPOSE_FILE" exec kafka kafka-topics --list --bootstrap-server localhost:9092
}

function stop_kafka {
    echo "🛑 Stopping Kafka development environment..."
    docker-compose -f "$COMPOSE_FILE" down
    echo "✅ Kafka environment stopped."
}

function restart_kafka {
    echo "🔄 Restarting Kafka development environment..."
    stop_kafka
    sleep 5
    start_kafka
}

function show_status {
    echo "📊 Kafka Environment Status:"
    docker-compose -f "$COMPOSE_FILE" ps
}

function show_logs {
    if [ -n "$2" ]; then
        echo "📋 Showing logs for $2..."
        docker-compose -f "$COMPOSE_FILE" logs -f "$2"
    else
        echo "📋 Showing logs for all services..."
        docker-compose -f "$COMPOSE_FILE" logs -f
    fi
}

function open_ui {
    echo "🌐 Opening Kafka UI..."
    if command -v xdg-open > /dev/null; then
        xdg-open http://localhost:8080
    elif command -v open > /dev/null; then
        open http://localhost:8080
    else
        echo "Please open http://localhost:8080 in your browser"
    fi
}

function list_topics {
    echo "📝 Kafka Topics:"
    docker-compose -f "$COMPOSE_FILE" exec kafka kafka-topics --list --bootstrap-server localhost:9092
    echo ""
    echo "📊 Topic Details:"
    docker-compose -f "$COMPOSE_FILE" exec kafka kafka-topics --describe --bootstrap-server localhost:9092
}

function monitor_consumers {
    echo "👥 Consumer Groups:"
    docker-compose -f "$COMPOSE_FILE" exec kafka kafka-consumer-groups --list --bootstrap-server localhost:9092
    echo ""
    echo "📊 Consumer Group Details (if any):"
    GROUPS=$(docker-compose -f "$COMPOSE_FILE" exec kafka kafka-consumer-groups --list --bootstrap-server localhost:9092 2>/dev/null | tr -d '\r')
    for group in $GROUPS; do
        if [ -n "$group" ]; then
            echo "Group: $group"
            docker-compose -f "$COMPOSE_FILE" exec kafka kafka-consumer-groups --describe --group "$group" --bootstrap-server localhost:9092 2>/dev/null || true
            echo ""
        fi
    done
}

function test_connectivity {
    echo "🧪 Testing Kafka connectivity..."
    
    echo "1. Testing topic creation..."
    docker-compose -f "$COMPOSE_FILE" exec kafka kafka-topics --create --if-not-exists \
        --bootstrap-server localhost:9092 --topic test-connectivity --partitions 1 --replication-factor 1
    
    echo "2. Testing producer..."
    echo "test-message-$(date +%s)" | docker-compose -f "$COMPOSE_FILE" exec -T kafka \
        kafka-console-producer --broker-list localhost:9092 --topic test-connectivity
    
    echo "3. Testing consumer..."
    timeout 5s docker-compose -f "$COMPOSE_FILE" exec kafka \
        kafka-console-consumer --bootstrap-server localhost:9092 --topic test-connectivity --from-beginning || true
    
    echo "✅ Connectivity test completed!"
}

function cleanup_kafka {
    echo "🧹 Cleaning up Kafka environment..."
    read -p "This will remove all Kafka data and containers. Are you sure? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose -f "$COMPOSE_FILE" down -v --remove-orphans
        docker volume prune -f
        echo "✅ Cleanup completed!"
    else
        echo "❌ Cleanup cancelled."
    fi
}

# Main command handling
case "${1:-help}" in
    start)
        start_kafka
        ;;
    stop)
        stop_kafka
        ;;
    restart)
        restart_kafka
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs "$@"
        ;;
    ui)
        open_ui
        ;;
    topics)
        list_topics
        ;;
    monitor)
        monitor_consumers
        ;;
    test)
        test_connectivity
        ;;
    cleanup)
        cleanup_kafka
        ;;
    help)
        show_help
        ;;
    *)
        echo "❌ Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac