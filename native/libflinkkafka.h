#ifndef LIBFLINKKAFKA_H
#define LIBFLINKKAFKA_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

// Forward declarations
typedef struct rd_kafka_s rd_kafka_t;
typedef struct rd_kafka_topic_s rd_kafka_topic_t;

// Error codes
#define FLINK_KAFKA_SUCCESS 0
#define FLINK_KAFKA_ERROR -1
#define FLINK_KAFKA_INVALID_PARAM -2
#define FLINK_KAFKA_PRODUCER_NOT_INITIALIZED -3

// Message structure for batch operations
typedef struct {
    void* key;
    size_t key_len;
    void* value;
    size_t value_len;
    int32_t partition;
} flink_kafka_message_t;

// Producer handle
typedef struct {
    rd_kafka_t* rk;
    rd_kafka_topic_t* rkt;
    char* topic_name;
    int initialized;
} flink_kafka_producer_t;

// Configuration structure for high-performance settings
typedef struct {
    const char* bootstrap_servers;
    const char* topic_name;
    int32_t batch_size;
    int32_t linger_ms;
    int32_t queue_buffering_max_kbytes;
    int32_t queue_buffering_max_messages;
    int32_t socket_send_buffer_bytes;
    int32_t socket_receive_buffer_bytes;
    const char* compression_type;
    int enable_idempotence;
    int acks;
    int retries;
    int32_t request_timeout_ms;
    int32_t message_timeout_ms;
} flink_kafka_config_t;

// Initialize producer with high-performance configuration
int flink_kafka_producer_init(flink_kafka_producer_t* producer, const flink_kafka_config_t* config);

// Produce messages in batch for maximum throughput
int flink_kafka_produce_batch(flink_kafka_producer_t* producer, 
                             const flink_kafka_message_t* messages, 
                             size_t message_count);

// Flush all pending messages
int flink_kafka_producer_flush(flink_kafka_producer_t* producer, int timeout_ms);

// Get statistics about producer performance
typedef struct {
    uint64_t messages_produced;
    uint64_t messages_failed;
    uint64_t bytes_produced;
    uint64_t queue_size;
    double current_rate;
} flink_kafka_stats_t;

int flink_kafka_get_stats(flink_kafka_producer_t* producer, flink_kafka_stats_t* stats);

// Cleanup producer
void flink_kafka_producer_destroy(flink_kafka_producer_t* producer);

// Get last error message
const char* flink_kafka_get_last_error();

#ifdef __cplusplus
}
#endif

#endif // LIBFLINKKAFKA_H