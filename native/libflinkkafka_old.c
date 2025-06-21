#include "libflinkkafka.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <dlfcn.h>

// Forward declarations for librdkafka functions we'll load dynamically
typedef struct rd_kafka_s rd_kafka_t;
typedef struct rd_kafka_topic_s rd_kafka_topic_t;
typedef struct rd_kafka_conf_s rd_kafka_conf_t;

typedef enum {
    RD_KAFKA_PRODUCER,
    RD_KAFKA_CONSUMER
} rd_kafka_type_t;

typedef enum {
    RD_KAFKA_RESP_ERR_NO_ERROR = 0,
    RD_KAFKA_PARTITION_UA = -1
} rd_kafka_resp_err_t;

typedef enum {
    RD_KAFKA_TIMESTAMP_NOT_AVAILABLE,    
    RD_KAFKA_TIMESTAMP_CREATE_TIME,      
    RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME   
} rd_kafka_timestamp_type_t;

// Define the rd_kafka_message_t structure based on librdkafka
typedef struct rd_kafka_message_s {
    rd_kafka_resp_err_t err;   
    rd_kafka_topic_t *rkt;     
    int32_t partition;         
    void *payload;             
    size_t len;                
    void *key;                 
    size_t key_len;            
    int64_t offset;            
    void *_private;            
    int64_t timestamp;         
    rd_kafka_timestamp_type_t tstype;
    int32_t msg_flags;         
} rd_kafka_message_t;

typedef int32_t rd_kafka_partition_t;

typedef enum {
    RD_KAFKA_MSG_F_COPY = 0x2
} rd_kafka_msg_flag_t;

typedef enum {
    RD_KAFKA_CONF_OK = 0,
    RD_KAFKA_CONF_INVALID = 1,
    RD_KAFKA_CONF_UNKNOWN = 2
} rd_kafka_conf_res_t;

// Function pointers for dynamically loaded librdkafka functions
static rd_kafka_conf_t* (*rd_kafka_conf_new)(void) = NULL;
static void (*rd_kafka_conf_destroy)(rd_kafka_conf_t *conf) = NULL;
static rd_kafka_conf_res_t (*rd_kafka_conf_set)(rd_kafka_conf_t *conf, const char *name, const char *value, char *errstr, size_t errstr_size) = NULL;
static void (*rd_kafka_conf_set_dr_msg_cb)(rd_kafka_conf_t *conf, void (*dr_msg_cb)(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)) = NULL;
static void (*rd_kafka_conf_set_error_cb)(rd_kafka_conf_t *conf, void (*error_cb)(rd_kafka_t *rk, int err, const char *reason, void *opaque)) = NULL;
static rd_kafka_t* (*rd_kafka_new)(rd_kafka_type_t type, rd_kafka_conf_t *conf, char *errstr, size_t errstr_size) = NULL;
static void (*rd_kafka_destroy)(rd_kafka_t *rk) = NULL;
static rd_kafka_topic_t* (*rd_kafka_topic_new)(rd_kafka_t *rk, const char *topic, rd_kafka_conf_t *conf) = NULL;
static void (*rd_kafka_topic_destroy)(rd_kafka_topic_t *rkt) = NULL;
static int (*rd_kafka_produce_batch)(rd_kafka_topic_t *rkt, rd_kafka_partition_t partition, int msgflags, rd_kafka_message_t *rkmessages, int message_cnt) = NULL;
static rd_kafka_resp_err_t (*rd_kafka_flush)(rd_kafka_t *rk, int timeout_ms) = NULL;
static const char* (*rd_kafka_err2str)(rd_kafka_resp_err_t err) = NULL;
static rd_kafka_resp_err_t (*rd_kafka_last_error)(void) = NULL;
static int (*rd_kafka_outq_len)(rd_kafka_t *rk) = NULL;
static char* (*rd_kafka_dump)(rd_kafka_t *rk, FILE *fp) = NULL;

static void* librdkafka_handle = NULL;

// Global error buffer
static char last_error[512] = {0};

// Global error buffer
static char last_error[512] = {0};

// Load librdkafka dynamically
static int load_librdkafka() {
    if (librdkafka_handle) return 1; // Already loaded

    // Try to load from current directory first (where we have the .so file)
    librdkafka_handle = dlopen("./librdkafka.so", RTLD_LAZY);
    if (!librdkafka_handle) {
        // Try system library path
        librdkafka_handle = dlopen("librdkafka.so.1", RTLD_LAZY);
    }
    if (!librdkafka_handle) {
        librdkafka_handle = dlopen("librdkafka.so", RTLD_LAZY);
    }
    
    if (!librdkafka_handle) {
        set_last_error("Failed to load librdkafka: %s", dlerror());
        return 0;
    }

    // Load function symbols
    #define LOAD_SYMBOL(name) \
        name = dlsym(librdkafka_handle, #name); \
        if (!name) { \
            set_last_error("Failed to load symbol " #name ": %s", dlerror()); \
            dlclose(librdkafka_handle); \
            librdkafka_handle = NULL; \
            return 0; \
        }

    LOAD_SYMBOL(rd_kafka_conf_new);
    LOAD_SYMBOL(rd_kafka_conf_destroy);
    LOAD_SYMBOL(rd_kafka_conf_set);
    LOAD_SYMBOL(rd_kafka_conf_set_dr_msg_cb);
    LOAD_SYMBOL(rd_kafka_conf_set_error_cb);
    LOAD_SYMBOL(rd_kafka_new);
    LOAD_SYMBOL(rd_kafka_destroy);
    LOAD_SYMBOL(rd_kafka_topic_new);
    LOAD_SYMBOL(rd_kafka_topic_destroy);
    LOAD_SYMBOL(rd_kafka_produce_batch);
    LOAD_SYMBOL(rd_kafka_flush);
    LOAD_SYMBOL(rd_kafka_err2str);
    LOAD_SYMBOL(rd_kafka_last_error);
    LOAD_SYMBOL(rd_kafka_outq_len);
    LOAD_SYMBOL(rd_kafka_dump);

    return 1;
}

// Delivery report callback for high throughput (minimal processing)
static void delivery_report_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
    // Minimal callback to avoid blocking - just count errors for statistics
    if (rkmessage->err) {
        // Error occurred - could increment error counter here if needed
        // For maximum performance, we minimize work in this callback
    }
}

// Error callback
static void error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque) {
    set_last_error("Kafka error: %s", reason);
}

int flink_kafka_producer_init(flink_kafka_producer_t* producer, const flink_kafka_config_t* config) {
    if (!producer || !config) {
        set_last_error("Invalid parameters: producer or config is null");
        return FLINK_KAFKA_INVALID_PARAM;
    }

    if (!config->bootstrap_servers || !config->topic_name) {
        set_last_error("Invalid config: bootstrap_servers or topic_name is null");
        return FLINK_KAFKA_INVALID_PARAM;
    }

    // Load librdkafka dynamically
    if (!load_librdkafka()) {
        return FLINK_KAFKA_ERROR;
    }

    // Initialize producer structure
    memset(producer, 0, sizeof(flink_kafka_producer_t));

    // Create configuration
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    char errstr[512];

    // High-performance configuration based on the issue requirements
    // Bootstrap servers
    if (rd_kafka_conf_set(conf, "bootstrap.servers", config->bootstrap_servers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        set_last_error("Failed to set bootstrap.servers: %s", errstr);
        rd_kafka_conf_destroy(conf);
        return FLINK_KAFKA_ERROR;
    }

    // Batch settings for high throughput
    char batch_size_str[32];
    snprintf(batch_size_str, sizeof(batch_size_str), "%d", config->batch_size);
    rd_kafka_conf_set(conf, "batch.size", batch_size_str, errstr, sizeof(errstr));

    char linger_ms_str[32];
    snprintf(linger_ms_str, sizeof(linger_ms_str), "%d", config->linger_ms);
    rd_kafka_conf_set(conf, "linger.ms", linger_ms_str, errstr, sizeof(errstr));

    // Queue settings for large message volumes
    char queue_kb_str[32];
    snprintf(queue_kb_str, sizeof(queue_kb_str), "%d", config->queue_buffering_max_kbytes);
    rd_kafka_conf_set(conf, "queue.buffering.max.kbytes", queue_kb_str, errstr, sizeof(errstr));

    char queue_msgs_str[32];
    snprintf(queue_msgs_str, sizeof(queue_msgs_str), "%d", config->queue_buffering_max_messages);
    rd_kafka_conf_set(conf, "queue.buffering.max.messages", queue_msgs_str, errstr, sizeof(errstr));

    // Socket buffer settings for network optimization
    char send_buffer_str[32];
    snprintf(send_buffer_str, sizeof(send_buffer_str), "%d", config->socket_send_buffer_bytes);
    rd_kafka_conf_set(conf, "socket.send.buffer.bytes", send_buffer_str, errstr, sizeof(errstr));

    char recv_buffer_str[32];
    snprintf(recv_buffer_str, sizeof(recv_buffer_str), "%d", config->socket_receive_buffer_bytes);
    rd_kafka_conf_set(conf, "socket.receive.buffer.bytes", recv_buffer_str, errstr, sizeof(errstr));

    // Compression
    if (config->compression_type) {
        rd_kafka_conf_set(conf, "compression.type", config->compression_type, errstr, sizeof(errstr));
    }

    // Reliability settings
    if (config->enable_idempotence) {
        rd_kafka_conf_set(conf, "enable.idempotence", "true", errstr, sizeof(errstr));
    }

    char acks_str[8];
    if (config->acks == -1) {
        strcpy(acks_str, "all");
    } else {
        snprintf(acks_str, sizeof(acks_str), "%d", config->acks);
    }
    rd_kafka_conf_set(conf, "acks", acks_str, errstr, sizeof(errstr));

    // Retry settings
    char retries_str[32];
    snprintf(retries_str, sizeof(retries_str), "%d", config->retries);
    rd_kafka_conf_set(conf, "retries", retries_str, errstr, sizeof(errstr));

    // Timeout settings
    char request_timeout_str[32];
    snprintf(request_timeout_str, sizeof(request_timeout_str), "%d", config->request_timeout_ms);
    rd_kafka_conf_set(conf, "request.timeout.ms", request_timeout_str, errstr, sizeof(errstr));

    char message_timeout_str[32];
    snprintf(message_timeout_str, sizeof(message_timeout_str), "%d", config->message_timeout_ms);
    rd_kafka_conf_set(conf, "message.timeout.ms", message_timeout_str, errstr, sizeof(errstr));

    // Performance optimizations
    rd_kafka_conf_set(conf, "socket.nagle.disable", "true", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "socket.keepalive.enable", "true", errstr, sizeof(errstr));
    
    // Set callbacks for minimal overhead
    rd_kafka_conf_set_dr_msg_cb(conf, delivery_report_cb);
    rd_kafka_conf_set_error_cb(conf, error_cb);

    // Create producer
    producer->rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer->rk) {
        set_last_error("Failed to create producer: %s", errstr);
        return FLINK_KAFKA_ERROR;
    }

    // Create topic handle
    producer->rkt = rd_kafka_topic_new(producer->rk, config->topic_name, NULL);
    if (!producer->rkt) {
        set_last_error("Failed to create topic: %s", rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(producer->rk);
        return FLINK_KAFKA_ERROR;
    }

    // Store topic name (using malloc instead of strdup for compatibility)
    size_t topic_len = strlen(config->topic_name) + 1;
    producer->topic_name = malloc(topic_len);
    if (!producer->topic_name) {
        set_last_error("Failed to allocate memory for topic name");
        rd_kafka_topic_destroy(producer->rkt);
        rd_kafka_destroy(producer->rk);
        return FLINK_KAFKA_ERROR;
    }
    strcpy(producer->topic_name, config->topic_name);
    producer->initialized = 1;

    return FLINK_KAFKA_SUCCESS;
}

int flink_kafka_produce_batch(flink_kafka_producer_t* producer, 
                             const flink_kafka_message_t* messages, 
                             size_t message_count) {
    if (!producer || !producer->initialized) {
        set_last_error("Producer not initialized");
        return FLINK_KAFKA_PRODUCER_NOT_INITIALIZED;
    }

    if (!messages || message_count == 0) {
        set_last_error("Invalid parameters: messages is null or message_count is 0");
        return FLINK_KAFKA_INVALID_PARAM;
    }

    // Use rd_kafka_produce_batch for maximum performance
    rd_kafka_message_t *rkmessages = malloc(sizeof(rd_kafka_message_t) * message_count);
    if (!rkmessages) {
        set_last_error("Failed to allocate memory for batch");
        return FLINK_KAFKA_ERROR;
    }

    // Prepare messages for batch
    for (size_t i = 0; i < message_count; i++) {
        rkmessages[i].payload = messages[i].value;
        rkmessages[i].len = messages[i].value_len;
        rkmessages[i].key = messages[i].key;
        rkmessages[i].key_len = messages[i].key_len;
        rkmessages[i].partition = messages[i].partition >= 0 ? messages[i].partition : RD_KAFKA_PARTITION_UA;
        rkmessages[i]._private = NULL;
        rkmessages[i].err = RD_KAFKA_RESP_ERR_NO_ERROR;
    }

    // Produce batch
    int result = rd_kafka_produce_batch(producer->rkt, RD_KAFKA_PARTITION_UA, 
                                       RD_KAFKA_MSG_F_COPY, 
                                       rkmessages, message_count);

    free(rkmessages);

    if (result == -1) {
        set_last_error("Batch produce failed: %s", rd_kafka_err2str(rd_kafka_last_error()));
        return FLINK_KAFKA_ERROR;
    }

    return FLINK_KAFKA_SUCCESS;
}

int flink_kafka_producer_flush(flink_kafka_producer_t* producer, int timeout_ms) {
    if (!producer || !producer->initialized) {
        set_last_error("Producer not initialized");
        return FLINK_KAFKA_PRODUCER_NOT_INITIALIZED;
    }

    int result = rd_kafka_flush(producer->rk, timeout_ms);
    if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
        set_last_error("Flush failed: %s", rd_kafka_err2str(result));
        return FLINK_KAFKA_ERROR;
    }

    return FLINK_KAFKA_SUCCESS;
}

int flink_kafka_get_stats(flink_kafka_producer_t* producer, flink_kafka_stats_t* stats) {
    if (!producer || !producer->initialized || !stats) {
        set_last_error("Invalid parameters");
        return FLINK_KAFKA_INVALID_PARAM;
    }

    // Get basic statistics from librdkafka
    char *stats_json = rd_kafka_dump(producer->rk, NULL);
    if (stats_json) {
        // For now, zero out stats - could parse JSON for detailed stats
        memset(stats, 0, sizeof(flink_kafka_stats_t));
        free(stats_json);
    }

    // Get queue size for immediate feedback
    stats->queue_size = rd_kafka_outq_len(producer->rk);

    return FLINK_KAFKA_SUCCESS;
}

void flink_kafka_producer_destroy(flink_kafka_producer_t* producer) {
    if (!producer) return;

    if (producer->rkt && rd_kafka_topic_destroy) {
        rd_kafka_topic_destroy(producer->rkt);
        producer->rkt = NULL;
    }

    if (producer->rk && rd_kafka_flush && rd_kafka_destroy) {
        // Final flush with short timeout
        rd_kafka_flush(producer->rk, 5000);
        rd_kafka_destroy(producer->rk);
        producer->rk = NULL;
    }

    if (producer->topic_name) {
        free(producer->topic_name);
        producer->topic_name = NULL;
    }

    producer->initialized = 0;

    // Cleanup librdkafka handle when last producer is destroyed
    if (librdkafka_handle) {
        dlclose(librdkafka_handle);
        librdkafka_handle = NULL;
    }
}

const char* flink_kafka_get_last_error() {
    return last_error;
}