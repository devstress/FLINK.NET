#include "nativekafkabridge.h"
#include <librdkafka/rdkafka.h>
#include <cstring>
#include <cstdlib>

struct nk_handle {
    rd_kafka_t* rk;
    rd_kafka_topic_t* rkt;
};

extern "C" {

void* nk_init_producer(const char* brokers, const char* topic, int partition_count) {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", brokers, nullptr, 0);
    rd_kafka_conf_set(conf, "queue.buffering.max.messages", "1000000", nullptr, 0);
    rd_kafka_conf_set(conf, "linger.ms", "5", nullptr, 0);
    rd_kafka_conf_set(conf, "enable.idempotence", "true", nullptr, 0);
    rd_kafka_conf_set(conf, "acks", "all", nullptr, 0);

    char errstr[512];
    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) return nullptr;

    rd_kafka_topic_conf_t* tconf = rd_kafka_topic_conf_new();
    rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk, topic, tconf);
    if (!rkt) {
        rd_kafka_destroy(rk);
        return nullptr;
    }
    nk_handle* handle = (nk_handle*)malloc(sizeof(nk_handle));
    handle->rk = rk;
    handle->rkt = rkt;
    return handle;
}

int nk_produce_batch(void* handle_ptr, const char** messages, const int* lengths, int count) {
    nk_handle* handle = (nk_handle*)handle_ptr;
    rd_kafka_message_t* msgs = (rd_kafka_message_t*)calloc(count, sizeof(rd_kafka_message_t));
    for (int i = 0; i < count; ++i) {
        msgs[i].payload = (void*)messages[i];
        msgs[i].len = lengths[i];
    }
    int produced = rd_kafka_produce_batch(handle->rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY, msgs, count);
    free(msgs);
    return produced;
}

void nk_flush(void* handle_ptr) {
    nk_handle* handle = (nk_handle*)handle_ptr;
    rd_kafka_flush(handle->rk, 10000);
}

void nk_destroy(void* handle_ptr) {
    nk_handle* handle = (nk_handle*)handle_ptr;
    rd_kafka_topic_destroy(handle->rkt);
    rd_kafka_destroy(handle->rk);
    free(handle);
}

}
