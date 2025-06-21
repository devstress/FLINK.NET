#pragma once
#ifdef __cplusplus
extern "C" {
#endif
void* nk_init_producer(const char* brokers, const char* topic, int partition_count);
int nk_produce_batch(void* handle, const char** messages, const int* lengths, int count);
void nk_flush(void* handle);
void nk_destroy(void* handle);
#ifdef __cplusplus
}
#endif
