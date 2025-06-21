CXXFLAGS=-fPIC -O2 -I/usr/include/librdkafka
LDFLAGS=-shared -L/usr/lib -lrdkafka
TARGET=libnativekafkabridge.so
SRCS=nativekafkabridge.cpp

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -f $(TARGET)
