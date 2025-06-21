CC=gcc
CFLAGS=-O3 -march=native -fPIC -Wall -Wextra -std=c11
LDFLAGS=-shared -ldl

# Output library name
TARGET=libflinkkafka.so

# Source files
SOURCES=libflinkkafka.c

# Build rule
$(TARGET): $(SOURCES)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $(TARGET) $(SOURCES)

# Clean rule
clean:
	rm -f $(TARGET)

# Install rule - copy to scripts/producer directory where librdkafka.so is located
install: $(TARGET)
	cp $(TARGET) ../scripts/producer/

.PHONY: clean install