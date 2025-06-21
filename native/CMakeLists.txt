cmake_minimum_required(VERSION 3.16)
project(libflinkkafka)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)

# Find librdkafka
find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED rdkafka)

# Add include directories
include_directories(${RDKAFKA_INCLUDE_DIRS})

# Create shared library
add_library(flinkkafka SHARED libflinkkafka.c)

# Link libraries
target_link_libraries(flinkkafka ${RDKAFKA_LIBRARIES})

# Set library properties for maximum performance
set_target_properties(flinkkafka PROPERTIES
    VERSION 1.0.0
    SOVERSION 1
    C_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN 1
)

# Compiler optimizations for high performance
target_compile_options(flinkkafka PRIVATE 
    -O3                    # Maximum optimization
    -march=native          # Use native CPU instructions
    -flto                  # Link-time optimization
    -ffast-math           # Fast math operations
    -funroll-loops        # Loop unrolling
)

# Install library
install(TARGETS flinkkafka
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

install(FILES libflinkkafka.h DESTINATION include)