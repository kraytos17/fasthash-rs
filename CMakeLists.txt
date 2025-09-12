cmake_minimum_required(VERSION 3.25.0)
project(FastHash VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_DEBUG_POSTFIX "-d")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)
FetchContent_Declare(nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json
    GIT_TAG v3.12.0
)

FetchContent_MakeAvailable(nlohmann_json)

add_executable(FastHash
    src/main.cpp
    src/core/command.cpp
    src/core/fast-hash.cpp
    src/core/ttl_manager.cpp
    src/core/parser.cpp
    src/persistence/aof/aof.cpp
)

target_include_directories(FastHash
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(FastHash
    PRIVATE
    nlohmann_json::nlohmann_json
)

if (MSVC)
    target_compile_options(FastHash PRIVATE /W4 /permissive-)
else()
    target_compile_options(FastHash PRIVATE -Wall -Wextra -Wpedantic)
endif()

include(CTest)
enable_testing()

include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.tar.gz
)
set(INSTALL_GTEST OFF CACHE BOOL "Disable installation of googletest" FORCE)
FetchContent_MakeAvailable(googletest)

add_subdirectory(tests)
