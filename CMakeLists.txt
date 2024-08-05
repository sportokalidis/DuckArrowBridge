cmake_minimum_required(VERSION 3.10)
project(DuckArrowBridge)

set(CMAKE_CXX_STANDARD 17)

# Set runtime library to Multi-threaded Debug
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")

# Use vcpkg for dependencies
# if(NOT DEFINED DCMAKE_PREFIX_PATH)
#     set(DCMAKE_PREFIX_PATH "C:/vcpkg/installed/x64-windows")
# endif()


# Include directories
include_directories(include)

# Link directories
link_directories(lib)

# Add source files, excluding main.cpp
file(GLOB SOURCES "src/*.cpp")

# Add the executable
add_executable(${PROJECT_NAME} ${SOURCES})

# Find Arrow package
find_package(Arrow CONFIG REQUIRED)

# Link libraries
target_link_libraries(${PROJECT_NAME} PRIVATE duckdb Arrow::arrow_shared)


# For Windows, copy the DLL to the output directory
if(WIN32)
    add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
        ${PROJECT_SOURCE_DIR}/dll/duckdb.dll
        $<TARGET_FILE_DIR:${PROJECT_NAME}>)
endif()