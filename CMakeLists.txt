cmake_minimum_required(VERSION 3.10)
project(DuckArrowBridge)

set(CMAKE_CXX_STANDARD 17)

# Set runtime library to Multi-threaded Debug
# set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")

# Use vcpkg for dependencies
# if(CMAKE_BUILD_TYPE STREQUAL "Debug")
#     set(DCMAKE_PREFIX_PATH "C:/vcpkg/installed/x64-windows/")
# else()
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

## Find Arrow package
find_package(Arrow CONFIG REQUIRED)


## Link libraries
target_link_libraries(${PROJECT_NAME} PRIVATE duckdb Arrow::arrow_shared)



# Define the DLL directory based on the build configuration
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(DLL_DIR "${PROJECT_SOURCE_DIR}/dll/Debug")
else()
    set(DLL_DIR "${PROJECT_SOURCE_DIR}/dll")
endif()

# List all required DLLs (add other required DLLs to this list)
set(REQUIRED_DLLS
#    "${DLL_DIR}/arrow.dll"
#    "${DLL_DIR}/brotlicommon.dll"
#    "${DLL_DIR}/brotlidec.dll"
#    "${DLL_DIR}/brotlienc.dll"
#    "${DLL_DIR}/bz2d.dll"
    "${DLL_DIR}/duckdb.dll"
#    "${DLL_DIR}/lz4d.dll"
#    "${DLL_DIR}/snappy.dll"
#    "${DLL_DIR}/zlibd1.dll"
#    "${DLL_DIR}/zstd.dll"
    # Add more DLLs here if needed
)

# For Windows, copy the DLL to the output directory
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    # Ensure the executable path is used for the destination of the DLLs
    foreach(DLL ${REQUIRED_DLLS})
        add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy
            ${DLL} $<TARGET_FILE_DIR:${PROJECT_NAME}>)
    endforeach()

    # add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
    #     COMMAND ${CMAKE_COMMAND} -E copy
    #     ${PROJECT_SOURCE_DIR}/dll/Debug/duckdb.dll
    #     $<TARGET_FILE_DIR:${PROJECT_NAME}>)
else()
    add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
        ${PROJECT_SOURCE_DIR}/dll/duckdb.dll
        $<TARGET_FILE_DIR:${PROJECT_NAME}>)
endif()