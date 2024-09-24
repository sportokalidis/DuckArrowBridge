#include "data_processor.hpp"
#include <iostream>
#include <arrow/c/bridge.h>  // For converting DuckDB Arrow to Arrow C++
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

DataProcessor::DataProcessor() {
    // Initialize DuckDB
    if (duckdb_open(NULL, &db) != DuckDBSuccess) {
        throw std::runtime_error("Failed to open DuckDB in-memory database");
    }

    if (duckdb_connect(db, &conn) != DuckDBSuccess) {
        throw std::runtime_error("Failed to connect to DuckDB");
    }
}

void DataProcessor::loadParquet(const std::string& filepath) {
    // Construct the SQL query to load the Parquet file
    std::string query = "SELECT * FROM parquet_scan('" + filepath + "')";
    duckdb_arrow result;

    if (duckdb_query_arrow(conn, query.c_str(), &result) != DuckDBSuccess) {
        throw std::runtime_error("Error loading Parquet file");
    }

    // Save the result in the DataProcessor object
    // You can store the result if needed for later access
}

std::shared_ptr<arrow::Table> DataProcessor::process(const std::string& filepath) {
    //std::string filepath = "..\\data\\test_output_large.parquet";  // Example file path
    std::string query = "SELECT * FROM parquet_scan('" + filepath + "')";
    duckdb_arrow result;

    if (duckdb_query_arrow(conn, query.c_str(), &result) != DuckDBSuccess) {
        std::cerr << "Error executing query and retrieving Arrow result" << std::endl;
        return nullptr;
    }

    // Allocate memory for ArrowSchema
    auto arrow_schema = static_cast<duckdb_arrow_schema>(malloc(sizeof(struct ArrowSchema)));
    if (!arrow_schema) {
        std::cerr << "Failed to allocate memory for Arrow schema" << std::endl;
        free(arrow_schema);
        duckdb_destroy_arrow(&result);
        return nullptr;
    }

    // Get Arrow schema
    if (duckdb_query_arrow_schema(result, &arrow_schema) != DuckDBSuccess) {
        std::cerr << "Error retrieving Arrow schema" << std::endl;
        free(arrow_schema);
        duckdb_destroy_arrow(&result);
        return nullptr;
    }

    // Import Arrow schema into Apache Arrow C++ object
    std::shared_ptr<arrow::Schema> schema = arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(arrow_schema)).ValueOrDie();

    // Vector to hold record batches
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    // Loop to fetch batches
    while (true) {
        auto arrow_array = static_cast<duckdb_arrow_array>(malloc(sizeof(struct ArrowArray)));
        if (!arrow_array) {
            std::cerr << "Failed to allocate memory for Arrow array" << std::endl;
            break;
        }

        // Get the next batch
        if (duckdb_query_arrow_array(result, &arrow_array) != DuckDBSuccess) {
            free(arrow_array);
            break;  // No more batches
        }

        std::shared_ptr<arrow::RecordBatch> record_batch = arrow::ImportRecordBatch(reinterpret_cast<struct ArrowArray*>(arrow_array), schema).ValueOrDie();
        batches.push_back(record_batch);

        free(arrow_array);

        // Stop if batch size is less than expected (no more data)
        // TODO: Find other way
        if (record_batch->num_rows() < 2048) {
            break;
        }
    }

    // Free Arrow schema memory
    free(arrow_schema);
    duckdb_destroy_arrow(&result);

    // Combine all batches into a single table
    return arrow::Table::FromRecordBatches(schema, batches).ValueOrDie();
}
