#include "data_processor.hpp"
#include "thread_pool.hpp"
#include <iostream>
#include <arrow/c/bridge.h>  // For converting DuckDB Arrow to Arrow C++
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <arrow/filesystem/localfs.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

void DataProcessor::WriteParquetFile(const std::shared_ptr<arrow::Table>& table, const std::string& filepath) {
    // Open file output stream
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
        outfile,
        arrow::io::FileOutputStream::Open(filepath)
    );

    // Set the Parquet writer properties
    std::shared_ptr<parquet::WriterProperties> writer_properties = parquet::WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build();
    std::shared_ptr<parquet::ArrowWriterProperties> arrow_properties = parquet::ArrowWriterProperties::Builder().store_schema()->build();

    // Create a Parquet file writer
    std::unique_ptr<parquet::arrow::FileWriter> parquet_writer;
    PARQUET_ASSIGN_OR_THROW(
        parquet_writer,
        parquet::arrow::FileWriter::Open(
            *table->schema().get(),                           // schema of the table
            arrow::default_memory_pool(),               // memory pool
            outfile,                                    // output stream
            writer_properties,                          // writer properties
            arrow_properties                            // arrow writer properties
        )
    );

    // Write the Arrow table to the Parquet file
    PARQUET_THROW_NOT_OK(parquet_writer->WriteTable(*table, table->num_rows()));

    // Finalize and close the writer
    PARQUET_THROW_NOT_OK(parquet_writer->Close());
    PARQUET_THROW_NOT_OK(outfile->Close());

    std::cout << "Data saved to " << filepath << std::endl;
}


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

// DataProcessor::process with multithreading
std::shared_ptr<arrow::Table> DataProcessor::process(const std::string& filepath) {
    ThreadPool pool(4);  // Maximum 4 threads for writing to parquet files
    std::string query = "SELECT * FROM parquet_scan('" + filepath + "')";
    duckdb_arrow result;

    if (duckdb_query_arrow(conn, query.c_str(), &result) != DuckDBSuccess) {
        std::cerr << "Error executing query and retrieving Arrow result" << std::endl;
        return nullptr;
    }

    auto arrow_schema = static_cast<duckdb_arrow_schema>(malloc(sizeof(struct ArrowSchema)));
    if (!arrow_schema) {
        std::cerr << "Failed to allocate memory for Arrow schema" << std::endl;
        free(arrow_schema);
        duckdb_destroy_arrow(&result);
        return nullptr;
    }

    if (duckdb_query_arrow_schema(result, &arrow_schema) != DuckDBSuccess) {
        std::cerr << "Error retrieving Arrow schema" << std::endl;
        free(arrow_schema);
        duckdb_destroy_arrow(&result);
        return nullptr;
    }

    std::shared_ptr<arrow::Schema> schema = arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(arrow_schema)).ValueOrDie();
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    int64_t total_rows = 0;
    int file_counter = 0;

    while (true) {
        auto arrow_array = static_cast<duckdb_arrow_array>(malloc(sizeof(struct ArrowArray)));
        if (!arrow_array) {
            std::cerr << "Failed to allocate memory for Arrow array" << std::endl;
            break;
        }

        if (duckdb_query_arrow_array(result, &arrow_array) != DuckDBSuccess) {
            free(arrow_array);
            break;
        }

        std::shared_ptr<arrow::RecordBatch> record_batch = arrow::ImportRecordBatch(reinterpret_cast<struct ArrowArray*>(arrow_array), schema).ValueOrDie();
        batches.push_back(record_batch);
        total_rows += record_batch->num_rows();

        if (total_rows >= 1000000) {
            // Capture the current batches and file counter to pass to the thread
            std::vector<std::shared_ptr<arrow::RecordBatch>> batches_to_write = std::move(batches);
            int current_file_counter = file_counter++;

            // Enqueue the task in the thread pool using enqueue_void for void return type
            pool.enqueue_void([batches_to_write, schema, current_file_counter, this] {
                auto arrow_table = arrow::Table::FromRecordBatches(schema, batches_to_write).ValueOrDie();
                std::string output_file = "./output_parquet/output_part_" + std::to_string(current_file_counter) + ".parquet";
                WriteParquetFile(arrow_table, output_file);
            });

            // Reset the batches and total rows
            batches.clear();
            total_rows = 0;
        }

        if (record_batch->num_rows() < 2048) {
            break;
        }

        free(arrow_array);
    }

    free(arrow_schema);
    duckdb_destroy_arrow(&result);

    return arrow::Table::FromRecordBatches(schema, batches).ValueOrDie();
}


