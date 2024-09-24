#ifndef DATA_PROCESSOR_HPP
#define DATA_PROCESSOR_HPP

#include <string>
#include <memory>
#include "duckdb.h"  // Include DuckDB C API header
#include <arrow/api.h>

class DataProcessor {
public:
    DataProcessor();
    void loadParquet(const std::string& filepath);
    std::shared_ptr<arrow::Table> process(const std::string& filepath);
private:
    duckdb_database db;
    duckdb_connection conn;
};

#endif // DATA_PROCESSOR_HPP
