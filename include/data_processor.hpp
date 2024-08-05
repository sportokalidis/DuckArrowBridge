#ifndef DATA_PROCESSOR_HPP
#define DATA_PROCESSOR_HPP

#include <string>
#include <memory>
#include "duckdb.hpp"
#include <arrow/api.h>


class DataProcessor {
public:
    DataProcessor();
    void loadParquet(const std::string& filepath);
     std::shared_ptr<arrow::Table> process();
private:
    std::unique_ptr<duckdb::DuckDB> db;
    std::unique_ptr<duckdb::Connection> conn;
};

#endif // DATA_PROCESSOR_HPP
