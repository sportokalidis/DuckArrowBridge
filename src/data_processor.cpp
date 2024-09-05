#include "data_processor.hpp"
#include <duckdb.hpp>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <iostream>

DataProcessor::DataProcessor() {
    db = std::make_unique<duckdb::DuckDB>(nullptr);
    conn = std::make_unique<duckdb::Connection>(*db);
}

void DataProcessor::loadParquet(const std::string& filepath) {
    try {
        std::string query = "CREATE TABLE tmp AS SELECT * FROM parquet_scan('" + filepath + "')";
        auto result = conn->Query(query);
        if (result->HasError()) {
            throw std::runtime_error(result->GetError());
        }
    } catch (const std::exception &e) {
        std::cerr << "Error loading Parquet file: " << e.what() << std::endl;
    }
}

std::shared_ptr<arrow::Table> DataProcessor::process() {
    // Example query that processes data
    auto result = conn->Query("SELECT * FROM tmp");

    if (result->HasError()) {
        std::cerr << "Query failed: " << result->GetError() << std::endl;
        return nullptr;
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> fields;

    while (true) {
        auto chunk = result->Fetch();
        if (!chunk || chunk->size() == 0) {
            break;
        }

        for (duckdb::idx_t col_idx = 0; col_idx < chunk->ColumnCount(); ++col_idx) {
            auto& vector = chunk->data[col_idx];
            auto logical_type = vector.GetType().id();
            auto column_name = result->names[col_idx];

            std::cout  << " -- column name: " << column_name << std::endl;
            if (logical_type == duckdb::LogicalTypeId::INTEGER) {
                arrow::Int32Builder builder;
                for (duckdb::idx_t row_idx = 0; row_idx < chunk->size(); ++row_idx) {
                    auto value = vector.GetValue(row_idx);
                    if (value.IsNull()) {
                        builder.AppendNull();
                    } else {
                        builder.Append(value.GetValue<int32_t>());
                    }
                }

                std::shared_ptr<arrow::Array> array;
                builder.Finish(&array);
                arrays.push_back(array);
                fields.push_back(arrow::field(column_name, arrow::int32()));
            }
            else if (logical_type == duckdb::LogicalTypeId::VARCHAR) {
                arrow::StringBuilder builder;
                for (duckdb::idx_t row_idx = 0; row_idx < chunk->size(); ++row_idx) {
                    auto value = vector.GetValue(row_idx);
                    if (value.IsNull()) {
                        builder.AppendNull();
                    } else {
                        builder.Append(value.GetValue<std::string>());
                    }
                }

                std::shared_ptr<arrow::Array> array;
                builder.Finish(&array);
                arrays.push_back(array);
                fields.push_back(arrow::field(column_name, arrow::utf8()));
            } 
            else {
                std::cerr << "Unsupported data type in column: " << column_name << std::endl;
                return nullptr;
            }
            // Handle other data types as needed
        }
    }

    auto schema = std::make_shared<arrow::Schema>(fields);
    return arrow::Table::Make(schema, arrays);
}
