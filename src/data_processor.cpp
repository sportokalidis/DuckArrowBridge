#include "data_processor.hpp"
#include <duckdb.hpp>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <iostream>
#include <windows.h>


DataProcessor::DataProcessor() {
    db = std::make_unique<duckdb::DuckDB>(nullptr);
    conn = std::make_unique<duckdb::Connection>(*db);
}

void DataProcessor::loadParquet(const std::string& filepath) {
    try {
        std::string query = "CREATE TABLE tmp AS SELECT * FROM parquet_scan('" + filepath + "')"; // avoid it
        auto result = conn->Query(query);
        if (result->HasError()) {
            throw std::runtime_error(result->GetError());
        }
        /*result = conn->Query("SELECT * FROM tmp");
        while (true) {
            Sleep(500);
            auto chunk = result->Fetch();
            if (!chunk || chunk->size() == 0) {
                break;
            }
            std::cout << "Chunk size: " << chunk->size() << std::endl;
        }*/
    } catch (const std::exception &e) {
        std::cerr << "Error loading Parquet file: " << e.what() << std::endl;
    }
}

std::shared_ptr<arrow::Table> DataProcessor::process() {
    //auto result = conn->Query("SELECT * FROM tmp");
    auto result = conn->Query("SELECT * FROM '..\\data\\test_output_light.parquet'");
    //auto result = conn->Query("SELECT * FROM 'C:\\Users\\stavr\\OneDrive\\Desktop\\DuckArrowBridge\\test_output.parquet' WHERE id > 10000000 AND id < 20000000 ");
    //auto result2 =  conn->Prepare("SELECT * FROM 'C:\\Users\\stavr\\OneDrive\\Desktop\\DuckArrowBridge\\test_output.parquet' WHERE id > 10000000 AND id < 20000000 ");

    //result2->
    //result2->Execute()->Fetch()->
    // Create an Arrow Schema and ArrayData object


    if (result->HasError()) {
        std::cerr << "Query failed: " << result->GetError() << std::endl;
        return nullptr;
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> fields;

    // Use DuckToArrow
    // PyBinding to pythnon package
    // Test to win10
    // See the chunk size 
    while (true) {
        auto chunk = result->Fetch(); // 
        
        if (!chunk || chunk->size() == 0) {
            break;
        }
        //std::cout << "Chunk size: " << chunk->size() << std::endl;
        //Sleep(500);
        for (duckdb::idx_t col_idx = 0; col_idx < chunk->ColumnCount(); ++col_idx) {
            auto& vector = chunk->data[col_idx];
            auto logical_type = vector.GetType().id();
            auto column_name = result->names[col_idx];

           // std::cout << "Col Name: " << column_name << std::endl;
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

                // Check if the builder contains data before finishing
                int64_t length;
                length = builder.length();  // Check if there are values in the builder

                if (length > 0) {
                    std::cout << "Number of elements in builder: " << length << std::endl;
                }
                else {
                    std::cerr << "Builder is empty before Finish()" << std::endl;
                }

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
            else if (logical_type == duckdb::LogicalTypeId::FLOAT) {
                arrow::FloatBuilder builder;
                for (duckdb::idx_t row_idx = 0; row_idx < chunk->size(); ++row_idx) {
                    auto value = vector.GetValue(row_idx);
                    if (value.IsNull()) {
                        builder.AppendNull();
                    } else {
                        builder.Append(value.GetValue<float>());
                    }
                }

                std::shared_ptr<arrow::Array> array;
                builder.Finish(&array);
                arrays.push_back(array);
                fields.push_back(arrow::field(column_name, arrow::float32()));
            } 
            else {
                std::cerr << "Unsupported data type in column: " << column_name << std::endl;
                return nullptr;
            }
        }
    }

    auto schema = std::make_shared<arrow::Schema>(fields);

    return arrow::Table::Make(schema, arrays);
}
