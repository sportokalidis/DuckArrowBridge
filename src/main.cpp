#include <iostream>
#include <string>
#include "data_processor.hpp"

// Function to print the data in an Apache Arrow Table
void PrintArrowTable(const std::shared_ptr<arrow::Table>& table) {
    if (!table) {
        std::cerr << "The table is empty or invalid." << std::endl;
        return;
    }

    // Print column names using the table's schema
    std::cout << "Num of Columns: " << table->num_columns() << std::endl;
    for (int i = 0; i < table->num_columns(); ++i) {
        std::cout << "Num of Columns: " << table->num_columns() << std::endl;
        std::cout << table->schema()->field(i)->name() << "\t";
    }
    std::cout << std::endl;

    // Print row data
    for (int64_t row_idx = 0; row_idx < table->num_rows(); ++row_idx) {
        for (int col_idx = 0; col_idx < table->num_columns(); ++col_idx) {
            auto column = table->column(col_idx);
            auto array = std::static_pointer_cast<arrow::Int32Array>(column->chunk(0)); // Assuming only one chunk

            switch (array->type_id()) {
                case arrow::Type::INT32: {
                    auto int_array = std::static_pointer_cast<arrow::Int32Array>(array);
                    if (int_array->IsNull(row_idx)) {
                        std::cout << "NULL";
                    } else {
                        std::cout << int_array->Value(row_idx);
                    }
                    break;
                }
                // Add cases for other data types as needed
                default:
                    std::cout << "Unsupported type";
                    break;
            }
            std::cout << "\t";
        }
        std::cout << std::endl;
    }
}

int main(int argc, char* argv[]) {
    // if (argc < 2) {
    //     std::cerr << "Usage: " << argv[0] << " <parquet_file>" << std::endl;
    //     return 1;
    // }

    std::string filepath = "C:\\Users\\stavr\\OneDrive\\Desktop\\DuckArrowBridge\\example.parquet";

    DataProcessor processor;
    processor.loadParquet(filepath);

    std::shared_ptr<arrow::Table> table = processor.process();

    if (table) {
        std::cout << "Successfully processed data into Arrow Table." << std::endl;
        PrintArrowTable(table);
    } else {
        std::cerr << "Failed to process data." << std::endl;
    }

    return 0;
}
