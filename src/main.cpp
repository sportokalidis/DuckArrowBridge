#include <iostream>
#include <string>
#include "data_processor.hpp"

#include <chrono>

// Function to print the data in an Apache Arrow Table
void PrintArrowTable(const std::shared_ptr<arrow::Table>& table) {
    if (!table) {
        std::cerr << "The table is empty or invalid." << std::endl;
        return;
    }

    // Print column names using the table's schema
    for (int i = 0; i < table->num_columns(); ++i) {
        std::cout << table->schema()->field(i)->name() << "\t";
    }
    std::cout << std::endl;

    // Print row data
    for (int64_t row_idx = 0; row_idx < table->num_rows(); ++row_idx) {
        for (int col_idx = 0; col_idx < table->num_columns(); ++col_idx) {
            auto column = table->column(col_idx);
            auto array = column->chunk(0); // Assuming there's only one chunk for simplicity

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
                case arrow::Type::STRING: {
                    auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
                    if (string_array->IsNull(row_idx)) {
                        std::cout << "NULL";
                    } else {
                        std::cout << string_array->GetString(row_idx);
                    }
                    break;
                }
                case arrow::Type::FLOAT: {
                    auto float_array = std::static_pointer_cast<arrow::FloatArray>(array);
                    if (float_array->IsNull(row_idx)) {
                        std::cout << "NULL";
                    } else {
                        std::cout << float_array->Value(row_idx);
                    }
                    break;
                }
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
     /*if (argc < 2) {
         std::cerr << "Usage: " << argv[0] << " <parquet_file>" << std::endl;
         return 1;
     }*/

    // TODO: Add Check method for parquet file and input via json file 
    // std::string filepath = argv[1];
    std::string filepath = "..\\data\\test_output_light.parquet";
   
    bool printTable = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--enable-print" /*|| arg == "-e"*/) {
            printTable = true;  // Set the flag to true if found
        }
    }

    DataProcessor processor;
    processor.loadParquet(filepath);

     // Start time point
    auto start = std::chrono::high_resolution_clock::now();
    std::shared_ptr<arrow::Table> table = processor.process();
    auto end = std::chrono::high_resolution_clock::now();

    // Calculate the duration
    std::chrono::duration<double> elapsed = end - start;

    // Output the elapsed time in seconds
    std::cout << "Time taken by process function: " << elapsed.count() << " seconds." << std::endl;

    if (table) {
        std::cout << "Successfully processed data into Arrow Table." << std::endl;
        if(printTable)
            PrintArrowTable(table);
    } else {
        std::cerr << "Failed to process data." << std::endl;
    }

    return 0;
}
