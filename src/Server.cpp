#include <cstring>
#include <iostream>
#include <fstream>

int main(int argc, char* argv[]) {
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cerr << "Logs from your program will appear here" << std::endl;

    if (argc != 3) {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }

    std::string database_file_path = argv[1];
    std::string command = argv[2];

    if (command == ".dbinfo") {
        std::ifstream database_file(database_file_path, std::ios::binary);
        if (!database_file) {
            std::cerr << "Failed to open the database file" << std::endl;
            return 1;
        }

        // Uncomment this to pass the first stage
        database_file.seekg(16);  // Skip the first 16 bytes of the header
        
        char buffer[2];
        database_file.read(buffer, 2);
        
        unsigned short page_size = (static_cast<unsigned char>(buffer[1]) | (static_cast<unsigned char>(buffer[0]) << 8));
        
        std::cout << "database page size: " << page_size << std::endl;

        // Read number of cells on the sqlite_schema page (page 1)
        // The b-tree page header starts immediately after the 100-byte file header.
        // The 2-byte big-endian cell count is located at offset 3 within the b-tree page header.
        database_file.seekg(100 + 3);
        char cell_count_bytes[2];
        database_file.read(cell_count_bytes, 2);
        unsigned short number_of_tables = (static_cast<unsigned char>(cell_count_bytes[1]) |
                                           (static_cast<unsigned char>(cell_count_bytes[0]) << 8));

        std::cout << "number of tables: " << number_of_tables << std::endl;
    }

    return 0;
}
