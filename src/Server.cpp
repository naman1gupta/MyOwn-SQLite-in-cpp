#include <cstring>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <utility>
#include <cstdint>

static std::pair<uint64_t, size_t> readVarint(const std::vector<unsigned char>& data, size_t start_index) {
    uint64_t value = 0;
    size_t i = 0;
    for (; i < 9; ++i) {
        unsigned char byte = data[start_index + i];
        if (i == 8) {
            value = (value << 8) | byte;
            ++i;
            break;
        }
        value = (value << 7) | (byte & 0x7Fu);
        if ((byte & 0x80u) == 0) {
            ++i;
            break;
        }
    }
    return {value, i};
}

static size_t serialTypePayloadLength(uint64_t serial_type_code) {
    switch (serial_type_code) {
        case 0: return 0; // NULL
        case 1: return 1; // int8
        case 2: return 2; // int16
        case 3: return 3; // int24
        case 4: return 4; // int32
        case 5: return 6; // int48
        case 6: return 8; // int64
        case 7: return 8; // float64
        case 8: return 0; // integer 0
        case 9: return 0; // integer 1
        case 10: return 0; // reserved
        case 11: return 0; // reserved
        default:
            if (serial_type_code >= 12) {
                if ((serial_type_code % 2) == 0) {
                    return static_cast<size_t>((serial_type_code - 12) / 2); // blob
                } else {
                    return static_cast<size_t>((serial_type_code - 13) / 2); // text
                }
            }
            return 0;
    }
}

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
    } else if (command == ".tables") {
        std::ifstream database_file(database_file_path, std::ios::binary);
        if (!database_file) {
            std::cerr << "Failed to open the database file" << std::endl;
            return 1;
        }

        // Read database page size from file header
        database_file.seekg(16);
        char ps_bytes[2];
        database_file.read(ps_bytes, 2);
        unsigned short page_size = (static_cast<unsigned char>(ps_bytes[1]) | (static_cast<unsigned char>(ps_bytes[0]) << 8));

        // Read the first page entirely (sqlite_schema page)
        database_file.seekg(0);
        std::vector<unsigned char> page(page_size);
        database_file.read(reinterpret_cast<char*>(page.data()), page.size());

        // Read flags to determine header size (leaf table b-tree page -> 0x0D -> 8 bytes header)
        unsigned char flags = page[100];
        size_t btree_header_size = (flags == 0x0D) ? 8 : ((flags == 0x05) ? 12 : 8);

        // Number of cells: big-endian 2 bytes at offset 3 within btree header
        unsigned short num_cells = static_cast<unsigned short>((page[100 + 3] << 8) | page[100 + 4]);

        // Cell pointer array starts immediately after the b-tree page header
        size_t cell_ptr_array_offset = 100 + btree_header_size;

        std::vector<std::string> table_names;
        table_names.reserve(num_cells);

        for (unsigned short i = 0; i < num_cells; ++i) {
            size_t ptr_pos = cell_ptr_array_offset + (i * 2);
            unsigned short cell_offset = static_cast<unsigned short>((page[ptr_pos] << 8) | page[ptr_pos + 1]);

            size_t p = cell_offset;
            // payload size (varint)
            auto [payload_size, payload_size_len] = readVarint(page, p);
            p += payload_size_len;

            // rowid (varint) - ignore value
            auto [/*rowid*/_, rowid_len] = readVarint(page, p);
            p += rowid_len;

            // p now at start of payload (record)
            size_t record_start = p;
            auto [header_size, header_size_len] = readVarint(page, record_start);
            size_t header_varints_pos = record_start + header_size_len;
            size_t header_end = record_start + static_cast<size_t>(header_size);

            std::vector<uint64_t> serial_types;
            size_t hp = header_varints_pos;
            while (hp < header_end) {
                auto [st, st_len] = readVarint(page, hp);
                serial_types.push_back(st);
                hp += st_len;
            }

            size_t body_pos = header_end;

            // We need the 3rd column (index 2) -> sqlite_schema.tbl_name
            size_t target_index = 2;

            // Compute offset into body for the target column
            size_t body_offset = 0;
            for (size_t col_index = 0; col_index < target_index && col_index < serial_types.size(); ++col_index) {
                body_offset += serialTypePayloadLength(serial_types[col_index]);
            }

            uint64_t tbl_name_serial_type = (target_index < serial_types.size()) ? serial_types[target_index] : 0;
            size_t tbl_name_len = serialTypePayloadLength(tbl_name_serial_type);

            // Extract text value
            std::string tbl_name;
            tbl_name.reserve(tbl_name_len);
            size_t tbl_name_start = body_pos + body_offset;
            for (size_t j = 0; j < tbl_name_len; ++j) {
                tbl_name.push_back(static_cast<char>(page[tbl_name_start + j]));
            }

            table_names.push_back(tbl_name);
        }

        for (size_t i = 0; i < table_names.size(); ++i) {
            if (i > 0) std::cout << " ";
            std::cout << table_names[i];
        }
        std::cout << std::endl;
    }

    return 0;
}
