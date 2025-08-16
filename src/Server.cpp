#include <cstring>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <utility>
#include <cstdint>
#include <algorithm>
#include <cctype>

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
        case 0: return 0;
        case 1: return 1;
        case 2: return 2;
        case 3: return 3;
        case 4: return 4;
        case 5: return 6;
        case 6: return 8;
        case 7: return 8;
        case 8: return 0;
        case 9: return 0;
        case 10: return 0;
        case 11: return 0;
        default:
            if (serial_type_code >= 12) {
                if ((serial_type_code % 2) == 0) {
                    return static_cast<size_t>((serial_type_code - 12) / 2);
                } else {
                    return static_cast<size_t>((serial_type_code - 13) / 2);
                }
            }
            return 0;
    }
}

static std::string rstrip_semicolon(const std::string& s) {
    if (!s.empty() && s.back() == ';') return s.substr(0, s.size() - 1);
    return s;
}

static std::string to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return static_cast<char>(std::toupper(c)); });
    return s;
}

static std::string trim(const std::string& s) {
    size_t b = 0;
    while (b < s.size() && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
    size_t e = s.size();
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
    return s.substr(b, e - b);
}

static int64_t readBigEndianSigned(const unsigned char* bytes, size_t len) {
    int64_t value = 0;
    for (size_t i = 0; i < len; ++i) {
        value = (value << 8) | bytes[i];
    }
    size_t total_bits = len * 8;
    if (len > 0 && (bytes[0] & 0x80u)) {
        if (total_bits < 64) {
            int64_t mask = -1;
            mask <<= total_bits;
            value |= mask;
        }
    }
    return value;
}

static std::string decodeValueToString(const std::vector<unsigned char>& buf, size_t start, uint64_t serial_type_code, size_t len) {
    switch (serial_type_code) {
        case 0: return "";
        case 1: return std::to_string(readBigEndianSigned(&buf[start], 1));
        case 2: return std::to_string(readBigEndianSigned(&buf[start], 2));
        case 3: return std::to_string(readBigEndianSigned(&buf[start], 3));
        case 4: return std::to_string(readBigEndianSigned(&buf[start], 4));
        case 5: return std::to_string(readBigEndianSigned(&buf[start], 6));
        case 6: return std::to_string(readBigEndianSigned(&buf[start], 8));
        case 7: {
            uint64_t u = 0;
            for (size_t i = 0; i < 8; ++i) u = (u << 8) | buf[start + i];
            double d;
            std::memcpy(&d, &u, sizeof(double));
            return std::to_string(d);
        }
        case 8: return "0";
        case 9: return "1";
        default: {
            if (serial_type_code >= 12 && (serial_type_code % 2) == 1) {
                return std::string(reinterpret_cast<const char*>(&buf[start]), len);
            } else {
                return std::string(reinterpret_cast<const char*>(&buf[start]), len);
            }
        }
    }
}

static bool fetchRowByRowId(std::ifstream& database_file,
                            unsigned short page_size,
                            uint32_t page_number,
                            uint64_t target_rowid,
                            const std::vector<size_t>& target_col_indices,
                            ssize_t rowid_alias_index) {
    std::vector<unsigned char> page(page_size);
    std::vector<uint64_t> serial_types;
    std::vector<size_t> col_lengths;
    std::vector<size_t> col_offsets;
    
    uint32_t current_page = page_number;
    while (true) {
        std::streamoff offset = static_cast<std::streamoff>((static_cast<uint64_t>(current_page) - 1) * static_cast<uint64_t>(page_size));
        database_file.seekg(offset);
        database_file.read(reinterpret_cast<char*>(page.data()), page.size());
        size_t header_offset = (current_page == 1 ? 100 : 0);
        unsigned char flags = page[header_offset + 0];
        
        if (flags == 0x05) {
            unsigned short num_cells = static_cast<unsigned short>((page[header_offset + 3] << 8) | page[header_offset + 4]);
            size_t cell_ptr_array_offset = header_offset + 12;
            uint32_t target_child = 0;
            
            int left = 0, right = num_cells - 1;
            while (left <= right) {
                int mid = (left + right) / 2;
                size_t ptr_pos = cell_ptr_array_offset + (mid * 2);
                unsigned short cell_offset = static_cast<unsigned short>((page[ptr_pos] << 8) | page[ptr_pos + 1]);
                uint32_t left_child = (static_cast<uint32_t>(page[cell_offset + 0]) << 24) | (static_cast<uint32_t>(page[cell_offset + 1]) << 16) | (static_cast<uint32_t>(page[cell_offset + 2]) << 8) | static_cast<uint32_t>(page[cell_offset + 3]);
                size_t p = cell_offset + 4;
                auto pr = readVarint(page, p);
                uint64_t key_rowid = pr.first;
                if (target_rowid <= key_rowid) {
                    target_child = left_child;
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            }
            
            if (target_child == 0) {
                uint32_t right_child = (static_cast<uint32_t>(page[header_offset + 8]) << 24) | (static_cast<uint32_t>(page[header_offset + 9]) << 16) | (static_cast<uint32_t>(page[header_offset + 10]) << 8) | static_cast<uint32_t>(page[header_offset + 11]);
                target_child = right_child;
            }
            current_page = target_child;
            continue;
            
        } else if (flags == 0x0D) {
            unsigned short num_cells = static_cast<unsigned short>((page[header_offset + 3] << 8) | page[header_offset + 4]);
            size_t btree_header_size = 8;
            size_t cell_ptr_array_offset = header_offset + btree_header_size;
            
            for (unsigned short i = 0; i < num_cells; ++i) {
                size_t ptr_pos = cell_ptr_array_offset + (i * 2);
                unsigned short cell_offset = static_cast<unsigned short>((page[ptr_pos] << 8) | page[ptr_pos + 1]);
                size_t p = cell_offset;
                auto pr = readVarint(page, p);
                p += pr.second;
                pr = readVarint(page, p);
                uint64_t rowid_value = pr.first;
                p += pr.second;
                
                if (rowid_value == target_rowid) {
                    size_t record_start = p;
                    pr = readVarint(page, record_start);
                    uint64_t header_size = pr.first;
                    size_t header_size_len = pr.second;
                    size_t header_varints_pos = record_start + header_size_len;
                    size_t header_end = record_start + static_cast<size_t>(header_size);
                    
                    serial_types.clear();
                    size_t hp = header_varints_pos;
                    while (hp < header_end) {
                        auto stp = readVarint(page, hp);
                        serial_types.push_back(stp.first);
                        hp += stp.second;
                    }
                    
                    col_lengths.clear();
                    col_lengths.reserve(serial_types.size());
                    for (size_t k = 0; k < serial_types.size(); ++k) col_lengths.push_back(serialTypePayloadLength(serial_types[k]));
                    
                    col_offsets.clear();
                    col_offsets.reserve(serial_types.size());
                    size_t acc = 0;
                    for (size_t k = 0; k < serial_types.size(); ++k) { 
                        col_offsets.push_back(acc); 
                        acc += col_lengths[k]; 
                    }
                    
                    size_t body_pos = header_end;
                    for (size_t j = 0; j < target_col_indices.size(); ++j) {
                        size_t col_idx = target_col_indices[j];
                        std::string out;
                        if (static_cast<ssize_t>(col_idx) == rowid_alias_index) {
                            out = std::to_string(static_cast<long long>(rowid_value));
                        } else {
                            size_t start = body_pos + (col_idx < col_offsets.size() ? col_offsets[col_idx] : 0);
                            size_t len = (col_idx < col_lengths.size() ? col_lengths[col_idx] : 0);
                            out = decodeValueToString(page, start, col_idx < serial_types.size() ? serial_types[col_idx] : 0, len);
                        }
                        if (j > 0) std::cout << '|';
                        std::cout << out;
                    }
                    std::cout << std::endl;
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    }
}

static void collectRowidsFromIndex(std::ifstream& database_file,
                                   unsigned short page_size,
                                   uint32_t page_number,
                                   size_t index_col_count,
                                   const std::string& where_value,
                                   std::vector<uint64_t>& out_rowids) {
    std::vector<unsigned char> page(page_size);
    std::vector<uint64_t> serial_types;
    std::vector<size_t> col_lengths;
    std::string first_val;
    first_val.reserve(256);
    
    uint32_t current_page = page_number;
    while (true) {
        std::streamoff offset = static_cast<std::streamoff>((static_cast<uint64_t>(current_page) - 1) * static_cast<uint64_t>(page_size));
        database_file.seekg(offset);
        database_file.read(reinterpret_cast<char*>(page.data()), page.size());
        size_t header_offset = (current_page == 1 ? 100 : 0);
        unsigned char flags = page[header_offset + 0];
        
        if (flags == 0x02) {
            unsigned short num_cells = static_cast<unsigned short>((page[header_offset + 3] << 8) | page[header_offset + 4]);
            size_t cell_ptr_array_offset = header_offset + 12;
            uint32_t target_child = 0;
            
            int left = 0, right = num_cells - 1;
            while (left <= right) {
                int mid = (left + right) / 2;
                size_t ptr_pos = cell_ptr_array_offset + (mid * 2);
                unsigned short cell_offset = static_cast<unsigned short>((page[ptr_pos] << 8) | page[ptr_pos + 1]);
                uint32_t left_child = (static_cast<uint32_t>(page[cell_offset + 0]) << 24) | (static_cast<uint32_t>(page[cell_offset + 1]) << 16) | (static_cast<uint32_t>(page[cell_offset + 2]) << 8) | static_cast<uint32_t>(page[cell_offset + 3]);
                
                size_t p = cell_offset + 4;
                auto pr = readVarint(page, p);
                p += pr.second;
                size_t record_start = p;
                pr = readVarint(page, record_start);
                uint64_t header_size = pr.first;
                size_t header_size_len = pr.second;
                size_t header_varints_pos = record_start + header_size_len;
                size_t header_end = record_start + static_cast<size_t>(header_size);
                
                serial_types.clear();
                size_t hp = header_varints_pos;
                while (hp < header_end) { 
                    auto t = readVarint(page, hp); 
                    serial_types.push_back(t.first); 
                    hp += t.second; 
                }
                
                size_t body_pos = header_end;
                size_t first_len = serial_types.empty() ? 0 : serialTypePayloadLength(serial_types[0]);
                
                first_val.clear();
                first_val.resize(first_len);
                for (size_t j = 0; j < first_len; ++j) first_val[j] = static_cast<char>(page[body_pos + j]);
                
                if (where_value < first_val) {
                    target_child = left_child;
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            }
            
            if (target_child == 0) {
                uint32_t right_child = (static_cast<uint32_t>(page[header_offset + 8]) << 24) | (static_cast<uint32_t>(page[header_offset + 9]) << 16) | (static_cast<uint32_t>(page[header_offset + 10]) << 8) | static_cast<uint32_t>(page[header_offset + 11]);
                target_child = right_child;
            }
            current_page = target_child;
            continue;
            
        } else if (flags == 0x0A) {
            unsigned short num_cells = static_cast<unsigned short>((page[header_offset + 3] << 8) | page[header_offset + 4]);
            size_t btree_header_size = 8;
            size_t cell_ptr_array_offset = header_offset + btree_header_size;
            
            for (unsigned short i = 0; i < num_cells; ++i) {
                size_t ptr_pos = cell_ptr_array_offset + (i * 2);
                unsigned short cell_offset = static_cast<unsigned short>((page[ptr_pos] << 8) | page[ptr_pos + 1]);
                size_t p = cell_offset;
                auto pr = readVarint(page, p);
                p += pr.second;
                size_t record_start = p;
                pr = readVarint(page, record_start);
                uint64_t header_size = pr.first;
                size_t header_size_len = pr.second;
                size_t header_varints_pos = record_start + header_size_len;
                size_t header_end = record_start + static_cast<size_t>(header_size);
                
                serial_types.clear();
                size_t hp = header_varints_pos;
                while (hp < header_end) { 
                    auto t = readVarint(page, hp); 
                    serial_types.push_back(t.first); 
                    hp += t.second; 
                }
                
                col_lengths.clear();
                col_lengths.reserve(serial_types.size());
                for (size_t k = 0; k < serial_types.size(); ++k) col_lengths.push_back(serialTypePayloadLength(serial_types[k]));
                
                size_t body_pos = header_end;
                size_t first_len = serial_types.empty() ? 0 : col_lengths[0];
                
                first_val.clear();
                first_val.resize(first_len);
                for (size_t j = 0; j < first_len; ++j) first_val[j] = static_cast<char>(page[body_pos + j]);
                
                if (first_val == where_value) {
                    size_t total_len = 0;
                    for (size_t k = 0; k < serial_types.size() - 1; ++k) total_len += col_lengths[k];
                    size_t rowid_pos = body_pos + total_len;
                    auto rv = readVarint(page, rowid_pos);
                    uint64_t rowid_value = rv.first;
                    out_rowids.push_back(rowid_value);
                }
            }
            return;
        } else {
            return;
        }
    }
}

static void traverseTableBtree(std::ifstream& database_file,
                               unsigned short page_size,
                               uint32_t page_number,
                               const std::vector<std::string>& column_names,
                               const std::vector<size_t>& target_col_indices,
                               bool has_where,
                               size_t where_col_idx,
                               const std::string& where_value,
                               ssize_t rowid_alias_index) {
    std::vector<unsigned char> page(page_size);
    std::streamoff offset = static_cast<std::streamoff>((static_cast<uint64_t>(page_number) - 1) * static_cast<uint64_t>(page_size));
    database_file.seekg(offset);
    database_file.read(reinterpret_cast<char*>(page.data()), page.size());
    size_t header_offset = (page_number == 1 ? 100 : 0);
    unsigned char flags = page[header_offset + 0];
    if (flags == 0x05) {
        unsigned short num_cells = static_cast<unsigned short>((page[header_offset + 3] << 8) | page[header_offset + 4]);
        size_t cell_ptr_array_offset = header_offset + 12;
        for (unsigned short i = 0; i < num_cells; ++i) {
            size_t ptr_pos = cell_ptr_array_offset + (i * 2);
            unsigned short cell_offset = static_cast<unsigned short>((page[ptr_pos] << 8) | page[ptr_pos + 1]);
            uint32_t left_child = (static_cast<uint32_t>(page[cell_offset + 0]) << 24) | (static_cast<uint32_t>(page[cell_offset + 1]) << 16) | (static_cast<uint32_t>(page[cell_offset + 2]) << 8) | static_cast<uint32_t>(page[cell_offset + 3]);
            traverseTableBtree(database_file, page_size, left_child, column_names, target_col_indices, has_where, where_col_idx, where_value, rowid_alias_index);
        }
        uint32_t right_child = (static_cast<uint32_t>(page[header_offset + 8]) << 24) | (static_cast<uint32_t>(page[header_offset + 9]) << 16) | (static_cast<uint32_t>(page[header_offset + 10]) << 8) | static_cast<uint32_t>(page[header_offset + 11]);
        traverseTableBtree(database_file, page_size, right_child, column_names, target_col_indices, has_where, where_col_idx, where_value, rowid_alias_index);
        return;
    } else if (flags != 0x0D) {
        return;
    }
    unsigned short num_cells = static_cast<unsigned short>((page[header_offset + 3] << 8) | page[header_offset + 4]);
    size_t btree_header_size = 8;
    size_t cell_ptr_array_offset = header_offset + btree_header_size;
    for (unsigned short i = 0; i < num_cells; ++i) {
        size_t ptr_pos = cell_ptr_array_offset + (i * 2);
        unsigned short cell_offset = static_cast<unsigned short>((page[ptr_pos] << 8) | page[ptr_pos + 1]);
        size_t p = cell_offset;
        auto pr = readVarint(page, p);
        uint64_t payload_size = pr.first;
        p += pr.second;
        pr = readVarint(page, p);
        uint64_t rowid_value = pr.first;
        p += pr.second;
        size_t record_start = p;
        pr = readVarint(page, record_start);
        uint64_t header_size = pr.first;
        size_t header_size_len = pr.second;
        size_t header_varints_pos = record_start + header_size_len;
        size_t header_end = record_start + static_cast<size_t>(header_size);
        std::vector<uint64_t> serial_types;
        size_t hp = header_varints_pos;
        while (hp < header_end) {
            auto stp = readVarint(page, hp);
            serial_types.push_back(stp.first);
            hp += stp.second;
        }
        std::vector<size_t> col_lengths(serial_types.size());
        for (size_t k = 0; k < serial_types.size(); ++k) col_lengths[k] = serialTypePayloadLength(serial_types[k]);
        std::vector<size_t> col_offsets(serial_types.size());
        size_t acc = 0;
        for (size_t k = 0; k < serial_types.size(); ++k) { col_offsets[k] = acc; acc += col_lengths[k]; }
        size_t body_pos = header_end;
        if (has_where) {
            if (where_col_idx == static_cast<size_t>(rowid_alias_index)) {
                std::string wval = std::to_string(static_cast<long long>(rowid_value));
                if (wval != where_value) continue;
            } else {
                if (where_col_idx >= col_offsets.size()) continue;
                size_t w_start = body_pos + col_offsets[where_col_idx];
                size_t w_len = col_lengths[where_col_idx];
                std::string wval = decodeValueToString(page, w_start, serial_types[where_col_idx], w_len);
                if (wval != where_value) continue;
            }
        }
        for (size_t j = 0; j < target_col_indices.size(); ++j) {
            size_t col_idx = target_col_indices[j];
            std::string out;
            if (static_cast<ssize_t>(col_idx) == rowid_alias_index) {
                out = std::to_string(static_cast<long long>(rowid_value));
            } else {
                size_t start = body_pos + (col_idx < col_offsets.size() ? col_offsets[col_idx] : 0);
                size_t len = (col_idx < col_lengths.size() ? col_lengths[col_idx] : 0);
                out = decodeValueToString(page, start, col_idx < serial_types.size() ? serial_types[col_idx] : 0, len);
            }
            if (j > 0) std::cout << '|';
            std::cout << out;
        }
        std::cout << std::endl;
    }
}

int main(int argc, char* argv[]) {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    std::cerr << "Logs from your program will appear here" << std::endl;
    if (argc != 3) {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }
    std::string database_file_path = argv[1];
    std::string command = argv[2];
    std::string command_upper = to_upper(command);
    if (command == ".dbinfo") {
        std::ifstream database_file(database_file_path, std::ios::binary);
        if (!database_file) {
            std::cerr << "Failed to open the database file" << std::endl;
            return 1;
        }
        database_file.seekg(16);
        char buffer[2];
        database_file.read(buffer, 2);
        unsigned short page_size = (static_cast<unsigned char>(buffer[1]) | (static_cast<unsigned char>(buffer[0]) << 8));
        std::cout << "database page size: " << page_size << std::endl;
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
        database_file.seekg(16);
        char ps_bytes[2];
        database_file.read(ps_bytes, 2);
        unsigned short page_size = (static_cast<unsigned char>(ps_bytes[1]) | (static_cast<unsigned char>(ps_bytes[0]) << 8));
        database_file.seekg(0);
        std::vector<unsigned char> page(page_size);
        database_file.read(reinterpret_cast<char*>(page.data()), page.size());
        unsigned char flags = page[100];
        size_t btree_header_size = (flags == 0x0D) ? 8 : ((flags == 0x05) ? 12 : 8);
        unsigned short num_cells = static_cast<unsigned short>((page[100 + 3] << 8) | page[100 + 4]);
        size_t cell_ptr_array_offset = 100 + btree_header_size;
        std::vector<std::string> table_names;
        table_names.reserve(num_cells);
        for (unsigned short i = 0; i < num_cells; ++i) {
            size_t ptr_pos = cell_ptr_array_offset + (i * 2);
            unsigned short cell_offset = static_cast<unsigned short>((page[ptr_pos] << 8) | page[ptr_pos + 1]);
            size_t p = cell_offset;
            auto pr = readVarint(page, p);
            p += pr.second;
            pr = readVarint(page, p);
            p += pr.second;
            size_t record_start = p;
            pr = readVarint(page, record_start);
            uint64_t header_size = pr.first;
            size_t header_size_len = pr.second;
            size_t header_varints_pos = record_start + header_size_len;
            size_t header_end = record_start + static_cast<size_t>(header_size);
            std::vector<uint64_t> serial_types;
            size_t hp = header_varints_pos;
            while (hp < header_end) {
                auto stp = readVarint(page, hp);
                serial_types.push_back(stp.first);
                hp += stp.second;
            }
            size_t body_pos = header_end;
            size_t target_index = 2;
            size_t body_offset = 0;
            for (size_t col_index = 0; col_index < target_index && col_index < serial_types.size(); ++col_index) {
                body_offset += serialTypePayloadLength(serial_types[col_index]);
            }
            uint64_t tbl_name_serial_type = (target_index < serial_types.size()) ? serial_types[target_index] : 0;
            size_t tbl_name_len = serialTypePayloadLength(tbl_name_serial_type);
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
    } else if (command_upper.rfind("SELECT", 0) == 0) {
        std::vector<std::string> tokens;
        {
            std::string cur;
            for (char c : command) {
                if (std::isspace(static_cast<unsigned char>(c))) {
                    if (!cur.empty()) { tokens.push_back(cur); cur.clear(); }
                } else {
                    cur.push_back(c);
                }
            }
            if (!cur.empty()) tokens.push_back(cur);
        }
        if (tokens.size() < 4) {
            std::cout << std::endl;
            return 0;
        }
        size_t from_index = std::string::npos;
        for (size_t i = 0; i < tokens.size(); ++i) {
            if (to_upper(tokens[i]) == "FROM") { from_index = i; break; }
        }
        if (from_index == std::string::npos || from_index >= tokens.size() - 1) {
            std::cout << std::endl;
            return 0;
        }
        std::string select_list_str;
        for (size_t i = 1; i < from_index; ++i) {
            if (!select_list_str.empty()) select_list_str.push_back(' ');
            select_list_str += tokens[i];
        }
        std::vector<std::string> select_cols_upper;
        {
            std::string cur;
            for (char c : select_list_str) {
                if (c == ',') {
                    std::string part = trim(cur);
                    if (!part.empty()) select_cols_upper.push_back(to_upper(part));
                    cur.clear();
                } else {
                    cur.push_back(c);
                }
            }
            std::string last = trim(cur);
            if (!last.empty()) select_cols_upper.push_back(to_upper(last));
        }
        std::string table_name = rstrip_semicolon(tokens[from_index + 1]);
        bool has_where = false;
        std::string where_col_upper;
        std::string where_value;
        size_t where_pos_ci = to_upper(command).find("WHERE");
        if (where_pos_ci != std::string::npos) {
            size_t i2 = where_pos_ci + 5;
            while (i2 < command.size() && std::isspace(static_cast<unsigned char>(command[i2]))) ++i2;
            size_t col_start = i2;
            while (i2 < command.size() && command[i2] != '=' && !std::isspace(static_cast<unsigned char>(command[i2]))) ++i2;
            std::string col_tok = command.substr(col_start, i2 - col_start);
            while (i2 < command.size() && std::isspace(static_cast<unsigned char>(command[i2]))) ++i2;
            if (i2 < command.size() && command[i2] == '=') ++i2;
            while (i2 < command.size() && std::isspace(static_cast<unsigned char>(command[i2]))) ++i2;
            std::string val_tok;
            if (i2 < command.size() && (command[i2] == '\'' || command[i2] == '"')) {
                char q = command[i2++];
                size_t start = i2;
                while (i2 < command.size() && command[i2] != q) ++i2;
                val_tok = command.substr(start, i2 - start);
            } else {
                size_t start = i2;
                while (i2 < command.size() && !std::isspace(static_cast<unsigned char>(command[i2])) && command[i2] != ';') ++i2;
                val_tok = command.substr(start, i2 - start);
            }
            where_col_upper = to_upper(col_tok);
            where_value = val_tok;
            has_where = true;
        }
        bool is_count = (select_cols_upper.size() == 1 && select_cols_upper[0] == "COUNT(*)");
        std::ifstream database_file(database_file_path, std::ios::binary);
        if (!database_file) {
            std::cerr << "Failed to open the database file" << std::endl;
            return 1;
        }
        database_file.seekg(16);
        char ps_bytes[2];
        database_file.read(ps_bytes, 2);
        unsigned short page_size = (static_cast<unsigned char>(ps_bytes[1]) | (static_cast<unsigned char>(ps_bytes[0]) << 8));
        database_file.seekg(0);
        std::vector<unsigned char> schema_page(page_size);
        database_file.read(reinterpret_cast<char*>(schema_page.data()), schema_page.size());
        unsigned char flags = schema_page[100];
        size_t btree_header_size = (flags == 0x0D) ? 8 : ((flags == 0x05) ? 12 : 8);
        unsigned short num_cells = static_cast<unsigned short>((schema_page[100 + 3] << 8) | schema_page[100 + 4]);
        size_t cell_ptr_array_offset = 100 + btree_header_size;
        uint64_t table_rootpage = 0;
        std::string create_sql;
        for (unsigned short i = 0; i < num_cells; ++i) {
            size_t ptr_pos = cell_ptr_array_offset + (i * 2);
            unsigned short cell_offset = static_cast<unsigned short>((schema_page[ptr_pos] << 8) | schema_page[ptr_pos + 1]);
            size_t p = cell_offset;
            auto pr = readVarint(schema_page, p);
            p += pr.second;
            pr = readVarint(schema_page, p);
            p += pr.second;
            size_t record_start = p;
            pr = readVarint(schema_page, record_start);
            uint64_t header_size = pr.first;
            size_t header_size_len = pr.second;
            size_t header_varints_pos = record_start + header_size_len;
            size_t header_end = record_start + static_cast<size_t>(header_size);
            std::vector<uint64_t> serial_types;
            size_t hp = header_varints_pos;
            while (hp < header_end) {
                auto stp = readVarint(schema_page, hp);
                serial_types.push_back(stp.first);
                hp += stp.second;
            }
            size_t body_pos = header_end;
            size_t type_index = 0;
            size_t name_index = 1;
            size_t tbl_index = 2;
            size_t root_index = 3;
            size_t sql_index = 4;
            size_t off = 0;
            for (size_t col_index = 0; col_index < type_index && col_index < serial_types.size(); ++col_index) off += serialTypePayloadLength(serial_types[col_index]);
            size_t type_len = serialTypePayloadLength(serial_types.size() > type_index ? serial_types[type_index] : 0);
            std::string typ;
            typ.reserve(type_len);
            size_t type_start = body_pos + off;
            for (size_t j = 0; j < type_len; ++j) typ.push_back(static_cast<char>(schema_page[type_start + j]));
            off = 0;
            for (size_t col_index = 0; col_index < tbl_index && col_index < serial_types.size(); ++col_index) off += serialTypePayloadLength(serial_types[col_index]);
            size_t tbl_len = serialTypePayloadLength(serial_types.size() > tbl_index ? serial_types[tbl_index] : 0);
            std::string tbl;
            tbl.reserve(tbl_len);
            size_t tbl_start = body_pos + off;
            for (size_t j = 0; j < tbl_len; ++j) tbl.push_back(static_cast<char>(schema_page[tbl_start + j]));
            if (to_upper(typ) == "TABLE" && tbl == table_name) {
                size_t off2 = 0;
                for (size_t col_index = 0; col_index < root_index && col_index < serial_types.size(); ++col_index) off2 += serialTypePayloadLength(serial_types[col_index]);
                size_t root_len = serialTypePayloadLength(serial_types.size() > root_index ? serial_types[root_index] : 0);
                size_t root_start = body_pos + off2;
                uint64_t root_val = 0;
                for (size_t j = 0; j < root_len; ++j) root_val = (root_val << 8) | schema_page[root_start + j];
                table_rootpage = root_val;
                size_t off3 = 0;
                for (size_t col_index = 0; col_index < sql_index && col_index < serial_types.size(); ++col_index) off3 += serialTypePayloadLength(serial_types[col_index]);
                size_t sql_len = serialTypePayloadLength(serial_types.size() > sql_index ? serial_types[sql_index] : 0);
                size_t sql_start = body_pos + off3;
                create_sql.clear();
                create_sql.reserve(sql_len);
                for (size_t j = 0; j < sql_len; ++j) create_sql.push_back(static_cast<char>(schema_page[sql_start + j]));
            }
        }
        if (table_rootpage == 0) {
            if (is_count) { std::cout << 0 << std::endl; } else { std::cout << std::endl; }
            return 0;
        }
        if (is_count) {
            std::vector<unsigned char> table_page(page_size);
            std::streamoff root_offset = static_cast<std::streamoff>((table_rootpage - 1) * static_cast<uint64_t>(page_size));
            database_file.seekg(root_offset);
            database_file.read(reinterpret_cast<char*>(table_page.data()), table_page.size());
            size_t page_header_offset = (table_rootpage == 1 ? 100 : 0);
            unsigned short row_count = static_cast<unsigned short>((table_page[page_header_offset + 3] << 8) | table_page[page_header_offset + 4]);
            std::cout << row_count << std::endl;
            return 0;
        }
        std::vector<std::string> column_names;
        std::vector<std::string> column_defs_upper;
        {
            std::string sql = create_sql;
            size_t lpar = sql.find('(');
            size_t rpar = sql.rfind(')');
            if (lpar != std::string::npos && rpar != std::string::npos && rpar > lpar) {
                std::string cols = sql.substr(lpar + 1, rpar - lpar - 1);
                std::string cur;
                int paren_depth = 0;
                for (char c : cols) {
                    if (c == '(') { ++paren_depth; cur.push_back(c); }
                    else if (c == ')') { --paren_depth; cur.push_back(c); }
                    else if (c == ',' && paren_depth == 0) {
                        std::string part = trim(cur);
                        if (!part.empty()) {
                            size_t sp = part.find_first_of(" \t\r\n");
                            column_names.push_back(to_upper(trim(sp == std::string::npos ? part : part.substr(0, sp))));
                            column_defs_upper.push_back(to_upper(part));
                        }
                        cur.clear();
                    } else {
                        cur.push_back(c);
                    }
                }
                std::string last = trim(cur);
                if (!last.empty()) {
                    size_t sp = last.find_first_of(" \t\r\n");
                    column_names.push_back(to_upper(trim(sp == std::string::npos ? last : last.substr(0, sp))));
                    column_defs_upper.push_back(to_upper(last));
                }
            }
        }
        ssize_t rowid_alias_index = -1;
        for (size_t i = 0; i < column_defs_upper.size(); ++i) {
            const std::string& def = column_defs_upper[i];
            if (def.find("PRIMARY KEY") != std::string::npos && (def.find("INTEGER") != std::string::npos || def.find(" INT") != std::string::npos)) {
                rowid_alias_index = static_cast<ssize_t>(i);
                break;
            }
        }
        std::vector<size_t> target_col_indices;
        target_col_indices.reserve(select_cols_upper.size());
        for (std::string col : select_cols_upper) {
            if (!col.empty() && (col.front() == '"' || col.front() == '\'' || col.front() == '`')) {
                if (col.size() >= 2) {
                    col = col.substr(1, col.size() - 2);
                    col = to_upper(col);
                }
            }
            size_t idx = std::string::npos;
            for (size_t i = 0; i < column_names.size(); ++i) {
                if (column_names[i] == col) { idx = i; break; }
            }
            if (idx == std::string::npos) {
                std::cout << std::endl;
                return 0;
            }
            target_col_indices.push_back(idx);
        }
        size_t where_col_idx = std::string::npos;
        if (has_where) {
            for (size_t i = 0; i < column_names.size(); ++i) {
                if (column_names[i] == where_col_upper) { where_col_idx = i; break; }
            }
            if (where_col_idx == std::string::npos) {
                return 0;
            }
        }
        uint64_t index_rootpage = 0;
        size_t index_col_count = 0;
        for (unsigned short i = 0; i < num_cells; ++i) {
            size_t ptr_pos = cell_ptr_array_offset + (i * 2);
            unsigned short cell_offset = static_cast<unsigned short>((schema_page[ptr_pos] << 8) | schema_page[ptr_pos + 1]);
            size_t p = cell_offset;
            auto pr = readVarint(schema_page, p);
            p += pr.second;
            pr = readVarint(schema_page, p);
            p += pr.second;
            size_t record_start = p;
            pr = readVarint(schema_page, record_start);
            uint64_t header_size = pr.first;
            size_t header_size_len = pr.second;
            size_t header_varints_pos = record_start + header_size_len;
            size_t header_end = record_start + static_cast<size_t>(header_size);
            std::vector<uint64_t> serial_types;
            size_t hp = header_varints_pos;
            while (hp < header_end) {
                auto stp = readVarint(schema_page, hp);
                serial_types.push_back(stp.first);
                hp += stp.second;
            }
            size_t body_pos = record_start + header_size;
            size_t type_index = 0;
            size_t name_index = 1;
            size_t tbl_index = 2;
            size_t root_index = 3;
            size_t sql_index = 4;
            size_t off0 = 0;
            for (size_t col_index = 0; col_index < type_index && col_index < serial_types.size(); ++col_index) off0 += serialTypePayloadLength(serial_types[col_index]);
            size_t type_len = serialTypePayloadLength(serial_types.size() > type_index ? serial_types[type_index] : 0);
            std::string typ;
            typ.reserve(type_len);
            size_t type_start = body_pos + off0;
            for (size_t j = 0; j < type_len; ++j) typ.push_back(static_cast<char>(schema_page[type_start + j]));
            off0 = 0;
            for (size_t col_index = 0; col_index < tbl_index && col_index < serial_types.size(); ++col_index) off0 += serialTypePayloadLength(serial_types[col_index]);
            size_t tbl_len = serialTypePayloadLength(serial_types.size() > tbl_index ? serial_types[tbl_index] : 0);
            std::string tbl;
            tbl.reserve(tbl_len);
            size_t tbl_start = body_pos + off0;
            for (size_t j = 0; j < tbl_len; ++j) tbl.push_back(static_cast<char>(schema_page[tbl_start + j]));
            if (to_upper(typ) == "INDEX" && tbl == table_name) {
                size_t off3 = 0;
                for (size_t col_index = 0; col_index < sql_index && col_index < serial_types.size(); ++col_index) off3 += serialTypePayloadLength(serial_types[col_index]);
                size_t sql_len = serialTypePayloadLength(serial_types.size() > sql_index ? serial_types[sql_index] : 0);
                size_t sql_start = body_pos + off3;
                std::string idx_sql;
                idx_sql.reserve(sql_len);
                for (size_t j = 0; j < sql_len; ++j) idx_sql.push_back(static_cast<char>(schema_page[sql_start + j]));
                std::string idx_upper = to_upper(idx_sql);
                size_t on_pos = idx_upper.find(" ON ");
                size_t lpar = idx_upper.find('(', on_pos == std::string::npos ? 0 : on_pos);
                size_t rpar = idx_upper.find(')', lpar == std::string::npos ? 0 : lpar);
                if (lpar != std::string::npos && rpar != std::string::npos && rpar > lpar) {
                    std::string cols = idx_upper.substr(lpar + 1, rpar - lpar - 1);
                    std::vector<std::string> idx_cols;
                    std::string curc;
                    for (char c : cols) {
                        if (c == ',') {
                            std::string part = trim(curc);
                            if (!part.empty()) {
                                size_t sp = part.find_first_of(" \t\r\n");
                                idx_cols.push_back(trim(sp == std::string::npos ? part : part.substr(0, sp)));
                            }
                            curc.clear();
                        } else {
                            curc.push_back(c);
                        }
                    }
                    std::string lastc = trim(curc);
                    if (!lastc.empty()) {
                        size_t sp = lastc.find_first_of(" \t\r\n");
                        idx_cols.push_back(trim(sp == std::string::npos ? lastc : lastc.substr(0, sp)));
                    }
                    if (!idx_cols.empty() && idx_cols[0] == where_col_upper) {
                        size_t offr = 0;
                        for (size_t col_index = 0; col_index < root_index && col_index < serial_types.size(); ++col_index) offr += serialTypePayloadLength(serial_types[col_index]);
                        size_t root_len = serialTypePayloadLength(serial_types.size() > root_index ? serial_types[root_index] : 0);
                        size_t root_start = body_pos + offr;
                        uint64_t root_val = 0;
                        for (size_t j = 0; j < root_len; ++j) root_val = (root_val << 8) | schema_page[root_start + j];
                        index_rootpage = root_val;
                        index_col_count = idx_cols.size();
                        break;
                    }
                }
            }
        }
        if (has_where && index_rootpage != 0) {
            uint32_t subtree_page = static_cast<uint32_t>(index_rootpage);
            std::vector<unsigned char> idx_page(page_size);
            std::streamoff idx_off = static_cast<std::streamoff>((static_cast<uint64_t>(index_rootpage) - 1) * static_cast<uint64_t>(page_size));
            database_file.seekg(idx_off);
            database_file.read(reinterpret_cast<char*>(idx_page.data()), idx_page.size());
            size_t idx_hdr = (index_rootpage == 1 ? 100 : 0);
            unsigned char iflags = idx_page[idx_hdr + 0];
            if (iflags == 0x02) {
                unsigned short inum = static_cast<unsigned short>((idx_page[idx_hdr + 3] << 8) | idx_page[idx_hdr + 4]);
                size_t iarr = idx_hdr + 12;
                bool chosen = false;
                for (unsigned short i = 0; i < inum; ++i) {
                    size_t iptr = iarr + i * 2;
                    unsigned short icell = static_cast<unsigned short>((idx_page[iptr] << 8) | idx_page[iptr + 1]);
                    uint32_t left_child = (static_cast<uint32_t>(idx_page[icell + 0]) << 24) | (static_cast<uint32_t>(idx_page[icell + 1]) << 16) | (static_cast<uint32_t>(idx_page[icell + 2]) << 8) | static_cast<uint32_t>(idx_page[icell + 3]);
                    size_t p = icell + 4;
                    auto pr = readVarint(idx_page, p);
                    uint64_t payload_size = pr.first;
                    p += pr.second;
                    size_t rec_start = p;
                    pr = readVarint(idx_page, rec_start);
                    uint64_t hsize = pr.first;
                    size_t hlen = pr.second;
                    size_t hpos = rec_start + hlen;
                    size_t hend = rec_start + static_cast<size_t>(hsize);
                    std::vector<uint64_t> st;
                    size_t hp = hpos;
                    while (hp < hend) { auto t = readVarint(idx_page, hp); st.push_back(t.first); hp += t.second; }
                    size_t bpos = hend;
                    size_t first_len = st.empty() ? 0 : serialTypePayloadLength(st[0]);
                    std::string first_val;
                    first_val.reserve(first_len);
                    for (size_t j = 0; j < first_len; ++j) first_val.push_back(static_cast<char>(idx_page[bpos + j]));
                    if (where_value < first_val) { subtree_page = left_child; chosen = true; break; }
                }
                if (!chosen) {
                    subtree_page = (static_cast<uint32_t>(idx_page[idx_hdr + 8]) << 24) | (static_cast<uint32_t>(idx_page[idx_hdr + 9]) << 16) | (static_cast<uint32_t>(idx_page[idx_hdr + 10]) << 8) | static_cast<uint32_t>(idx_page[idx_hdr + 11]);
                }
            }
            std::vector<uint64_t> rowids;
            rowids.reserve(128);
            collectRowidsFromIndex(database_file, page_size, subtree_page, index_col_count, where_value, rowids);
            for (uint64_t rid : rowids) {
                fetchRowByRowId(database_file, page_size, static_cast<uint32_t>(table_rootpage), rid, target_col_indices, rowid_alias_index);
            }
            return 0;
        }
        traverseTableBtree(database_file, page_size, static_cast<uint32_t>(table_rootpage), column_names, target_col_indices, has_where, where_col_idx, where_value, rowid_alias_index);
    }
    return 0;
}
