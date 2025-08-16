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
            auto [payload_size, payload_size_len] = readVarint(page, p);
            p += payload_size_len;
            auto [_, rowid_len] = readVarint(page, p);
            p += rowid_len;

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
        // Tokenize
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
        std::string select_expr = tokens[1];
        std::string from_token_upper = to_upper(tokens[2]);
        size_t from_index = 2;
        if (from_token_upper != "FROM") {
            for (size_t i = 0; i < tokens.size(); ++i) {
                if (to_upper(tokens[i]) == "FROM") { from_index = i; break; }
            }
        }
        if (from_index >= tokens.size() - 1) {
            std::cout << std::endl;
            return 0;
        }
        std::string table_name = rstrip_semicolon(tokens[from_index + 1]);

        bool is_count = to_upper(select_expr) == "COUNT(*)";

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

        uint64_t rootpage = 0;
        std::string create_sql;
        for (unsigned short i = 0; i < num_cells; ++i) {
            size_t ptr_pos = cell_ptr_array_offset + (i * 2);
            unsigned short cell_offset = static_cast<unsigned short>((schema_page[ptr_pos] << 8) | schema_page[ptr_pos + 1]);

            size_t p = cell_offset;
            auto [payload_size, payload_size_len] = readVarint(schema_page, p);
            p += payload_size_len;
            auto [_, rowid_len] = readVarint(schema_page, p);
            p += rowid_len;

            size_t record_start = p;
            auto [header_size, header_size_len] = readVarint(schema_page, record_start);
            size_t header_varints_pos = record_start + header_size_len;
            size_t header_end = record_start + static_cast<size_t>(header_size);

            std::vector<uint64_t> serial_types;
            size_t hp = header_varints_pos;
            while (hp < header_end) {
                auto [st, st_len] = readVarint(schema_page, hp);
                serial_types.push_back(st);
                hp += st_len;
            }

            size_t body_pos = header_end;

            size_t tbl_index = 2;
            size_t off = 0;
            for (size_t col_index = 0; col_index < tbl_index && col_index < serial_types.size(); ++col_index) off += serialTypePayloadLength(serial_types[col_index]);
            size_t tbl_len = serialTypePayloadLength(serial_types.size() > tbl_index ? serial_types[tbl_index] : 0);
            std::string tbl;
            tbl.reserve(tbl_len);
            size_t tbl_start = body_pos + off;
            for (size_t j = 0; j < tbl_len; ++j) tbl.push_back(static_cast<char>(schema_page[tbl_start + j]));

            if (tbl == table_name) {
                size_t root_index = 3;
                size_t off2 = 0;
                for (size_t col_index = 0; col_index < root_index && col_index < serial_types.size(); ++col_index) off2 += serialTypePayloadLength(serial_types[col_index]);
                size_t root_len = serialTypePayloadLength(serial_types.size() > root_index ? serial_types[root_index] : 0);
                size_t root_start = body_pos + off2;
                uint64_t root_val = 0;
                for (size_t j = 0; j < root_len; ++j) root_val = (root_val << 8) | schema_page[root_start + j];
                rootpage = root_val;

                size_t sql_index = 4;
                size_t off3 = 0;
                for (size_t col_index = 0; col_index < sql_index && col_index < serial_types.size(); ++col_index) off3 += serialTypePayloadLength(serial_types[col_index]);
                size_t sql_len = serialTypePayloadLength(serial_types.size() > sql_index ? serial_types[sql_index] : 0);
                size_t sql_start = body_pos + off3;
                create_sql.clear();
                create_sql.reserve(sql_len);
                for (size_t j = 0; j < sql_len; ++j) create_sql.push_back(static_cast<char>(schema_page[sql_start + j]));

                break;
            }
        }

        if (rootpage == 0) {
            if (is_count) { std::cout << 0 << std::endl; } else { std::cout << std::endl; }
            return 0;
        }

        if (is_count) {
            std::vector<unsigned char> table_page(page_size);
            std::streamoff root_offset = static_cast<std::streamoff>((rootpage - 1) * static_cast<uint64_t>(page_size));
            database_file.seekg(root_offset);
            database_file.read(reinterpret_cast<char*>(table_page.data()), table_page.size());
            size_t page_header_offset = (rootpage == 1 ? 100 : 0);
            unsigned short row_count = static_cast<unsigned short>((table_page[page_header_offset + 3] << 8) | table_page[page_header_offset + 4]);
            std::cout << row_count << std::endl;
            return 0;
        }

        // Parse column order from CREATE TABLE
        std::vector<std::string> column_names;
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
                }
            }
        }

        std::string wanted_col = to_upper(select_expr);
        // Strip optional quotes/backticks if any
        if (!wanted_col.empty() && (wanted_col.front() == '"' || wanted_col.front() == '\'' || wanted_col.front() == '`')) {
            wanted_col = wanted_col.substr(1, wanted_col.size() - 2);
            wanted_col = to_upper(wanted_col);
        }

        size_t target_col_index = std::string::npos;
        for (size_t i = 0; i < column_names.size(); ++i) {
            if (column_names[i] == wanted_col) { target_col_index = i; break; }
        }
        if (target_col_index == std::string::npos) {
            std::cout << std::endl;
            return 0;
        }

        // Read table page and print the requested column from each row
        std::vector<unsigned char> table_page(page_size);
        std::streamoff root_offset = static_cast<std::streamoff>((rootpage - 1) * static_cast<uint64_t>(page_size));
        database_file.seekg(root_offset);
        database_file.read(reinterpret_cast<char*>(table_page.data()), table_page.size());
        size_t page_header_offset = (rootpage == 1 ? 100 : 0);
        unsigned char tflags = table_page[page_header_offset + 0];
        size_t t_btree_header_size = (tflags == 0x0D) ? 8 : ((tflags == 0x05) ? 12 : 8);
        unsigned short t_num_cells = static_cast<unsigned short>((table_page[page_header_offset + 3] << 8) | table_page[page_header_offset + 4]);
        size_t t_cell_ptr_array_offset = page_header_offset + t_btree_header_size;

        for (unsigned short i = 0; i < t_num_cells; ++i) {
            size_t ptr_pos = t_cell_ptr_array_offset + (i * 2);
            unsigned short cell_offset = static_cast<unsigned short>((table_page[ptr_pos] << 8) | table_page[ptr_pos + 1]);

            size_t p = cell_offset;
            auto [payload_size, payload_size_len] = readVarint(table_page, p);
            p += payload_size_len;
            auto [__, rowid_len2] = readVarint(table_page, p);
            p += rowid_len2;

            size_t record_start = p;
            auto [header_size, header_size_len] = readVarint(table_page, record_start);
            size_t header_varints_pos = record_start + header_size_len;
            size_t header_end = record_start + static_cast<size_t>(header_size);

            std::vector<uint64_t> serial_types;
            size_t hp = header_varints_pos;
            while (hp < header_end) {
                auto [st, st_len] = readVarint(table_page, hp);
                serial_types.push_back(st);
                hp += st_len;
            }

            size_t body_pos = header_end;
            size_t body_offset = 0;
            for (size_t col_index = 0; col_index < target_col_index && col_index < serial_types.size(); ++col_index) {
                body_offset += serialTypePayloadLength(serial_types[col_index]);
            }
            uint64_t col_serial = (target_col_index < serial_types.size()) ? serial_types[target_col_index] : 0;
            size_t col_len = serialTypePayloadLength(col_serial);

            std::string out;
            out.reserve(col_len);
            size_t col_start = body_pos + body_offset;
            for (size_t j = 0; j < col_len; ++j) out.push_back(static_cast<char>(table_page[col_start + j]));
            std::cout << out << std::endl;
        }
    }

    return 0;
}
