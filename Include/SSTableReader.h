#pragma once
#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <algorithm>
enum class SearchResult {
    FOUND,     
    DELETED,    
    NOT_FOUND
};
class SSTableReader {
public:
    SSTableReader(const std::string& filename) {
        infile_.open(filename, std::ios::binary | std::ios::ate); 
        if (!infile_.is_open()) {
            throw std::runtime_error("无法打开 SSTable 文件: " + filename);
        }
        size_t file_size = infile_.tellg();
        if (file_size < 8) {
            throw std::runtime_error("SSTable 文件损坏（过小）");
        }
        infile_.seekg(file_size - 8);
        uint32_t index_offset;
        uint32_t magic_number;
        infile_.read(reinterpret_cast<char*>(&index_offset), sizeof(index_offset));
        infile_.read(reinterpret_cast<char*>(&magic_number), sizeof(magic_number));
        if (magic_number != 0xA1B2C3D4) {
            throw std::runtime_error("SSTable 魔数校验失败，文件已损坏！");
        }
        infile_.seekg(index_offset);
        while (infile_.tellg() < file_size - 8) {
            IndexEntry entry;
            uint32_t key_len;
            infile_.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
            entry.last_key.resize(key_len);
            infile_.read(&entry.last_key[0], key_len);
            infile_.read(reinterpret_cast<char*>(&entry.offset), sizeof(entry.offset));
            infile_.read(reinterpret_cast<char*>(&entry.block_size), sizeof(entry.block_size));
            
            index_entries_.push_back(entry);
        }
    }

    ~SSTableReader() {
        if (infile_.is_open()) {
            infile_.close();
        }
    }
    SearchResult get(const std::string& target_key, std::string& value) {
        infile_.clear();
        if (index_entries_.empty()) return SearchResult::NOT_FOUND;
        auto it = std::lower_bound(index_entries_.begin(), index_entries_.end(), target_key,
            [](const IndexEntry& entry, const std::string& key) {
                return entry.last_key < key;
            });
        if (it == index_entries_.end()) {
            return SearchResult::NOT_FOUND;
        }
        infile_.seekg(it->offset);
        std::vector<char> block_data(it->block_size);
        infile_.read(block_data.data(), it->block_size);
        size_t pos = 0;
        while (pos < it->block_size) {
            char type = block_data[pos];
            pos += 1;
            uint32_t key_len = *reinterpret_cast<uint32_t*>(&block_data[pos]);
            pos += sizeof(uint32_t);
            std::string current_key(&block_data[pos], key_len);
            pos += key_len;
            uint32_t val_len = *reinterpret_cast<uint32_t*>(&block_data[pos]);
            pos += sizeof(uint32_t);
            if (current_key == target_key) {
                if (type == 1) {
                    return SearchResult::DELETED; 
                } else {
                    value = std::string(&block_data[pos], val_len);
                    return SearchResult::FOUND;  
                }
            }

            pos += val_len;
        }

        return SearchResult::NOT_FOUND;
    }

private:
    std::ifstream infile_;
    struct IndexEntry {
        std::string last_key;
        uint32_t offset;
        uint32_t block_size;
    };
    std::vector<IndexEntry> index_entries_;
};