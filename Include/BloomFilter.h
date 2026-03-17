#pragma once
#include <string>
#include <vector>
#include <functional>

class BloomFilter {
public:
    BloomFilter(size_t keys_count = 10000) {
        bits_per_key_ = 10;
        k_ = 3; 
        size_t bit_size = keys_count * bits_per_key_;
        size_t byte_size = (bit_size + 7) / 8;
        bit_array_.resize(byte_size, 0); 
    }
    BloomFilter(const std::string& data) : bit_array_(data.begin(), data.end()) {
        bits_per_key_ = 10;
        k_ = 3;
    }

    void add(const std::string& key) {
        uint32_t h1 = std::hash<std::string>{}(key);
        uint32_t h2 = (h1 >> 17) | (h1 << 15); 
        
        size_t bit_size = bit_array_.size() * 8;
        if (bit_size == 0) return;

        for (int i = 0; i < k_; ++i) {
            uint32_t combined_hash = h1 + i * h2;
            uint32_t bit_pos = combined_hash % bit_size;
            uint32_t byte_pos = bit_pos / 8;
            uint32_t bit_offset = bit_pos % 8;
            bit_array_[byte_pos] |= (1 << bit_offset);
        }
    }

    bool might_contain(const std::string& key) const {
        uint32_t h1 = std::hash<std::string>{}(key);
        uint32_t h2 = (h1 >> 17) | (h1 << 15);
        
        size_t bit_size = bit_array_.size() * 8;
        if (bit_size == 0) return false;

        for (int i = 0; i < k_; ++i) {
            uint32_t combined_hash = h1 + i * h2;
            uint32_t bit_pos = combined_hash % bit_size;
            
            uint32_t byte_pos = bit_pos / 8;
            uint32_t bit_offset = bit_pos % 8;
            if ((bit_array_[byte_pos] & (1 << bit_offset)) == 0) {
                return false;
            }
        }
        return true;
    }
    const std::string data() const {
        return std::string(bit_array_.begin(), bit_array_.end());
    }

private:
    std::vector<char> bit_array_; 
    int bits_per_key_;
    int k_;
};