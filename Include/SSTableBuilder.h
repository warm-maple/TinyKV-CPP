#pragma once
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
class SSTableBuilder{
    public:
    SSTableBuilder(const std::string& filename):stored_number_(0),offset_(0){
        outfile_.open(filename,std::ios::binary | std::ios::trunc);
    }
    ~SSTableBuilder(){
        outfile_.close();
    }
    void add(bool is_delete,const std::string& key,const std::string& value){
        std::string record;
        int type=is_delete?1:0;
        char t=static_cast<char>(type);
        record.append(reinterpret_cast<char*>(&t),1);
        uint32_t key_len=key.size();
        record.append(reinterpret_cast<char*>(&key_len),sizeof(key_len));
        record.append(key.data(),key_len);
        uint32_t value_len=is_delete?0:value.size();
        record.append(reinterpret_cast<char*>(&value_len),sizeof(value_len));
        if(!is_delete)record.append(value.data(),value_len);
        outfile_.write(record.data(),record.size());
        last_key_=key;
            stored_number_+=record.size();
        if(stored_number_>=kMaxMem_){
            block_index_.push_back({last_key_,offset_,stored_number_});
            offset_+=stored_number_;
            stored_number_=0;
        }
    }
    void finish(){
        if(stored_number_>0){
             block_index_.push_back({last_key_,offset_,stored_number_});
            offset_+=stored_number_;
            stored_number_=0;

        }
        uint32_t index_offset = offset_;
        std::string record;
        for(auto& obj : block_index_){
            uint32_t key_len=obj.last_key.size();
            record.append(reinterpret_cast<char*>(&key_len),sizeof(key_len));
            record.append(obj.last_key.data(),obj.last_key.size());
            record.append(reinterpret_cast<char*>(&obj.offset),sizeof(obj.offset));
            record.append(reinterpret_cast<char*>(&obj.block_size),sizeof(obj.block_size));
            outfile_.write(record.data(),record.size());
            record.clear();
        }
        uint32_t magic_number = 0xA1B2C3D4; 
        outfile_.write(reinterpret_cast<const char*>(&index_offset), sizeof(index_offset));
        outfile_.write(reinterpret_cast<const char*>(&magic_number), sizeof(magic_number));

        outfile_.flush();
    }

    private:
    std::ofstream outfile_;
    const int kMaxMem_=4096;
    uint32_t stored_number_;
    uint32_t offset_;
    std::string last_key_;
    struct index_block_{
        std::string last_key;
        uint32_t offset;
        uint32_t block_size;
    };
    std::vector <index_block_> block_index_;
};