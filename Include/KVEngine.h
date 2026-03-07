#pragma once
#include <fstream>
#include <pthread.h>
#include <iostream>
#include <memory>
#include "SkipList.h"
#include <mutex>
#include <condition_variable>
#include <thread>
#include "SSTableBuilder.h"
class KVEngine
{
public:
    KVEngine() : stop_bg_thread_(false), sst_file_count_(0)
    {
        active_mem_ = std::make_shared<SkipList<std::string, std::string>>();
        imm_mem_ = nullptr;
        LoadFromWal("kv_log.wal");
        wal_file_.open("kv_log.wal", std::ios::app | std::ios::binary);
        bg_thread_ = std::thread(&KVEngine::BackgroundFlushThread, this);
    }
    ~KVEngine()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        stop_bg_thread_ = true;
        bg_cv_.notify_all();
        if (bg_thread_.joinable())
        {
            bg_thread_.join();
        }
    }
    void put(const std::string &key, const std::string &value)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        while (active_mem_->get_current_size() >= kMaxMemTableSize && imm_mem_ != nullptr)
        {
            bg_cv_.wait(lock);
        }
        if (active_mem_->get_current_size() >= kMaxMemTableSize)
        {
            imm_mem_ = active_mem_;
            active_mem_ = std::make_shared<SkipList<std::string, std::string>>();
            bg_cv_.notify_one();
        }
        WriteWal(0, key, value);
        active_mem_->insert(key, value);
    }

private:
    std::shared_ptr<SkipList<std::string, std::string>> active_mem_;
    std::shared_ptr<SkipList<std::string, std::string>> imm_mem_;
    std::mutex mutex_;
    std::condition_variable bg_cv_;
    std::thread bg_thread_;
    bool stop_bg_thread_;
    int sst_file_count_;
    const uint32_t kMaxMemTableSize = 4 * 1024 * 1024;
      std::ofstream wal_file_;
      void WriteWal(int type, const std::string& key, const std::string& value) {
        if (!wal_file_.is_open()) return;
        char t = static_cast<char>(type);
        wal_file_.write(reinterpret_cast<const char*>(&t), sizeof(t));
        
        uint32_t key_len = key.size();
        wal_file_.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
        wal_file_.write(key.data(), key_len);
        
        uint32_t value_len = value.size();
        wal_file_.write(reinterpret_cast<const char*>(&value_len), sizeof(value_len));
        wal_file_.write(value.data(), value_len);
        
        wal_file_.flush();
    }
    void LoadFromWal(const std::string& wal_log_path) {
        std::ifstream in(wal_log_path, std::ios::binary);
        if (!in.is_open()) return;
        char type;
        while (in.read(&type, sizeof(type))) {
            uint32_t key_len;
            in.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
            std::string key_str(key_len, '\0');
            in.read(&key_str[0], key_len);
            
            uint32_t value_len;
            in.read(reinterpret_cast<char*>(&value_len), sizeof(value_len));
            std::string value_str(value_len, '\0');
            in.read(&value_str[0], value_len);
            if (type == 0) {
                active_mem_->insert(key_str, value_str);
            } else {
                active_mem_->remove(key_str); 
            }
        }
        in.close();
        std::cout << "[KVEngine] 已成功从 WAL 日志重放数据。\n";
    }
    void BackgroundFlushThread()
    {
        while(true){
            std::shared_ptr<SkipList<std::string, std::string>>mem_to_flush;
             
            {
            std::unique_lock<std::mutex> lock(mutex_); 
            bg_cv_.wait(lock,[this]{
                return imm_mem_!=nullptr||stop_bg_thread_;
            });
            if(stop_bg_thread_ && imm_mem_==nullptr)break;
            mem_to_flush=imm_mem_;
            }
            if(mem_to_flush){
                std::string sst_name = "data_" + std::to_string(sst_file_count_++) + ".sst";
                SSTableBuilder builder(sst_name);
                std::cout << "[BG_Thread] 开始将 MemTable 落盘至: " << sst_name << " ...\n";
                auto iter = mem_to_flush->GetIterator();
                while (iter.Valid()) {

                builder.add(iter.IsDelete(), iter.Key(), iter.Value());
                iter.Next();
            }
                std::cout << "[BG Tread] " << sst_name << " 落盘完成！\n";
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                imm_mem_ = nullptr; 
                
                if (wal_file_.is_open()) {
                    wal_file_.close();
                }
                wal_file_.open("kv_log.wal", std::ios::out | std::ios::binary | std::ios::trunc);
                std::cout << "[KVEngine] WAL 日志已清空重置。\n";
                
                bg_cv_.notify_all(); 
                }
            }

        }

    }
};