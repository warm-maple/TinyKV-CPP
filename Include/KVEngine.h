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
#include "SSTableReader.h"
#include <filesystem>
#include <algorithm>
class KVEngine
{
public:
    KVEngine() : stop_bg_thread_(false), sst_file_count_(0),
                 data_dir_("data"), log_dir_("log")
    {
        std::filesystem::create_directories(data_dir_);
        std::filesystem::create_directories(log_dir_);
        active_mem_ = std::make_shared<SkipList<std::string, std::string>>();
        imm_mem_ = nullptr;
        LoadSSTables();
        std::string wal_path = log_dir_ + "/kv_log.wal";
        LoadFromWal(wal_path);
        wal_file_.open(wal_path, std::ios::app | std::ios::binary);
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
    bool get(const std::string& key, std::string& value) {
        std::shared_ptr<SkipList<std::string, std::string>> active;
        std::shared_ptr<SkipList<std::string, std::string>> imm;
        std::vector<std::shared_ptr<SSTableReader>> readers;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            active = active_mem_;
            imm = imm_mem_;
        }
        {
            std::unique_lock<std::mutex> r_lock(reader_mutex_);
            readers = sst_readers_; 
        }

        int active_res = active->search(key, value);
        if (active_res == 1) return true;       
        if (active_res == -1) return false;    
        if (imm) {
            int imm_res = imm->search(key, value);
            if (imm_res == 1) return true;
            if (imm_res == -1) return false;
        }
        for (auto& reader : readers) {
            SearchResult res = reader->get(key, value);
            if (res == SearchResult::FOUND) {
                return true;
            } else if (res == SearchResult::DELETED) {
                return false; 
            }

        }

        return false;
    }
    void remove(const std::string& key) {
        std::unique_lock<std::mutex> lock(mutex_);
        
        while (active_mem_->get_current_size() >= kMaxMemTableSize && imm_mem_ != nullptr) {
            bg_cv_.wait(lock);
        }
        if (active_mem_->get_current_size() >= kMaxMemTableSize) {
            imm_mem_ = active_mem_;
            active_mem_ = std::make_shared<SkipList<std::string, std::string>>();
            bg_cv_.notify_one();
        }
        WriteWal(1, key, "");
        active_mem_->remove(key);
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
      std::mutex reader_mutex_; 
    std::vector<std::shared_ptr<SSTableReader>> sst_readers_;
    const std::string data_dir_;
    const std::string log_dir_;
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
    void LoadSSTables() {
        std::vector<std::pair<int, std::string>> sst_files;
        for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                if (filename.find("data_") == 0 && filename.find(".sst") != std::string::npos) {
                    int num = std::stoi(filename.substr(5, filename.find(".sst") - 5));
                    sst_files.push_back({num, entry.path().string()});
                }
            }
        }
        std::sort(sst_files.begin(), sst_files.end(), [](const auto& a, const auto& b) {
            return a.first > b.first; 
        });
        for (const auto& file : sst_files) {
            sst_readers_.push_back(std::make_shared<SSTableReader>(file.second));
        }
        if (!sst_files.empty()) {
            sst_file_count_ = sst_files.front().first + 1;
            std::cout << "[KVEngine] 成功挂载 " << sst_files.size() << " 个历史 SSTable 文件！\n";
        }
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
                std::string sst_name = data_dir_ + "/data_" + std::to_string(sst_file_count_++) + ".sst";
                SSTableBuilder builder(sst_name);
                std::cout << "[BG_Thread] 开始将 MemTable 落盘至: " << sst_name << " ...\n";
                auto iter = mem_to_flush->GetIterator();
                while (iter.Valid()) {
                    builder.add(iter.IsDelete(), iter.Key(), iter.Value());
                    iter.Next();
                }
                builder.finish();
                std::cout << "[BG Tread] " << sst_name << " 落盘完成！\n";
                auto new_reader = std::make_shared<SSTableReader>(sst_name);
                {
                    std::unique_lock<std::mutex> r_lock(reader_mutex_);
                    sst_readers_.insert(sst_readers_.begin(), new_reader);
                }
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                imm_mem_ = nullptr; 
                
                if (wal_file_.is_open()) {
                    wal_file_.close();
                }
                std::string wal_path = log_dir_ + "/kv_log.wal";
                wal_file_.open(wal_path, std::ios::out | std::ios::binary | std::ios::trunc);
                std::cout << "[KVEngine] WAL 日志已清空重置。\n";
                
                bg_cv_.notify_all(); 
                }
            }

        }

    }
};