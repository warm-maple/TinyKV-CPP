#pragma once
#include <vector>
#include <memory>
#include <random>
#include <cstring>
#include <iostream>
#include <fstream>
template <typename K, typename V>
class SkipList
{
public:
    void display()
    {
        std::cout << "\n--- SkipList Structure ---" << std::endl;
        for (int i = level_; i >= 0; i--)
        {
            Node *acc = head_->forward[i];
            while (acc != nullptr && acc->forward[i] != nullptr)
            {
                std::cout << acc->key << " ";
                acc = acc->forward[i];
            }
            std::cout << std::endl;
        }
    }
    void remove(const K &key)
    {
        write_log(1, std::to_string(key), "");
        internal_insert(key, V(), true);
    }
    bool search(const K &key, V &value)
    {
        Node *accr = head_;
        for (int i = level_; i >= 0; i--)
        {
            while (accr->forward[i] != nullptr && accr->forward[i]->key < key)
            {
                accr = accr->forward[i];
            }
        }
        if (accr->forward[0] != nullptr && accr->forward[0]->key == key && accr->forward[0]->is_delete == false)
        {
            value = accr->forward[0]->value;
            return true;
        }
        else
            return false;
    }
    SkipList()
    {   
        head_ = new Node(K(), V(), kMaxLevel,false);
        level_ = 0;
        load_from_wal("kv_log.wal");
        log_file_.open("kv_log.wal", std::ios::app | std::ios::binary);
    }
    ~SkipList()
    {
        log_file_.close();
    }
    void insert(const K &key, const V &value)
    {
        write_log(0, std::to_string(key), value);
        internal_insert(key, value, false);
    }

private:
    struct Node
    {
        K key;
        V value;
        Node **forward;
        bool is_delete;
        int node_level;
        Node(K key_, V value_, int level,int is_delete_) : key(key_), value(value_), node_level(level), is_delete(is_delete_)
        {
            forward = new Node *[node_level + 1];
            memset(forward, 0, sizeof(Node *) * (node_level + 1));
        }
        ~Node()
        {
            delete[] forward;
        }
    };
    void load_from_wal(const std::string & wal_log_path){
        std::ifstream in(wal_log_path,std::ios::binary);
        if(!in.is_open())return;
        char type;
        while(in.read(&(type),sizeof(type))){
            uint32_t key_len;
            in.read(reinterpret_cast<char*>(&key_len),sizeof(key_len));
            std::string key_str(key_len,'\0');
            in.read(&key_str[0],key_len);
            uint32_t value_len;
            in.read(reinterpret_cast<char*>(&value_len),sizeof(value_len));
            std::string value_str(value_len,'\0');
            in.read(&value_str[0],value_len);
            int key=std::stoi(key_str);
            if(type==0){
                internal_insert(key,value_str,false);
            }else{internal_insert(key,"",true);}
           
        }
         in.close();
            std::cout << "--- 已成功从 WAL 日志重放数据，跳表恢复至断电前状态 ---" << std::endl;

    }
    void internal_insert(const K &key, const V &value, bool is_delete_mark)
    {
        int newlevel = GetRandomLevel();
        Node *position[kMaxLevel] = {nullptr};
        Node *nowpos = head_;
        for (int i = level_; i >= 0; i--)
        {
            while (nowpos->forward[i] != nullptr && nowpos->forward[i]->key < key)
            {
                nowpos = nowpos->forward[i];
            }
            position[i] = nowpos;
        }
        nowpos = position[0]->forward[0];
        if (nowpos != nullptr && nowpos->key == key)
        {
            nowpos->value = value;
            nowpos->is_delete = is_delete_mark;
            return;
        }
        if (newlevel > level_)
        {
            for (int i = level_ + 1; i <= newlevel; i++)
            {
                position[i] = head_;
            }
            level_ = newlevel;
        }
        Node *newnode = new Node(key, value, newlevel, is_delete_mark);

        for (int i = 0; i <= newlevel; i++)
        {
            newnode->forward[i] = position[i]->forward[i];
            position[i]->forward[i] = newnode;
        }
        node_count_++;
    }
    std::ofstream log_file_;
    Node *head_;
    int level_;
    int node_count_ = 0;
    const int kMaxLevel = 32;
    int GetRandomLevel()
    {
        int k = 0;
        while (rand() % 2)
            k++;
        k = k < kMaxLevel ? k : kMaxLevel;
        return k;
    }
    void write_log(int type, const std::string &key_str, const std::string &value_str)
    {
        if (!log_file_.is_open())
            return;
        char t = static_cast<char>(type);
        log_file_.write(reinterpret_cast<char *>(&t), sizeof(t));
        uint32_t key_len = key_str.size();
        log_file_.write(reinterpret_cast<char *>(&key_len), sizeof(key_len));
        log_file_.write(key_str.data(), key_len);
        uint32_t value_len = value_str.size();
        log_file_.write(reinterpret_cast<char *>(&value_len), sizeof(value_len));
        log_file_.write(value_str.data(), value_len);
        log_file_.flush();
    }
};
