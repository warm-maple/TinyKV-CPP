#pragma once
#include <vector>
#include <memory>
#include <random>
#include <cstring>
#include <string>
#include <iostream>
#include <fstream>
#include <type_traits>
template <typename K, typename V>
class SkipList
{
public:
  struct Node;
class Iterator {
    public:
        Iterator(Node* node) : current_(node) {}
        
        bool Valid() const { return current_ != nullptr; }
        void Next() { if (current_) current_ = current_->forward[0]; }
        K& Key() const { return current_->key; }
        V& Value() const { return current_->value; }
        bool IsDelete() const { return current_->is_delete; }
        
    private:
        Node* current_;
    };
    Iterator GetIterator() {
        return Iterator(head_->forward[0]); 
    }
    void display()
    {
        std::cout << "\n--- SkipList Structure ---" << std::endl;
        for (int i = level_; i >= 0; i--)
        {
            Node *acc = head_->forward[i];
            while (acc != nullptr)
            {
                std::cout << acc->key << " ";
                acc = acc->forward[i];
            }
            std::cout << std::endl;
        }
    }
    void remove(const K &key)
    {
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
    SkipList() : is_recovering_(false), level_(0), node_count_(0), sst_file_count_(0), current_mem_size_(0)
    {
        head_ = new Node(K(), V(), kMaxLevel, false);
    }
    ~SkipList()
    {
        Node *acc = head_->forward[0];
        Node *next = acc;
        while (next != nullptr)
        {
            next = acc->forward[0];
            delete (acc);
            acc = next;
        }
        delete (head_);
    }
    void insert(const K &key, const V &value)
    {
        internal_insert(key, value, false);
    }
    uint32_t get_current_size()
    {
        return current_mem_size_;
    }

private:
    struct Node
    {
        K key;
        V value;
        std::vector<Node *> forward;
        bool is_delete;
        int node_level;
        Node(K key_, V value_, int level, bool is_delete_) : key(key_), value(value_), forward(level + 1, nullptr), is_delete(is_delete_), node_level(level)
        {
        }
    };

    Node *head_;
    int level_;
    int node_count_;
    const int kMaxLevel = 32;
    std::mt19937 generator_{std::random_device{}()};
    uint32_t current_mem_size_;
    const uint32_t kMaxMemTableSize = 4 * 1024 * 1024;
    int sst_file_count_;
    void internal_insert(const K &key, const V &value, bool is_delete_mark)
    {
        int newlevel = GetRandomLevel();
        std::vector<Node *> position(kMaxLevel + 1, nullptr);
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
        uint32_t node_size = sizeof(Node) + (newlevel + 1) * sizeof(Node *);
        if constexpr (std::is_same_v<K, std::string>)
            node_size += key.size();
        else
            node_size += sizeof(key);

        if (!is_delete_mark)
        {
            if constexpr (std::is_same_v<V, std::string>)
                node_size += value.size();
            else
                node_size += sizeof(value);
        }
        current_mem_size_+=node_size;
    }
    int GetRandomLevel()
    {
        int k = 0;
        std::uniform_int_distribution<int> dist(0, 1);
        while (dist(generator_) && k < kMaxLevel)
        {
            k++;
        }
        return k;
    }
};
