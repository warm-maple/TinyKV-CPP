#include <iostream>
#include <fstream>
std::fstream log_file;
int main(){
    log_file.open("wal.log",std::ios::app|std::ios::binary);
    if(!log_file.is_open())std::cout << "水管没接上（文件打开失败）！" << std::endl;
    int type=1;
    log_file.write(reinterpret_cast<char*>(&type),sizeof(type));
    std::string key = "BUPT";
    int key_len = key.size();
    log_file.write(reinterpret_cast<char*>(&key_len), sizeof(key_len));
    log_file.write(key.data(), key_len);
    log_file.flush(); 
    log_file.close();

}