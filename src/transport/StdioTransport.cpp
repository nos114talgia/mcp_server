#include <iostream>
#include "StdioTransport.h"
#include "aixlog.hpp"

namespace vx::transport {
    std::pair<size_t, std::string> Stdio::Read() {
        std::string json_data;

        while(true){
            int c;
            while((c = std::getc(stdin)) != EOF && c != '\n'){
                json_data += (char)c;
            }
            break;
        }

        return std::make_pair(json_data.length(), json_data);
    }

    std::future<std::pair<size_t, std::string>> Stdio::ReadAsync() {
        return std::async(std::launch::async, [this](){
            return Read();
        });
    }

    void Stdio::Write(const std::string& json_data){
        std::cout << json_data << '\n' << std::flush;
    }

    std::future<void> Stdio::WriteAsync(const std::string& json_data){
        return std::async(std::launch::async, [this, json_data](){
            Write(json_data);
        });
    }
}