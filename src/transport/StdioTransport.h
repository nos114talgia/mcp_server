#ifndef MCP_SERVER_STDIOTRANSPORT_H
#define MCP_SERVER_STDIOTRANSPORT_H 

#include "ITransport.h"

namespace vx::transport{
    class Stdio : public ITransport {
    public:
        Stdio() = default;
        ~Stdio() = default;

        std::pair<size_t, std::string> Read() override;
        std::future<std::pair<size_t, std::string>> ReadAsync() override;
        void Write(const std::string& json_data) override;
        std::future<void> WriteAsync(const std::string& json_data) override;

        int GetPort() override { return 0;}
        std::string GetName() override { return "stdio";}
        std::string GetVersion() override {return "0.2.0";}

        bool IsRunning() override { return true;}
        bool Start() override {return true;}
        void Stop() override {}
    };

}

#endif