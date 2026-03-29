#ifndef MCP_SERVER_ITRANSPORT_H
#define MCP_SERVER_ITRANSPORT_H 

#include <string>
#include <future>

namespace vx {

    class ITransport{
    public:
        ITransport() = default;
        ~ITransport() = default;

        virtual bool Start() = 0;
        virtual void Stop() = 0;
        virtual bool IsRunning() = 0;

        virtual std::pair<size_t, std::string> Read() = 0;
        virtual void Write(const std::string& json_data) = 0;

        virtual std::future<std::pair<size_t, std::string>> ReadAsync() = 0;
        virtual std::future<void> WriteAsync(const std::string& json_data) = 0;

        virtual std::string GetName() = 0;
        virtual std::string GetVersion() = 0;
        virtual int GetPort() = 0;
    };
}

#endif