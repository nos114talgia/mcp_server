#ifndef MCP_SERVER_SSE_TRANSPORT_HPP
#define MCP_SERVER_SSE_TRANSPORT_HPP

#include "ITransport.h"
#include <memory>
#include <atomic>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <httplib.h>
#include "utils/SessionBuilder.h"

namespace vx::transport {

    class SSE : public vx::ITransport {
    public:
        explicit SSE(int port = 8080, std::string  host = "0.0.0.0");
        ~SSE();
        SSE(const SSE&) = delete;
        SSE& operator=(const SSE&) = delete;

        SSE(SSE&&) = delete;
        SSE& operator=(SSE&&) = delete;

        // Transport interface
        std::pair<size_t, std::string> Read() override;
        void Write(const std::string& json_data) override;

        std::future<std::pair<size_t, std::string>> ReadAsync() override;
        std::future<void> WriteAsync(const std::string& json_data) override;

        std::string GetName() override { return "sse"; };
        std::string GetVersion() override { return "0.4"; };
        int GetPort() override { return port_; };

        bool Start() override;
        void Stop() override;
        bool IsRunning() override { return server_running_.load(); }

    private:
        void SetupRoutes();
        void HandleSSEConnection(const httplib::Request& req, httplib::Response& res);
        void HandlePostMessage(const httplib::Request& req, httplib::Response& res);

        static void HandleOptionsRequest(const httplib::Request& req, httplib::Response& res);
        static void SetCORSHeaders(httplib::Response& res);

        std::string host_;
        int port_;
        std::unique_ptr<httplib::Server> server_;
        std::thread server_thread_;
        std::atomic<bool> server_running_ {false};
        std::atomic<bool> client_connected_ {false};

        // Message queues for bidirectional connections
        std::queue<std::string> incoming_messages_;
        std::queue<std::string> outgoing_messages_;
        std::mutex incoming_mutex_;
        std::mutex outgoing_mutex_;
        std::condition_variable incoming_cv_;
        std::condition_variable outgoing_cv_;

        // SSE connection management
        std::atomic<bool> sse_active_ {false};
    };
}

#endif