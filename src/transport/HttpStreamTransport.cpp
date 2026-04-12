#include "HttpStreamTransport.hpp"

namespace vx::transport {
    HttpStream::HttpStream(int port, std::string host): port_(port), host_(std::move(host)),
        server_(std::make_unique<httplib::Server>()) {
            SetupRoutes();
        }

    HttpStream::~HttpStream() {
        HttpStream::Stop();
    }

    bool HttpStream::Start() {
        if(server_running_.exchange(true)){
            return false;
        }

        server_thread_ = std::thread([this]() {
            LOG(INFO) << "Starting HTTP server on: " << host_ << ":" << port_ << "..." << std::endl;
            if(!server_->listen(host_.c_str(), port_)) {
                LOG(ERROR) << "Failed to start HTTP server on: " << host_ << ":" << port_ << std::endl;
                server_running_.store(false);
            }
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return server_running_.load();
    }

    void HttpStream::Stop() {
        if(!server_running_.exchange(false)){
            return;
        }

        client_connected_.store(false);
        sse_stream_active_.store(false);

        if(server_){
            server_->stop();
        }

        if(server_thread_.joinable()){
            server_thread_.join();
        }

        incoming_cv_.notify_all();
        sse_cv_.notify_all();

        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            for(auto& [id, pending] : pending_requests_) {
                try{
                    pending->promise.set_value("");
                } catch(...) {
                    LOG(ERROR) << "Failed to set promise value for pending request: " << id << std::endl;
                }
            }
            pending_requests_.clear();
        }
    }

    std::pair<size_t, std::string> HttpStream::Read() {
        std::unique_lock<std::mutex> lock(incoming_mutex_);
        incoming_cv_.wait(lock, [this]() {
            return !incoming_messages_.empty() || !server_running_.load();
        });

        if(!server_running_.load() && incoming_messages_.empty()) {
            return {0, ""};
        }

        if(!incoming_messages_.empty()){
            std::string message = std::move(incoming_messages_.front());
            incoming_messages_.pop();
            size_t len = message.length();
            return {len, std::move(message)};
        }

        return {0, ""};
    }

    void HttpStream::Write(const std::string& json_data) {
        if(!client_connected_.load()){
            return;
        }

        try{
            auto parsed = nlohmann::json::parse(json_data);

            if(parsed.contains("id") && (parsed.contains("result") || parsed.contains("error"))) {
                std::string id_str;
                if(parsed["id"].is_number()) {
                    id_str = std::to_string(parsed["id"].get<int>());
                } else {
                    id_str = parsed["id"].get<std::string>();
                }

                std::lock_guard<std::mutex> lock(pending_mutex_);
                auto it = pending_requests_.find(id_str);
                if(it != pending_requests_.end()){
                    LOG(DEBUG) << "Resolving pending request: " << id_str << std::endl;
                    it->second->promise.set_value(json_data);
                    pending_requests_.erase(it);
                    return;
                }
            }
            if(sse_stream_active_.load()){
                std::lock_guard<std::mutex> lock(sse_mutex_);
                sse_notifications_.push(json_data);
                sse_cv_.notify_one();
            }
        } catch(const std::exception& e) {
            LOG(ERROR) << "Error in Writing: " << e.what() << std::endl;
        }
    }

    std::future<std::pair<size_t, std::string>> HttpStream::ReadAsync() {
        return std::async(std::launch::async, [this]() -> std::pair<size_t, std::string> {
            return Read();
        });
    }

    std::future<void> HttpStream::WriteAsync(const std::string& json_data) {
        return std::async(std::launch::async, [this, json_data]() {
            Write(json_data);
        });
    }

    void HttpStream::SetupRoutes() {
        server_->Options("/.*", [](const httplib::Request& req, httplib::Response& res){
            HandleOptionsRequest(req, res);
        });

        server_->Get("/health", [](const httplib::Request& req, httplib::Response& res){
            res.set_content("{\"status\": \"OK\"}", "application/json");
        });

        server_->Get("/mcp", [this](const httplib::Request& req, httplib::Response& res){
            HandleGetSSE(req, res);
        });

        server_->Post("/mcp", [this](const httplib::Request& req, httplib::Response& res){
            HandlePostMessage(req, res);
        });

        server_->Delete("/mcp", [this](const httplib::Request& req, httplib::Response& res){
            HandleDeleteSession(req, res);
        });
    }



    void HttpStream::HandleDeleteSession(const httplib::Request& req, httplib::Response& res) {
        SetCORSHeaders(res);

        if(!session_initialized_){
            res.status = 404;
            res.set_content("{\"error\":\"No active session\"}", "application/json");
            return;
        }

        if(!ValidateSession(req, res)){
            return;
        }

        LOG(INFO) << "Deleting session: " << session_id_ << std::endl;

        session_initialized_ = false;
        client_connected_.store(false);
        sse_stream_active_.store(false);

        incoming_cv_.notify_all();
        sse_cv_.notify_all();

        res.status = 200;
        res.set_content("{\"status\": \"session terminated\"}", "application/json");
    }

    bool HttpStream::ValidateSession(const httplib::Request& req, httplib::Response& res) const{ 
        auto client_session = req.get_header_value("Mcp-Session-Id");
        if(client_session.empty() || client_session != session_id_){
            LOG(ERROR) << "Invalid session: " << client_session << std::endl;
            res.status = 404;
            res.set_content("{\"error\":\"Invalid or missing session ID\"}", "application/json");
            return false;
        }
        return true;
    }

    void HttpStream::HandleOptionsRequest(const httplib::Request& req, httplib::Response& res) {
        SetCORSHeaders(res);
        res.status = 200;
    }

    // set cross-origin resource sharing headers
    void HttpStream::SetCORSHeaders(httplib::Response& res) {
        res.set_header("Access-Control-Allow-Origin", "*");
        res.set_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
        res.set_header("Access-Control-Allow-Headers", "Content-Type, Authorization, Mcp-Session-Id");
        res.set_header("Access-Control-Expose-Headers", "Content-Type, Mcp-Session-Id");
        res.set_header("Access-Control-Max-Age", "86400");
    }

}