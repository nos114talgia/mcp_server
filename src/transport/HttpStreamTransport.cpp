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

        incoming_cv_.notify_all();
        sse_cv_.notify_all();

        if(server_){
            server_->stop();
        }

        if(server_thread_.joinable()){
            server_thread_.join();
        }

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
    void HttpStream::HandlePostMessage(const httplib::Request& req, httplib::Response& res) {
        SetCORSHeaders(res);

        auto content_type = req.get_header_value("Content-Type");
        if(content_type.find("application/json") == std::string::npos) {
            res.status = 415;
            res.set_content("{\"error\":\"Invalid Content-Type. Expected application/json\"}", "application/json");
            return;
        }

        auto accept = req.get_header_value("Accept");
        if(!accept.empty() && accept.find("application/json") == std::string::npos) {
            res.status = 406;
            res.set_content("{\"error\":\"Not Acceptable. Expected application/json\"}", "application/json");
            return;
        }

        std::string message = req.body;
        if(message.empty()){
            res.status = 400;
            res.set_content("{\"error\":\"Empty message body\"}", "application/json");
            return;
        }

        nlohmann::json parsed;
        try{
            parsed = nlohmann::json::parse(message);
        } catch (const nlohmann::json::parse_error& e) {
            res.status = 400;
            res.set_content("{\"error\":\"Invalid JSON\"}", "application/json");
            return;
        }

        bool is_initialize = parsed.contains("method") && parsed["method"] == "initialize";
        if(is_initialize){ 
            session_id_ = vx::utils::SessionBuilder::GenerateUniqueSessionID();
            session_initialized_ = true;
            client_connected_.store(true);
            LOG(INFO) << "Initializing session: " << session_id_ << std::endl;
        } else if(session_initialized_){
            if(!ValidateSession(req, res)){
                return;
            }
        }

        bool is_notification = !parsed.contains("id");
        if(is_notification){
            LOG(DEBUG) << "Received notification via POST: " << message << std::endl;
            {
                std::lock_guard<std::mutex> lock(incoming_mutex_);
                incoming_messages_.push(message);
            }
            incoming_cv_.notify_one();

            res.status = 202;
            if(session_initialized_){
                res.set_header("Mcp-Session-Id", session_id_);
            }
            return;
        }

        std::string id_str;
        if(parsed["id"].is_number()) {
            id_str = std::to_string(parsed["id"].get<int>());
        } else {
            id_str = parsed["id"].get<std::string>();
        }

        LOG(DEBUG) << "Received request via POST (id=" << id_str << "): " << message << std::endl;

        auto pending = std::make_shared<PendingRequest>();
        std::future<std::string> response_future = pending->promise.get_future();
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_requests_.emplace(id_str, pending);
        }
        {
            std::lock_guard<std::mutex> lock(incoming_mutex_);
            incoming_messages_.push(message);
        }
        incoming_cv_.notify_one();

        auto status = response_future.wait_for(std::chrono::seconds(30));
        if(status == std::future_status::timeout){
            LOG(ERROR) << "Request timed out (id=" << id_str << ")" << std::endl;
            {
                std::lock_guard<std::mutex> lock(pending_mutex_);
                pending_requests_.erase(id_str);
            }
            res.status = 504;
            res.set_content("{\"error\":\"Request timed out\"}", "application/json");
            return;
        }

        std::string response_data = response_future.get();
        if(response_data.empty()){
            res.status = 500;
            res.set_content("{\"error\":\"Internal Server Error\"}", "application/json");
            return;
        }

        res.status = 200;
        res.set_content(response_data, "application/json");
        if(session_initialized_){
            res.set_header("Mcp-Session-Id", session_id_);
        }
        return;
    }

    void HttpStream::HandleGetSSE(const httplib::Request& req, httplib::Response& res) { 
        SetCORSHeaders(res);

        if(session_initialized_ && !ValidateSession(req, res)){
            return;
        }

        LOG(DEBUG) << "SSE stream client connected" << std::endl;
        res.set_header("Content-Type", "text/event-stream");
        res.set_header("Cache-Control", "no-cache");
        res.set_header("Connection", "keep-alive");
        if(session_initialized_){
            res.set_header("Mcp-Session-Id", session_id_);
        }

        sse_stream_active_.store(true);

        res.set_content_provider("text/event-stream",
            [this](size_t offset, httplib::DataSink& sink) -> bool {
                using clock = std::chrono::steady_clock;
                static thread_local auto last_ping = clock::now();
                const auto ping_interval = std::chrono::seconds(5);

                auto terminate = [this]() ->bool {
                    sse_stream_active_.store(false);
                    sse_cv_.notify_all();
                    return false;
                };

                try{
                    if(clock::now() - last_ping > ping_interval){
                        const char* ping = ": ping\n\n";
                        if(!sink.write(ping, std::strlen(ping))){
                            LOG(ERROR) << "SSE keep-alive write failed" << std::endl;
                            return terminate();
                        }
                        last_ping = clock::now();
                    }

                    std::unique_lock<std::mutex> lock(sse_mutex_);
                    sse_cv_.wait_for(lock, std::chrono::milliseconds(200), [this]() {
                        return !sse_notifications_.empty() || !sse_stream_active_.load();
                    });

                    if(!sse_stream_active_.load()){
                        return terminate();
                    }

                    if(!sse_notifications_.empty()){
                        std::string message = std::move(sse_notifications_.front());
                        sse_notifications_.pop();
                        lock.unlock();

                        std::string sse_msg = "event: message\ndata: " + message + "\n\n";
                        LOG(DEBUG) << "SSE stream message: " << sse_msg << std::endl;

                        if(!sink.write(sse_msg.c_str(), sse_msg.length())){
                            LOG(ERROR) << "SSE stream write failed" << std::endl;
                            return terminate();
                        }
                    }
                    return true;

                } catch (const std::exception& e) {
                    LOG(ERROR) << "SSE stream error: " << e.what() << std::endl;
                    return terminate();
                } catch (...) {
                    LOG(ERROR) << "Unknown error" << std::endl;
                    return terminate();
                }

            }
        );
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