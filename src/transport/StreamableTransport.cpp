/**
 * @file StreamableTransport.cpp
 * @brief MCP Streamable HTTP 传输层实现 (2025-03-26 规范)
 *
 * 实现细节：
 *   1. POST /mcp — 动态响应策略（JSON / SSE 流式切换）
 *   2. GET  /mcp — SSE 长连接，接收服务器通知
 *   3. DELETE /mcp — 会话销毁
 *   4. CORS 预检处理
 */

#include "StreamableTransport.h"

namespace vx::transport {

    // =========================================================================
    // 构造 / 析构
    // =========================================================================

    StreamableTransport::StreamableTransport(int port, std::string host, std::string endpoint)
        : port_(port)
        , host_(std::move(host))
        , endpoint_(std::move(endpoint))
        , server_(std::make_unique<httplib::Server>())
    {
        SetupRoutes();
    }

    StreamableTransport::~StreamableTransport() {
        StreamableTransport::Stop();
    }

    // =========================================================================
    // ITransport 生命周期
    // =========================================================================

    bool StreamableTransport::Start() {
        if (running_.exchange(true)) {
            LOG(WARNING) << "StreamableTransport already running" << std::endl;
            return false;
        }

        server_thread_ = std::thread([this]() {
            LOG(INFO) << "Streamable HTTP server starting on " << host_ << ":" << port_
                      << " (endpoint: " << endpoint_ << ")" << std::endl;
            if (!server_->listen(host_.c_str(), port_)) {
                LOG(ERROR) << "Streamable HTTP server failed to start on "
                           << host_ << ":" << port_ << std::endl;
                running_.store(false);
            }
        });

        // 等待服务启动
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return running_.load();
    }

    void StreamableTransport::Stop() {
        if (!running_.exchange(false)) {
            return;
        }

        LOG(INFO) << "Stopping Streamable HTTP server..." << std::endl;

        // 通知所有等待的线程
        client_connected_.store(false);
        sse_stream_active_.store(false);
        incoming_cv_.notify_all();
        sse_cv_.notify_all();

        // 停止 HTTP 服务
        if (server_) {
            server_->stop();
        }

        // 等待服务线程退出
        if (server_thread_.joinable()) {
            server_thread_.join();
        }

        // 清理所有待处理请求：设置空值让 future 解除阻塞
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            for (auto& [id, pending] : pending_requests_) {
                try {
                    pending->json_promise.set_value("");
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Failed to resolve pending request during shutdown: "
                               << id << " (" << e.what() << ")" << std::endl;
                }
            }
            pending_requests_.clear();
        }

        LOG(INFO) << "Streamable HTTP server stopped" << std::endl;
    }

    // =========================================================================
    // ITransport 读写接口
    // =========================================================================

    std::pair<size_t, std::string> StreamableTransport::Read() {
        std::unique_lock<std::mutex> lock(incoming_mutex_);
        incoming_cv_.wait(lock, [this]() {
            return !incoming_queue_.empty() || !running_.load();
        });

        if (!running_.load() && incoming_queue_.empty()) {
            return {0, ""};
        }

        if (!incoming_queue_.empty()) {
            std::string message = std::move(incoming_queue_.front());
            incoming_queue_.pop();
            return {message.length(), std::move(message)};
        }

        return {0, ""};
    }

    std::future<std::pair<size_t, std::string>> StreamableTransport::ReadAsync() {
        return std::async(std::launch::async, [this]() -> std::pair<size_t, std::string> {
            return Read();
        });
    }

    void StreamableTransport::Write(const std::string& json_data) {
        if (!client_connected_.load()) {
            return;
        }

        try {
            auto parsed = nlohmann::json::parse(json_data);

            // --- 路由判断：是某个请求的响应还是服务器通知？ ---

            // 条件：含 id 且含 result 或 error → 这是某个 POST 请求的响应
            if (parsed.contains("id") && (parsed.contains("result") || parsed.contains("error"))) {
                std::string id_str;
                if (parsed["id"].is_number()) {
                    id_str = std::to_string(parsed["id"].get<int>());
                } else if (parsed["id"].is_string()) {
                    id_str = parsed["id"].get<std::string>();
                } else {
                    id_str = "null";
                }

                std::lock_guard<std::mutex> lock(pending_mutex_);
                auto it = pending_requests_.find(id_str);
                if (it != pending_requests_.end()) {
                    auto& pending = it->second;

                    if (pending->mode == ResponseMode::JSON) {
                        // ---- JSON 模式：通过 promise 回传给 POST handler ----
                        LOG(DEBUG) << "[Streamable] Resolving JSON response for request " << id_str << std::endl;
                        pending->json_promise.set_value(json_data);
                    } else {
                        // ---- SSE 模式：通过 DataSink 推送 SSE 事件 ----
                        std::lock_guard<std::mutex> sink_lock(pending->sink_mutex);
                        if (pending->stream_active.load() && pending->sse_sink != nullptr) {
                            std::string sse_event = FormatSSEEvent(json_data);
                            LOG(DEBUG) << "[Streamable] Streaming SSE response for request "
                                       << id_str << ": " << sse_event << std::endl;
                            if (!pending->sse_sink->write(sse_event.c_str(), sse_event.length())) {
                                LOG(ERROR) << "[Streamable] Failed to write SSE event for request "
                                           << id_str << std::endl;
                                pending->stream_active.store(false);
                            }
                        }
                    }
                    pending_requests_.erase(it);
                    return;
                }
            }

            // --- 不是请求响应 → 视为服务器通知，推送到 GET SSE 流 ---
            if (sse_stream_active_.load()) {
                std::lock_guard<std::mutex> lock(sse_mutex_);
                sse_notifications_.push(json_data);
                sse_cv_.notify_one();
                LOG(DEBUG) << "[Streamable] Queued notification for GET SSE stream" << std::endl;
            }

        } catch (const nlohmann::json::exception& e) {
            LOG(ERROR) << "[Streamable] JSON parse error in Write(): " << e.what() << std::endl;
        } catch (const std::exception& e) {
            LOG(ERROR) << "[Streamable] Error in Write(): " << e.what() << std::endl;
        }
    }

    std::future<void> StreamableTransport::WriteAsync(const std::string& json_data) {
        return std::async(std::launch::async, [this, json_data]() {
            Write(json_data);
        });
    }

    // =========================================================================
    // 路由注册
    // =========================================================================

    void StreamableTransport::SetupRoutes() {
        // CORS 预检
        server_->Options("/.*", [](const httplib::Request& req, httplib::Response& res) {
            HandleOptionsRequest(req, res);
        });

        // 健康检查
        server_->Get("/health", [](const httplib::Request& req, httplib::Response& res) {
            res.set_content(R"({"status":"ok","transport":"streamable-http"})", "application/json");
        });

        // POST /mcp — 接收 JSON-RPC 消息
        server_->Post(endpoint_, [this](const httplib::Request& req, httplib::Response& res) {
            HandlePostMessage(req, res);
        });

        // GET /mcp — SSE 长连接
        server_->Get(endpoint_, [this](const httplib::Request& req, httplib::Response& res) {
            HandleGetSSE(req, res);
        });

        // DELETE /mcp — 销毁会话
        server_->Delete(endpoint_, [this](const httplib::Request& req, httplib::Response& res) {
            HandleDeleteSession(req, res);
        });

        LOG(INFO) << "[Streamable] Routes registered: POST/GET/DELETE " << endpoint_ << std::endl;
    }

    // =========================================================================
    // POST /mcp — 核心请求处理器
    // =========================================================================

    void StreamableTransport::HandlePostMessage(const httplib::Request& req, httplib::Response& res) {
        SetCORSHeaders(res);

        // ---- 1. Content-Type 验证 ----
        auto content_type = req.get_header_value("Content-Type");
        if (content_type.find("application/json") == std::string::npos) {
            res.status = 415;
            res.set_content(R"({"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Content-Type. Expected application/json"},"id":null})", "application/json");
            return;
        }

        // ---- 2. Accept 头验证 ----
        auto accept = req.get_header_value("Accept");
        if (!accept.empty() &&
            accept.find("application/json") == std::string::npos &&
            accept.find("text/event-stream") == std::string::npos) {
            res.status = 406;
            res.set_content(R"({"jsonrpc":"2.0","error":{"code":-32600,"message":"Not Acceptable. Expected application/json or text/event-stream"},"id":null})", "application/json");
            return;
        }

        // ---- 3. 消息体验证 ----
        std::string message = req.body;
        if (message.empty()) {
            res.status = 400;
            res.set_content(R"({"jsonrpc":"2.0","error":{"code":-32600,"message":"Empty request body"},"id":null})", "application/json");
            return;
        }

        nlohmann::json parsed;
        try {
            parsed = nlohmann::json::parse(message);
        } catch (const nlohmann::json::parse_error& e) {
            res.status = 400;
            res.set_content(R"({"jsonrpc":"2.0","error":{"code":-32700,"message":"Parse error"},"id":null})", "application/json");
            return;
        }

        // ---- 4. 会话管理 ----
        bool is_initialize = parsed.contains("method") && parsed["method"] == "initialize";

        if (is_initialize) {
            // 创建新会话
            session_id_ = vx::utils::SessionBuilder::GenerateUniqueSessionID();
            session_initialized_ = true;
            client_connected_.store(true);
            LOG(INFO) << "[Streamable] Session created: " << session_id_ << std::endl;
        } else if (session_initialized_) {
            // 非 initialize 请求必须携带有效会话 ID
            if (!ValidateSession(req, res)) {
                return;
            }
        }

        // ---- 5. 通知消息 (无 id) → 202 Accepted ----
        bool is_notification = !parsed.contains("id");
        if (is_notification) {
            LOG(DEBUG) << "[Streamable] Received notification: " << message << std::endl;
            {
                std::lock_guard<std::mutex> lock(incoming_mutex_);
                incoming_queue_.push(message);
            }
            incoming_cv_.notify_one();

            res.status = 202;
            if (session_initialized_) {
                res.set_header("MCP-Session-Id", session_id_);
            }
            return;
        }

        // ---- 6. 请求消息 (有 id) → 入队等待处理 ----
        std::string id_str;
        if (parsed["id"].is_number()) {
            id_str = std::to_string(parsed["id"].get<int>());
        } else if (parsed["id"].is_string()) {
            id_str = parsed["id"].get<std::string>();
        } else {
            id_str = "null";
        }

        LOG(DEBUG) << "[Streamable] Received request id=" << id_str << ": " << message << std::endl;

        // ---- 7. 动态响应策略：根据 Accept 头决定 JSON 或 SSE ----
        bool use_sse = ClientAcceptsSSE(req);

        auto pending = std::make_shared<PendingRequest>();
        pending->mode = use_sse ? ResponseMode::SSE : ResponseMode::JSON;

        if (!use_sse) {
            // ==== JSON 模式 ====
            // 通过 promise/future 同步等待 Server 处理结果
            std::future<std::string> response_future = pending->json_promise.get_future();

            {
                std::lock_guard<std::mutex> lock(pending_mutex_);
                pending_requests_.emplace(id_str, pending);
            }
            {
                std::lock_guard<std::mutex> lock(incoming_mutex_);
                incoming_queue_.push(message);
            }
            incoming_cv_.notify_one();

            // 等待 Server 处理完成（超时 30 秒）
            auto status = response_future.wait_for(std::chrono::seconds(30));
            if (status == std::future_status::timeout) {
                LOG(ERROR) << "[Streamable] Request timed out (id=" << id_str << ")" << std::endl;
                {
                    std::lock_guard<std::mutex> lock(pending_mutex_);
                    pending_requests_.erase(id_str);
                }
                res.status = 504;
                res.set_content(R"({"jsonrpc":"2.0","error":{"code":-32603,"message":"Request timed out"},"id":null})", "application/json");
                return;
            }

            std::string response_data = response_future.get();
            if (response_data.empty()) {
                res.status = 500;
                res.set_content(R"({"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal server error"},"id":null})", "application/json");
                return;
            }

            res.status = 200;
            res.set_content(response_data, "application/json");
            if (session_initialized_) {
                res.set_header("MCP-Session-Id", session_id_);
            }

        } else {
            // ==== SSE 流式模式 ====
            // 设置响应头为 SSE
            res.set_header("Content-Type", "text/event-stream");
            res.set_header("Cache-Control", "no-cache");
            res.set_header("Connection", "keep-alive");
            if (session_initialized_) {
                res.set_header("MCP-Session-Id", session_id_);
            }

            // 注册到待处理映射
            pending->stream_active.store(true);
            {
                std::lock_guard<std::mutex> lock(pending_mutex_);
                pending_requests_.emplace(id_str, pending);
            }
            // 将消息入队给 Server 处理
            {
                std::lock_guard<std::mutex> lock(incoming_mutex_);
                incoming_queue_.push(message);
            }
            incoming_cv_.notify_one();

            // 使用 content_provider 持续推送 SSE 事件
            res.set_content_provider("text/event-stream",
                [this, id_str, pending_weak = std::weak_ptr<PendingRequest>(pending)](
                    size_t offset, httplib::DataSink& sink) -> bool {

                    auto pending = pending_weak.lock();
                    if (!pending) {
                        return false;  // 请求已被清理
                    }

                    // 首次进入：将 sink 存入 PendingRequest，供 Write() 使用
                    // 注意：DataSink 不可拷贝，存原始指针（生命周期由 httplib 管理）
                    if (!pending->sse_sink) {
                        {
                            std::lock_guard<std::mutex> lock(pending->sink_mutex);
                            pending->sse_sink = &sink;
                        }
                        LOG(DEBUG) << "[Streamable] SSE sink attached for request " << id_str << std::endl;
                    }

                    // 等待直到流结束或服务停止
                    // Write() 会通过 sink 推送数据
                    // 这里只需要保持连接存活，定期发送心跳
                    using clock = std::chrono::steady_clock;
                    auto wait_start = clock::now();
                    const auto max_wait = std::chrono::seconds(60);

                    while (pending->stream_active.load() && running_.load()) {
                        if (clock::now() - wait_start > max_wait) {
                            LOG(WARNING) << "[Streamable] SSE stream timeout for request " << id_str << std::endl;
                            break;
                        }

                        // 检查 pending 是否还在 map 中（Write 会 erase）
                        {
                            std::lock_guard<std::mutex> lock(pending_mutex_);
                            if (pending_requests_.find(id_str) == pending_requests_.end()) {
                                // 已被 Write() 处理完毕
                                return false;
                            }
                        }

                        std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    }

                    return false;
                }
            );
        }
    }

    // =========================================================================
    // GET /mcp — SSE 长连接（服务器通知通道）
    // =========================================================================

    void StreamableTransport::HandleGetSSE(const httplib::Request& req, httplib::Response& res) {
        SetCORSHeaders(res);

        // 会话验证
        if (session_initialized_ && !ValidateSession(req, res)) {
            return;
        }

        LOG(INFO) << "[Streamable] GET SSE stream connected" << std::endl;

        // 设置 SSE 响应头
        res.set_header("Content-Type", "text/event-stream");
        res.set_header("Cache-Control", "no-cache");
        res.set_header("Connection", "keep-alive");
        if (session_initialized_) {
            res.set_header("MCP-Session-Id", session_id_);
        }

        sse_stream_active_.store(true);

        // 使用 content_provider 实现持续的 SSE 推送
        res.set_content_provider("text/event-stream",
            [this](size_t offset, httplib::DataSink& sink) -> bool {
                using clock = std::chrono::steady_clock;
                static thread_local auto last_ping = clock::now();
                const auto ping_interval = std::chrono::seconds(5);

                auto terminate = [this]() -> bool {
                    sse_stream_active_.store(false);
                    sse_cv_.notify_all();
                    return false;
                };

                try {
                    // 心跳保活
                    if (clock::now() - last_ping > ping_interval) {
                        const char* ping = ": keepalive\n\n";
                        if (!sink.write(ping, std::strlen(ping))) {
                            LOG(ERROR) << "[Streamable] GET SSE keepalive write failed" << std::endl;
                            return terminate();
                        }
                        last_ping = clock::now();
                    }

                    // 等待通知到达
                    std::unique_lock<std::mutex> lock(sse_mutex_);
                    sse_cv_.wait_for(lock, std::chrono::milliseconds(200), [this]() {
                        return !sse_notifications_.empty() || !sse_stream_active_.load();
                    });

                    if (!sse_stream_active_.load()) {
                        return terminate();
                    }

                    // 推送通知事件
                    if (!sse_notifications_.empty()) {
                        std::string notification = std::move(sse_notifications_.front());
                        sse_notifications_.pop();
                        lock.unlock();

                        std::string sse_event = FormatSSEEvent(notification);
                        LOG(DEBUG) << "[Streamable] GET SSE push: " << sse_event << std::endl;

                        if (!sink.write(sse_event.c_str(), sse_event.length())) {
                            LOG(ERROR) << "[Streamable] GET SSE write failed" << std::endl;
                            return terminate();
                        }
                    }

                    return true;

                } catch (const std::exception& e) {
                    LOG(ERROR) << "[Streamable] GET SSE error: " << e.what() << std::endl;
                    return terminate();
                } catch (...) {
                    LOG(ERROR) << "[Streamable] GET SSE unknown error" << std::endl;
                    return terminate();
                }
            }
        );
    }

    // =========================================================================
    // DELETE /mcp — 会话销毁
    // =========================================================================

    void StreamableTransport::HandleDeleteSession(const httplib::Request& req, httplib::Response& res) {
        SetCORSHeaders(res);

        if (!session_initialized_) {
            res.status = 404;
            res.set_content(R"({"jsonrpc":"2.0","error":{"code":-32600,"message":"No active session"},"id":null})", "application/json");
            return;
        }

        if (!ValidateSession(req, res)) {
            return;
        }

        LOG(INFO) << "[Streamable] Deleting session: " << session_id_ << std::endl;

        // 清理会话状态
        session_initialized_ = false;
        client_connected_.store(false);
        sse_stream_active_.store(false);

        // 唤醒所有等待线程
        incoming_cv_.notify_all();
        sse_cv_.notify_all();

        // 清理待处理请求
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            for (auto& [id, pending] : pending_requests_) {
                pending->stream_active.store(false);
                try {
                    pending->json_promise.set_value("");
                } catch (...) {}
            }
            pending_requests_.clear();
        }

        res.status = 200;
        res.set_content(R"({"status":"session terminated"})", "application/json");
    }

    // =========================================================================
    // 会话验证
    // =========================================================================

    bool StreamableTransport::ValidateSession(const httplib::Request& req, httplib::Response& res) const {
        auto client_session = req.get_header_value("MCP-Session-Id");
        if (client_session.empty() || client_session != session_id_) {
            LOG(WARNING) << "[Streamable] Invalid session ID: '" << client_session
                         << "' (expected: '" << session_id_ << "')" << std::endl;
            res.status = 404;
            res.set_content(R"({"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid or missing MCP-Session-Id"},"id":null})", "application/json");
            return false;
        }
        return true;
    }

    // =========================================================================
    // 静态工具方法
    // =========================================================================

    bool StreamableTransport::ClientAcceptsSSE(const httplib::Request& req) {
        auto accept = req.get_header_value("Accept");
        if (accept.empty()) {
            return false;
        }
        // 客户端 Accept 头包含 text/event-stream 则使用 SSE 模式
        return accept.find("text/event-stream") != std::string::npos;
    }

    std::string StreamableTransport::FormatSSEEvent(const std::string& data) {
        // 严格遵循 MCP SSE 规范：event: message + data: {json}
        return "event: message\ndata: " + data + "\n\n";
    }

    void StreamableTransport::HandleOptionsRequest(const httplib::Request& /*req*/, httplib::Response& res) {
        SetCORSHeaders(res);
        res.status = 200;
    }

    void StreamableTransport::SetCORSHeaders(httplib::Response& res) {
        res.set_header("Access-Control-Allow-Origin", "*");
        res.set_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
        res.set_header("Access-Control-Allow-Headers", "Content-Type, Authorization, MCP-Session-Id");
        res.set_header("Access-Control-Expose-Headers", "Content-Type, MCP-Session-Id");
        res.set_header("Access-Control-Max-Age", "86400");
    }

} // namespace vx::transport