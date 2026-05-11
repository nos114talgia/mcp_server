#ifndef MCP_SERVER_STREAMABLE_TRANSPORT_H
#define MCP_SERVER_STREAMABLE_TRANSPORT_H

/**
 * @file StreamableTransport.h
 * @brief MCP Streamable HTTP 传输层 (2025-03-26 规范)
 *
 * 实现 MCP 规范中的 Streamable HTTP 传输协议：
 *   - POST /mcp  — 接收 JSON-RPC 消息，动态返回 application/json 或 text/event-stream
 *   - GET  /mcp  — 建立 SSE 长连接，接收服务器主动通知
 *   - DELETE /mcp — 销毁会话
 *
 * 关键特性：
 *   1. 动态响应策略：根据客户端 Accept 头和应用层决策，自动切换普通 JSON 响应与 SSE 流式响应
 *   2. 标准 SSE 格式：严格遵循 event: message + data: {json} 规范
 *   3. 会话管理：通过 MCP-Session-Id HTTP Header 实现会话识别
 *   4. 线程安全：核心队列和连接状态均使用互斥锁保护
 *
 * 设计约束：单客户端场景
 */

#include "ITransport.h"
#include "httplib.h"
#include "json.hpp"
#include "aixlog.hpp"
#include "utils/SessionBuilder.h"

#include <memory>
#include <atomic>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <unordered_map>
#include <future>
#include <functional>

namespace vx::transport {

    /**
     * @class StreamableTransport
     * @brief 符合 MCP 2025-03-26 Streamable HTTP 规范的传输层实现
     *
     * 继承 ITransport 接口，通过 cpp-httplib 构建 HTTP 服务端，
     * 支持客户端以 POST 方式发送 JSON-RPC 消息，并通过 Accept 头
     * 指示期望的响应格式（JSON 或 SSE 流）。
     */
    class StreamableTransport : public ITransport {
    public:
        /**
         * @brief 构造函数
         * @param port  HTTP 监听端口
         * @param host  绑定地址
         * @param endpoint  API 端点路径（默认 "/mcp"）
         */
        explicit StreamableTransport(int port = 8080,
                                     std::string host = "0.0.0.0",
                                     std::string endpoint = "/mcp");
        ~StreamableTransport();

        // 禁用拷贝和移动
        StreamableTransport(const StreamableTransport&) = delete;
        StreamableTransport(StreamableTransport&&) = delete;
        StreamableTransport& operator=(const StreamableTransport&) = delete;
        StreamableTransport& operator=(StreamableTransport&&) = delete;

        // ========== ITransport 接口 ==========

        bool Start() override;
        void Stop() override;
        bool IsRunning() override { return running_.load(); }

        /**
         * @brief 阻塞式读取一条来自客户端的 JSON-RPC 消息
         * @return {长度, JSON 字符串}，停止时返回 {0, ""}
         */
        std::pair<size_t, std::string> Read() override;

        /**
         * @brief 将服务器响应/通知写回客户端
         *
         * 路由逻辑：
         *   - 如果是某个待处理请求的响应（含 id + result/error）：
        *     · JSON 模式：通过 promise 回传给等待的 POST handler
        *     · SSE 模式：通过 DataSink 推送 SSE 事件
         *   - 如果是服务器通知（无 id）：推送到 GET SSE 流
         */
        void Write(const std::string& json_data) override;

        std::future<std::pair<size_t, std::string>> ReadAsync() override;
        std::future<void> WriteAsync(const std::string& json_data) override;

        std::string GetName() override { return "StreamableHTTP"; }
        std::string GetVersion() override { return "1.0.0"; }
        int GetPort() override { return port_; }

    private:
        // ========== 响应模式枚举 ==========

        /**
         * @brief POST 请求的响应模式
         *   - JSON: 返回 application/json（快速响应）
         *   - SSE:  升级为 text/event-stream（流式响应）
         */
        enum class ResponseMode {
            JSON,   ///< 标准 JSON 响应
            SSE     ///< Server-Sent Events 流式响应
        };

        // ========== 待处理请求结构 ==========

        /**
         * @brief 跟踪一个已入队但尚未完成的 POST 请求
         *
         * 当 POST /mcp 收到一个带 id 的请求时，创建此结构。
         * Server 层处理完毕后通过 Write() 回调，将结果分发给
         * 对应的响应通道（JSON promise 或 SSE sink）。
         */
        struct PendingRequest {
            ResponseMode mode;                          ///< 响应模式
            std::promise<std::string> json_promise;     ///< JSON 模式：结果通过 future 返回
            httplib::DataSink* sse_sink{nullptr};        ///< SSE 模式：结果通过 sink 推送（非拥有指针，生命周期由 httplib 管理）
            std::mutex sink_mutex;                      ///< 保护 sse_sink 的并发访问
            std::atomic<bool> stream_active{false};     ///< SSE 流是否仍然活跃
        };

        // ========== 路由与处理器 ==========

        /** @brief 注册 HTTP 路由 */
        void SetupRoutes();

        /**
         * @brief 处理 POST /mcp 请求
         *
         * 解析 JSON-RPC 消息，根据 Accept 头和消息类型决定响应策略：
         *   - 通知消息 (无 id) → 返回 202 Accepted
         *   - 请求消息 (有 id) → 入队等待 Server 处理，然后返回 JSON 或 SSE
         */
        void HandlePostMessage(const httplib::Request& req, httplib::Response& res);

        /**
         * @brief 处理 GET /mcp 请求 — SSE 长连接
         *
         * 建立 SSE 事件流，用于接收服务器主动推送的通知和流式事件。
         * 客户端必须携带有效的 MCP-Session-Id。
         */
        void HandleGetSSE(const httplib::Request& req, httplib::Response& res);

        /**
         * @brief 处理 DELETE /mcp 请求 — 销毁会话
         */
        void HandleDeleteSession(const httplib::Request& req, httplib::Response& res);

        /** @brief 处理 OPTIONS 预检请求 (CORS) */
        static void HandleOptionsRequest(const httplib::Request& req, httplib::Response& res);

        /** @brief 设置 CORS 响应头 */
        static void SetCORSHeaders(httplib::Response& res);

        /**
         * @brief 验证请求中的 MCP-Session-Id
         * @return true 验证通过，false 验证失败（已设置 404 响应）
         */
        bool ValidateSession(const httplib::Request& req, httplib::Response& res) const;

        /**
         * @brief 判断客户端是否接受 SSE 流式响应
         * @return true 客户端 Accept 头包含 text/event-stream
         */
        static bool ClientAcceptsSSE(const httplib::Request& req);

        /**
         * @brief 格式化 SSE 事件
         * @param data JSON 数据字符串
         * @return 符合规范的 SSE 事件文本: "event: message\ndata: {data}\n\n"
         */
        static std::string FormatSSEEvent(const std::string& data);

        // ========== 成员变量 ==========

        // HTTP 服务
        std::unique_ptr<httplib::Server> server_;   ///< cpp-httplib 服务实例
        std::thread server_thread_;                  ///< 服务监听线程
        std::atomic<bool> running_{false};           ///< 服务运行状态

        // 配置
        int port_;                  ///< 监听端口
        std::string host_;          ///< 绑定地址
        std::string endpoint_;      ///< API 端点路径

        // 单客户端会话
        std::string session_id_;                ///< 当前会话 ID
        bool session_initialized_{false};       ///< 会话是否已初始化
        std::atomic<bool> client_connected_{false}; ///< 客户端是否已连接

        // 入站消息队列 (POST → Server::Read() 消费)
        std::queue<std::string> incoming_queue_;
        std::mutex incoming_mutex_;
        std::condition_variable incoming_cv_;

        // 待处理请求映射 (request id → PendingRequest)
        std::unordered_map<std::string, std::shared_ptr<PendingRequest>> pending_requests_;
        std::mutex pending_mutex_;

        // GET /mcp SSE 通知通道 (服务器主动推送)
        std::queue<std::string> sse_notifications_;
        std::mutex sse_mutex_;
        std::condition_variable sse_cv_;
        std::atomic<bool> sse_stream_active_{false};
    };

} // namespace vx::transport

#endif // MCP_SERVER_STREAMABLE_TRANSPORT_H