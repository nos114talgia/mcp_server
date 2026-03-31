#include "version.h"
#include "httplib.h"
#include "popl.hpp"
#include "aixlog.hpp"
#include "json.hpp"
#include "StdioTransport.h"
// #include "SseTransport.h"
#include "server/Server.h"
#include "loader/PluginsLoader.h"
#include "utils/MCPBuilder.h"
#include <csignal>

using namespace popl;

std::shared_ptr<vx::mcp::Server> server;

struct NotificationState{
    std::mutex ServerNotificationMutex;
};
NotificationState notificationState;

void stop_handler(sig_atomic_t s){
    std::cout << "Stopping server..." << std::endl;
    if(server && server->IsValid()){
        server->Stop();
    }
    std::cout << "Server stopped." << std::endl;
    exit(0);
}

void ClientNotificationCallbackImpl(const char* pluginName, const char* notification){
    std::lock_guard<std::mutex> lock(notificationState.ServerNotificationMutex);
    if(server && server->IsValid()){
        server->SendNotification(pluginName, notification);
    }
}

int main(int argc, char** argv){
    std::string name;
    std::string plugins_directory;
    std::string logs_directory;
    bool verbose = false;

    std::shared_ptr<vx::ITransport> transport;
    auto loader = std::make_shared<vx::mcp::PluginsLoader>();
    server = std::make_shared<vx::mcp::Server>();

    signal(SIGINT, stop_handler);

    OptionParser op("Allowed options");
    auto help_option = op.add<Switch>("h" ,"help", "produce help message");
    auto name_option = op.add<Value<std::string>>("n", "name", "name of the plugin", "mcp-server");
    auto plugins_directory_option = op.add<Value<std::string>>("p", "plugins", "directory with plugins", "./plugins");
    auto logs_directory_option = op.add<Value<std::string>>("l", "logs", "directory with logs", "./logs");
    auto verbose_option = op.add<Value<bool>>("v", "verbose", "enable verbose", verbose);
    auto use_see_server = op.add<Switch>("s", "see", "start as see server");
    name_option->assign_to(&name);
    verbose_option->assign_to(&verbose);
    plugins_directory_option->assign_to(&plugins_directory);
    logs_directory_option->assign_to(&logs_directory);

    try{
        op.parse(argc, argv);
        if(help_option->is_set()){
            std::cout << op << std::endl;
            return 0;
        }
    } catch(const popl::invalid_option& e){
        std::cerr << "Invalid Option Exception: " << e.what() << std::endl;
        return -1;
    } catch(const std::exception& e){
        std::cerr << "Exception: " << e.what() << std::endl;
        return -1;
    }
/*
    if(use_see_server->is_set()){
        transport = std::make_shared<vx::transport::SSE>();
    } else {
        transport = std::make_shared<vx::transport::Stdio>();
    }
*/
    transport = std::make_shared<vx::transport::Stdio>();

    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::gmtime(&time_t_now), "&Y-%m-%dT%H-%M-%S");
    std::string iso_date = ss.str();

    std::string logFilename = logs_directory + "/mcp-server_" + iso_date + ".log";
    auto sink_file = std::make_shared<AixLog::SinkFile>(AixLog::Severity::trace, logFilename);
    AixLog::Log::init({sink_file});

    LOG(INFO) << " __  __  _____ _____        _____ ______ _______      ________ _____  " << std::endl;
    LOG(INFO) << "|  \\/  |/ ____|  __ \\      / ____|  ____|  __ \\ \\    / /  ____|  __ \\ " << std::endl;
    LOG(INFO) << "| \\  / | |    | |__) |____| (___ | |__  | |__) \\ \\  / /| |__  | |__) |" << std::endl;
    LOG(INFO) << "| |\\/| | |    |  ___/______\\___ \\|  __| |  _  / \\ \\/ / |  __| |  _  / " << std::endl;
    LOG(INFO) << "| |  | | |____| |          ____) | |____| | \\ \\  \\  /  | |____| | \\ \\ " << std::endl;
    LOG(INFO) << "|_|  |_|\\_____|_|         |_____/|______|_|  \\_\\  \\/   |______|_|  \\_\\" << std::endl;
    LOG(INFO) << "Starting mcp-server v" << PROJECT_VERSION << " (transport: " << transport->GetName() << " v" << transport->GetVersion() << ") on port: " << transport->GetPort() << std::endl;
    LOG(INFO) << "Press Ctrl+C to exit." << std::endl;

    if(loader->LoadPlugins(plugins_directory)){
        LOG(INFO) << "Successfully loaded plugins." << std::endl;
    }

    for(auto& plugin : loader->GetPlugins()){
        plugin.instance->notifications = new NotificationSystem();
        plugin.instance->notifications->SendToClient = ClientNotificationCallbackImpl;
    }

    server->Name(name);
    server->VerboseLevel(verbose);

    server->OverrideCallback("tools/list", [&loader](const json& request){
        nlohmann::ordered_json response = MCPBuilder::Response(request);
        response["result"]["tools"] = json::array();
        for(const auto& plugin : loader->GetPlugins()){
            if(plugin.instance->GetType() == PLUGIN_TYPE_TOOLS){
                for(int i=0; i<plugin.instance->GetToolCount(); i++){
                    nlohmann::ordered_json tool;
                    auto* PluginTool = plugin.instance->GetTool(i);
                    tool["name"] = PluginTool->name;
                    tool["description"] = PluginTool->description;
                    tool["inputSchema"] = nlohmann::json::parse(PluginTool->inputSchema);
                    response["result"]["tools"].push_back(tool);
                }
            }
        }
        return response;
    });

    server->OverrideCallback("tools/call", [&loader](const json& request){
        nlohmann::ordered_json response = MCPBuilder::Response(request);
        char* res_ptr = nullptr;

        for(const auto& plugin : loader->GetPlugins()){
            if(plugin.instance->GetType() == PLUGIN_TYPE_TOOLS){
                for(int i = 0; i < plugin.instance->GetToolCount(); i++){
                    auto* PluginTool = plugin.instance->GetTool(i);
                    if(PluginTool->name == request["params"]["name"]){
                        res_ptr = plugin.instance->HandleRequest(request.dump().c_str());
                        if(res_ptr != nullptr){
                            try{
                                response["result"] = json::parse(res_ptr);
                                response["result"]["isError"] = false;
                            } catch(const json::parse_error& e){
                                response["result"]["isError"] = true;
                                response["result"]["content"] = json::array();
                                response["result"]["content"].push_back({{"type", "text"}, {"text", "Plugin returned malformed data."}});                     
                            }
                            delete[] res_ptr;
                        }else{
                            LOG(ERROR) << "Plugin returned nullptr." << std::endl;
                        }
                        return response;
                    }
                }
            }
        }
        response["result"]["isError"] = true;
        response["result"]["content"] = json::array();
        response["result"]["content"].push_back({
            {"type", "text"},
            {"text", "Tool not found: " + request["params"]["name"].get<std::string>()}
        });                     
        return response;
    });

    server->OverrideCallback("prompts/list", [&loader](const json& request) {
        nlohmann::ordered_json response = MCPBuilder::Response(request);
        response["result"]["prompts"] = json::array();

        for (const auto& plugin : loader->GetPlugins()) {
            if (plugin.instance->GetType() == PLUGIN_TYPE_PROMPTS) {
                for (int i = 0; i < plugin.instance->GetPromptCount(); i++) {
                    nlohmann::ordered_json prompt;
                    auto pluginPrompt = plugin.instance->GetPrompt(i);
                    prompt["name"] = pluginPrompt->name;
                    prompt["description"] = pluginPrompt->description;
                    prompt["arguments"] = nlohmann::json::parse(pluginPrompt->arguments);
                    response["result"]["prompts"].push_back(prompt);
                }
            }
        }

        return response;
    });
    server->OverrideCallback("prompts/get", [&loader](const json& request) {
        nlohmann::ordered_json response = MCPBuilder::Response(request);

        char* res_ptr = nullptr;

        for (const auto& plugin : loader->GetPlugins()) {
            if (plugin.instance->GetType() == PLUGIN_TYPE_PROMPTS) {
                for (int i = 0; i < plugin.instance->GetPromptCount(); i++) {
                    auto pluginPrompt = plugin.instance->GetPrompt(i);
                    if (pluginPrompt->name == request["params"]["name"]) {
                        res_ptr = plugin.instance->HandleRequest(request.dump().c_str());
                        if (res_ptr) {
                            try {
                                response["result"] = json::parse(res_ptr);
                            } catch (const json::parse_error& e) {
                                LOG(ERROR) << "Plugin " << pluginPrompt->name << " returned malformed data." << std::endl;
                                // TODO: how can we handle error here ?
                            }
                            // --- Free the allocated memory ---
                            delete[] res_ptr;
                        }
                        return response;
                    }
                }
            }
        }

        return response;
    });
    server->OverrideCallback("resources/list", [&loader](const json& request) {
        nlohmann::ordered_json response = MCPBuilder::Response(request);
        response["result"]["resources"] = json::array();

        for (const auto& plugin : loader->GetPlugins()) {
            if (plugin.instance->GetType() == PLUGIN_TYPE_RESOURCES) {
                for (int i = 0; i < plugin.instance->GetResourceCount(); i++) {
                    nlohmann::ordered_json resource;
                    auto pluginResource = plugin.instance->GetResource(i);
                    resource["name"] = pluginResource->name;
                    resource["description"] = pluginResource->description;
                    resource["uri"] = pluginResource->uri;
                    resource["mimeType"] = pluginResource->mime;
                    response["result"]["resources"].push_back(resource);
                }
            }
        }

        return response;
    });
    server->OverrideCallback("resources/read", [&loader](const json& request) {
        nlohmann::ordered_json response = MCPBuilder::Response(request);

        char* res_ptr = nullptr;

        for (const auto& plugin : loader->GetPlugins()) {
            if (plugin.instance->GetType() == PLUGIN_TYPE_RESOURCES) {
                for (int i = 0; i < plugin.instance->GetResourceCount(); i++) {
                    auto pluginResource = plugin.instance->GetResource(i);
                    if (pluginResource->uri == request["params"]["uri"]) {
                        res_ptr = plugin.instance->HandleRequest(request.dump().c_str());
                        if (res_ptr) {
                            try {
                                response["result"] = json::parse(res_ptr);
                            } catch (const json::parse_error& e) {
                                LOG(ERROR) << "Plugin " << pluginResource->name << " returned malformed data." << std::endl;
                                // TODO: how can we handle error here ?
                            }
                            // --- Free the allocated memory ---
                            delete[] res_ptr;
                        }
                    }
                }
            }
        }

        return response;
    });

    server->Connect(transport);

    return 0;

}
