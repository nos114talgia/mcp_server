#ifndef MCP_SERVER_PLUGINS_LOADER_H
#define MCP_SERVER_PLUGINS_LOADER_H 

#include <dlfcn.h>
typedef void* LibraryHandle;

#include <string>
#include <vector>
#include <memory>
#include <iostream>
#include <filesystem>
#include <algorithm>

#include "aixlog.hpp"
#include "PluginAPI.h"

namespace vx::mcp {
    struct PluginEntry {
        std::string name;
        LibraryHandle handle;
        PluginAPI* instance;

        PluginAPI* (*createFunc)();
        void (*destroyFunc)(PluginAPI*);
    };

    class PluginsLoader {
    public:
        PluginsLoader();
        ~PluginsLoader();

        bool LoadPlugins(const std::string& directory);
        void UnloadPlugins();
        const std::vector<PluginEntry>& GetPlugins() const;
    
    private:
        bool LoadPlugin(const std::string& path);
        void UnloadPlugin(PluginEntry& entry);

    private:
        std::vector<PluginEntry> m_plugins;
    };
}


#endif