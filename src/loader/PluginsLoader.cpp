#include "PluginsLoader.h"

namespace vx::mcp {
    PluginsLoader::PluginsLoader() = default;
    PluginsLoader::~PluginsLoader(){
        UnloadPlugins();
    }

    bool PluginsLoader::LoadPlugins(const std::string& directory) { 
        try{
            for(const auto& entry : std::filesystem::recursive_directory_iterator(directory)){
                if(entry.is_regular_file()){
                    std::string extension = entry.path().extension().string();
                    bool should_load = false;
#if defined(__APPLE__)
                    if(extension == ".dylib" || extension == ".so") should_load = true;
#elif defined(__linux__)
                    if(extension == ".so") should_load = true;
#endif
                    if(should_load){
                        LoadPlugin(entry.path().string());
                    }
                }
            }
            return true;
        } catch (const std::exception& ex){
            LOG(ERROR) << "Error loading plugins: " << ex.what() << std::endl;
            return false;
        }
    }

    bool PluginsLoader::LoadPlugin(const std::string& path){
        PluginEntry entry;
        entry.path = path;

        entry.handle = dlopen(path.c_str(), RTLD_LAZY);
        if(entry.handle == nullptr){
            LOG(ERROR) << "Error loading plugin: " << path << "-" << dlerror() << std::endl;
            return false;
        }
        entry.createFunc = (PluginAPI* (*)())dlsym(entry.handle, "CreatePlugin");
        entry.destroyFunc = (void (*)(PluginAPI*))dlsym(entry.handle, "DestroyPlugin");

        if(entry.createFunc == nullptr || entry.destroyFunc == nullptr){
            LOG(ERROR) << "Error loading plugin: " << path << "-" << dlerror() << std::endl;
            dlclose(entry.handle);
            return false;
        }

        entry.instance = entry.createFunc();
        if(entry.instance->Initialize() == false){
            LOG(ERROR) << "Error initializing plugin: " << path << std::endl;
            entry.destroyFunc(entry.instance);
            dlclose(entry.handle);
            return false;
        }

        m_plugins.push_back(entry);
        LOG(INFO) << "Loaded plugin: " << entry.instance->GetName()
                  << " v" << entry.instance->GetVersion() << std::endl;
        return true;
    }

    void PluginsLoader::UnloadPlugins() {
        for(auto& entry : m_plugins){
            UnloadPlugin(entry);
        }
        m_plugins.clear();
    }

    void PluginsLoader::UnloadPlugin(PluginEntry& entry) { 
        if(entry.instance != nullptr){
            entry.instance->Shutdown();
            entry.destroyFunc(entry.instance);
            entry.instance = nullptr;
        }
        if(entry.handle != nullptr){
            dlclose(entry.handle);
            entry.handle = nullptr;
        }
    }

    const std::vector<PluginEntry>& PluginsLoader::GetPlugins() const {
        return m_plugins;
    }
}