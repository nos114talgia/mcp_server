#ifndef MCP_SERVER_TSINGLETON_H
#define MCP_SERVER_TSINGLETON_H

#include <memory>
#include <mutex>

template <typename T>
class TSingleton {
public:
    TSingleton(const TSingleton&) = delete;
    TSingleton(TSingleton&&) = delete;
    TSingleton& operator=(const TSingleton&) = delete;
    TSingleton& operator=(TSingleton&&) = delete;
    
    static T& GetInstance(){
        std::call_once(initFlag, [](){
            instance.reset(new T());
        });
        return *instance;
    }
protected:
    virtual ~TSingleton() = default;
    TSingleton() = default;

private:
    inline static std::unique_ptr<T> instance = nullptr;
    inline static std::once_flag initFlag;
    
};

#endif