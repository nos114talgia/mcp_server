cmake_minimum_required(VERSION 3.10)

add_library(weather SHARED
    ${PROJECT_SOURCE_DIR}/plugins/weather/Weather.cpp    
)

set_target_properties(weather PROPERTIES POSITION_INDEPENDENT_CODE ON)

find_package(Threads REQUIRED)
target_link_libraries(weather PRIVATE Threads::Threads)

target_compile_definitions(weather PRIVATE WEATHER_EXPORTS)
target_include_directories(weather PRIVATE ${PROJECT_SOURCE_DIR}/include ${PROJECT_SOURCE_DIR}/src/interface)