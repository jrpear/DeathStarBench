add_executable(
    SocialGraphService
    SocialGraphService.cpp
    ${THRIFT_GEN_CPP_DIR}/SocialGraphService.cpp
    ${THRIFT_GEN_CPP_DIR}/UserService.cpp
    ${THRIFT_GEN_CPP_DIR}/SocialGraphStorageService.cpp
    ${THRIFT_GEN_CPP_DIR}/social_network_types.cpp
)

target_include_directories(
    SocialGraphService PRIVATE
    /usr/local/include/jaegertracing
)

target_link_libraries(
    SocialGraphService
    ${THRIFT_LIB}
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    nlohmann_json::nlohmann_json
    Boost::log
    Boost::log_setup
    Boost::program_options
    /usr/local/lib/libjaegertracing.so
    OpenSSL::SSL
)

install(TARGETS SocialGraphService DESTINATION ./)
