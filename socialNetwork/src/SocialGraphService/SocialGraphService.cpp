#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>

#include "../utils.h"
#include "../utils_thrift.h"
#include "SocialGraphHandler.h"

using json = nlohmann::json;
using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::transport::TServerSocket;
using namespace social_network;

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();
  SetUpTracer("config/jaeger-config.yml", "social-graph-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["social-graph-service"]["port"];

  std::string social_graph_storage_addr = config_json["social-graph-storage-service"]["addr"];
  int social_graph_storage_port = config_json["social-graph-storage-service"]["port"];
  int social_graph_storage_conns = config_json["social-graph-storage-service"]["connections"];
  int social_graph_storage_timeout = config_json["social-graph-storage-service"]["timeout_ms"];
  int social_graph_storage_keepalive = config_json["social-graph-storage-service"]["keepalive_ms"];

  std::string user_addr = config_json["user-service"]["addr"];
  int user_port = config_json["user-service"]["port"];
  int user_conns = config_json["user-service"]["connections"];
  int user_timeout = config_json["user-service"]["timeout_ms"];
  int user_keepalive = config_json["user-service"]["keepalive_ms"];

  ClientPool<ThriftClient<UserServiceClient>> user_client_pool(
      "social-graph", user_addr, user_port, 0, user_conns, user_timeout,
      user_keepalive, config_json);

  ClientPool<ThriftClient<SocialGraphStorageServiceClient>> social_graph_storage_client_pool(
      "social-graph", social_graph_storage_addr, user_port, 0, user_conns, user_timeout,
      social_graph_storage_keepalive, config_json);

  std::shared_ptr<TServerSocket> server_socket =
      get_server_socket(config_json, "0.0.0.0", port);

  TThreadedServer server(
      std::make_shared<SocialGraphServiceProcessor>(
          std::make_shared<SocialGraphHandler>(
              &social_graph_storage_client_pool, &user_client_pool)),
      server_socket, std::make_shared<TFramedTransportFactory>(),
      std::make_shared<TBinaryProtocolFactory>());
  LOG(info) << "Starting the social-graph-service server ...";
  server.serve();
}
