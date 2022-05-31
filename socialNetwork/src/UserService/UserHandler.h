#ifndef SOCIAL_NETWORK_MICROSERVICES_USERHANDLER_H
#define SOCIAL_NETWORK_MICROSERVICES_USERHANDLER_H

#include <iomanip>
#include <iostream>
#include <jwt/jwt.hpp>
#include <nlohmann/json.hpp>
#include <random>
#include <string>

#include "../../gen-cpp/SocialGraphService.h"
#include "../../gen-cpp/UserStorageService.h"
#include "../../gen-cpp/UserService.h"
#include "../../gen-cpp/social_network_types.h"
#include "../../third_party/PicoSHA2/picosha2.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"

// Custom Epoch (January 1, 2018 Midnight GMT = 2018-01-01T00:00:00Z)
#define CUSTOM_EPOCH 1514764800000
#define MONGODB_TIMEOUT_MS 100

namespace social_network {

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::system_clock;
using namespace jwt::params;

static int64_t current_timestamp = -1;
static int counter = 0;

static int GetCounter(int64_t timestamp) {
  if (current_timestamp > timestamp) {
    LOG(fatal) << "Timestamps are not incremental.";
    exit(EXIT_FAILURE);
  }
  if (current_timestamp == timestamp) {
    return counter++;
  } else {
    current_timestamp = timestamp;
    counter = 0;
    return counter++;
  }
}

std::string GenRandomString(const int len) {
  static const std::string alphanum =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dist(
      0, static_cast<int>(alphanum.length() - 1));
  std::string s;
  for (int i = 0; i < len; ++i) {
    s += alphanum[dist(gen)];
  }
  return s;
}

class UserHandler : public UserServiceIf {
 public:
  UserHandler(std::mutex *, const std::string &, const std::string &,
              ClientPool<ThriftClient<UserStorageServiceClient>> *,
              ClientPool<ThriftClient<SocialGraphServiceClient>> *);
  ~UserHandler() override = default;
  void RegisterUser(int64_t, const std::string &, const std::string &,
                    const std::string &, const std::string &,
                    const std::map<std::string, std::string> &) override;
  void RegisterUserWithId(int64_t, const std::string &, const std::string &,
                          const std::string &, const std::string &, int64_t,
                          const std::map<std::string, std::string> &) override;

  void ComposeCreatorWithUserId(
      Creator &, int64_t, int64_t, const std::string &,
      const std::map<std::string, std::string> &) override;
  void ComposeCreatorWithUsername(
      Creator &, int64_t, const std::string &,
      const std::map<std::string, std::string> &) override;
  void Login(std::string &, int64_t, const std::string &, const std::string &,
             const std::map<std::string, std::string> &) override;
  int64_t GetUserId(int64_t, const std::string &,
                    const std::map<std::string, std::string> &) override;

 private:
  std::string _machine_id;
  std::string _secret;
  std::mutex *_thread_lock;
  ClientPool<ThriftClient<SocialGraphServiceClient>> *_social_graph_client_pool;
  ClientPool<ThriftClient<UserStorageServiceClient>> *_user_storage_client_pool;
};

UserHandler::UserHandler(std::mutex *thread_lock, const std::string &machine_id,
                         const std::string &secret,
                         ClientPool<ThriftClient<UserStorageServiceClient>>
                             *user_storage_client_pool,
                         ClientPool<ThriftClient<SocialGraphServiceClient>>
                             *social_graph_client_pool) {
  _thread_lock = thread_lock;
  _machine_id = machine_id;
  _secret = secret;
  _social_graph_client_pool = social_graph_client_pool;
  _user_storage_client_pool = user_storage_client_pool;
}

void UserHandler::RegisterUserWithId(
    const int64_t req_id, const std::string &first_name,
    const std::string &last_name, const std::string &username,
    const std::string &password, const int64_t user_id,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "register_user_withid_server",
      {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto user_storage_client_wrapper = _user_storage_client_pool->Pop();
  if (!user_storage_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to user-storage-service";
    throw se;
  }
  auto user_storage_client = user_storage_client_wrapper->GetClient();

  // Check if the username has existed in the database
  User u;
  bool found;
  try {
    user_storage_client->ReadUser(u, user_id);
    found = u.user_id != -1;
  } catch (...) {
    _user_storage_client_pool->Remove(user_storage_client_wrapper);
    LOG(error) << "Failed to read user from user-storage-client";
    throw;
  }

  if (found) {
    LOG(warning) << "User " << username << " already existed.";
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "User " + username + " already existed";
    _user_storage_client_pool->Remove(user_storage_client_wrapper);
    throw se;
  } else {
    User u;
    auto user_insert_span = opentracing::Tracer::Global()->StartSpan(
        "user_mongo_insert_cilent", {opentracing::ChildOf(&span->context())});

    u.user_id = user_id;
    u.username = username;
    u.first_name = first_name;
    u.last_name = last_name;
    u.salt = GenRandomString(32);
    u.password_hashed = picosha2::hash256_hex_string(password + u.salt);

    user_storage_client->WriteUser(u);

    user_insert_span->Finish();
  }

  if (!found) {
    auto social_graph_client_wrapper = _social_graph_client_pool->Pop();
    if (!social_graph_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to social-graph-service";
      throw se;
    }
    auto social_graph_client = social_graph_client_wrapper->GetClient();
    try {
      social_graph_client->InsertUser(req_id, user_id, writer_text_map);
    } catch (...) {
      _social_graph_client_pool->Remove(social_graph_client_wrapper);
      LOG(error) << "Failed to insert user to social-graph-client";
      throw;
    }
    _social_graph_client_pool->Keepalive(social_graph_client_wrapper);
  }
  _user_storage_client_pool->Keepalive(user_storage_client_wrapper);

  span->Finish();
}

void UserHandler::RegisterUser(
    const int64_t req_id, const std::string &first_name,
    const std::string &last_name, const std::string &username,
    const std::string &password,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "register_user_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  // Compose user_id
  _thread_lock->lock();
  int64_t timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count() -
      CUSTOM_EPOCH;
  int idx = GetCounter(timestamp);
  _thread_lock->unlock();

  std::stringstream sstream;
  sstream << std::hex << timestamp;
  std::string timestamp_hex(sstream.str());
  if (timestamp_hex.size() > 10) {
    timestamp_hex.erase(0, timestamp_hex.size() - 10);
  } else if (timestamp_hex.size() < 10) {
    timestamp_hex = std::string(10 - timestamp_hex.size(), '0') + timestamp_hex;
  }
  // Empty the sstream buffer.
  sstream.clear();
  sstream.str(std::string());

  sstream << std::hex << idx;
  std::string counter_hex(sstream.str());

  if (counter_hex.size() > 3) {
    counter_hex.erase(0, counter_hex.size() - 3);
  } else if (counter_hex.size() < 3) {
    counter_hex = std::string(3 - counter_hex.size(), '0') + counter_hex;
  }
  std::string user_id_str = _machine_id + timestamp_hex + counter_hex;
  int64_t user_id = stoul(user_id_str, nullptr, 16) & 0x7FFFFFFFFFFFFFFF;
  ;
  LOG(debug) << "The user_id of the request " << req_id << " is " << user_id;

  UserHandler::RegisterUserWithId(req_id, first_name, last_name, username,
      password, user_id, carrier);

  span->Finish();
}

void UserHandler::ComposeCreatorWithUsername(
    Creator &_return, const int64_t req_id, const std::string &username,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "compose_creator_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);


  Creator creator;
  creator.username = username;
  creator.user_id = GetUserId(req_id, username, carrier);

  if (creator.user_id != -1) {
    _return = creator;
  }

  span->Finish();
}

void UserHandler::ComposeCreatorWithUserId(
    Creator &_return, int64_t req_id, int64_t user_id,
    const std::string &username,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "compose_creator_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  Creator creator;
  creator.username = username;
  creator.user_id = user_id;

  _return = creator;

  span->Finish();
}

void UserHandler::Login(std::string &_return, int64_t req_id,
                        const std::string &username,
                        const std::string &password,
                        const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "login_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto user_storage_client_wrapper = _user_storage_client_pool->Pop();
  if (!user_storage_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to user-storage-service";
    throw se;
  }
  auto user_storage_client = user_storage_client_wrapper->GetClient();

  std::string password_stored;
  std::string salt_stored;
  int64_t user_id_stored = -1;
  bool cached = false;
  json login_json;


  auto find_span = opentracing::Tracer::Global()->StartSpan(
      "user_mongo_find_client", {opentracing::ChildOf(&span->context())});
  User u;
  bool found;
  try {
    user_storage_client->FindUser(u, username);
    found = u.user_id != -1;
  } catch (...) {
    _user_storage_client_pool->Remove(user_storage_client_wrapper);
    LOG(error) << "Failed to find_user in user-storage-client";
    throw;
  }
  _user_storage_client_pool->Keepalive(user_storage_client_wrapper);
  find_span->Finish();

  if (u.user_id != -1) {
    bool auth =
        picosha2::hash256_hex_string(password + u.salt) == u.password_hashed;
    if (auth) {
      auto user_id_str = std::to_string(u.user_id);
      auto timestamp_str = std::to_string(
          duration_cast<seconds>(system_clock::now().time_since_epoch())
              .count());
      jwt::jwt_object obj{algorithm("HS256"), secret(_secret),
                          payload({{"user_id", user_id_str},
                                   {"username", u.username},
                                   {"timestamp", timestamp_str},
                                   {"ttl", "3600"}})};
      _return = obj.signature();
    } else {
      ServiceException se;
      se.errorCode = ErrorCode::SE_UNAUTHORIZED;
      se.message = "Incorrect username or password";
      throw se;
    }
  } else {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "Username: " + username + " does not exist";
    throw se;
  }

  span->Finish();
}

int64_t UserHandler::GetUserId(
    int64_t req_id, const std::string &username,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "get_user_id_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  int64_t user_id = -1;
  bool cached = false;

  auto user_storage_client_wrapper = _user_storage_client_pool->Pop();
  if (!user_storage_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to user-storage-service";
    throw se;
  }
  auto user_storage_client = user_storage_client_wrapper->GetClient();

  auto find_span = opentracing::Tracer::Global()->StartSpan(
      "user_mongo_find_client", {opentracing::ChildOf(&span->context())});
  User u;
  bool found;
  try {
    user_storage_client->FindUser(u, username);
    found = u.user_id != -1;
  } catch (...) {
    _user_storage_client_pool->Remove(user_storage_client_wrapper);
    LOG(error) << "Failed to find user from user-storage-client";
    throw;
  }
  find_span->Finish();

  if (!found) {
    LOG(warning) << "User: " << username << " doesn't exist in MongoDB";
    _user_storage_client_pool->Remove(user_storage_client_wrapper);
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "User: " + username + " is not registered";
    throw se;
  } else {
    LOG(debug) << "User: " << username << " found in MongoDB";
  }

  _user_storage_client_pool->Keepalive(user_storage_client_wrapper);
  span->Finish();
  return u.user_id;
}

/*
 * The following code which obtaines machine ID from machine's MAC address was
 * inspired from https://stackoverflow.com/a/16859693.
 *
 * MAC address is obtained from /sys/class/net/<netif>/address
 */
u_int16_t HashMacAddressPid(const std::string &mac) {
  u_int16_t hash = 0;
  std::string mac_pid = mac + std::to_string(getpid());
  for (unsigned int i = 0; i < mac_pid.size(); i++) {
    hash += (mac[i] << ((i & 1) * 8));
  }
  return hash;
}

std::string GetMachineId(std::string &netif) {
  std::string mac_hash;

  std::string mac_addr_filename = "/sys/class/net/" + netif + "/address";
  std::ifstream mac_addr_file;
  mac_addr_file.open(mac_addr_filename);
  if (!mac_addr_file) {
    LOG(fatal) << "Cannot read MAC address from net interface " << netif;
    return "";
  }
  std::string mac;
  mac_addr_file >> mac;
  if (mac == "") {
    LOG(fatal) << "Cannot read MAC address from net interface " << netif;
    return "";
  }
  mac_addr_file.close();

  LOG(info) << "MAC address = " << mac;

  std::stringstream stream;
  stream << std::hex << HashMacAddressPid(mac);
  mac_hash = stream.str();

  if (mac_hash.size() > 3) {
    mac_hash.erase(0, mac_hash.size() - 3);
  } else if (mac_hash.size() < 3) {
    mac_hash = std::string(3 - mac_hash.size(), '0') + mac_hash;
  }
  return mac_hash;
}
}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_USERHANDLER_H
