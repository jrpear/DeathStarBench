#ifndef SOCIAL_NETWORK_MICROSERVICES_SOCIALGRAPHHANDLER_H
#define SOCIAL_NETWORK_MICROSERVICES_SOCIALGRAPHHANDLER_H

#include <chrono>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "../../gen-cpp/SocialGraphService.h"
#include "../../gen-cpp/SocialGraphStorageService.h"
#include "../../gen-cpp/UserService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"

namespace social_network {

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

class SocialGraphHandler : public SocialGraphServiceIf {
 public:
  SocialGraphHandler(ClientPool<ThriftClient<SocialGraphStorageServiceClient>> *,
                     ClientPool<ThriftClient<UserServiceClient>> *);
  ~SocialGraphHandler() override = default;
  void GetFollowers(std::vector<int64_t> &, int64_t, int64_t,
                    const std::map<std::string, std::string> &) override;
  void GetFollowees(std::vector<int64_t> &, int64_t, int64_t,
                    const std::map<std::string, std::string> &) override;
  void Follow(int64_t, int64_t, int64_t,
              const std::map<std::string, std::string> &) override;
  void Unfollow(int64_t, int64_t, int64_t,
                const std::map<std::string, std::string> &) override;
  void FollowWithUsername(int64_t, const std::string &, const std::string &,
                          const std::map<std::string, std::string> &) override;
  void UnfollowWithUsername(
      int64_t, const std::string &, const std::string &,
      const std::map<std::string, std::string> &) override;
  void InsertUser(int64_t, int64_t,
                  const std::map<std::string, std::string> &) override;

 private:
  ClientPool<ThriftClient<SocialGraphStorageServiceClient>> *_social_graph_storage_client_pool;
  ClientPool<ThriftClient<UserServiceClient>> *_user_service_client_pool;
};

SocialGraphHandler::SocialGraphHandler(
    ClientPool<ThriftClient<SocialGraphStorageServiceClient>> *social_graph_storage_client_pool,
    ClientPool<ThriftClient<UserServiceClient>> *user_service_client_pool) {
  _social_graph_storage_client_pool = social_graph_storage_client_pool;
  _user_service_client_pool = user_service_client_pool;
}

void SocialGraphHandler::Follow(
    int64_t req_id, int64_t user_id, int64_t followee_id,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "follow_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  int64_t timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count();

  std::future<void> update_follower_future =
      std::async(std::launch::async, [&]() {
        auto social_graph_storage_client_wrapper = _social_graph_storage_client_pool->Pop();
        if (!social_graph_storage_client_wrapper) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
          se.message = "Failed to connect to social-graph-storage-service";
          throw se;
        }
        auto social_graph_storage_client = social_graph_storage_client_wrapper->GetClient();

        // Update follower->followee edges
        try {
          social_graph_storage_client->AddFollowee(user_id, followee_id);
        } catch (...) {
          _social_graph_storage_client_pool->Remove(social_graph_storage_client_wrapper);
          LOG(error) << "Failed to add followee to social_graph_storage";
          throw;
        }
        _social_graph_storage_client_pool->Keepalive(social_graph_storage_client_wrapper);

      });

  std::future<void> update_followee_future =
      std::async(std::launch::async, [&]() {
        auto social_graph_storage_client_wrapper = _social_graph_storage_client_pool->Pop();
        if (!social_graph_storage_client_wrapper) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
          se.message = "Failed to connect to social-graph-storage-service";
          throw se;
        }
        auto social_graph_storage_client = social_graph_storage_client_wrapper->GetClient();

        // Update followee->follower edges
        try {
          social_graph_storage_client->AddFollower(followee_id, user_id);
        } catch (...) {
          _social_graph_storage_client_pool->Remove(social_graph_storage_client_wrapper);
          LOG(error) << "Failed to add follower to social_graph_storage";
          throw;
        }
        _social_graph_storage_client_pool->Keepalive(social_graph_storage_client_wrapper);
      });

  try {
    update_follower_future.get();
    update_followee_future.get();
  } catch (const std::exception &e) {
    LOG(warning) << e.what();
    throw;
  } catch (...) {
    throw;
  }

  span->Finish();
}

void SocialGraphHandler::Unfollow(
    int64_t req_id, int64_t user_id, int64_t followee_id,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "follow_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  int64_t timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count();

  std::future<void> update_follower_future =
      std::async(std::launch::async, [&]() {
        auto social_graph_storage_client_wrapper = _social_graph_storage_client_pool->Pop();
        if (!social_graph_storage_client_wrapper) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
          se.message = "Failed to connect to social-graph-storage-service";
          throw se;
        }
        auto social_graph_storage_client = social_graph_storage_client_wrapper->GetClient();

        // Update follower->followee edges
        try {
          social_graph_storage_client->RemoveFollowee(user_id, followee_id);
        } catch (...) {
          _social_graph_storage_client_pool->Remove(social_graph_storage_client_wrapper);
          LOG(error) << "Failed to remove followee from social_graph_storage";
          throw;
        }
        _social_graph_storage_client_pool->Keepalive(social_graph_storage_client_wrapper);
      });

  std::future<void> update_followee_future =
      std::async(std::launch::async, [&]() {
        auto social_graph_storage_client_wrapper = _social_graph_storage_client_pool->Pop();
        if (!social_graph_storage_client_wrapper) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
          se.message = "Failed to connect to social-graph-storage-service";
          throw se;
        }
        auto social_graph_storage_client = social_graph_storage_client_wrapper->GetClient();

        // Update followee->follower edges
        try {
          social_graph_storage_client->RemoveFollower(followee_id, user_id);
        } catch (...) {
          _social_graph_storage_client_pool->Remove(social_graph_storage_client_wrapper);
          LOG(error) << "Failed to remove follower from social_graph_storage";
          throw;
        }
        _social_graph_storage_client_pool->Keepalive(social_graph_storage_client_wrapper);
      });

  try {
    update_follower_future.get();
    update_followee_future.get();
  } catch (const std::exception &e) {
    LOG(warning) << e.what();
    throw;
  } catch (...) {
    throw;
  }

  span->Finish();
}

void SocialGraphHandler::GetFollowers(
    std::vector<int64_t> &_return, const int64_t req_id, const int64_t user_id,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "get_followers_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto social_graph_storage_client_wrapper = _social_graph_storage_client_pool->Pop();
  if (!social_graph_storage_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to social-graph-storage-service";
    throw se;
  }
  auto social_graph_storage_client = social_graph_storage_client_wrapper->GetClient();

  std::vector<int64_t> followers;
  try {
    social_graph_storage_client->ReadFollowers(followers, user_id);
  } catch (...) {
    _social_graph_storage_client_pool->Remove(social_graph_storage_client_wrapper);
    LOG(error) << "Failed to get read followers from social_graph-storage-service";
    throw;
  }
  _social_graph_storage_client_pool->Keepalive(social_graph_storage_client_wrapper);

  bool found = true;
  if (found) {
      _return = followers;
  } else {
      LOG(warning) << "user_id: " << user_id << " not found";
  }
  span->Finish();
}

void SocialGraphHandler::GetFollowees(
    std::vector<int64_t> &_return, const int64_t req_id, const int64_t user_id,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "get_followers_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto social_graph_storage_client_wrapper = _social_graph_storage_client_pool->Pop();
  if (!social_graph_storage_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to social-graph-storage-service";
    throw se;
  }
  auto social_graph_storage_client = social_graph_storage_client_wrapper->GetClient();

  std::vector<int64_t> followees;
  try {
    social_graph_storage_client->ReadFollowees(followees, user_id);
  } catch (...) {
    _social_graph_storage_client_pool->Remove(social_graph_storage_client_wrapper);
    LOG(error) << "Failed to read followees from social_graph_storage";
    throw;
  }
  _social_graph_storage_client_pool->Keepalive(social_graph_storage_client_wrapper);

  bool found = true;
  if (found) {
      _return = followees;
  } else {
      LOG(warning) << "user_id: " << user_id << " not found";
  }
  span->Finish();
}

void SocialGraphHandler::InsertUser(
    int64_t req_id, int64_t user_id,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "insert_user_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  // users are implicitly created when referenced

  span->Finish();
}

void SocialGraphHandler::FollowWithUsername(
    int64_t req_id, const std::string &user_name,
    const std::string &followee_name,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "follow_with_username_server",
      {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::future<int64_t> user_id_future = std::async(std::launch::async, [&]() {
    auto user_client_wrapper = _user_service_client_pool->Pop();
    if (!user_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to social-graph-service";
      throw se;
    }
    auto user_client = user_client_wrapper->GetClient();
    int64_t _return;
    try {
      _return = user_client->GetUserId(req_id, user_name, writer_text_map);
    } catch (...) {
      _user_service_client_pool->Remove(user_client_wrapper);
      LOG(error) << "Failed to get user_id from user-service";
      throw;
    }
    _user_service_client_pool->Keepalive(user_client_wrapper);
    return _return;
  });

  std::future<int64_t> followee_id_future =
      std::async(std::launch::async, [&]() {
        auto user_client_wrapper = _user_service_client_pool->Pop();
        if (!user_client_wrapper) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
          se.message = "Failed to connect to social-graph-service";
          throw se;
        }
        auto user_client = user_client_wrapper->GetClient();
        int64_t _return;
        try {
          _return =
              user_client->GetUserId(req_id, followee_name, writer_text_map);
        } catch (...) {
          _user_service_client_pool->Remove(user_client_wrapper);
          LOG(error) << "Failed to get user_id from user-service";
          throw;
        }
        _user_service_client_pool->Keepalive(user_client_wrapper);
        return _return;
      });

  int64_t user_id;
  int64_t followee_id;
  try {
    user_id = user_id_future.get();
    followee_id = followee_id_future.get();
  } catch (const std::exception &e) {
    LOG(warning) << e.what();
    throw;
  }

  if (user_id >= 0 && followee_id >= 0) {
    Follow(req_id, user_id, followee_id, writer_text_map);
  }
  span->Finish();
}

void SocialGraphHandler::UnfollowWithUsername(
    int64_t req_id, const std::string &user_name,
    const std::string &followee_name,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "unfollow_with_username_server",
      {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::future<int64_t> user_id_future = std::async(std::launch::async, [&]() {
    auto user_client_wrapper = _user_service_client_pool->Pop();
    if (!user_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to social-graph-service";
      throw se;
    }
    auto user_client = user_client_wrapper->GetClient();
    int64_t _return;
    try {
      _return = user_client->GetUserId(req_id, user_name, writer_text_map);
    } catch (...) {
      _user_service_client_pool->Remove(user_client_wrapper);
      LOG(error) << "Failed to get user_id from user-service";
      throw;
    }
    _user_service_client_pool->Keepalive(user_client_wrapper);
    return _return;
  });

  std::future<int64_t> followee_id_future =
      std::async(std::launch::async, [&]() {
        auto user_client_wrapper = _user_service_client_pool->Pop();
        if (!user_client_wrapper) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
          se.message = "Failed to connect to social-graph-service";
          throw se;
        }
        auto user_client = user_client_wrapper->GetClient();
        int64_t _return;
        try {
          _return =
              user_client->GetUserId(req_id, followee_name, writer_text_map);
        } catch (...) {
          _user_service_client_pool->Remove(user_client_wrapper);
          LOG(error) << "Failed to get user_id from user-service";
          throw;
        }
        _user_service_client_pool->Keepalive(user_client_wrapper);
        return _return;
      });

  int64_t user_id;
  int64_t followee_id;
  try {
    user_id = user_id_future.get();
    followee_id = followee_id_future.get();
  } catch (...) {
    throw;
  }

  if (user_id >= 0 && followee_id >= 0) {
    try {
      Unfollow(req_id, user_id, followee_id, writer_text_map);
    } catch (...) {
      throw;
    }
  }
  span->Finish();
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_SOCIALGRAPHHANDLER_H
