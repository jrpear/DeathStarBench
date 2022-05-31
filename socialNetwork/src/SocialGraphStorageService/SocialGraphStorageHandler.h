#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_MEDIASERVICE_MEDIAHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_MEDIASERVICE_MEDIAHANDLER_H_

#include <chrono>
#include <iostream>
#include <string>

#include "../../gen-cpp/SocialGraphStorageService.h"
#include "../logger.h"
#include "../tracing.h"

// 2018-01-01 00:00:00 UTC
#define CUSTOM_EPOCH 1514764800000

namespace social_network {

class SocialGraphStorageHandler : public SocialGraphStorageServiceIf {
public:
    SocialGraphStorageHandler() = default;
    ~SocialGraphStorageHandler() override = default;

    virtual void ReadFollowers(std::vector<int64_t> & _return, const int64_t user_id) override;
    virtual void ReadFollowees(std::vector<int64_t> & _return, const int64_t user_id) override;
    virtual void AddFollower(const int64_t followee_id, const int64_t follower_id) override;
    virtual void AddFollowee(const int64_t follower_id, const int64_t followee_id) override;
    virtual void RemoveFollower(const int64_t followee_id, const int64_t follower_id) override;
    virtual void RemoveFollowee(const int64_t follower_id, const int64_t followee_id) override;

private:
    std::map<int64_t, std::set<int64_t>> m_followers;
    std::map<int64_t, std::set<int64_t>> m_followees;
};

void SocialGraphStorageHandler::ReadFollowers(std::vector<int64_t> & _return, const int64_t user_id) {
    std::set<int64_t> followers = m_followers[user_id];
    std::vector<int64_t> result(followers.begin(), followers.end());
    _return = std::move(result);
}

void SocialGraphStorageHandler::ReadFollowees(std::vector<int64_t> & _return, const int64_t user_id) {
    std::set<int64_t> followees = m_followees[user_id];
    std::vector<int64_t> result(followees.begin(), followees.end());
    _return = std::move(result);
}

void SocialGraphStorageHandler::AddFollower(const int64_t followee_id, const int64_t follower_id) {
    m_followers[followee_id].insert(follower_id);
}

void SocialGraphStorageHandler::AddFollowee(const int64_t follower_id, const int64_t followee_id) {
    m_followees[follower_id].insert(followee_id);
}

void SocialGraphStorageHandler::RemoveFollower(const int64_t followee_id, const int64_t follower_id) {
    m_followers[followee_id].erase(follower_id);
}

void SocialGraphStorageHandler::RemoveFollowee(const int64_t follower_id, const int64_t followee_id) {
    m_followees[follower_id].erase(followee_id);
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_SRC_MEDIASERVICE_MEDIAHANDLER_H_
