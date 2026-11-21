#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace native_campaign {

constexpr int64_t SEVEN_DAYS_MS = 7LL * 24LL * 60LL * 60LL * 1000LL;

struct CampaignSession {
    std::string person_id;
    std::string day;            // YYYY-MM-DD
    int64_t session_start_ms = 0;
    int64_t revenue_cents = 0;
    bool has_purchase = false;
};

struct CampaignDailyRow {
    std::string campaign_id;
    std::string channel;
    std::string day;            // YYYY-MM-DD
    int64_t sessions = 0;
    int64_t purchasing_sessions = 0;
    int64_t revenue_cents = 0;
    int64_t assisted_sessions = 0;
};

// Fast ISO-8601 timestamp parser (returns false on invalid format/date).
bool parse_iso8601_to_ms(const std::string& s, int64_t& out_ms);

// Load campaigns.csv -> {campaign_id: channel}
std::unordered_map<std::string, std::string> load_campaigns_csv(const std::string& campaigns_csv_path);

// Full native C++ campaign pipeline:
// 1. Reads campaigns.csv and clicks/*.csv from data_dir
// 2. Deduplicates clicks by click_id (before validation), validates campaign & window
// 3. Computes 7-day last-click attribution (bytewise smallest click_id tie-breaker)
// 4. Computes 7-day interval-merged distinct assisting-campaign counts
std::vector<CampaignDailyRow> compute_campaign_daily_native(
    const std::string& data_dir,
    const std::unordered_map<std::string, std::string>& canonical_map,
    int64_t window_start_ms,
    int64_t window_end_ms,
    const std::vector<CampaignSession>& sessions
);

}  // namespace native_campaign
