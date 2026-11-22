#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace native_parquet {

struct UserDailyRow {
    std::string user_id;
    std::string day;           // YYYY-MM-DD
    int64_t sessions = 0;
    int64_t events = 0;
    int64_t revenue_cents = 0;
    std::string top_category;
    bool top_category_is_null = false;
};

struct CampaignDailyRow {
    std::string campaign_id;
    std::string channel;
    std::string day;           // YYYY-MM-DD
    int64_t sessions = 0;
    int64_t purchasing_sessions = 0;
    int64_t revenue_cents = 0;
    int64_t assisted_sessions = 0;
};

// Convert "YYYY-MM-DD" to Parquet DATE (days since 1970-01-01).
int32_t parse_date_days(std::string_view ymd);

// Streaming, memory-bounded Parquet writer for user_daily.
// Flushes row groups automatically every row_group_size rows.
class UserDailyParquetWriter {
public:
    explicit UserDailyParquetWriter(const std::string& path, size_t row_group_size = 262144);
    ~UserDailyParquetWriter();

    void write_row(
        std::string_view user_id,
        int32_t day_days,
        int64_t sessions,
        int64_t events,
        int64_t revenue_cents,
        std::string_view top_category,
        bool top_category_is_null = false
    );

    void write_row(const UserDailyRow& r);
    void finish();

private:
    struct Impl;
    Impl* impl_;
};

// Streaming, memory-bounded Parquet writer for campaign_daily.
class CampaignDailyParquetWriter {
public:
    explicit CampaignDailyParquetWriter(const std::string& path, size_t row_group_size = 262144);
    ~CampaignDailyParquetWriter();

    void write_row(
        std::string_view campaign_id,
        std::string_view channel,
        int32_t day_days,
        int64_t sessions,
        int64_t purchasing_sessions,
        int64_t revenue_cents,
        int64_t assisted_sessions
    );

    void write_row(const CampaignDailyRow& r);
    void finish();

private:
    struct Impl;
    Impl* impl_;
};

// Convenience batch helpers (create parent directory and write part-0.parquet).
void write_user_daily_parquet(const std::string& parquet_path, const std::vector<UserDailyRow>& rows);
void write_campaign_daily_parquet(const std::string& parquet_path, const std::vector<CampaignDailyRow>& rows);

}  // namespace native_parquet
