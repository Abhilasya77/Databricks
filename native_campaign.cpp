#include "native_campaign.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string_view>

namespace fs = std::filesystem;

namespace native_campaign {

namespace {

inline uint64_t fnv1a_64(std::string_view sv) {
    uint64_t h = 14695981039346656037ULL;
    for (unsigned char c : sv) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    return h ? h : 1ULL;
}

// Flat open-addressing hash map: string_view -> uint32_t (zero heap nodes)
class FlatSVMap {
public:
    void reserve(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 2) cap <<= 1;
        rehash(cap);
    }

    uint32_t* find(std::string_view key) {
        if (slots_.empty()) return nullptr;
        const uint64_t h = fnv1a_64(key);
        const size_t mask = slots_.size() - 1;
        size_t idx = static_cast<size_t>(h) & mask;
        while (true) {
            Slot& s = slots_[idx];
            if (s.hash == 0) return nullptr;
            if (s.hash == h && s.key == key) return &s.val;
            idx = (idx + 1) & mask;
        }
    }

    uint32_t get_or_insert(std::string_view key, uint32_t new_val, bool& inserted) {
        if ((size_ + 1) * 10 >= slots_.size() * 7) {
            rehash(slots_.empty() ? 16 : slots_.size() * 2);
        }
        const uint64_t h = fnv1a_64(key);
        const size_t mask = slots_.size() - 1;
        size_t idx = static_cast<size_t>(h) & mask;
        while (true) {
            Slot& s = slots_[idx];
            if (s.hash == 0) {
                s.hash = h;
                s.key = key;
                s.val = new_val;
                ++size_;
                inserted = true;
                return new_val;
            }
            if (s.hash == h && s.key == key) {
                inserted = false;
                return s.val;
            }
            idx = (idx + 1) & mask;
        }
    }

    size_t size() const { return size_; }

private:
    struct Slot {
        uint64_t hash = 0;
        std::string_view key;
        uint32_t val = 0;
    };

    void rehash(size_t new_cap) {
        std::vector<Slot> next(new_cap);
        const size_t mask = new_cap - 1;
        for (const auto& s : slots_) {
            if (s.hash == 0) continue;
            size_t idx = static_cast<size_t>(s.hash) & mask;
            while (next[idx].hash != 0) idx = (idx + 1) & mask;
            next[idx] = s;
        }
        slots_.swap(next);
    }

    std::vector<Slot> slots_;
    size_t size_ = 0;
};

// Flat open-addressing hash set for click_id deduplication backed by a contiguous char arena.
class FlatClickIdSet {
public:
    void reserve(size_t expected_items, size_t expected_bytes) {
        size_t cap = 64;
        while (cap < expected_items * 2) cap <<= 1;
        slots_.assign(cap, Slot{});
        arena_.reserve(expected_bytes);
    }

    // Inserts key if unseen; returns true if newly inserted and sets out_off/out_len in arena.
    bool insert(std::string_view key, uint32_t& out_off, uint32_t& out_len) {
        if ((size_ + 1) * 10 >= slots_.size() * 7) {
            rehash(slots_.empty() ? 64 : slots_.size() * 2);
        }
        const uint64_t h = fnv1a_64(key);
        const size_t mask = slots_.size() - 1;
        size_t idx = static_cast<size_t>(h) & mask;
        const char* base = arena_.data();
        while (true) {
            Slot& s = slots_[idx];
            if (s.hash == 0) {
                out_off = static_cast<uint32_t>(arena_.size());
                out_len = static_cast<uint32_t>(key.size());
                arena_.insert(arena_.end(), key.data(), key.data() + key.size());
                s.hash = h;
                s.off = out_off;
                s.len = out_len;
                ++size_;
                return true;
            }
            if (s.hash == h && s.len == key.size() &&
                std::memcmp(base + s.off, key.data(), s.len) == 0) {
                return false;
            }
            idx = (idx + 1) & mask;
        }
    }

    const char* arena_data() const { return arena_.data(); }

    // Free hash table slots while keeping the arena alive for click_id tie-breaking.
    void release_table() {
        slots_.clear();
        std::vector<Slot>().swap(slots_);
    }

    void release_all() {
        release_table();
        arena_.clear();
        std::vector<char>().swap(arena_);
    }

private:
    struct Slot {
        uint64_t hash = 0;
        uint32_t off = 0;
        uint32_t len = 0;
    };

    void rehash(size_t new_cap) {
        std::vector<Slot> next(new_cap);
        const size_t mask = new_cap - 1;
        for (const auto& s : slots_) {
            if (s.hash == 0) continue;
            size_t idx = static_cast<size_t>(s.hash) & mask;
            while (next[idx].hash != 0) idx = (idx + 1) & mask;
            next[idx] = s;
        }
        slots_.swap(next);
    }

    std::vector<Slot> slots_;
    std::vector<char> arena_;
    size_t size_ = 0;
};

inline bool is_leap(int y) {
    return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
}

inline int days_in_month(int y, int m) {
    static constexpr int kDays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (m == 2 && is_leap(y)) return 29;
    return kDays[m - 1];
}

inline int64_t days_from_civil(int64_t y, unsigned m, unsigned d) {
    y -= (m <= 2);
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int64_t>(doe) - 719468;
}

inline bool parse_2digits(const char* p, int& out) {
    if (p[0] < '0' || p[0] > '9' || p[1] < '0' || p[1] > '9') return false;
    out = (p[0] - '0') * 10 + (p[1] - '0');
    return true;
}

inline bool parse_4digits(const char* p, int& out) {
    int a = 0, b = 0;
    if (!parse_2digits(p, a) || !parse_2digits(p + 2, b)) return false;
    out = a * 100 + b;
    return true;
}

inline bool parse_iso8601_view_to_ms(std::string_view s, int64_t& out_ms) {
    if (s.empty()) return false;
    const size_t n = s.size();
    const char* p = s.data();

    bool all_digits = true;
    int64_t epoch_val = 0;
    for (size_t k = 0; k < n; ++k) {
        const char ch = p[k];
        if (ch < '0' || ch > '9') {
            all_digits = false;
            break;
        }
        const int64_t digit = static_cast<int64_t>(ch - '0');
        if (epoch_val > (INT64_MAX - digit) / 10) return false;
        epoch_val = epoch_val * 10 + digit;
    }
    if (all_digits) {
        out_ms = epoch_val;
        return true;
    }

    if (n < 20) return false;
    int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
    if (!parse_4digits(p, year)) return false;
    if (p[4] != '-') return false;
    if (!parse_2digits(p + 5, month)) return false;
    if (p[7] != '-') return false;
    if (!parse_2digits(p + 8, day)) return false;
    if (p[10] != 'T') return false;
    if (!parse_2digits(p + 11, hour)) return false;
    if (p[13] != ':') return false;
    if (!parse_2digits(p + 14, minute)) return false;
    if (p[16] != ':') return false;
    if (!parse_2digits(p + 17, second)) return false;

    if (month < 1 || month > 12) return false;
    if (day < 1 || day > days_in_month(year, month)) return false;
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) return false;

    size_t idx = 19;
    if (idx < n && p[idx] == '.') return false;

    int tz_offset_sec = 0;
    if (idx >= n) return false;
    if (p[idx] == 'Z') {
        ++idx;
        if (idx != n) return false;
    } else if (p[idx] == '+' || p[idx] == '-') {
        int sign = (p[idx] == '+') ? 1 : -1;
        ++idx;
        if (idx + 5 != n) return false;
        int tzh = 0, tzm = 0;
        if (!parse_2digits(p + idx, tzh) || p[idx + 2] != ':' || !parse_2digits(p + idx + 3, tzm)) {
            return false;
        }
        if (tzh > 23 || tzm > 59) return false;
        tz_offset_sec = sign * (tzh * 3600 + tzm * 60);
    } else {
        return false;
    }

    int64_t days = days_from_civil(year, static_cast<unsigned>(month), static_cast<unsigned>(day));
    int64_t total_sec = days * 86400LL + hour * 3600LL + minute * 60LL + second - tz_offset_sec;
    out_ms = total_sec * 1000LL;
    return true;
}

// Strict LF-only CSV: quotes may wrap a whole field but a value may
// NEVER contain an embedded quote or comma, even escaped as "".
inline bool extract_strict_field_view(
    std::string_view line,
    size_t& pos,
    bool is_last_col,
    std::string_view& out_view
) {
    const size_t n = line.size();
    if (pos < n && line[pos] == '"') {
        ++pos;
        const size_t start = pos;
        while (pos < n && line[pos] != '"') {
            if (line[pos] == ',') return false;
            ++pos;
        }
        if (pos == n) return false;
        out_view = line.substr(start, pos - start);
        ++pos;
        if (is_last_col) return pos == n;
        if (pos >= n || line[pos] != ',') return false;
        ++pos;
        return true;
    }

    const size_t start = pos;
    while (pos < n && line[pos] != ',') {
        if (line[pos] == '"') return false;
        ++pos;
    }
    out_view = line.substr(start, pos - start);
    if (is_last_col) return pos == n;
    if (pos == n) return false;
    ++pos;
    return true;
}

inline bool parse_strict_4cols(
    std::string_view line,
    std::string_view& c0,
    std::string_view& c1,
    std::string_view& c2,
    std::string_view& c3
) {
    size_t pos = 0;
    if (!extract_strict_field_view(line, pos, false, c0)) return false;
    if (!extract_strict_field_view(line, pos, false, c1)) return false;
    if (!extract_strict_field_view(line, pos, false, c2)) return false;
    if (!extract_strict_field_view(line, pos, true, c3)) return false;
    return pos == line.size();
}

void parse_csv_line_any(std::string_view line, std::vector<std::string>& fields) {
    fields.clear();
    const size_t n = line.size();
    size_t i = 0;
    while (true) {
        std::string field;
        if (i < n && line[i] == '"') {
            ++i;
            while (i < n) {
                if (line[i] == '"') {
                    if (i + 1 < n && line[i + 1] == '"') {
                        field.push_back('"');
                        i += 2;
                    } else {
                        ++i;
                        break;
                    }
                } else {
                    field.push_back(line[i++]);
                }
            }
            while (i < n && line[i] != ',') ++i;
        } else {
            size_t start = i;
            while (i < n && line[i] != ',') ++i;
            field.assign(line.substr(start, i - start));
        }
        fields.push_back(std::move(field));
        if (i == n) break;
        ++i;
    }
}

struct ClickRec {
    uint32_t person_idx;
    uint32_t campaign_idx;
    int64_t ts_ms;
    uint32_t click_id_off;
    uint32_t click_id_len;
};

struct SessionRef {
    uint32_t person_idx;
    uint32_t day_idx;
    int64_t start_ms;
    int64_t revenue_cents;
    bool has_purchase;
};

struct AggStats {
    int64_t sessions = 0;
    int64_t purchasing_sessions = 0;
    int64_t revenue_cents = 0;
    int64_t assisted_sessions = 0;
};

}  // namespace

bool parse_iso8601_to_ms(const std::string& s, int64_t& out_ms) {
    return parse_iso8601_view_to_ms(s, out_ms);
}

std::unordered_map<std::string, std::string> load_campaigns_csv(const std::string& campaigns_csv_path) {
    std::unordered_map<std::string, std::string> campaigns;
    std::ifstream in(campaigns_csv_path, std::ios::binary);
    if (!in) return campaigns;

    std::string line;
    if (!std::getline(in, line)) return campaigns;
    if (!line.empty() && line.back() == '\r') line.pop_back();

    std::vector<std::string> cols;
    parse_csv_line_any(line, cols);
    int cid_col = -1, ch_col = -1;
    for (size_t i = 0; i < cols.size(); ++i) {
        if (cols[i] == "campaign_id") cid_col = static_cast<int>(i);
        else if (cols[i] == "channel") ch_col = static_cast<int>(i);
    }
    if (cid_col < 0 || ch_col < 0) return campaigns;

    while (std::getline(in, line)) {
        if (line.empty()) continue;
        parse_csv_line_any(line, cols);
        if (static_cast<int>(cols.size()) > std::max(cid_col, ch_col)) {
            const std::string& cid = cols[cid_col];
            if (!cid.empty()) {
                campaigns[cid] = cols[ch_col];
            }
        }
    }
    return campaigns;
}

std::vector<CampaignDailyRow> compute_campaign_daily_native(
    const std::string& data_dir,
    const std::unordered_map<std::string, std::string>& canonical_map,
    int64_t window_start_ms,
    int64_t window_end_ms,
    const std::vector<CampaignSession>& sessions
) {
    const bool do_profile = (std::getenv("SHOPPING_PROFILE") != nullptr &&
                             std::strcmp(std::getenv("SHOPPING_PROFILE"), "1") == 0);
    const auto t_start = std::chrono::steady_clock::now();

    const fs::path base(data_dir);
    auto campaigns = load_campaigns_csv((base / "campaigns.csv").string());

    std::vector<std::string> campaign_ids;
    std::vector<std::string> campaign_channels;
    campaign_ids.reserve(campaigns.size() + 1);
    campaign_channels.reserve(campaigns.size() + 1);
    campaign_ids.push_back("organic");
    campaign_channels.push_back("organic");

    for (const auto& kv : campaigns) {
        campaign_ids.push_back(kv.first);
        campaign_channels.push_back(kv.second);
    }

    FlatSVMap campaign_to_idx;
    campaign_to_idx.reserve(campaign_ids.size());
    for (uint32_t i = 0; i < static_cast<uint32_t>(campaign_ids.size()); ++i) {
        bool ins = false;
        campaign_to_idx.get_or_insert(campaign_ids[i], i, ins);
    }

    FlatSVMap person_to_idx;
    person_to_idx.reserve(sessions.size() / 2 + 16);
    FlatSVMap day_to_idx;
    day_to_idx.reserve(64);
    std::vector<std::string> idx_to_day;
    idx_to_day.reserve(64);

    std::vector<SessionRef> srefs;
    srefs.reserve(sessions.size());
    for (const auto& s : sessions) {
        bool ins_p = false;
        const uint32_t pidx = person_to_idx.get_or_insert(
            s.person_id, static_cast<uint32_t>(person_to_idx.size()), ins_p);

        bool ins_d = false;
        const uint32_t didx = day_to_idx.get_or_insert(
            s.day, static_cast<uint32_t>(idx_to_day.size()), ins_d);
        if (ins_d) idx_to_day.push_back(s.day);

        srefs.push_back(SessionRef{pidx, didx, s.session_start_ms, s.revenue_cents, s.has_purchase});
    }

    std::sort(srefs.begin(), srefs.end(), [](const SessionRef& a, const SessionRef& b) {
        if (a.person_idx != b.person_idx) return a.person_idx < b.person_idx;
        return a.start_ms < b.start_ms;
    });

    const auto t_sess = std::chrono::steady_clock::now();

    const int64_t min_ts = window_start_ms - SEVEN_DAYS_MS;
    FlatClickIdSet seen_click_ids;
    std::vector<ClickRec> clicks;

    const fs::path clicks_dir = base / "clicks";
    if (fs::exists(clicks_dir) && fs::is_directory(clicks_dir)) {
        std::vector<fs::path> click_files;
        uint64_t total_click_bytes = 0;
        for (const auto& entry : fs::directory_iterator(clicks_dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                click_files.push_back(entry.path());
                std::error_code ec;
                auto sz = entry.file_size(ec);
                if (!ec) total_click_bytes += sz;
            }
        }
        std::sort(click_files.begin(), click_files.end(), [](const fs::path& a, const fs::path& b) {
            return a.filename().string() < b.filename().string();
        });

        // Estimate ~64 bytes/line to pre-size hash table and arena cleanly
        const size_t est_clicks = static_cast<size_t>(total_click_bytes / 64) + 64;
        seen_click_ids.reserve(est_clicks, est_clicks * 16);
        clicks.reserve(est_clicks / 2 + 64);

        const bool has_canon = !canonical_map.empty();
        std::string user_buf;
        std::string file_buf;

        for (const auto& file_path : click_files) {
            std::ifstream in(file_path, std::ios::binary | std::ios::ate);
            if (!in) continue;
            const std::streamsize fsize = in.tellg();
            if (fsize <= 0) continue;
            file_buf.resize(static_cast<size_t>(fsize));
            in.seekg(0, std::ios::beg);
            if (!in.read(file_buf.data(), fsize)) continue;

            const char* p = file_buf.data();
            const char* end = p + fsize;

            // Skip header line
            const char* nl = static_cast<const char*>(std::memchr(p, '\n', static_cast<size_t>(end - p)));
            if (!nl) continue;
            p = nl + 1;

            while (p < end) {
                nl = static_cast<const char*>(std::memchr(p, '\n', static_cast<size_t>(end - p)));
                const char* line_end = nl ? nl : end;
                const size_t line_len = static_cast<size_t>(line_end - p);
                std::string_view line(p, line_len);
                p = nl ? (nl + 1) : end;

                if (line.empty()) continue;
                std::string_view c0, c1, c2, c3;
                if (!parse_strict_4cols(line, c0, c1, c2, c3)) continue;

                // Deduplicate first before validation per pipeline rules
                if (c0.empty()) continue;
                uint32_t cid_off = 0, cid_len = 0;
                if (!seen_click_ids.insert(c0, cid_off, cid_len)) continue;

                if (c1.empty()) continue;

                uint32_t* cit = campaign_to_idx.find(c2);
                if (!cit || *cit == 0) continue;

                int64_t ts_ms = 0;
                if (!parse_iso8601_view_to_ms(c3, ts_ms)) continue;
                if (ts_ms < min_ts || ts_ms >= window_end_ms) continue;

                std::string_view person_sv = c1;
                if (has_canon) {
                    user_buf.assign(c1.data(), c1.size());
                    auto mit = canonical_map.find(user_buf);
                    if (mit != canonical_map.end()) {
                        person_sv = mit->second;
                    }
                }

                uint32_t* pit = person_to_idx.find(person_sv);
                if (!pit) continue;

                clicks.push_back(ClickRec{*pit, *cit, ts_ms, cid_off, cid_len});
            }
        }
    }

    // Free hash table slots immediately; arena stays alive for click_id tie-breaking during sort
    seen_click_ids.release_table();
    const auto t_clicks = std::chrono::steady_clock::now();

    const size_t num_campaigns = campaign_ids.size();
    const size_t num_days = idx_to_day.size();
    std::vector<AggStats> agg(num_campaigns * num_days);

    auto add_attributed = [&](uint32_t cidx, const SessionRef& s) {
        AggStats& st = agg[static_cast<size_t>(cidx) * num_days + s.day_idx];
        st.sessions += 1;
        if (s.has_purchase) st.purchasing_sessions += 1;
        st.revenue_cents += s.revenue_cents;
    };

    const char* arena_base = seen_click_ids.arena_data();
    std::sort(clicks.begin(), clicks.end(), [arena_base](const ClickRec& a, const ClickRec& b) {
        if (a.person_idx != b.person_idx) return a.person_idx < b.person_idx;
        if (a.ts_ms != b.ts_ms) return a.ts_ms < b.ts_ms;
        const uint32_t min_len = a.click_id_len < b.click_id_len ? a.click_id_len : b.click_id_len;
        const int cmp = std::memcmp(arena_base + a.click_id_off, arena_base + b.click_id_off, min_len);
        if (cmp != 0) return cmp < 0;
        return a.click_id_len < b.click_id_len;
    });

    // Arena is no longer needed once click_id tie-break order is established
    seen_click_ids.release_all();
    const auto t_sort = std::chrono::steady_clock::now();

    // Single pass per person: compute last-click attribution AND interval-merged assists
    // while srefs[si..s_end) and clicks[c_start..c_end) are hot in L1 cache.
    size_t si = 0, ci = 0;
    const size_t sn = srefs.size(), cn = clicks.size();

    while (si < sn) {
        const uint32_t pidx = srefs[si].person_idx;
        size_t s_end = si + 1;
        while (s_end < sn && srefs[s_end].person_idx == pidx) ++s_end;

        while (ci < cn && clicks[ci].person_idx < pidx) ++ci;
        const size_t c_start = ci;
        while (ci < cn && clicks[ci].person_idx == pidx) ++ci;
        const size_t c_end = ci;

        if (c_start == c_end) {
            for (size_t i = si; i < s_end; ++i) add_attributed(0, srefs[i]);
        } else {
            // 1. Last-click attribution
            size_t c_ptr = c_start;
            int64_t best_ts = -1;
            uint32_t best_camp = 0;
            bool has_best = false;

            for (size_t i = si; i < s_end; ++i) {
                const int64_t s_ts = srefs[i].start_ms;
                while (c_ptr < c_end && clicks[c_ptr].ts_ms <= s_ts) {
                    if (!has_best || clicks[c_ptr].ts_ms != best_ts) {
                        best_ts = clicks[c_ptr].ts_ms;
                        best_camp = clicks[c_ptr].campaign_idx;
                        has_best = true;
                    }
                    ++c_ptr;
                }
                if (has_best && (s_ts - best_ts) <= SEVEN_DAYS_MS) {
                    add_attributed(best_camp, srefs[i]);
                } else {
                    add_attributed(0, srefs[i]);
                }
            }

            // 2. Assisted sessions: sort only this person's click slice by (campaign_idx, ts_ms)
            if (c_end - c_start > 1) {
                std::sort(clicks.begin() + static_cast<std::ptrdiff_t>(c_start),
                          clicks.begin() + static_cast<std::ptrdiff_t>(c_end),
                          [](const ClickRec& a, const ClickRec& b) {
                              if (a.campaign_idx != b.campaign_idx) return a.campaign_idx < b.campaign_idx;
                              return a.ts_ms < b.ts_ms;
                          });
            }

            size_t ac = c_start;
            while (ac < c_end) {
                const uint32_t cidx = clicks[ac].campaign_idx;
                const size_t camp_offset = static_cast<size_t>(cidx) * num_days;
                size_t s_ptr = si;

                int64_t iv_start = clicks[ac].ts_ms;
                int64_t iv_end = iv_start + SEVEN_DAYS_MS;
                ++ac;

                while (ac < c_end && clicks[ac].campaign_idx == cidx) {
                    const int64_t t = clicks[ac].ts_ms;
                    if (t <= iv_end) {
                        iv_end = t + SEVEN_DAYS_MS;
                    } else {
                        while (s_ptr < s_end && srefs[s_ptr].start_ms < iv_start) ++s_ptr;
                        while (s_ptr < s_end && srefs[s_ptr].start_ms <= iv_end) {
                            agg[camp_offset + srefs[s_ptr].day_idx].assisted_sessions += 1;
                            ++s_ptr;
                        }
                        iv_start = t;
                        iv_end = t + SEVEN_DAYS_MS;
                    }
                    ++ac;
                }
                while (s_ptr < s_end && srefs[s_ptr].start_ms < iv_start) ++s_ptr;
                while (s_ptr < s_end && srefs[s_ptr].start_ms <= iv_end) {
                    agg[camp_offset + srefs[s_ptr].day_idx].assisted_sessions += 1;
                    ++s_ptr;
                }
            }
        }
        si = s_end;
    }

    const auto t_attrib = std::chrono::steady_clock::now();

    std::vector<CampaignDailyRow> result;
    for (size_t c = 0; c < num_campaigns; ++c) {
        const size_t camp_offset = c * num_days;
        for (size_t d = 0; d < num_days; ++d) {
            const AggStats& st = agg[camp_offset + d];
            if (st.sessions == 0 && st.assisted_sessions == 0) continue;
            result.push_back(CampaignDailyRow{
                campaign_ids[c],
                campaign_channels[c],
                idx_to_day[d],
                st.sessions,
                st.purchasing_sessions,
                st.revenue_cents,
                st.assisted_sessions
            });
        }
    }

    std::sort(result.begin(), result.end(), [](const CampaignDailyRow& a, const CampaignDailyRow& b) {
        if (a.campaign_id != b.campaign_id) return a.campaign_id < b.campaign_id;
        return a.day < b.day;
    });

    if (do_profile) {
        const auto t_end = std::chrono::steady_clock::now();
        auto sec = [](auto a, auto b) { return std::chrono::duration<double>(b - a).count(); };
        std::cerr << "PROFILE campaign_index_sessions_seconds=" << sec(t_start, t_sess) << "\n"
                  << "PROFILE campaign_load_clicks_seconds=" << sec(t_sess, t_clicks) << "\n"
                  << "PROFILE campaign_sort_clicks_seconds=" << sec(t_clicks, t_sort) << "\n"
                  << "PROFILE campaign_attrib_assist_seconds=" << sec(t_sort, t_attrib) << "\n"
                  << "PROFILE campaign_total_seconds=" << sec(t_start, t_end) << "\n";
    }

    return result;
}

}  // namespace native_campaign

#ifdef NATIVE_CAMPAIGN_CLI
int main(int argc, char** argv) {
    if (argc != 7) {
        std::cerr << "usage: native_campaign_cli DATA_DIR WS_MS WE_MS CANON_TSV SESS_TSV OUT_TSV\n";
        return 2;
    }
    const std::string data_dir = argv[1];
    const int64_t ws_ms = std::stoll(argv[2]);
    const int64_t we_ms = std::stoll(argv[3]);
    const std::string canon_path = argv[4];
    const std::string sess_path = argv[5];
    const std::string out_path = argv[6];

    std::unordered_map<std::string, std::string> canon;
    {
        std::ifstream in(canon_path);
        std::string line;
        while (std::getline(in, line)) {
            size_t tab = line.find('\t');
            if (tab != std::string::npos) {
                canon.emplace(line.substr(0, tab), line.substr(tab + 1));
            }
        }
    }

    std::vector<native_campaign::CampaignSession> sessions;
    {
        std::ifstream in(sess_path);
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            size_t t1 = line.find('\t');
            size_t t2 = line.find('\t', t1 + 1);
            size_t t3 = line.find('\t', t2 + 1);
            size_t t4 = line.find('\t', t3 + 1);
            native_campaign::CampaignSession s;
            s.person_id = line.substr(0, t1);
            s.day = line.substr(t1 + 1, t2 - t1 - 1);
            s.session_start_ms = std::stoll(line.substr(t2 + 1, t3 - t2 - 1));
            s.revenue_cents = std::stoll(line.substr(t3 + 1, t4 - t3 - 1));
            s.has_purchase = (line.substr(t4 + 1) == "1");
            sessions.push_back(std::move(s));
        }
    }

    auto t0 = std::chrono::steady_clock::now();
    auto rows = native_campaign::compute_campaign_daily_native(data_dir, canon, ws_ms, we_ms, sessions);
    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();
    std::cerr << "NATIVE_CAMPAIGN_SECONDS=" << elapsed << "\n";

    std::ofstream out(out_path);
    for (const auto& r : rows) {
        out << r.campaign_id << '\t' << r.channel << '\t' << r.day << '\t'
            << r.sessions << '\t' << r.purchasing_sessions << '\t'
            << r.revenue_cents << '\t' << r.assisted_sessions << '\n';
    }
    return 0;
}
#endif
