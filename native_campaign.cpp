#include "native_campaign.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string_view>
#include <thread>

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

class FlatSVMap {
public:
    void reserve(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 2) cap <<= 1;
        rehash(cap);
    }

    const uint32_t* find(std::string_view key) const {
        if (slots_.empty()) return nullptr;
        const uint64_t h = fnv1a_64(key);
        const size_t mask = slots_.size() - 1;
        size_t idx = static_cast<size_t>(h) & mask;
        while (true) {
            const Slot& s = slots_[idx];
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

class FlatClickIdSet {
public:
    void reserve(size_t expected_items, size_t expected_bytes) {
        size_t cap = 64;
        while (cap < expected_items * 2) cap <<= 1;
        slots_.assign(cap, Slot{});
        arena_.reserve(expected_bytes);
    }

    bool insert_with_hash(std::string_view key, uint64_t h, uint32_t& out_off, uint32_t& out_len) {
        if ((size_ + 1) * 10 >= slots_.size() * 7) {
            rehash(slots_.empty() ? 64 : slots_.size() * 2);
        }
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

struct ParsedClickRow {
    uint64_t cid_hash;
    uint32_t cid_off;
    uint32_t cid_len;
    uint32_t person_idx;
    uint32_t campaign_idx;
    int64_t ts_ms;
    uint8_t valid;
};

struct ParsedClickFile {
    std::string file_buf;
    std::vector<ParsedClickRow> rows;
};

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

size_t get_worker_count(size_t max_work) {
    if (max_work <= 1) return 1;
    unsigned hw = std::thread::hardware_concurrency();
    size_t n = hw ? static_cast<size_t>(hw) : 4;
    if (const char* env = std::getenv("NCPU")) {
        int parsed = std::atoi(env);
        if (parsed > 0) n = static_cast<size_t>(parsed);
    }
    if (n > 8) n = 8;
    return std::min(n, max_work);
}

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
    person_to_idx.reserve(sessions.size() + canonical_map.size() + 16);
    FlatSVMap day_to_idx;
    day_to_idx.reserve(64);
    std::vector<std::string> idx_to_day;
    idx_to_day.reserve(64);

    std::vector<SessionRef> raw_srefs;
    raw_srefs.reserve(sessions.size());
    std::vector<uint32_t> person_sess_counts;
    person_sess_counts.reserve(sessions.size() / 2 + 16);

    std::string_view last_day;
    uint32_t last_didx = 0;
    bool has_last_day = false;

    for (const auto& s : sessions) {
        bool ins_p = false;
        const uint32_t pidx = person_to_idx.get_or_insert(
            s.person_id, static_cast<uint32_t>(person_sess_counts.size()), ins_p);
        if (ins_p) person_sess_counts.push_back(1);
        else person_sess_counts[pidx] += 1;

        uint32_t didx = 0;
        if (has_last_day && s.day == last_day) {
            didx = last_didx;
        } else {
            bool ins_d = false;
            didx = day_to_idx.get_or_insert(
                s.day, static_cast<uint32_t>(idx_to_day.size()), ins_d);
            if (ins_d) idx_to_day.push_back(s.day);
            last_day = idx_to_day[didx];
            last_didx = didx;
            has_last_day = true;
        }

        raw_srefs.push_back(SessionRef{pidx, didx, s.session_start_ms, s.revenue_cents, s.has_purchase});
    }

    const uint32_t num_persons = static_cast<uint32_t>(person_sess_counts.size());

    std::vector<uint32_t> person_sess_offsets(num_persons + 1, 0);
    for (uint32_t p = 0; p < num_persons; ++p) {
        person_sess_offsets[p + 1] = person_sess_offsets[p] + person_sess_counts[p];
    }

    std::vector<SessionRef> srefs(raw_srefs.size());
    std::vector<uint32_t> write_pos = person_sess_offsets;
    for (const auto& sr : raw_srefs) {
        srefs[write_pos[sr.person_idx]++] = sr;
    }
    raw_srefs.clear();
    std::vector<SessionRef>().swap(raw_srefs);

    const size_t sort_workers = (num_persons >= 4096) ? get_worker_count(8) : 1;
    if (sort_workers <= 1) {
        for (uint32_t p = 0; p < num_persons; ++p) {
            const uint32_t b = person_sess_offsets[p];
            const uint32_t e = person_sess_offsets[p + 1];
            if (e - b > 1) {
                std::sort(srefs.begin() + b, srefs.begin() + e, [](const SessionRef& a, const SessionRef& b) {
                    return a.start_ms < b.start_ms;
                });
            }
        }
    } else {
        std::vector<std::thread> threads;
        threads.reserve(sort_workers);
        const uint32_t chunk = (num_persons + static_cast<uint32_t>(sort_workers) - 1) / static_cast<uint32_t>(sort_workers);
        for (size_t t = 0; t < sort_workers; ++t) {
            const uint32_t pb = static_cast<uint32_t>(t) * chunk;
            const uint32_t pe = std::min(num_persons, pb + chunk);
            if (pb >= pe) break;
            threads.emplace_back([&, pb, pe]() {
                for (uint32_t p = pb; p < pe; ++p) {
                    const uint32_t b = person_sess_offsets[p];
                    const uint32_t e = person_sess_offsets[p + 1];
                    if (e - b > 1) {
                        std::sort(srefs.begin() + b, srefs.begin() + e, [](const SessionRef& a, const SessionRef& b) {
                            return a.start_ms < b.start_ms;
                        });
                    }
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    for (const auto& kv : canonical_map) {
        const uint32_t* pit = person_to_idx.find(kv.second);
        if (pit) {
            bool ins = false;
            person_to_idx.get_or_insert(kv.first, *pit, ins);
        }
    }

    const auto t_sess = std::chrono::steady_clock::now();

    const int64_t min_ts = window_start_ms - SEVEN_DAYS_MS;
    FlatClickIdSet seen_click_ids;
    std::vector<ClickRec> raw_clicks;
    std::vector<uint32_t> person_click_counts(num_persons, 0);

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

        const size_t num_files = click_files.size();
        std::vector<ParsedClickFile> parsed_files(num_files);

        auto parse_one_file = [&](size_t fidx) {
            ParsedClickFile& pf = parsed_files[fidx];
            std::ifstream in(click_files[fidx], std::ios::binary | std::ios::ate);
            if (!in) return;
            const std::streamsize fsize = in.tellg();
            if (fsize <= 0) return;
            pf.file_buf.resize(static_cast<size_t>(fsize));
            in.seekg(0, std::ios::beg);
            if (!in.read(pf.file_buf.data(), fsize)) {
                pf.file_buf.clear();
                return;
            }

            pf.rows.reserve(static_cast<size_t>(fsize / 64) + 16);
            const char* base_ptr = pf.file_buf.data();
            const char* p = base_ptr;
            const char* end = p + fsize;

            const char* nl = static_cast<const char*>(std::memchr(p, '\n', static_cast<size_t>(end - p)));
            if (!nl) return;
            p = nl + 1;

            while (p < end) {
                nl = static_cast<const char*>(std::memchr(p, '\n', static_cast<size_t>(end - p)));
                const char* line_end = nl ? nl : end;
                std::string_view line(p, static_cast<size_t>(line_end - p));
                p = nl ? (nl + 1) : end;

                if (line.empty()) continue;
                std::string_view c0, c1, c2, c3;
                if (!parse_strict_4cols(line, c0, c1, c2, c3)) continue;
                if (c0.empty()) continue;

                ParsedClickRow row{};
                row.cid_hash = fnv1a_64(c0);
                row.cid_off = static_cast<uint32_t>(c0.data() - base_ptr);
                row.cid_len = static_cast<uint32_t>(c0.size());
                row.valid = 0;

                if (!c1.empty()) {
                    const uint32_t* cit = campaign_to_idx.find(c2);
                    if (cit && *cit != 0) {
                        int64_t ts_ms = 0;
                        if (parse_iso8601_view_to_ms(c3, ts_ms) && ts_ms >= min_ts && ts_ms < window_end_ms) {
                            const uint32_t* pit = person_to_idx.find(c1);
                            if (pit) {
                                row.person_idx = *pit;
                                row.campaign_idx = *cit;
                                row.ts_ms = ts_ms;
                                row.valid = 1;
                            }
                        }
                    }
                }
                pf.rows.push_back(row);
            }
        };

        const size_t workers = get_worker_count(num_files);
        if (workers <= 1) {
            for (size_t f = 0; f < num_files; ++f) parse_one_file(f);
        } else {
            std::atomic<size_t> next_file{0};
            std::vector<std::thread> threads;
            threads.reserve(workers);
            for (size_t t = 0; t < workers; ++t) {
                threads.emplace_back([&]() {
                    while (true) {
                        const size_t f = next_file.fetch_add(1, std::memory_order_relaxed);
                        if (f >= num_files) break;
                        parse_one_file(f);
                    }
                });
            }
            for (auto& th : threads) th.join();
        }

        const size_t est_clicks = static_cast<size_t>(total_click_bytes / 64) + 64;
        seen_click_ids.reserve(est_clicks, est_clicks * 16);
        raw_clicks.reserve(est_clicks / 2 + 64);

        for (size_t f = 0; f < num_files; ++f) {
            ParsedClickFile& pf = parsed_files[f];
            const char* base_ptr = pf.file_buf.data();
            for (const auto& r : pf.rows) {
                std::string_view cid_sv(base_ptr + r.cid_off, r.cid_len);
                uint32_t arena_off = 0, arena_len = 0;
                if (!seen_click_ids.insert_with_hash(cid_sv, r.cid_hash, arena_off, arena_len)) {
                    continue;
                }
                if (r.valid) {
                    raw_clicks.push_back(ClickRec{r.person_idx, r.campaign_idx, r.ts_ms, arena_off, arena_len});
                    person_click_counts[r.person_idx] += 1;
                }
            }
            pf.rows.clear();
            std::vector<ParsedClickRow>().swap(pf.rows);
            pf.file_buf.clear();
            std::string().swap(pf.file_buf);
        }
    }

    seen_click_ids.release_table();
    const auto t_clicks = std::chrono::steady_clock::now();

    std::vector<uint32_t> person_click_offsets(num_persons + 1, 0);
    for (uint32_t p = 0; p < num_persons; ++p) {
        person_click_offsets[p + 1] = person_click_offsets[p] + person_click_counts[p];
    }

    std::vector<ClickRec> clicks(raw_clicks.size());
    std::vector<uint32_t> cwrite = person_click_offsets;
    for (const auto& c : raw_clicks) {
        clicks[cwrite[c.person_idx]++] = c;
    }
    raw_clicks.clear();
    std::vector<ClickRec>().swap(raw_clicks);

    const auto t_sort = std::chrono::steady_clock::now();

    const size_t num_campaigns = campaign_ids.size();
    const size_t num_days = idx_to_day.size();
    const size_t agg_len = num_campaigns * num_days;
    const char* arena_base = seen_click_ids.arena_data();

    auto process_person_range = [&](uint32_t pb, uint32_t pe, std::vector<AggStats>& local_agg) {
        auto add_attributed = [&](uint32_t cidx, const SessionRef& s) {
            AggStats& st = local_agg[static_cast<size_t>(cidx) * num_days + s.day_idx];
            st.sessions += 1;
            if (s.has_purchase) st.purchasing_sessions += 1;
            st.revenue_cents += s.revenue_cents;
        };

        for (uint32_t p = pb; p < pe; ++p) {
            const size_t si = person_sess_offsets[p];
            const size_t s_end = person_sess_offsets[p + 1];
            const size_t c_start = person_click_offsets[p];
            const size_t c_end = person_click_offsets[p + 1];

            if (c_start == c_end) {
                for (size_t i = si; i < s_end; ++i) add_attributed(0, srefs[i]);
                continue;
            }

            const size_t c_count = c_end - c_start;
            if (c_count > 1) {
                std::sort(clicks.begin() + static_cast<std::ptrdiff_t>(c_start),
                          clicks.begin() + static_cast<std::ptrdiff_t>(c_end),
                          [arena_base](const ClickRec& a, const ClickRec& b) {
                              if (a.ts_ms != b.ts_ms) return a.ts_ms < b.ts_ms;
                              const uint32_t min_len = a.click_id_len < b.click_id_len ? a.click_id_len : b.click_id_len;
                              const int cmp = std::memcmp(arena_base + a.click_id_off, arena_base + b.click_id_off, min_len);
                              if (cmp != 0) return cmp < 0;
                              return a.click_id_len < b.click_id_len;
                          });
            }

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

            if (c_count > 1) {
                const uint32_t first_c = clicks[c_start].campaign_idx;
                bool multi_camp = false;
                for (size_t k = c_start + 1; k < c_end; ++k) {
                    if (clicks[k].campaign_idx != first_c) {
                        multi_camp = true;
                        break;
                    }
                }
                if (multi_camp) {
                    std::sort(clicks.begin() + static_cast<std::ptrdiff_t>(c_start),
                              clicks.begin() + static_cast<std::ptrdiff_t>(c_end),
                              [](const ClickRec& a, const ClickRec& b) {
                                  if (a.campaign_idx != b.campaign_idx) return a.campaign_idx < b.campaign_idx;
                                  return a.ts_ms < b.ts_ms;
                              });
                }
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
                            local_agg[camp_offset + srefs[s_ptr].day_idx].assisted_sessions += 1;
                            ++s_ptr;
                        }
                        iv_start = t;
                        iv_end = t + SEVEN_DAYS_MS;
                    }
                    ++ac;
                }
                while (s_ptr < s_end && srefs[s_ptr].start_ms < iv_start) ++s_ptr;
                while (s_ptr < s_end && srefs[s_ptr].start_ms <= iv_end) {
                    local_agg[camp_offset + srefs[s_ptr].day_idx].assisted_sessions += 1;
                    ++s_ptr;
                }
            }
        }
    };

    std::vector<AggStats> agg(agg_len);
    const size_t attrib_workers = (num_persons >= 4096 && agg_len > 0) ? get_worker_count(8) : 1;
    if (attrib_workers <= 1) {
        process_person_range(0, num_persons, agg);
    } else {
        std::vector<std::vector<AggStats>> thread_aggs(attrib_workers, std::vector<AggStats>(agg_len));
        std::vector<std::thread> threads;
        threads.reserve(attrib_workers);
        const uint32_t chunk = (num_persons + static_cast<uint32_t>(attrib_workers) - 1) / static_cast<uint32_t>(attrib_workers);
        for (size_t t = 0; t < attrib_workers; ++t) {
            const uint32_t pb = static_cast<uint32_t>(t) * chunk;
            const uint32_t pe = std::min(num_persons, pb + chunk);
            if (pb >= pe) break;
            threads.emplace_back([&, t, pb, pe]() {
                process_person_range(pb, pe, thread_aggs[t]);
            });
        }
        for (auto& th : threads) th.join();

        for (size_t t = 0; t < threads.size(); ++t) {
            for (size_t i = 0; i < agg_len; ++i) {
                agg[i].sessions += thread_aggs[t][i].sessions;
                agg[i].purchasing_sessions += thread_aggs[t][i].purchasing_sessions;
                agg[i].revenue_cents += thread_aggs[t][i].revenue_cents;
                agg[i].assisted_sessions += thread_aggs[t][i].assisted_sessions;
            }
        }
    }

    seen_click_ids.release_all();
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
    if (argc == 4 && std::string(argv[1]) == "--bench") {
        const size_t n_sess = std::stoull(argv[2]);
        const size_t n_clicks = std::stoull(argv[3]);
        const fs::path tmp = fs::temp_directory_path() / "campaign_bench_v43";
        fs::create_directories(tmp / "clicks");

        {
            std::ofstream cf(tmp / "campaigns.csv", std::ios::binary);
            cf << "campaign_id,channel\n";
            for (int c = 1; c <= 50; ++c) cf << "camp_" << c << ",search\n";
        }
        const int num_files = 8;
        for (int f = 0; f < num_files; ++f) {
            std::ofstream clf(tmp / "clicks" / ("part_" + std::to_string(f) + ".csv"), std::ios::binary);
            clf << "click_id,user_id,campaign_id,ts\n";
            const size_t per_file = n_clicks / num_files;
            for (size_t i = 0; i < per_file; ++i) {
                const size_t idx = static_cast<size_t>(f) * per_file + i;
                clf << "ck_" << idx << ",u_" << (idx % (n_sess / 2)) << ",camp_" << ((idx % 50) + 1)
                    << "," << (1760000000000LL + static_cast<int64_t>((idx % 86400) * 1000LL)) << "\n";
            }
        }

        std::vector<native_campaign::CampaignSession> sessions;
        sessions.reserve(n_sess);
        for (size_t i = 0; i < n_sess; ++i) {
            native_campaign::CampaignSession s;
            s.person_id = "u_" + std::to_string(i % (n_sess / 2));
            s.day = "2026-10-10";
            s.session_start_ms = 1760000000000LL + static_cast<int64_t>((i % 86400) * 1000LL) + 500000LL;
            s.revenue_cents = (i % 5 == 0) ? 2500 : 0;
            s.has_purchase = (i % 5 == 0);
            sessions.push_back(std::move(s));
        }

        std::unordered_map<std::string, std::string> canon;
        auto t0 = std::chrono::steady_clock::now();
        auto rows = native_campaign::compute_campaign_daily_native(
            tmp.string(), canon, 1759900000000LL, 1761000000000LL, sessions);
        auto t1 = std::chrono::steady_clock::now();
        std::cout << "BENCH_CAMPAIGN_SECONDS=" << std::chrono::duration<double>(t1 - t0).count()
                  << " rows_out=" << rows.size() << "\n";
        fs::remove_all(tmp);
        return 0;
    }

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
