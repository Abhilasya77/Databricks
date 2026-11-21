#include "native_campaign.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string_view>
#include <unordered_set>

namespace fs = std::filesystem;

namespace native_campaign {

namespace {

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

// Fast single-field view extraction for LF-only strict CSV lines without allocating per field.
// If escaped quotes ("") are present, materializes into scratch and points out_view there.
bool extract_strict_field_view(
    std::string_view line,
    size_t& pos,
    bool is_last_col,
    std::string_view& out_view,
    std::string& scratch
) {
    bool has_quotes = false;
    const size_t n = line.size();
    if (pos < n && line[pos] == '"') {
        ++pos;  // skip opening quote
        const size_t content_start = pos;
        bool closed = false;
        while (pos < n) {
            if (line[pos] == '"') {
                if (!is_last_col && pos + 1 < n && line[pos + 1] == '"') {
                    has_quotes = true;
                    pos += 2;
                } else {
                    const size_t content_end = pos;
                    ++pos;
                    closed = true;
                    out_view = line.substr(content_start, content_end - content_start);
                    break;
                }
            } else {
                ++pos;
            }
        }
        if (!closed) return false;
        if (!is_last_col) {
            if (pos >= n || line[pos] != ',') return false;
            ++pos;  // skip comma
        } else {
            if (pos != n) return false;
        }
        if (has_quotes) {
            scratch.clear();
            scratch.reserve(out_view.size());
            for (size_t k = 0; k < out_view.size(); ++k) {
                if (out_view[k] == '"' && k + 1 < out_view.size() && out_view[k + 1] == '"') {
                    scratch.push_back('"');
                    ++k;
                } else {
                    scratch.push_back(out_view[k]);
                }
            }
            out_view = scratch;
        }
        return true;
    }

    const size_t start = pos;
    if (is_last_col) {
        while (pos < n) {
            if (line[pos] == '"') return false;
            ++pos;
        }
        out_view = line.substr(start, n - start);
        return true;
    }

    while (pos < n && line[pos] != ',') {
        if (line[pos] == '"') return false;
        ++pos;
    }
    if (pos >= n || line[pos] != ',') return false;
    out_view = line.substr(start, pos - start);
    ++pos;  // skip comma
    return true;
}

bool parse_strict_4cols(
    std::string_view line,
    std::string_view& c0,
    std::string_view& c1,
    std::string_view& c2,
    std::string_view& c3,
    std::string& scratch0,
    std::string& scratch1,
    std::string& scratch2,
    std::string& scratch3
) {
    size_t pos = 0;
    if (!extract_strict_field_view(line, pos, false, c0, scratch0)) return false;
    if (!extract_strict_field_view(line, pos, false, c1, scratch1)) return false;
    if (!extract_strict_field_view(line, pos, false, c2, scratch2)) return false;
    if (!extract_strict_field_view(line, pos, true, c3, scratch3)) return false;
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
    std::string click_id;
};

struct AssistClick {
    uint32_t person_idx;
    uint32_t campaign_idx;
    int64_t ts_ms;
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
        epoch_val = epoch_val * 10 + static_cast<int64_t>(ch - '0');
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
    const fs::path base(data_dir);
    auto campaigns = load_campaigns_csv((base / "campaigns.csv").string());

    std::vector<std::string> campaign_ids;
    std::vector<std::string> campaign_channels;
    std::unordered_map<std::string, uint32_t> campaign_to_idx;
    campaign_ids.reserve(campaigns.size() + 1);
    campaign_channels.reserve(campaigns.size() + 1);
    campaign_to_idx.reserve(campaigns.size() + 1);
    campaign_ids.push_back("organic");
    campaign_channels.push_back("organic");
    campaign_to_idx.emplace("organic", 0u);

    for (const auto& kv : campaigns) {
        uint32_t idx = static_cast<uint32_t>(campaign_ids.size());
        campaign_ids.push_back(kv.first);
        campaign_channels.push_back(kv.second);
        campaign_to_idx.emplace(kv.first, idx);
    }

    std::unordered_map<std::string, uint32_t> person_to_idx;
    person_to_idx.reserve(sessions.size() / 2 + 16);
    std::unordered_map<std::string, uint32_t> day_to_idx;
    std::vector<std::string> idx_to_day;

    std::vector<SessionRef> srefs;
    srefs.reserve(sessions.size());
    for (const auto& s : sessions) {
        auto pit = person_to_idx.find(s.person_id);
        uint32_t pidx;
        if (pit == person_to_idx.end()) {
            pidx = static_cast<uint32_t>(person_to_idx.size());
            person_to_idx.emplace(s.person_id, pidx);
        } else {
            pidx = pit->second;
        }

        auto dit = day_to_idx.find(s.day);
        uint32_t didx;
        if (dit == day_to_idx.end()) {
            didx = static_cast<uint32_t>(idx_to_day.size());
            day_to_idx.emplace(s.day, didx);
            idx_to_day.push_back(s.day);
        } else {
            didx = dit->second;
        }

        srefs.push_back(SessionRef{pidx, didx, s.session_start_ms, s.revenue_cents, s.has_purchase});
    }

    std::sort(srefs.begin(), srefs.end(), [](const SessionRef& a, const SessionRef& b) {
        if (a.person_idx != b.person_idx) return a.person_idx < b.person_idx;
        return a.start_ms < b.start_ms;
    });

    const int64_t min_ts = window_start_ms - SEVEN_DAYS_MS;
    std::unordered_set<std::string> seen_click_ids;
    std::vector<ClickRec> clicks;

    const fs::path clicks_dir = base / "clicks";
    if (fs::exists(clicks_dir) && fs::is_directory(clicks_dir)) {
        std::vector<fs::path> click_files;
        for (const auto& entry : fs::directory_iterator(clicks_dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                click_files.push_back(entry.path());
            }
        }
        std::sort(click_files.begin(), click_files.end(), [](const fs::path& a, const fs::path& b) {
            return a.filename().string() < b.filename().string();
        });

        std::string scratch0, scratch1, scratch2, scratch3;
        std::string line;

        for (const auto& file_path : click_files) {
            std::ifstream in(file_path, std::ios::binary);
            if (!in) continue;
            if (!std::getline(in, line)) continue;

            while (std::getline(in, line)) {
                if (line.empty()) continue;
                std::string_view c0, c1, c2, c3;
                if (!parse_strict_4cols(line, c0, c1, c2, c3, scratch0, scratch1, scratch2, scratch3)) continue;

                // Deduplicate first before validation per pipeline rules
                if (c0.empty()) continue;
                std::string click_id_str(c0);
                if (!seen_click_ids.insert(click_id_str).second) continue;

                if (c1.empty()) continue;

                auto cit = campaign_to_idx.find(std::string(c2));
                if (cit == campaign_to_idx.end() || cit->second == 0) continue;

                int64_t ts_ms = 0;
                if (!parse_iso8601_to_ms(std::string(c3), ts_ms)) continue;
                if (ts_ms < min_ts || ts_ms >= window_end_ms) continue;

                std::string user_str(c1);
                auto mit = canonical_map.find(user_str);
                const std::string& person_id = (mit != canonical_map.end()) ? mit->second : user_str;

                auto pit = person_to_idx.find(person_id);
                if (pit == person_to_idx.end()) continue;

                clicks.push_back(ClickRec{pit->second, cit->second, ts_ms, std::move(click_id_str)});
            }
        }
    }

    // Free seen_click_ids before sorting to keep peak memory low
    seen_click_ids.clear();
    std::unordered_set<std::string>().swap(seen_click_ids);

    const size_t num_campaigns = campaign_ids.size();
    const size_t num_days = idx_to_day.size();
    std::vector<AggStats> agg(num_campaigns * num_days);

    auto add_attributed = [&](uint32_t cidx, const SessionRef& s) {
        AggStats& st = agg[static_cast<size_t>(cidx) * num_days + s.day_idx];
        st.sessions += 1;
        if (s.has_purchase) st.purchasing_sessions += 1;
        st.revenue_cents += s.revenue_cents;
    };

    // Sort by (person_idx, ts_ms, click_id) so first click at each ts_ms has bytewise smallest click_id
    std::sort(clicks.begin(), clicks.end(), [](const ClickRec& a, const ClickRec& b) {
        if (a.person_idx != b.person_idx) return a.person_idx < b.person_idx;
        if (a.ts_ms != b.ts_ms) return a.ts_ms < b.ts_ms;
        return a.click_id < b.click_id;
    });

    size_t si = 0, ci = 0;
    const size_t sn = srefs.size(), cn = clicks.size();

    while (si < sn) {
        const uint32_t pidx = srefs[si].person_idx;
        size_t s_end = si + 1;
        while (s_end < sn && srefs[s_end].person_idx == pidx) ++s_end;

        while (ci < cn && clicks[ci].person_idx < pidx) ++ci;
        size_t c_start = ci;
        while (ci < cn && clicks[ci].person_idx == pidx) ++ci;
        size_t c_end = ci;

        if (c_start == c_end) {
            for (size_t i = si; i < s_end; ++i) add_attributed(0, srefs[i]);
        } else {
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
        }
        si = s_end;
    }

    // Build compact POD assist records without click_id strings and release clicks memory
    std::vector<AssistClick> assist_clicks;
    assist_clicks.reserve(cn);
    for (const auto& c : clicks) {
        assist_clicks.push_back(AssistClick{c.person_idx, c.campaign_idx, c.ts_ms});
    }
    clicks.clear();
    std::vector<ClickRec>().swap(clicks);

    std::sort(assist_clicks.begin(), assist_clicks.end(), [](const AssistClick& a, const AssistClick& b) {
        if (a.person_idx != b.person_idx) return a.person_idx < b.person_idx;
        if (a.campaign_idx != b.campaign_idx) return a.campaign_idx < b.campaign_idx;
        return a.ts_ms < b.ts_ms;
    });

    si = 0;
    ci = 0;
    const size_t acn = assist_clicks.size();
    while (si < sn && ci < acn) {
        const uint32_t pidx = srefs[si].person_idx;
        size_t s_end = si + 1;
        while (s_end < sn && srefs[s_end].person_idx == pidx) ++s_end;

        while (ci < acn && assist_clicks[ci].person_idx < pidx) ++ci;
        while (ci < acn && assist_clicks[ci].person_idx == pidx) {
            const uint32_t cidx = assist_clicks[ci].campaign_idx;
            const size_t camp_offset = static_cast<size_t>(cidx) * num_days;
            size_t s_ptr = si;

            int64_t iv_start = assist_clicks[ci].ts_ms;
            int64_t iv_end = iv_start + SEVEN_DAYS_MS;
            ++ci;

            while (ci < acn && assist_clicks[ci].person_idx == pidx && assist_clicks[ci].campaign_idx == cidx) {
                const int64_t t = assist_clicks[ci].ts_ms;
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
                ++ci;
            }
            while (s_ptr < s_end && srefs[s_ptr].start_ms < iv_start) ++s_ptr;
            while (s_ptr < s_end && srefs[s_ptr].start_ms <= iv_end) {
                agg[camp_offset + srefs[s_ptr].day_idx].assisted_sessions += 1;
                ++s_ptr;
            }
        }
        si = s_end;
    }

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


