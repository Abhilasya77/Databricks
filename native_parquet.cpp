#include "native_parquet.hpp"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <thread>

namespace native_parquet {

namespace {

// Parquet Thrift enums
constexpr int32_t PT_INT32 = 1;
constexpr int32_t PT_INT64 = 2;
constexpr int32_t PT_BYTE_ARRAY = 6;

constexpr int32_t REP_OPTIONAL = 1;

constexpr int32_t CT_UTF8 = 0;
constexpr int32_t CT_DATE = 6;
constexpr int32_t CT_NONE = -1;

constexpr int32_t ENC_PLAIN = 0;
constexpr int32_t ENC_RLE = 3;

constexpr int32_t CODEC_UNCOMPRESSED = 0;
constexpr int32_t PAGE_DATA = 0;

class ThriftCompact {
public:
    std::vector<uint8_t> buf;

    void write_uvarint(uint64_t v) {
        while (v >= 0x80) {
            buf.push_back(static_cast<uint8_t>((v & 0x7F) | 0x80));
            v >>= 7;
        }
        buf.push_back(static_cast<uint8_t>(v));
    }

    void write_zigzag32(int32_t v) {
        uint32_t zz = (static_cast<uint32_t>(v) << 1) ^ static_cast<uint32_t>(v >> 31);
        write_uvarint(zz);
    }

    void write_zigzag64(int64_t v) {
        uint64_t zz = (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
        write_uvarint(zz);
    }

    void write_field_header(int16_t& last_id, int16_t field_id, uint8_t type_id) {
        int16_t delta = field_id - last_id;
        if (delta > 0 && delta <= 15) {
            buf.push_back(static_cast<uint8_t>((delta << 4) | type_id));
        } else {
            buf.push_back(type_id);
            write_zigzag32(field_id);
        }
        last_id = field_id;
    }

    void write_i32_field(int16_t& last_id, int16_t field_id, int32_t v) {
        write_field_header(last_id, field_id, 5);
        write_zigzag32(v);
    }

    void write_i64_field(int16_t& last_id, int16_t field_id, int64_t v) {
        write_field_header(last_id, field_id, 6);
        write_zigzag64(v);
    }

    void write_string_raw(std::string_view s) {
        write_uvarint(s.size());
        buf.insert(buf.end(), s.begin(), s.end());
    }

    void write_string_field(int16_t& last_id, int16_t field_id, std::string_view s) {
        write_field_header(last_id, field_id, 8);
        write_string_raw(s);
    }

    void write_list_header(int16_t& last_id, int16_t field_id, uint8_t elem_type, size_t count) {
        write_field_header(last_id, field_id, 9);
        if (count < 15) {
            buf.push_back(static_cast<uint8_t>((count << 4) | elem_type));
        } else {
            buf.push_back(static_cast<uint8_t>(0xF0 | elem_type));
            write_uvarint(count);
        }
    }

    void write_struct_begin(int16_t& last_id, int16_t field_id) {
        write_field_header(last_id, field_id, 12);
    }

    void write_stop() { buf.push_back(0); }
};

struct ColSpec {
    std::string name;
    int32_t physical_type;
    int32_t converted_type;
};

struct ColChunkMeta {
    int64_t data_page_offset;
    int64_t total_size;
    int64_t num_values;
};

struct RowGroupMeta {
    int64_t num_rows;
    int64_t total_byte_size;
    std::vector<ColChunkMeta> cols;
};

inline void append_le32_vec(std::vector<uint8_t>& out, uint32_t v) {
    const size_t old_sz = out.size();
    out.resize(old_sz + 4);
    std::memcpy(out.data() + old_sz, &v, 4);
}

inline void append_str_bytes(std::vector<uint8_t>& out, std::string_view s) {
    const uint32_t len = static_cast<uint32_t>(s.size());
    const size_t old_sz = out.size();
    out.resize(old_sz + 4 + len);
    uint8_t* dst = out.data() + old_sz;
    std::memcpy(dst, &len, 4);
    if (len > 0) {
        std::memcpy(dst + 4, s.data(), len);
    }
}

// Encode RLE definition levels (bit-width = 1) prefixed with 4-byte LE byte length.
void encode_rle_def_levels(std::vector<uint8_t>& out, size_t count, const std::vector<uint8_t>* nulls) {
    out.clear();
    ThriftCompact rle;
    if (!nulls) {
        rle.write_uvarint(static_cast<uint64_t>(count) << 1);
        rle.buf.push_back(1);
    } else {
        size_t i = 0;
        while (i < count) {
            uint8_t val = (*nulls)[i] ? 0 : 1;
            size_t j = i + 1;
            while (j < count && (((*nulls)[j] ? 0 : 1) == val)) ++j;
            rle.write_uvarint(static_cast<uint64_t>(j - i) << 1);
            rle.buf.push_back(val);
            i = j;
        }
    }
    append_le32_vec(out, static_cast<uint32_t>(rle.buf.size()));
    out.insert(out.end(), rle.buf.begin(), rle.buf.end());
}

class ParquetFileCore {
public:
    ParquetFileCore(const std::string& path, std::vector<ColSpec> schema)
        : io_buf_(1 << 20), schema_(std::move(schema)) {
        fp_ = std::fopen(path.c_str(), "wb");
        if (!fp_) throw std::runtime_error("Failed to open Parquet output: " + path);
        // Thread-safe per-instance 1 MB stdio buffer
        std::setvbuf(fp_, io_buf_.data(), _IOFBF, io_buf_.size());
        write_bytes("PAR1", 4);
    }

    ~ParquetFileCore() {
        if (fp_) {
            std::fclose(fp_);
            fp_ = nullptr;
        }
    }

    // Zero-copy data page write: emits header + RLE def levels + raw column data directly.
    ColChunkMeta write_data_page(
        size_t num_values,
        const std::vector<uint8_t>& def_levels,
        const void* values_data,
        size_t values_bytes
    ) {
        const int64_t offset = pos_;
        const size_t payload_size = def_levels.size() + values_bytes;

        hdr_.buf.clear();
        int16_t last = 0;
        hdr_.write_i32_field(last, 1, PAGE_DATA);
        hdr_.write_i32_field(last, 2, static_cast<int32_t>(payload_size));
        hdr_.write_i32_field(last, 3, static_cast<int32_t>(payload_size));
        hdr_.write_struct_begin(last, 5);  // DataPageHeader
        {
            int16_t dlast = 0;
            hdr_.write_i32_field(dlast, 1, static_cast<int32_t>(num_values));
            hdr_.write_i32_field(dlast, 2, ENC_PLAIN);
            hdr_.write_i32_field(dlast, 3, ENC_RLE);
            hdr_.write_i32_field(dlast, 4, ENC_RLE);
            hdr_.write_stop();
        }
        hdr_.write_stop();

        write_bytes(hdr_.buf.data(), hdr_.buf.size());
        if (!def_levels.empty()) {
            write_bytes(def_levels.data(), def_levels.size());
        }
        if (values_bytes > 0) {
            write_bytes(values_data, values_bytes);
        }
        return ColChunkMeta{offset, pos_ - offset, static_cast<int64_t>(num_values)};
    }

    void add_row_group(RowGroupMeta rg) {
        total_rows_ += rg.num_rows;
        row_groups_.push_back(std::move(rg));
    }

    void finish() {
        if (finished_) return;
        finished_ = true;

        ThriftCompact fm;
        int16_t last = 0;
        fm.write_i32_field(last, 1, 1);  // version = 1

        fm.write_list_header(last, 2, 12, schema_.size() + 1);
        {
            int16_t sl = 0;
            fm.write_string_field(sl, 4, "schema");
            fm.write_i32_field(sl, 5, static_cast<int32_t>(schema_.size()));
            fm.write_stop();
        }
        for (const auto& col : schema_) {
            int16_t sl = 0;
            fm.write_i32_field(sl, 1, col.physical_type);
            fm.write_i32_field(sl, 3, REP_OPTIONAL);
            fm.write_string_field(sl, 4, col.name);
            if (col.converted_type != CT_NONE) {
                fm.write_i32_field(sl, 6, col.converted_type);
            }
            fm.write_stop();
        }

        fm.write_i64_field(last, 3, total_rows_);

        fm.write_list_header(last, 4, 12, row_groups_.size());
        for (const auto& rg : row_groups_) {
            int16_t rl = 0;
            fm.write_list_header(rl, 1, 12, rg.cols.size());
            for (size_t c = 0; c < rg.cols.size(); ++c) {
                const auto& cm = rg.cols[c];
                int16_t cl = 0;
                fm.write_i64_field(cl, 2, cm.data_page_offset);
                fm.write_struct_begin(cl, 3);  // ColumnMetaData
                {
                    int16_t ml = 0;
                    fm.write_i32_field(ml, 1, schema_[c].physical_type);
                    fm.write_list_header(ml, 2, 5, 2);
                    fm.write_zigzag32(ENC_PLAIN);
                    fm.write_zigzag32(ENC_RLE);
                    fm.write_list_header(ml, 3, 8, 1);
                    fm.write_string_raw(schema_[c].name);
                    fm.write_i32_field(ml, 4, CODEC_UNCOMPRESSED);
                    fm.write_i64_field(ml, 5, cm.num_values);
                    fm.write_i64_field(ml, 6, cm.total_size);
                    fm.write_i64_field(ml, 7, cm.total_size);
                    fm.write_i64_field(ml, 9, cm.data_page_offset);
                    fm.write_stop();
                }
                fm.write_stop();
            }
            fm.write_i64_field(rl, 2, rg.total_byte_size);
            fm.write_i64_field(rl, 3, rg.num_rows);
            fm.write_stop();
        }

        fm.write_string_field(last, 6, "native_parquet_cpp17");
        fm.write_stop();

        write_bytes(fm.buf.data(), fm.buf.size());
        uint32_t footer_len = static_cast<uint32_t>(fm.buf.size());
        write_bytes(&footer_len, 4);
        write_bytes("PAR1", 4);

        if (fp_) {
            std::fclose(fp_);
            fp_ = nullptr;
        }
    }

private:
    void write_bytes(const void* data, size_t n) {
        if (n == 0) return;
        if (std::fwrite(data, 1, n, fp_) != n) {
            throw std::runtime_error("Failed writing Parquet bytes");
        }
        pos_ += static_cast<int64_t>(n);
    }

    std::vector<char> io_buf_;
    FILE* fp_ = nullptr;
    std::vector<ColSpec> schema_;
    std::vector<RowGroupMeta> row_groups_;
    ThriftCompact hdr_;
    int64_t pos_ = 0;
    int64_t total_rows_ = 0;
    bool finished_ = false;
};

}  // namespace

int32_t parse_date_days(std::string_view ymd) {
    if (ymd.size() < 10) return 0;
    int64_t y = (ymd[0] - '0') * 1000 + (ymd[1] - '0') * 100 + (ymd[2] - '0') * 10 + (ymd[3] - '0');
    unsigned m = static_cast<unsigned>((ymd[5] - '0') * 10 + (ymd[6] - '0'));
    unsigned d = static_cast<unsigned>((ymd[8] - '0') * 10 + (ymd[9] - '0'));
    y -= (m <= 2);
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return static_cast<int32_t>(era * 146097 + static_cast<int64_t>(doe) - 719468);
}

// ---------------- UserDailyParquetWriter ----------------

struct UserDailyParquetWriter::Impl {
    ParquetFileCore core;
    size_t rg_limit;
    size_t rg_rows = 0;

    std::vector<uint8_t> buf_user_id;
    std::vector<int32_t> buf_day;
    std::vector<int64_t> buf_sessions;
    std::vector<int64_t> buf_events;
    std::vector<int64_t> buf_rev;
    std::vector<uint8_t> buf_cat;
    std::vector<uint8_t> cat_nulls;
    bool has_any_cat_null = false;

    std::vector<uint8_t> def_all_present;
    std::vector<uint8_t> def_nullable;

    char last_ymd[10] = {};
    int32_t last_days = 0;
    bool has_last_ymd = false;

    Impl(const std::string& path, size_t limit)
        : core(path, {
              {"user_id", PT_BYTE_ARRAY, CT_UTF8},
              {"day", PT_INT32, CT_DATE},
              {"sessions", PT_INT64, CT_NONE},
              {"events", PT_INT64, CT_NONE},
              {"revenue_cents", PT_INT64, CT_NONE},
              {"top_category", PT_BYTE_ARRAY, CT_UTF8},
          }),
          rg_limit(limit) {
        buf_user_id.reserve(limit * 16);
        buf_day.reserve(limit);
        buf_sessions.reserve(limit);
        buf_events.reserve(limit);
        buf_rev.reserve(limit);
        buf_cat.reserve(limit * 16);
        cat_nulls.reserve(limit);
    }

    int32_t parse_date_cached(std::string_view ymd) {
        if (ymd.size() == 10 && has_last_ymd && std::memcmp(last_ymd, ymd.data(), 10) == 0) {
            return last_days;
        }
        int32_t d = parse_date_days(ymd);
        if (ymd.size() == 10) {
            std::memcpy(last_ymd, ymd.data(), 10);
            last_days = d;
            has_last_ymd = true;
        }
        return d;
    }

    void flush_rg() {
        if (rg_rows == 0) return;
        RowGroupMeta rg;
        rg.num_rows = static_cast<int64_t>(rg_rows);
        rg.total_byte_size = 0;
        rg.cols.reserve(6);

        encode_rle_def_levels(def_all_present, rg_rows, nullptr);

        auto emit_raw = [&](const void* ptr, size_t bytes, const std::vector<uint8_t>& def_buf) {
            ColChunkMeta cm = core.write_data_page(rg_rows, def_buf, ptr, bytes);
            rg.total_byte_size += cm.total_size;
            rg.cols.push_back(cm);
        };

        emit_raw(buf_user_id.data(), buf_user_id.size(), def_all_present);
        emit_raw(buf_day.data(), buf_day.size() * sizeof(int32_t), def_all_present);
        emit_raw(buf_sessions.data(), buf_sessions.size() * sizeof(int64_t), def_all_present);
        emit_raw(buf_events.data(), buf_events.size() * sizeof(int64_t), def_all_present);
        emit_raw(buf_rev.data(), buf_rev.size() * sizeof(int64_t), def_all_present);

        if (has_any_cat_null) {
            encode_rle_def_levels(def_nullable, rg_rows, &cat_nulls);
            emit_raw(buf_cat.data(), buf_cat.size(), def_nullable);
        } else {
            emit_raw(buf_cat.data(), buf_cat.size(), def_all_present);
        }

        core.add_row_group(std::move(rg));

        buf_user_id.clear();
        buf_day.clear();
        buf_sessions.clear();
        buf_events.clear();
        buf_rev.clear();
        buf_cat.clear();
        cat_nulls.clear();
        has_any_cat_null = false;
        rg_rows = 0;
    }
};

UserDailyParquetWriter::UserDailyParquetWriter(const std::string& path, size_t row_group_size)
    : impl_(new Impl(path, row_group_size)) {}

UserDailyParquetWriter::~UserDailyParquetWriter() {
    if (impl_) {
        impl_->flush_rg();
        impl_->core.finish();
        delete impl_;
    }
}

void UserDailyParquetWriter::write_row(
    std::string_view user_id,
    int32_t day_days,
    int64_t sessions,
    int64_t events,
    int64_t revenue_cents,
    std::string_view top_category,
    bool top_category_is_null
) {
    append_str_bytes(impl_->buf_user_id, user_id);
    impl_->buf_day.push_back(day_days);
    impl_->buf_sessions.push_back(sessions);
    impl_->buf_events.push_back(events);
    impl_->buf_rev.push_back(revenue_cents);

    impl_->cat_nulls.push_back(top_category_is_null ? 1 : 0);
    if (top_category_is_null) {
        impl_->has_any_cat_null = true;
    } else {
        append_str_bytes(impl_->buf_cat, top_category);
    }

    if (++impl_->rg_rows >= impl_->rg_limit) {
        impl_->flush_rg();
    }
}

void UserDailyParquetWriter::write_row(const UserDailyRow& r) {
    write_row(r.user_id, impl_->parse_date_cached(r.day), r.sessions, r.events,
              r.revenue_cents, r.top_category, r.top_category_is_null);
}

void UserDailyParquetWriter::finish() {
    impl_->flush_rg();
    impl_->core.finish();
}

// ---------------- CampaignDailyParquetWriter ----------------

struct CampaignDailyParquetWriter::Impl {
    ParquetFileCore core;
    size_t rg_limit;
    size_t rg_rows = 0;

    std::vector<uint8_t> buf_cid;
    std::vector<uint8_t> buf_channel;
    std::vector<int32_t> buf_day;
    std::vector<int64_t> buf_sessions;
    std::vector<int64_t> buf_purch;
    std::vector<int64_t> buf_rev;
    std::vector<int64_t> buf_assisted;
    std::vector<uint8_t> def_all_present;

    char last_ymd[10] = {};
    int32_t last_days = 0;
    bool has_last_ymd = false;

    Impl(const std::string& path, size_t limit)
        : core(path, {
              {"campaign_id", PT_BYTE_ARRAY, CT_UTF8},
              {"channel", PT_BYTE_ARRAY, CT_UTF8},
              {"day", PT_INT32, CT_DATE},
              {"sessions", PT_INT64, CT_NONE},
              {"purchasing_sessions", PT_INT64, CT_NONE},
              {"revenue_cents", PT_INT64, CT_NONE},
              {"assisted_sessions", PT_INT64, CT_NONE},
          }),
          rg_limit(limit) {}

    int32_t parse_date_cached(std::string_view ymd) {
        if (ymd.size() == 10 && has_last_ymd && std::memcmp(last_ymd, ymd.data(), 10) == 0) {
            return last_days;
        }
        int32_t d = parse_date_days(ymd);
        if (ymd.size() == 10) {
            std::memcpy(last_ymd, ymd.data(), 10);
            last_days = d;
            has_last_ymd = true;
        }
        return d;
    }

    void flush_rg() {
        if (rg_rows == 0) return;
        RowGroupMeta rg;
        rg.num_rows = static_cast<int64_t>(rg_rows);
        rg.total_byte_size = 0;
        rg.cols.reserve(7);

        encode_rle_def_levels(def_all_present, rg_rows, nullptr);

        auto emit_raw = [&](const void* ptr, size_t bytes) {
            ColChunkMeta cm = core.write_data_page(rg_rows, def_all_present, ptr, bytes);
            rg.total_byte_size += cm.total_size;
            rg.cols.push_back(cm);
        };

        emit_raw(buf_cid.data(), buf_cid.size());
        emit_raw(buf_channel.data(), buf_channel.size());
        emit_raw(buf_day.data(), buf_day.size() * sizeof(int32_t));
        emit_raw(buf_sessions.data(), buf_sessions.size() * sizeof(int64_t));
        emit_raw(buf_purch.data(), buf_purch.size() * sizeof(int64_t));
        emit_raw(buf_rev.data(), buf_rev.size() * sizeof(int64_t));
        emit_raw(buf_assisted.data(), buf_assisted.size() * sizeof(int64_t));

        core.add_row_group(std::move(rg));

        buf_cid.clear();
        buf_channel.clear();
        buf_day.clear();
        buf_sessions.clear();
        buf_purch.clear();
        buf_rev.clear();
        buf_assisted.clear();
        rg_rows = 0;
    }
};

CampaignDailyParquetWriter::CampaignDailyParquetWriter(const std::string& path, size_t row_group_size)
    : impl_(new Impl(path, row_group_size)) {}

CampaignDailyParquetWriter::~CampaignDailyParquetWriter() {
    if (impl_) {
        impl_->flush_rg();
        impl_->core.finish();
        delete impl_;
    }
}

void CampaignDailyParquetWriter::write_row(
    std::string_view campaign_id,
    std::string_view channel,
    int32_t day_days,
    int64_t sessions,
    int64_t purchasing_sessions,
    int64_t revenue_cents,
    int64_t assisted_sessions
) {
    append_str_bytes(impl_->buf_cid, campaign_id);
    append_str_bytes(impl_->buf_channel, channel);
    impl_->buf_day.push_back(day_days);
    impl_->buf_sessions.push_back(sessions);
    impl_->buf_purch.push_back(purchasing_sessions);
    impl_->buf_rev.push_back(revenue_cents);
    impl_->buf_assisted.push_back(assisted_sessions);

    if (++impl_->rg_rows >= impl_->rg_limit) {
        impl_->flush_rg();
    }
}

void CampaignDailyParquetWriter::write_row(const CampaignDailyRow& r) {
    write_row(r.campaign_id, r.channel, impl_->parse_date_cached(r.day), r.sessions,
              r.purchasing_sessions, r.revenue_cents, r.assisted_sessions);
}

void CampaignDailyParquetWriter::finish() {
    impl_->flush_rg();
    impl_->core.finish();
}

void write_user_daily_parquet(const std::string& parquet_path, const std::vector<UserDailyRow>& rows) {
    UserDailyParquetWriter w(parquet_path);
    for (const auto& r : rows) w.write_row(r);
    w.finish();
}

void write_campaign_daily_parquet(const std::string& parquet_path, const std::vector<CampaignDailyRow>& rows) {
    CampaignDailyParquetWriter w(parquet_path);
    for (const auto& r : rows) w.write_row(r);
    w.finish();
}

}  // namespace native_parquet

#ifdef NATIVE_PARQUET_CLI
int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: native_parquet_cli bench <out_path> <num_rows> [threads=1] [rg_size=262144]\n"
                  << "       native_parquet_cli test_samples <dir>\n";
        return 2;
    }
    std::string mode = argv[1];
    if (mode == "bench" && argc >= 4) {
        const std::string out_path = argv[2];
        const size_t n = std::stoull(argv[3]);
        const size_t threads = (argc >= 5) ? std::stoull(argv[4]) : 1;
        const size_t rg_size = (argc >= 6) ? std::stoull(argv[5]) : 262144;

        // Pre-build a small pool of realistic strings to isolate pure writer time from snprintf
        std::vector<std::string> uids(65536);
        for (size_t i = 0; i < uids.size(); ++i) uids[i] = "user_" + std::to_string(1000000 + i);
        std::vector<std::string> cats(16);
        for (size_t i = 0; i < cats.size(); ++i) cats[i] = "category_" + std::to_string(i);
        const int32_t base_day = native_parquet::parse_date_days("2026-10-01");

        auto t0 = std::chrono::steady_clock::now();
        if (threads <= 1) {
            native_parquet::UserDailyParquetWriter w(out_path, rg_size);
            for (size_t i = 0; i < n; ++i) {
                w.write_row(
                    uids[i & 65535],
                    base_day + static_cast<int32_t>(i % 28),
                    static_cast<int64_t>((i % 5) + 1),
                    static_cast<int64_t>((i % 20) + 1),
                    static_cast<int64_t>((i % 100) * 100),
                    cats[i & 15],
                    false
                );
            }
            w.finish();
        } else {
            std::vector<std::thread> pool;
            pool.reserve(threads);
            const size_t chunk = (n + threads - 1) / threads;
            for (size_t t = 0; t < threads; ++t) {
                const size_t begin = t * chunk;
                const size_t end = std::min(n, begin + chunk);
                if (begin >= end) break;
                const std::string shard = out_path + "." + std::to_string(t) + ".parquet";
                pool.emplace_back([&, begin, end, shard]() {
                    native_parquet::UserDailyParquetWriter w(shard, rg_size);
                    for (size_t i = begin; i < end; ++i) {
                        w.write_row(
                            uids[i & 65535],
                            base_day + static_cast<int32_t>(i % 28),
                            static_cast<int64_t>((i % 5) + 1),
                            static_cast<int64_t>((i % 20) + 1),
                            static_cast<int64_t>((i % 100) * 100),
                            cats[i & 15],
                            false
                        );
                    }
                    w.finish();
                });
            }
            for (auto& th : pool) th.join();
        }
        auto t1 = std::chrono::steady_clock::now();
        double sec = std::chrono::duration<double>(t1 - t0).count();
        std::cout << "CPP_DIRECT_PARQUET_SECONDS=" << sec
                  << " rows=" << n << " threads=" << threads << " rg=" << rg_size << "\n";
        return 0;
    }
    if (mode == "test_samples") {
        const std::string dir = argv[2];
        native_parquet::write_user_daily_parquet(dir + "/empty_user.parquet", {});
        native_parquet::write_campaign_daily_parquet(dir + "/empty_campaign.parquet", {});

        native_parquet::write_user_daily_parquet(dir + "/sample_user.parquet", {
            {"u_1", "2026-10-01", 2, 10, 5000, "electronics", false},
            {"u_2", "2026-10-02", 1, 3, 0, "", true},
            {"u_\"3\"", "2026-10-03", 4, 12, 9999, "caf\xC3\xA9,books", false},
        });

        native_parquet::write_campaign_daily_parquet(dir + "/sample_campaign.parquet", {
            {"camp_1", "search", "2026-10-01", 10, 4, 25000, 12},
            {"organic", "organic", "2026-10-02", 5, 1, 1500, 0},
        });
        return 0;
    }
    return 2;
}
#endif
