#include "native_parquet.hpp"

#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>

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

// Minimal Thrift Compact Protocol encoder
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

inline void append_le32(std::vector<uint8_t>& out, uint32_t v) {
    uint8_t b[4] = {
        static_cast<uint8_t>(v & 0xFF),
        static_cast<uint8_t>((v >> 8) & 0xFF),
        static_cast<uint8_t>((v >> 16) & 0xFF),
        static_cast<uint8_t>((v >> 24) & 0xFF)
    };
    out.insert(out.end(), b, b + 4);
}

inline void append_le64(std::vector<uint8_t>& out, uint64_t v) {
    uint8_t b[8];
    std::memcpy(b, &v, 8);
    out.insert(out.end(), b, b + 8);
}

// Encode RLE definition levels (bit-width = 1) prefixed with 4-byte LE byte length.
void encode_rle_def_levels(std::vector<uint8_t>& out, size_t count, const std::vector<uint8_t>* nulls) {
    ThriftCompact rle;
    if (!nulls) {
        // All non-null: single RLE run of 1s
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
    append_le32(out, static_cast<uint32_t>(rle.buf.size()));
    out.insert(out.end(), rle.buf.begin(), rle.buf.end());
}

class ParquetFileCore {
public:
    ParquetFileCore(const std::string& path, std::vector<ColSpec> schema)
        : out_(path, std::ios::binary | std::ios::trunc), schema_(std::move(schema)) {
        if (!out_) throw std::runtime_error("Failed to open Parquet output: " + path);
        static char io_buf[1 << 20];
        out_.rdbuf()->pubsetbuf(io_buf, sizeof(io_buf));
        write_bytes("PAR1", 4);
    }

    ColChunkMeta write_data_page(size_t num_values, const std::vector<uint8_t>& page_payload) {
        const int64_t offset = pos_;
        ThriftCompact hdr;
        int16_t last = 0;
        hdr.write_i32_field(last, 1, PAGE_DATA);
        hdr.write_i32_field(last, 2, static_cast<int32_t>(page_payload.size()));
        hdr.write_i32_field(last, 3, static_cast<int32_t>(page_payload.size()));
        hdr.write_struct_begin(last, 5);  // DataPageHeader
        {
            int16_t dlast = 0;
            hdr.write_i32_field(dlast, 1, static_cast<int32_t>(num_values));
            hdr.write_i32_field(dlast, 2, ENC_PLAIN);
            hdr.write_i32_field(dlast, 3, ENC_RLE);
            hdr.write_i32_field(dlast, 4, ENC_RLE);
            hdr.write_stop();
        }
        hdr.write_stop();

        write_bytes(reinterpret_cast<const char*>(hdr.buf.data()), hdr.buf.size());
        if (!page_payload.empty()) {
            write_bytes(reinterpret_cast<const char*>(page_payload.data()), page_payload.size());
        }
        const int64_t total = pos_ - offset;
        return ColChunkMeta{offset, total, static_cast<int64_t>(num_values)};
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

        // schema (field 2, list<SchemaElement>)
        fm.write_list_header(last, 2, 12, schema_.size() + 1);
        {
            // Root schema element
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

        // row_groups (field 4, list<RowGroup>)
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

        write_bytes(reinterpret_cast<const char*>(fm.buf.data()), fm.buf.size());
        uint32_t footer_len = static_cast<uint32_t>(fm.buf.size());
        write_bytes(reinterpret_cast<const char*>(&footer_len), 4);
        write_bytes("PAR1", 4);
        out_.flush();
    }

private:
    void write_bytes(const char* data, size_t n) {
        out_.write(data, static_cast<std::streamsize>(n));
        pos_ += static_cast<int64_t>(n);
    }

    std::ofstream out_;
    std::vector<ColSpec> schema_;
    std::vector<RowGroupMeta> row_groups_;
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
    std::vector<uint8_t> buf_day;
    std::vector<uint8_t> buf_sessions;
    std::vector<uint8_t> buf_events;
    std::vector<uint8_t> buf_rev;
    std::vector<uint8_t> buf_cat;
    std::vector<uint8_t> cat_nulls;
    bool has_any_cat_null = false;

    std::vector<uint8_t> page_tmp;

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
        buf_day.reserve(limit * 4);
        buf_sessions.reserve(limit * 8);
        buf_events.reserve(limit * 8);
        buf_rev.reserve(limit * 8);
        buf_cat.reserve(limit * 16);
        cat_nulls.reserve(limit);
    }

    void flush_rg() {
        if (rg_rows == 0) return;
        RowGroupMeta rg;
        rg.num_rows = static_cast<int64_t>(rg_rows);
        rg.total_byte_size = 0;
        rg.cols.reserve(6);

        auto emit_col = [&](const std::vector<uint8_t>& values, const std::vector<uint8_t>* nulls) {
            page_tmp.clear();
            encode_rle_def_levels(page_tmp, rg_rows, nulls);
            page_tmp.insert(page_tmp.end(), values.begin(), values.end());
            ColChunkMeta cm = core.write_data_page(rg_rows, page_tmp);
            rg.total_byte_size += cm.total_size;
            rg.cols.push_back(cm);
        };

        emit_col(buf_user_id, nullptr);
        emit_col(buf_day, nullptr);
        emit_col(buf_sessions, nullptr);
        emit_col(buf_events, nullptr);
        emit_col(buf_rev, nullptr);
        emit_col(buf_cat, has_any_cat_null ? &cat_nulls : nullptr);

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
    append_le32(impl_->buf_user_id, static_cast<uint32_t>(user_id.size()));
    impl_->buf_user_id.insert(impl_->buf_user_id.end(), user_id.begin(), user_id.end());
    append_le32(impl_->buf_day, static_cast<uint32_t>(day_days));
    append_le64(impl_->buf_sessions, static_cast<uint64_t>(sessions));
    append_le64(impl_->buf_events, static_cast<uint64_t>(events));
    append_le64(impl_->buf_rev, static_cast<uint64_t>(revenue_cents));

    impl_->cat_nulls.push_back(top_category_is_null ? 1 : 0);
    if (top_category_is_null) {
        impl_->has_any_cat_null = true;
    } else {
        append_le32(impl_->buf_cat, static_cast<uint32_t>(top_category.size()));
        impl_->buf_cat.insert(impl_->buf_cat.end(), top_category.begin(), top_category.end());
    }

    if (++impl_->rg_rows >= impl_->rg_limit) {
        impl_->flush_rg();
    }
}

void UserDailyParquetWriter::write_row(const UserDailyRow& r) {
    write_row(r.user_id, parse_date_days(r.day), r.sessions, r.events,
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
    std::vector<uint8_t> buf_day;
    std::vector<uint8_t> buf_sessions;
    std::vector<uint8_t> buf_purch;
    std::vector<uint8_t> buf_rev;
    std::vector<uint8_t> buf_assisted;
    std::vector<uint8_t> page_tmp;

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

    void flush_rg() {
        if (rg_rows == 0) return;
        RowGroupMeta rg;
        rg.num_rows = static_cast<int64_t>(rg_rows);
        rg.total_byte_size = 0;
        rg.cols.reserve(7);

        auto emit_col = [&](const std::vector<uint8_t>& values) {
            page_tmp.clear();
            encode_rle_def_levels(page_tmp, rg_rows, nullptr);
            page_tmp.insert(page_tmp.end(), values.begin(), values.end());
            ColChunkMeta cm = core.write_data_page(rg_rows, page_tmp);
            rg.total_byte_size += cm.total_size;
            rg.cols.push_back(cm);
        };

        emit_col(buf_cid);
        emit_col(buf_channel);
        emit_col(buf_day);
        emit_col(buf_sessions);
        emit_col(buf_purch);
        emit_col(buf_rev);
        emit_col(buf_assisted);

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
    append_le32(impl_->buf_cid, static_cast<uint32_t>(campaign_id.size()));
    impl_->buf_cid.insert(impl_->buf_cid.end(), campaign_id.begin(), campaign_id.end());
    append_le32(impl_->buf_channel, static_cast<uint32_t>(channel.size()));
    impl_->buf_channel.insert(impl_->buf_channel.end(), channel.begin(), channel.end());
    append_le32(impl_->buf_day, static_cast<uint32_t>(day_days));
    append_le64(impl_->buf_sessions, static_cast<uint64_t>(sessions));
    append_le64(impl_->buf_purch, static_cast<uint64_t>(purchasing_sessions));
    append_le64(impl_->buf_rev, static_cast<uint64_t>(revenue_cents));
    append_le64(impl_->buf_assisted, static_cast<uint64_t>(assisted_sessions));

    if (++impl_->rg_rows >= impl_->rg_limit) {
        impl_->flush_rg();
    }
}

void CampaignDailyParquetWriter::write_row(const CampaignDailyRow& r) {
    write_row(r.campaign_id, r.channel, parse_date_days(r.day), r.sessions,
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
        std::cerr << "usage: native_parquet_cli bench <out_path> <num_rows>\n"
                  << "       native_parquet_cli test_samples <dir>\n";
        return 2;
    }
    std::string mode = argv[1];
    if (mode == "bench" && argc == 4) {
        const std::string out_path = argv[2];
        const size_t n = std::stoull(argv[3]);
        auto t0 = std::chrono::steady_clock::now();
        native_parquet::UserDailyParquetWriter w(out_path, 262144);
        int32_t base_day = native_parquet::parse_date_days("2026-10-01");
        char uid_buf[32];
        char cat_buf[32];
        for (size_t i = 0; i < n; ++i) {
            int ulen = std::snprintf(uid_buf, sizeof(uid_buf), "u_%zu", i);
            int clen = std::snprintf(cat_buf, sizeof(cat_buf), "cat_%zu", i % 15);
            w.write_row(
                std::string_view(uid_buf, ulen),
                base_day + static_cast<int32_t>(i % 28),
                static_cast<int64_t>((i % 5) + 1),
                static_cast<int64_t>((i % 20) + 1),
                static_cast<int64_t>((i % 100) * 100),
                std::string_view(cat_buf, clen),
                false
            );
        }
        w.finish();
        auto t1 = std::chrono::steady_clock::now();
        double sec = std::chrono::duration<double>(t1 - t0).count();
        std::cout << "CPP_DIRECT_PARQUET_SECONDS=" << sec << "\n";
        return 0;
    }
    if (mode == "test_samples") {
        const std::string dir = argv[2];
        // 1. Empty user_daily & campaign_daily
        native_parquet::write_user_daily_parquet(dir + "/empty_user.parquet", {});
        native_parquet::write_campaign_daily_parquet(dir + "/empty_campaign.parquet", {});

        // 2. Populated user_daily with nulls, empty strings, UTF-8, quotes
        native_parquet::write_user_daily_parquet(dir + "/sample_user.parquet", {
            {"u_1", "2026-10-01", 2, 10, 5000, "electronics", false},
            {"u_2", "2026-10-02", 1, 3, 0, "", true},
            {"u_\"3\"", "2026-10-03", 4, 12, 9999, "caf\xC3\xA9,books", false},
        });

        // 3. Populated campaign_daily
        native_parquet::write_campaign_daily_parquet(dir + "/sample_campaign.parquet", {
            {"camp_1", "search", "2026-10-01", 10, 4, 25000, 12},
            {"organic", "organic", "2026-10-02", 5, 1, 1500, 0},
        });
        return 0;
    }
    return 2;
}
#endif
