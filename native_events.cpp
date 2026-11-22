// Shopping Sessions: native event cleaning, deduplication, sessionization,
// and user_daily aggregation. Strictly follows the challenge CSV dialect.
// Compile: c++ -std=c++17 -O3 -DNDEBUG native_events.cpp -o native_events
#include <algorithm>
#include <array>
#include <string_view>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <numeric>
#include <atomic>
#include <mutex>
#include <thread>
#include <sstream>
#include <iterator>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <memory>
#include <cstring>
#include <cerrno>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "native_campaign.hpp"
#include "native_parquet.hpp"
using namespace std;
namespace fs = std::filesystem;
using i64 = int64_t;
static constexpr i64 DAY_MS=86400000, GAP_MS=1800000;

static bool digits(string_view s, size_t a, size_t b){
  if(a>=b || b>s.size())return false;
  for(size_t i=a;i<b;i++)if(s[i]<'0'||s[i]>'9')return false;
  return true;
}
static int number(string_view s,size_t a,size_t b){int n=0; for(size_t i=a;i<b;i++)n=n*10+(s[i]-'0'); return n;}
static bool leap(int y){return y%4==0&&(y%100!=0||y%400==0);}
// Day number of 1970-01-01 is zero. Howard Hinnant civil calendar algorithm.
static i64 days_from_civil(int y,unsigned m,unsigned d){
  y -= m<=2;
  const int era=(y>=0?y:y-399)/400;
  const unsigned yoe=static_cast<unsigned>(y-era*400);
  const unsigned doy=(153*(m+(m>2?-3:9))+2)/5+d-1;
  const unsigned yday=yoe*365+yoe/4-yoe/100+doy;
  return era*146097+static_cast<int>(yday)-719468;
}
static bool parse_ts(string_view s,i64 &out){
  if(s.empty())return false;
  // V53: the common epoch-millisecond path must not scan every digit twice.
  // Stop at the first non-digit and fall through to the exact ISO validator.
  i64 acc=0;
  size_t numeric_chars=0;
  for(;numeric_chars<s.size();++numeric_chars){
    const unsigned z=static_cast<unsigned char>(s[numeric_chars])-'0';
    if(z>9)break;
    if(acc>(INT64_MAX-static_cast<i64>(z))/10)return false;
    acc=acc*10+static_cast<i64>(z);
  }
  if(numeric_chars==s.size()) {out=acc;return true;}
  if(s.size()!=20&&s.size()!=25)return false;
  if(s[4]!='-'||s[7]!='-'||s[10]!='T'||s[13]!=':'||s[16]!=':'||
     !digits(s,0,4)||!digits(s,5,7)||!digits(s,8,10)||
     !digits(s,11,13)||!digits(s,14,16)||!digits(s,17,19))return false;
  int y=number(s,0,4),m=number(s,5,7),d=number(s,8,10);
  int hh=number(s,11,13),mm=number(s,14,16),ss=number(s,17,19);
  if(y<1||m<1||m>12||hh>23||mm>59||ss>59)return false;
  static const int month_days[]={0,31,28,31,30,31,30,31,31,30,31,30,31};
  int md=month_days[m]+(m==2&&leap(y)); if(d<1||d>md)return false;
  int offset=0;
  if(s.size()==20){if(s[19]!='Z')return false;}
  else {
    if((s[19]!='+'&&s[19]!='-')||s[22]!=':'||!digits(s,20,22)||!digits(s,23,25))return false;
    int oh=number(s,20,22),om=number(s,23,25);
    if(oh>23||om>59)return false;
    offset=(s[19]=='+'?1:-1)*(oh*60+om)*60000;
  }
  out=days_from_civil(y,m,d)*DAY_MS+hh*3600000LL+mm*60000LL+ss*1000LL-offset;
  return true;
}
static bool parse_line(const string &line,size_t fields,vector<string> &out){
  if(line.empty())return false;
  out.clear(); out.reserve(fields);
  size_t begin=0;
  while(true){
    size_t pos=line.find(',',begin),end=pos==string::npos?line.size():pos;
    string field=line.substr(begin,end-begin);
    if(field.find('"')!=string::npos){
      if(field.size()<2||field.front()!='"'||field.back()!='"'||
         field.find('"',1)!=field.size()-1)return false;
      field=field.substr(1,field.size()-2);
    }
    out.push_back(std::move(field));
    if(out.size()>fields)return false;
    if(pos==string::npos)break;
    begin=pos+1;
  }
  return out.size()==fields;
}
// Fast 64-bit hash for event IDs (inlined, 8-byte chunked + avalanche).
static inline uint64_t fast_hash_sv(string_view s) noexcept {
  const char *p = s.data();
  size_t n = s.size();
  uint64_t h = 0x9e3779b97f4a7c15ULL ^ (static_cast<uint64_t>(n) * 0xbf58476d1ce4e5b9ULL);
  while (n >= 8) {
    uint64_t v;
    memcpy(&v, p, 8);
    h ^= v;
    h *= 0x94d049bb133111ebULL;
    h ^= h >> 31;
    p += 8;
    n -= 8;
  }
  uint64_t tail = 0;
  if (n > 0) memcpy(&tail, p, n);
  h ^= tail;
  h ^= h >> 30;
  h *= 0xbf58476d1ce4e5b9ULL;
  h ^= h >> 27;
  h *= 0x94d049bb133111ebULL;
  h ^= h >> 31;
  return h;
}

// Zero-allocation parser for event rows. Views remain valid while `line` lives.
// The challenge dialect has no escaped quotes and no quoted commas.
static bool parse_event_views(string_view line, array<string_view,7> &out){
  if(line.empty())return false;
  const char *base=line.data();
  const size_t len=line.size();
  if(!memchr(base,'"',len)){
    size_t begin=0;
    for(size_t i=0;i<6;++i){
      const void *comma=memchr(base+begin,',',len-begin);
      if(!comma)return false;
      const size_t end=static_cast<const char*>(comma)-base;
      out[i]=string_view(base+begin,end-begin);
      begin=end+1;
    }
    if(memchr(base+begin,',',len-begin))return false;
    out[6]=string_view(base+begin,len-begin);
    return true;
  }
  size_t begin=0;
  for(size_t i=0;i<7;++i){
    size_t end=line.find(',',begin);
    if(i<6 && end==string::npos)return false;
    if(i==6 && end!=string::npos)return false;
    if(end==string::npos)end=len;
    string_view value(base+begin,end-begin);
    if(value.find('"')!=string_view::npos){
      if(value.size()<2||value.front()!='"'||value.back()!='"'||
         value.find('"',1)!=value.size()-1)return false;
      value.remove_prefix(1);value.remove_suffix(1);
    }
    out[i]=value;
    begin=end+1;
  }
  return true;
}

static vector<fs::path> parts(const fs::path &dir){
  vector<fs::path> files;
  if(!fs::exists(dir))return files;
  for(const auto &f:fs::directory_iterator(dir))if(f.is_regular_file()&&f.path().extension()==".csv") files.push_back(f.path());
  sort(files.begin(),files.end());return files;
}
template<typename F> static void each_line(const fs::path &p,size_t fields,F fn){
  ifstream in(p,basic_ios<char>::in|ios::binary);
  if(!in)throw runtime_error("can't open "+p.string());
  string line;getline(in,line);vector<string> c;
  while(getline(in,line))if(parse_line(line,fields,c))fn(c);
}
static string json_escape(const string &s){
  static const char* hex="0123456789abcdef";
  string out;out.reserve(s.size()+2);out.push_back('"');
  for(unsigned char c:s){
    if(c=='"'||c=='\\'){out.push_back('\\');out.push_back(char(c));}
    else if(c=='\n')out+="\\n";
    else if(c=='\r')out+="\\r";
    else if(c=='\t')out+="\\t";
    else if(c<32){out+="\\u00";out.push_back(hex[c>>4]);out.push_back(hex[c&15]);}
    else out.push_back(char(c));
  }
  out.push_back('"');return out;
}
static void read_map(const string &path,unordered_map<string,string> &dst){
  ifstream in(path,ios::binary);if(!in)throw runtime_error("no identity map file");
  uint32_t n=0;
  while(in.read(reinterpret_cast<char*>(&n),4)){
    if(n>1000000)throw runtime_error("identity id too large");
    string a(n,'\0');in.read(a.data(),n);
    if(!in.read(reinterpret_cast<char*>(&n),4)||n>1000000)throw runtime_error("identity map corrupt");
    string b(n,'\0');in.read(b.data(),n);
    if(!in)throw runtime_error("identity map truncated");
    dst.emplace(move(a),move(b));
  }
}
static string json_property(const string &data,const string &key){
  const string search="\""+key+"\"";
  size_t p=data.find(search);if(p==string::npos)throw runtime_error("bad manifest: "+key);
  p=data.find(':',p+search.size());if(p==string::npos)throw runtime_error("manifest syntax");
  p=data.find('"',p+1);if(p==string::npos)throw runtime_error("manifest syntax");
  auto q=data.find('"',p+1);if(q==string::npos)throw runtime_error("manifest syntax");
  return data.substr(p+1,q-p-1);
}
static string day_string(i64 ts){
  // Use inverse Gregorian civil conversion. Floor division for pre-epoch.
  i64 z=ts/DAY_MS;
  if(ts<0&&ts%DAY_MS) --z;
  z+=719468;
  const i64 era=(z>=0?z:z-146096)/146097;
  const unsigned doe=(unsigned)(z-era*146097);
  const unsigned yoe=(doe-doe/1460+doe/36524-doe/146096)/365;
  int y=(int)yoe+(int)era*400;
  const unsigned doy=doe-(365*yoe+yoe/4-yoe/100);
  const unsigned mp=(5*doy+2)/153;
  const unsigned d=doy-(153*mp+2)/5+1;
  const unsigned m=mp+(mp<10?3:-9);
  y+=(m<=2);
  char str[16];snprintf(str,sizeof(str),"%04d-%02u-%02u",y,m,d);
  return str;
}
struct Raw{string_view line; uint32_t ingest_pos=0, ingest_len=0;};
struct Event{ i64 ts=0, cents=0; uint32_t person=0, category=0; bool purchase=false;};
struct Session{
  string person, day; i64 start=0,events=0,revenue=0;
  bool purchased=false;
  unordered_map<string,i64> category_revenue;
};
struct Daily{string person,day; i64 sessions=0,events=0,revenue=0;unordered_map<string,i64> categories;};
int main(int argc,char**argv){
 try{
  auto last_profile=chrono::steady_clock::now();
  auto profile=[&](const char *phase){
    auto now=chrono::steady_clock::now();
    if(const char* p=std::getenv("SHOPPING_PROFILE"); p && *p == '1'){
      double sec=chrono::duration<double>(now-last_profile).count();
      cerr<<"PROFILE native_"<<phase<<"_seconds="<<sec<<"\n";
    }
    last_profile=now;
  };
  if(argc!=5 && argc!=6 && argc!=7){cerr<<"usage: native_events DATA_DIR ID_MAP_BIN USERS_JSONL SESSIONS_JSONL [CAMPAIGN_JSONL]\n";return 2;}
  const bool direct_parquet = argc==7;
  if(direct_parquet){
    fs::create_directories(fs::path(argv[6])/"user_daily");
    fs::create_directories(fs::path(argv[6])/"campaign_daily");
  }
  fs::path data=argv[1];unordered_map<string,string> ids;
  read_map(argv[2],ids);
  ifstream mi(data/"manifest.json",ios::binary);
  if(!mi)throw runtime_error("manifest missing");
  string manifest((istreambuf_iterator<char>(mi)),istreambuf_iterator<char>());
  i64 lo,hi;if(!parse_ts(json_property(manifest,"window_start"),lo)||
              !parse_ts(json_property(manifest,"window_end"),hi))throw runtime_error("bad window");
  unordered_map<string,string> categories;
  each_line(data/"products.csv",2,[&](const vector<string>&v){categories.emplace(v[0],v[1]);});
  // V43: stable borrowed-key indexes eliminate temporary std::string
  // construction for each unique raw user and purchased product lookup.
  // Keep owning maps unmodified for the lifetime of these string_views.
  unordered_map<string_view,string_view> identity_lookup;
  identity_lookup.reserve(ids.size());
  for(const auto &kv:ids)identity_lookup.emplace(string_view(kv.first),string_view(kv.second));
  unordered_map<string_view,string_view> product_lookup;
  product_lookup.reserve(categories.size());
  for(const auto &kv:categories)product_lookup.emplace(string_view(kv.first),string_view(kv.second));
  profile("metadata_and_identity");
  // Store one compact raw line per event_id instead of six separate strings.
  // Dedup still precedes validation (including late/invalid timestamps).
  // Independent byte ranges let many workers read even a single large CSV.
  // Lines are delimited ONLY by LF; each range owns lines whose first byte
  // falls inside [begin,end). The first range consumes the CSV header.
  // V26: zero-copy mmap reader. Raw lines and dedup IDs point directly into
  // read-only event files, avoiding ~2 allocations and copies per unique ID.
  // All mappings stay alive until after the parallel validation + interning
  // phase, so all borrowed string_views remain valid.
  struct MappedFile {
    const char *data=nullptr;
    size_t size=0;
    explicit MappedFile(const fs::path &path){
      int fd=open(path.c_str(), O_RDONLY);
      if(fd<0)throw runtime_error("cannot open event file: "+path.string());
      struct stat st{};
      if(fstat(fd,&st)!=0 || st.st_size<0){close(fd);throw runtime_error("cannot stat event file");}
      size=static_cast<size_t>(st.st_size);
      if(static_cast<off_t>(size)!=st.st_size){close(fd);throw runtime_error("event file too big");}
      if(size){
        void *p=mmap(nullptr,size,PROT_READ,MAP_PRIVATE,fd,0);
        if(p==MAP_FAILED){close(fd);throw runtime_error("event mmap failed");}
        data=static_cast<const char*>(p);
      }
      close(fd);
    }
    ~MappedFile(){if(data)munmap(const_cast<char*>(data),size);}
    MappedFile(const MappedFile&)=delete;
    MappedFile& operator=(const MappedFile&)=delete;
  };
  struct Task { const MappedFile *file; uint64_t begin,end; };
  constexpr uint64_t CHUNK_BYTES=8ULL*1024*1024;
  vector<unique_ptr<MappedFile>> mappings;
  vector<Task> tasks;
  uint64_t estimated_bytes=0;
  for(const auto &p:parts(data/"events")){
    auto f=make_unique<MappedFile>(p);
    const uint64_t len=f->size;
    estimated_bytes+=len;
    if(len){
      for(uint64_t begin=0;begin<len;begin+=CHUNK_BYTES)
        tasks.push_back(Task{f.get(),begin,min(len,begin+CHUNK_BYTES)});
    }
    mappings.push_back(move(f));
  }
  // V58 architectural redesign:
  // 1) Zero-lock parallel CSV scan: each worker extracts compact mmap-backed
  //    winner references (line ptr/len, event_id & ingest_ts offsets, 64-bit hash)
  //    into thread-local partition buckets (64 partitions). Zero mutex contention.
  // 2) Zero-lock parallel partition dedup + validation: each worker owns one
  //    disjoint partition at a time, deduplicates in a single reusable L2/L3-friendly
  //    open-addressed table (preserving exact smallest lexicographic ingest_ts),
  //    and immediately validates winners into parsed[part].
  constexpr size_t SHARD_COUNT=64;
  struct CompactRow {
    const char *line_ptr;
    uint32_t line_len;
    uint16_t id_off, id_len;
    uint16_t ing_off, ing_len;
    uint64_t hash_code;
  };
  size_t thread_count=min<size_t>(8,thread::hardware_concurrency()?thread::hardware_concurrency():4);
  if(const char* cpus=getenv("NCPU")){
    char *stop=nullptr;
    long requested=strtol(cpus,&stop,10);
    if(stop!=cpus&&*stop=='\0'&&requested>0)
      thread_count=min<size_t>(16,static_cast<size_t>(requested));
  }
  if(const char* cpus=getenv("SHOPPING_NATIVE_THREADS")){
    char *stop=nullptr;
    long requested=strtol(cpus,&stop,10);
    if(stop!=cpus&&*stop=='\0'&&requested>0)
      thread_count=min<size_t>(16,static_cast<size_t>(requested));
  }
  thread_count=max<size_t>(1,min(thread_count,tasks.size()?tasks.size():size_t(1)));

  // Per-worker partitioned compact row buffers: worker_buckets[tid][part]
  vector<array<vector<CompactRow>,SHARD_COUNT>> worker_buckets(thread_count);
  const size_t per_worker_part_hint=min<size_t>(
      65536, static_cast<size_t>(estimated_bytes/140/(thread_count*SHARD_COUNT)+64));
  for(auto &wb:worker_buckets)
    for(auto &b:wb)b.reserve(per_worker_part_hint);

  auto scan_task=[&](const Task &task, array<vector<CompactRow>,SHARD_COUNT> &local_parts){
    const MappedFile &f=*task.file;
    const char *const base=f.data;
    const size_t file_end=f.size;
    size_t start=static_cast<size_t>(task.begin);
    if(start==0){
      const void *lf=memchr(base,'\n',file_end);
      if(!lf)return;
      start=static_cast<const char*>(lf)-base+1;
    }else{
      if(base[start-1]!='\n'){
        const void *lf=memchr(base+start,'\n',file_end-start);
        if(!lf)return;
        start=static_cast<const char*>(lf)-base+1;
      }
    }
    array<string_view,7> fields;
    while(start<task.end && start<file_end){
      const void *lf=memchr(base+start,'\n',file_end-start);
      const size_t stop=lf?static_cast<const char*>(lf)-base:file_end;
      const string_view line(base+start,stop-start);
      if(parse_event_views(line,fields)&&!fields[0].empty()){
        const uint64_t h=fast_hash_sv(fields[0]);
        const uint16_t id_off=static_cast<uint16_t>(fields[0].data()-line.data());
        const uint16_t id_len=static_cast<uint16_t>(fields[0].size());
        const uint16_t ing_off=static_cast<uint16_t>(fields[3].data()-line.data());
        const uint16_t ing_len=static_cast<uint16_t>(fields[3].size());
        local_parts[h&(SHARD_COUNT-1)].push_back(CompactRow{
            line.data(), static_cast<uint32_t>(line.size()),
            id_off, id_len, ing_off, ing_len, h});
      }
      start=stop+static_cast<size_t>(!!lf);
    }
  };

  if(thread_count==1){
    for(const auto &task:tasks)scan_task(task,worker_buckets[0]);
  }else{
    atomic<size_t> next{0};
    exception_ptr failure;
    mutex failure_lock;
    auto worker=[&](size_t tid){
      try{
        auto &local_parts=worker_buckets[tid];
        for(;;){
          size_t idx=next.fetch_add(1,memory_order_relaxed);
          if(idx>=tasks.size())break;
          scan_task(tasks[idx],local_parts);
        }
      }catch(...){
        lock_guard<mutex> guard(failure_lock);
        if(!failure)failure=current_exception();
      }
    };
    vector<thread> workers;
    workers.reserve(thread_count);
    for(size_t i=0;i<thread_count;++i)workers.emplace_back(worker,i);
    for(auto &worker_thread:workers)worker_thread.join();
    if(failure)rethrow_exception(failure);
  }
  profile("event_dedup");
  size_t n_raw=0;
  for(const auto &wb:worker_buckets)for(const auto &b:wb)n_raw+=b.size();
  vector<Event> events;events.reserve(n_raw);
  // Intern the canonical person and purchase category once. Events now store
  // 32-byte POD records instead of two separately allocated strings.
  // Borrow canonical names from stable identity-map values or event mmaps.
  // All referenced storage outlives interning and person-id ranking.
  // V51: canonical-person table uses contiguous entries and open-addressed
  // slots. Avoid one heap allocation per unique canonical user.
  // Cache canonical numeric IDs by original raw user ID. The keys borrow
  // stable bytes from the read-only event mmaps, which outlive validation.
  // Repeated users bypass BOTH identity-map and person-interning lookups.
  // V36 experiment: contiguous open-addressing cache for raw user IDs.
  // Keys borrow mapped CSV bytes and remain alive until the cache is cleared.
  struct FlatUserCache {
    struct Entry { string_view key; uint32_t value; size_t hash_code; };
    vector<Entry> entries;
    vector<uint32_t> slots;
    void rehash_to(size_t capacity){
      vector<uint32_t> next(capacity,0);
      for(size_t i=0;i<entries.size();++i){
        size_t pos=(entries[i].hash_code>>3)&(capacity-1);
        while(next[pos])pos=(pos+1)&(capacity-1);
        next[pos]=static_cast<uint32_t>(i+1);
      }
      slots.swap(next);
    }
    pair<uint32_t*,bool> try_emplace(string_view key,uint32_t initial){
      if(slots.empty())rehash_to(1024);
      const size_t h=hash<string_view>{}(key);
      size_t pos=(h>>3)&(slots.size()-1);
      while(slots[pos]){
        Entry &e=entries[slots[pos]-1];
        if(e.hash_code==h && e.key==key)return {&e.value,false};
        pos=(pos+1)&(slots.size()-1);
      }
      if(entries.size()==UINT32_MAX)throw runtime_error("too many users");
      // Rehash before inserting, so existing pointers are never reused after a grow.
      if((entries.size()+1)*10>=slots.size()*7){
        rehash_to(slots.size()*2);
        pos=(h>>3)&(slots.size()-1);
        while(slots[pos])pos=(pos+1)&(slots.size()-1);
      }
      entries.push_back(Entry{key,initial,h});
      slots[pos]=static_cast<uint32_t>(entries.size());
      return {&entries.back().value,true};
    }
    void reserve(size_t expected){
      entries.reserve(expected);
      size_t cap=1024;
      while(cap<expected*2 && cap<(size_t(1)<<31))cap*=2;
      rehash_to(cap);
    }
    void clear(){vector<Entry>().swap(entries);vector<uint32_t>().swap(slots);}
  };
  FlatUserCache raw_user_cache;
  FlatUserCache person_ids;
  // Canonical IDs still follow first occurrence; the later lexicographic
  // ranking pass is unchanged. Both caches borrow stable mmap/identity bytes.
  person_ids.reserve(min<size_t>(n_raw / 4, 5000000));
  // V48: allocate the intern cache once, rather than repeatedly growing and
  // rehashing millions of user keys during the serial conversion pass.
  // Cap the hint to avoid excessive memory use on high-cardinality fixtures.
  // V49 experiment: reduce large raw-user cache rehash passes on datasets
  // containing millions of distinct users. Open addressing requires slack;
  // this is a capacity hint only and does not change key/winner semantics.
  // The cap keeps the up-front allocation bounded on very large inputs.
  raw_user_cache.reserve(min<size_t>(n_raw / 4, 5000000));
  vector<string> people;
  unordered_map<string,uint32_t> category_ids;
  // Purchase product IDs are also repeated; resolve their category only once.
  unordered_map<string_view,uint32_t> product_category_cache;
  vector<string> category_names;
  auto category_number=[&](const string &name)->uint32_t{
    auto [it,inserted]=category_ids.try_emplace(name,static_cast<uint32_t>(category_names.size()));
    if(inserted)category_names.push_back(name);
    return it->second;
  };
  // V22: parse and validate each disjoint dedup shard in parallel.
  // Borrow string_views from Raw rows until after all users/categories are
  // interned. Consume shard results in baseline order for deterministic output.
  struct ParsedEvent {
    i64 ts, cents;
    string_view user, product;
    bool purchase;
  };
  vector<vector<ParsedEvent>> parsed(SHARD_COUNT);
  // Fused lock-free per-partition dedup + validation using a reusable thread-local table.
  struct LocalDedupTable {
    vector<CompactRow> winners;
    vector<uint32_t> slots;
    void reset(size_t expected){
      winners.clear();
      winners.reserve(expected);
      size_t cap=16;
      while(cap<expected*2)cap*=2;
      slots.assign(cap,0);
    }
    void rehash(size_t cap){
      vector<uint32_t> next(cap,0);
      const size_t mask=cap-1;
      for(size_t i=0;i<winners.size();++i){
        size_t pos=(winners[i].hash_code>>6)&mask;
        while(next[pos])pos=(pos+1)&mask;
        next[pos]=static_cast<uint32_t>(i+1);
      }
      slots.swap(next);
    }
    void upsert(const CompactRow &c){
      if((winners.size()+1)*10>=slots.size()*7)rehash(slots.size()*2);
      const size_t mask=slots.size()-1;
      size_t pos=(c.hash_code>>6)&mask;
      const string_view cid(c.line_ptr+c.id_off,c.id_len);
      while(uint32_t s=slots[pos]){
        CompactRow &w=winners[s-1];
        if(w.hash_code==c.hash_code &&
           w.id_len==c.id_len &&
           memcmp(w.line_ptr+w.id_off,cid.data(),c.id_len)==0){
          const string_view prev_ing(w.line_ptr+w.ing_off,w.ing_len);
          const string_view cand_ing(c.line_ptr+c.ing_off,c.ing_len);
          if(cand_ing<prev_ing)w=c;
          return;
        }
        pos=(pos+1)&mask;
      }
      winners.push_back(c);
      slots[pos]=static_cast<uint32_t>(winners.size());
    }
  };

  auto validate_shard=[&](size_t slot, LocalDedupTable &tbl){
    size_t total_in=0;
    for(size_t tid=0;tid<thread_count;++tid)total_in+=worker_buckets[tid][slot].size();
    if(total_in==0)return;
    tbl.reset(total_in);
    for(size_t tid=0;tid<thread_count;++tid){
      auto &b=worker_buckets[tid][slot];
      for(const CompactRow &c:b)tbl.upsert(c);
      vector<CompactRow>().swap(b); // free partition input immediately
    }
    auto &out=parsed[slot];
    out.reserve(tbl.winners.size());
    array<string_view,7> columns;
    for(const CompactRow &r:tbl.winners){
      const string_view line(r.line_ptr,r.line_len);
      if(!parse_event_views(line,columns))throw runtime_error("internal CSV invariant");
      const string_view user=columns[1], tsstr=columns[2],
                        event_type=columns[4], product=columns[5], price=columns[6];
      if(user.empty()||product.empty()||(event_type!="view"&&event_type!="cart"&&event_type!="purchase"))continue;
      i64 ts,ingest;
      if(!parse_ts(tsstr,ts)||ts<lo||ts>=hi||!parse_ts(columns[3],ingest)||ingest-ts>DAY_MS)continue;
      i64 cents=0;
      const bool purchase=event_type=="purchase";
      if(purchase){
        if(price.empty())continue;
        bool valid_price=true;
        for(char c:price){
          const unsigned digit=static_cast<unsigned char>(c)-'0';
          if(digit>9 || cents>10000000/10){valid_price=false;break;}
          cents=cents*10+digit;
          if(cents>10000000){valid_price=false;break;}
        }
        if(!valid_price||cents<1)continue;
      }
      out.push_back(ParsedEvent{ts,cents,user,product,purchase});
    }
  };
  size_t validation_threads=min<size_t>(thread_count,SHARD_COUNT);
  if(validation_threads<=1){
    LocalDedupTable tbl;
    for(size_t s=0;s<SHARD_COUNT;++s)validate_shard(s,tbl);
  }else{
    atomic<size_t> next_shard{0};
    exception_ptr failure;
    mutex failure_lock;
    auto worker=[&](){
      try{
        LocalDedupTable tbl;
        for(;;){
          auto slot=next_shard.fetch_add(1,memory_order_relaxed);
          if(slot>=SHARD_COUNT)break;
          validate_shard(slot,tbl);
        }
      }catch(...){
        lock_guard<mutex> g(failure_lock);
        if(!failure)failure=current_exception();
      }
    };
    vector<thread> workers;
    workers.reserve(validation_threads);
    for(size_t i=0;i<validation_threads;++i)workers.emplace_back(worker);
    for(auto &t:workers)t.join();
    if(failure)rethrow_exception(failure);
  }
  profile("event_parse_parallel");
  const auto intern_start=chrono::steady_clock::now();
  // Preserve the baseline's single-threaded person/category interning order.
  for(auto &bucket:parsed)for(const ParsedEvent &p:bucket){
    auto [raw_it,raw_inserted]=raw_user_cache.try_emplace(p.user,0U);
    if(raw_inserted){
      auto id=identity_lookup.find(p.user);
      const string_view person_name=id==identity_lookup.end()?p.user:id->second;
      auto [person_it,inserted]=person_ids.try_emplace(person_name,static_cast<uint32_t>(people.size()));
      if(inserted)people.emplace_back(person_name);
      *raw_it=*person_it;
    }
    uint32_t cat_id=0;
    if(p.purchase){
      auto [cat_cached,cat_inserted]=product_category_cache.try_emplace(p.product,0U);
      if(cat_inserted){
        auto cat_it=product_lookup.find(p.product);
        static const string unknown="unknown";
        // Convert only a found category name when interning its numeric ID.
        cat_cached->second=category_number(cat_it==product_lookup.end()?unknown:string(cat_it->second));
      }
      cat_id=cat_cached->second;
    }
    events.push_back(Event{p.ts,p.cents,*raw_it,cat_id,p.purchase});
  }
  const auto intern_complete=chrono::steady_clock::now();
  vector<vector<ParsedEvent>>().swap(parsed);
  // Drop borrowed-key caches before the underlying mappings are released.
  raw_user_cache.clear();
  product_category_cache.clear();product_category_cache.rehash(0);
  const auto release_complete=chrono::steady_clock::now();
  if(const char *p=getenv("SHOPPING_PROFILE");p && *p=='1'){
    cerr<<"PROFILE native_event_intern_only_seconds="
        <<chrono::duration<double>(intern_complete-intern_start).count()<<"\n";
    cerr<<"PROFILE native_event_release_only_seconds="
        <<chrono::duration<double>(release_complete-intern_complete).count()<<"\n";
    cerr<<"PROFILE native_event_intern_and_release_seconds="
        <<chrono::duration<double>(release_complete-intern_start).count()<<"\n";
  }
  profile("event_validate");
  // Release the dedup payloads before sorting compact events.
  worker_buckets.clear();
  // V63: In direct-Parquet mode, skip lexicographic person sorting and the
  // 22.6M-row person-rank remap pass; keep canonical person IDs in interned
  // order. Preserve exact lexicographic ranking for legacy JSONL / argc==5.
  vector<string> sorted_people;
  if(direct_parquet){
    sorted_people=move(people);
  }else{
    vector<uint32_t> order(people.size());
    iota(order.begin(),order.end(),0U);
    sort(order.begin(),order.end(),[&](uint32_t a,uint32_t b){return people[a]<people[b];});
    vector<uint32_t> rank(people.size());
    sorted_people.resize(people.size());
    for(size_t i=0;i<order.size();++i){
      rank[order[i]]=static_cast<uint32_t>(i);
      sorted_people[i]=move(people[order[i]]);
    }
    for(auto &e:events)e.person=rank[e.person];
    vector<string>().swap(people);
  }
  person_ids.clear();

  // V63: Parallel lock-free counting-distribute by canonical person ID.
  // Per-person timestamp sorting is fused directly into sessionization below
  // so each person's event span is sorted in L1 cache right before consumption.
  const size_t num_people=sorted_people.size();
  vector<size_t> person_ends(num_people+1,0);
  if(!events.empty()){
    auto &ends=person_ends;
    const size_t max_hist_threads=num_people?max<size_t>(1,(32ULL*1024*1024)/num_people):1;
    const size_t dist_threads=min<size_t>(thread_count,max_hist_threads);
    if(dist_threads>1 && events.size()>=200000 && num_people>0){
      const size_t n=events.size();
      vector<vector<uint32_t>> local_counts(dist_threads,vector<uint32_t>(num_people,0));
      vector<thread> workers;
      workers.reserve(dist_threads);
      for(size_t t=0;t<dist_threads;++t){
        const size_t b=(n*t)/dist_threads, e=(n*(t+1))/dist_threads;
        workers.emplace_back([&,t,b,e](){
          auto &cnt=local_counts[t];
          for(size_t i=b;i<e;++i)++cnt[events[i].person];
        });
      }
      for(auto &w:workers)w.join();
      workers.clear();

      // Compute global person_ends and disjoint per-thread write offsets.
      vector<vector<size_t>> local_off(dist_threads,vector<size_t>(num_people,0));
      size_t running=0;
      for(size_t p=0;p<num_people;++p){
        ends[p]=running;
        for(size_t t=0;t<dist_threads;++t){
          local_off[t][p]=running;
          running+=local_counts[t][p];
        }
      }
      ends[num_people]=running;
      local_counts.clear();

      vector<Event> grouped(n);
      for(size_t t=0;t<dist_threads;++t){
        const size_t b=(n*t)/dist_threads, e=(n*(t+1))/dist_threads;
        workers.emplace_back([&,t,b,e](){
          auto &off=local_off[t];
          for(size_t i=b;i<e;++i){
            const Event &ev=events[i];
            grouped[off[ev.person]++]=ev;
          }
        });
      }
      for(auto &w:workers)w.join();
      events.swap(grouped);
    }else{
      for(const Event &e:events)++ends[static_cast<size_t>(e.person)+1];
      for(size_t i=1;i<ends.size();++i)ends[i]+=ends[i-1];
      vector<size_t> write_pos=ends;
      vector<Event> grouped(events.size());
      for(const Event &e:events)grouped[write_pos[e.person]++]=e;
      events.swap(grouped);
    }
  }
  profile("event_sort");
  ofstream so(argv[4],ios::binary);
  ofstream uo(argv[3],ios::binary);
  if(!so||!uo)throw runtime_error("output not writable");
  vector<native_campaign::CampaignSession> campaign_sessions;
  i64 session_count=0, daily_count=0;
  // V34: each worker owns whole canonical-person ranges. All output stays
  // ordered by lexicographic person via deterministic ordered concatenation.
  // Legacy argc==5 output retains its global session numbering unchanged.
  if(argc>=6 && thread_count>1 && sorted_people.size()>1000){
    struct Block {
      string users_json;
      vector<native_campaign::CampaignSession> sessions;
      i64 session_count=0, daily_count=0;
    };
    const size_t worker_count=min<size_t>(thread_count,sorted_people.size());
    vector<size_t> boundaries(worker_count+1,0);
    // Balance by *event count*, not by user count. Respect person boundaries.
    for(size_t w=1;w<worker_count;++w){
      size_t target=(events.size()*w)/worker_count;
      auto it=lower_bound(person_ends.begin(),person_ends.end(),target);
      boundaries[w]=static_cast<size_t>(it-person_ends.begin());
    }
    boundaries[worker_count]=sorted_people.size();
    vector<Block> blocks(worker_count);
    atomic<size_t> next_block{0};
    exception_ptr error;
    mutex error_lock;
    auto process_block=[&](size_t bi){
      Block &out=blocks[bi];
      ostringstream users;
      unique_ptr<native_parquet::UserDailyParquetWriter> parquet;
      if(direct_parquet)parquet=make_unique<native_parquet::UserDailyParquetWriter>(
        (fs::path(argv[6])/"user_daily"/("part-"+to_string(bi)+".parquet")).string());
      Session cur;
      Daily daily;
      bool have_session=false,have_daily=false;
      i64 prev=0;
      uint32_t last_person=UINT32_MAX;
      auto flush_daily=[&](){
        if(!have_daily)return;
        string best;bool has=false;i64 bestsum=0;
        for(const auto &kv:daily.categories)
          if(!has||kv.second>bestsum||(kv.second==bestsum&&kv.first<best)){
            best=kv.first;bestsum=kv.second;has=true;
          }
        if(direct_parquet){
          parquet->write_row(daily.person,native_parquet::parse_date_days(daily.day),
            daily.sessions,daily.events,daily.revenue,best,!has);
        }else{
          users<<"{\"user_id\":"<<json_escape(daily.person)<<",\"day\":"<<json_escape(daily.day)
            <<",\"sessions\":"<<daily.sessions<<",\"events\":"<<daily.events
            <<",\"revenue_cents\":"<<daily.revenue<<",\"top_category\":"
            <<(has?json_escape(best):"null")<<"}\n";
        }
        ++out.daily_count;
      };
      auto flush_session=[&](){
        if(!have_session)return;
        ++out.session_count;
        out.sessions.push_back(native_campaign::CampaignSession{cur.person,cur.day,cur.start,cur.revenue,cur.purchased});
        if(!have_daily||daily.person!=cur.person||daily.day!=cur.day){
          flush_daily();
          daily=Daily{cur.person,cur.day};
          have_daily=true;
        }
        daily.sessions++;daily.events+=cur.events;daily.revenue+=cur.revenue;
        for(const auto &c:cur.category_revenue)daily.categories[c.first]+=c.second;
      };
      const size_t p_from=boundaries[bi], p_to=boundaries[bi+1];
      const size_t first=person_ends[p_from], last=person_ends[p_to];
      out.sessions.reserve((last-first)/5);
      for(size_t p=p_from;p<p_to;++p){
        const size_t from=person_ends[p], to=person_ends[p+1];
        if(to-from>1)
          sort(events.begin()+from,events.begin()+to,
               [](const Event&a,const Event&b){return a.ts<b.ts;});
        for(size_t k=from;k<to;++k){
          const Event &e=events[k];
          if(!have_session||e.person!=last_person||e.ts-prev>GAP_MS){
            flush_session();
            cur=Session{sorted_people[e.person],day_string(e.ts),e.ts};
            have_session=true;
          }
          ++cur.events;
          if(e.purchase){
            cur.purchased=true;cur.revenue+=e.cents;
            cur.category_revenue[category_names[e.category]]+=e.cents;
          }
          prev=e.ts;last_person=e.person;
        }
      }
      flush_session();flush_daily();
      if(parquet)parquet->finish();
      out.users_json=users.str();
    };
    auto worker=[&](){
      try{
        for(;;){
          const size_t b=next_block.fetch_add(1,memory_order_relaxed);
          if(b>=worker_count)break;
          process_block(b);
        }
      }catch(...){
        lock_guard<mutex> guard(error_lock);
        if(!error)error=current_exception();
      }
    };
    vector<thread> workers;
    for(size_t i=0;i<worker_count;++i)workers.emplace_back(worker);
    for(auto &t:workers)t.join();
    if(error)rethrow_exception(error);
    size_t total_campaign_sessions=0;
    for(const auto &b:blocks)total_campaign_sessions+=b.sessions.size();
    campaign_sessions.reserve(total_campaign_sessions);
    for(auto &b:blocks){
      uo.write(b.users_json.data(),static_cast<streamsize>(b.users_json.size()));
      session_count+=b.session_count;
      daily_count+=b.daily_count;
      campaign_sessions.insert(campaign_sessions.end(),
        make_move_iterator(b.sessions.begin()),make_move_iterator(b.sessions.end()));
      b.users_json.clear();b.sessions.clear();
    }
  }else{

  // Stream finished sessions and user-day rows immediately instead of retaining
  // millions of session objects. Input is ordered by canonical person + time,
  // so days are monotone within a person.
  Session cur;
  unique_ptr<native_parquet::UserDailyParquetWriter> parquet;
  if(direct_parquet)parquet=make_unique<native_parquet::UserDailyParquetWriter>(
    (fs::path(argv[6])/"user_daily"/"part-0.parquet").string());
  if(argc>=6) campaign_sessions.reserve(events.size()/5);
  Daily daily;
  bool have_session=false, have_daily=false;
  i64 prev=0;
  uint32_t last_person=UINT32_MAX;
  auto flush_daily=[&](){
    if(!have_daily)return;
    string best;bool has=false;i64 bestsum=0;
    for(const auto &kv:daily.categories)
      if(!has||kv.second>bestsum||(kv.second==bestsum&&kv.first<best)){
        best=kv.first;bestsum=kv.second;has=true;
      }
    if(direct_parquet){
      parquet->write_row(daily.person,native_parquet::parse_date_days(daily.day),
        daily.sessions,daily.events,daily.revenue,best,!has);
    }else{
      uo<<"{\"user_id\":"<<json_escape(daily.person)<<",\"day\":"<<json_escape(daily.day)
        <<",\"sessions\":"<<daily.sessions<<",\"events\":"<<daily.events
        <<",\"revenue_cents\":"<<daily.revenue<<",\"top_category\":"
        <<(has?json_escape(best):"null")<<"}\n";
    }
    ++daily_count;
  };
  auto flush_session=[&](){
    if(!have_session)return;
    // Direct-native campaign mode receives sessions in memory.  No Python or
    // SQL consumer reads the session JSONL in that mode, so avoid formatting
    // and writing hundreds of thousands of redundant session records.
    // Preserve the four-argument legacy interface byte-for-byte.
    if(argc==5){
      so<<"{\"session_id\":"<<session_count<<",\"person_id\":"<<json_escape(cur.person)
        <<",\"session_start_ms\":"<<cur.start<<",\"day\":"<<json_escape(cur.day)
        <<",\"events\":"<<cur.events<<",\"revenue_cents\":"<<cur.revenue
        <<",\"has_purchase\":"<<(cur.purchased?"true":"false")<<"}\n";
    }
    ++session_count;
    if(argc>=6) campaign_sessions.push_back(native_campaign::CampaignSession{cur.person,cur.day,cur.start,cur.revenue,cur.purchased});
    if(!have_daily||daily.person!=cur.person||daily.day!=cur.day){
      flush_daily();
      daily=Daily{cur.person,cur.day};
      have_daily=true;
    }
    daily.sessions++;daily.events+=cur.events;daily.revenue+=cur.revenue;
    for(const auto &c:cur.category_revenue)daily.categories[c.first]+=c.second;
  };
  for(size_t p=0;p<sorted_people.size();++p){
    const size_t from=person_ends[p], to=person_ends[p+1];
    if(to-from>1)
      sort(events.begin()+from,events.begin()+to,
           [](const Event&a,const Event&b){return a.ts<b.ts;});
    for(size_t k=from;k<to;++k){
      const Event &e=events[k];
      if(!have_session||e.person!=last_person||e.ts-prev>GAP_MS){
        flush_session();
        cur=Session{sorted_people[e.person],day_string(e.ts),e.ts};
        have_session=true;
      }
      ++cur.events;
      if(e.purchase){
        cur.purchased=true;cur.revenue+=e.cents;
        cur.category_revenue[category_names[e.category]]+=e.cents;
      }
      prev=e.ts;last_person=e.person;
    }
  }
  flush_session();flush_daily();
  if(parquet)parquet->finish();

  }
  so.close();uo.close();
  profile("sessionize_and_json_output");
  if(argc>=6){
    auto rows=native_campaign::compute_campaign_daily_native(data.string(),ids,lo,hi,campaign_sessions);
    if(direct_parquet){
      native_parquet::CampaignDailyParquetWriter cw(
        (fs::path(argv[6])/"campaign_daily"/"part-0.parquet").string());
      for(const auto &r:rows)cw.write_row(r.campaign_id,r.channel,
        native_parquet::parse_date_days(r.day),r.sessions,r.purchasing_sessions,
        r.revenue_cents,r.assisted_sessions);
      cw.finish();
    }else{
      ofstream co(argv[5],ios::binary);
      if(!co)throw runtime_error("campaign output not writable");
      for(const auto &r:rows)
        co<<"{\"campaign_id\":"<<json_escape(r.campaign_id)
          <<",\"channel\":"<<json_escape(r.channel)
          <<",\"day\":"<<json_escape(r.day)
          <<",\"sessions\":"<<r.sessions
          <<",\"purchasing_sessions\":"<<r.purchasing_sessions
          <<",\"revenue_cents\":"<<r.revenue_cents
          <<",\"assisted_sessions\":"<<r.assisted_sessions<<"}\n";
      co.close();
    }
    profile("campaign_native");
  }
  cerr<<"native_events: clean="<<events.size()<<" sessions="<<session_count<<" user_days="<<daily_count<<"\n";
  return 0;
 }catch(const exception &e){cerr<<"native_events error: "<<e.what()<<"\n";return 1;}
}
