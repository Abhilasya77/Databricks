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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "native_campaign.hpp"
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
  if(digits(s,0,s.size())){
    i64 acc=0;
    for(char c:s){int z=c-'0';if(acc>(INT64_MAX-z)/10)return false; acc=acc*10+z;}
    out=acc; return true;
  }
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
// Zero-allocation parser for event rows. Views remain valid while `line` lives.
// The challenge dialect has no escaped quotes and no quoted commas.
static bool parse_event_views(const string &line, array<string_view,7> &out){
  if(line.empty())return false;
  size_t begin=0;
  for(size_t i=0;i<7;++i){
    size_t end=line.find(',',begin);
    if(i<6 && end==string::npos)return false;
    if(i==6 && end!=string::npos)return false;
    if(end==string::npos)end=line.size();
    string_view value(line.data()+begin,end-begin);
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
struct Raw{string line; uint32_t ingest_pos=0, ingest_len=0;};
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
  if(argc!=5 && argc!=6){cerr<<"usage: native_events DATA_DIR ID_MAP_BIN USERS_JSONL SESSIONS_JSONL [CAMPAIGN_JSONL]\n";return 2;}
  fs::path data=argv[1];unordered_map<string,string> ids;
  read_map(argv[2],ids);
  ifstream mi(data/"manifest.json",ios::binary);
  if(!mi)throw runtime_error("manifest missing");
  string manifest((istreambuf_iterator<char>(mi)),istreambuf_iterator<char>());
  i64 lo,hi;if(!parse_ts(json_property(manifest,"window_start"),lo)||
              !parse_ts(json_property(manifest,"window_end"),hi))throw runtime_error("bad window");
  unordered_map<string,string> categories;
  each_line(data/"products.csv",2,[&](const vector<string>&v){categories.emplace(v[0],v[1]);});
  profile("metadata_and_identity");
  // Store one compact raw line per event_id instead of six separate strings.
  // Dedup still precedes validation (including late/invalid timestamps).
  // Independent byte ranges let many workers read even a single large CSV.
  // Lines are delimited ONLY by LF; each range owns lines whose first byte
  // falls inside [begin,end). The first range consumes the CSV header.
  struct Task { fs::path path; uint64_t begin, end; };
  constexpr uint64_t CHUNK_BYTES=8ULL*1024*1024;
  vector<Task> tasks;
  uint64_t estimated_bytes=0;
  for(const auto &p:parts(data/"events")){
    const uint64_t len=fs::file_size(p);
    estimated_bytes+=len;
    if(!len)continue;
    for(uint64_t begin=0;begin<len;begin+=CHUNK_BYTES)
      tasks.push_back(Task{p,begin,min(len,begin+CHUNK_BYTES)});
  }
  constexpr size_t SHARD_COUNT=128;
  struct Shard { mutex lock; unordered_map<string,Raw> rows; };
  array<Shard,SHARD_COUNT> shards;
  auto read_task=[&](const Task &task){
    ifstream in(task.path,ios::binary);
    if(!in)throw runtime_error("can't open "+task.path.string());
    string line;
    if(task.begin==0){
      // Every input file has one header, even when empty after it.
      if(!getline(in,line))return;
      uint64_t offset=line.size()+1;
      while(offset<task.end && getline(in,line)){
        const uint64_t length=line.size()+1;
        array<string_view,7> fields;
        if(parse_event_views(line,fields)&&!fields[0].empty()){
          const uint32_t ip=static_cast<uint32_t>(fields[3].data()-line.data());
          const uint32_t il=static_cast<uint32_t>(fields[3].size());
          Shard &shard=shards[hash<string_view>{}(fields[0])&(SHARD_COUNT-1)];
          lock_guard<mutex> guard(shard.lock);
          auto [it,inserted]=shard.rows.try_emplace(string(fields[0]));
          if(inserted)it->second=Raw{line,ip,il};
          else {
            Raw &prev=it->second;
            if(fields[3]<string_view(prev.line.data()+prev.ingest_pos,prev.ingest_len))
              prev=Raw{line,ip,il};
          }
        }
        offset+=length;
      }
    }else{
      // Seek one byte before the split and discard the remainder of that line.
      // This also handles splits exactly on a newline without losing a record.
      in.seekg(static_cast<streamoff>(task.begin-1));
      if(!getline(in,line))return;
      uint64_t offset=task.begin-1+line.size()+1;
      while(offset<task.end && getline(in,line)){
        const uint64_t length=line.size()+1;
        array<string_view,7> fields;
        if(parse_event_views(line,fields)&&!fields[0].empty()){
          const uint32_t ip=static_cast<uint32_t>(fields[3].data()-line.data());
          const uint32_t il=static_cast<uint32_t>(fields[3].size());
          Shard &shard=shards[hash<string_view>{}(fields[0])&(SHARD_COUNT-1)];
          lock_guard<mutex> guard(shard.lock);
          auto [it,inserted]=shard.rows.try_emplace(string(fields[0]));
          if(inserted)it->second=Raw{line,ip,il};
          else {
            Raw &prev=it->second;
            if(fields[3]<string_view(prev.line.data()+prev.ingest_pos,prev.ingest_len))
              prev=Raw{line,ip,il};
          }
        }
        offset+=length;
      }
    }
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
  thread_count=min(thread_count,tasks.size());
  if(thread_count==1){
    for(const auto &task:tasks)read_task(task);
  }else if(thread_count>1){
    atomic<size_t> next{0};
    exception_ptr failure;
    mutex failure_lock;
    auto worker=[&](){
      try{
        for(;;){
          size_t idx=next.fetch_add(1,memory_order_relaxed);
          if(idx>=tasks.size())break;
          read_task(tasks[idx]);
        }
      }catch(...){
        lock_guard<mutex> guard(failure_lock);
        if(!failure)failure=current_exception();
      }
    };
    vector<thread> workers;
    workers.reserve(thread_count);
    for(size_t i=0;i<thread_count;++i)workers.emplace_back(worker);
    for(auto &worker_thread:workers)worker_thread.join();
    if(failure)rethrow_exception(failure);
  }
  profile("event_dedup");
  size_t n_raw=0;
  for(const auto &shard:shards)n_raw+=shard.rows.size();
  vector<Event> events;events.reserve(n_raw);
  // Intern the canonical person and purchase category once. Events now store
  // 32-byte POD records instead of two separately allocated strings.
  unordered_map<string,uint32_t> person_ids;
  vector<string> people;
  unordered_map<string,uint32_t> category_ids;
  vector<string> category_names;
  auto category_number=[&](const string &name)->uint32_t{
    auto [it,inserted]=category_ids.try_emplace(name,static_cast<uint32_t>(category_names.size()));
    if(inserted)category_names.push_back(name);
    return it->second;
  };
  array<string_view,7> columns;
  for(const auto &shard:shards)for(const auto &entry:shard.rows){
    const Raw &r=entry.second;
    // Every stored line was already checked as syntactically valid CSV.
    if(!parse_event_views(r.line,columns))throw runtime_error("internal CSV invariant");
    const string_view user=columns[1], tsstr=columns[2],
                      event_type=columns[4], product=columns[5], price=columns[6];
    if(user.empty()||product.empty()||(event_type!="view"&&event_type!="cart"&&event_type!="purchase"))continue;
    i64 ts,ingest;
    if(!parse_ts(tsstr,ts)||ts<lo||ts>=hi||!parse_ts(columns[3],ingest)||ingest-ts>DAY_MS)continue;
    i64 cents=0;
    const bool purchase=event_type=="purchase";
    if(purchase){
      if(!digits(price,0,price.size()))continue;
      for(char c:price){int x=c-'0';if(cents>10000000/10) {cents=10000001;break;}cents=cents*10+x;if(cents>10000000)break;}
      if(cents<1||cents>10000000)continue;
    }
    auto id=ids.find(string(user));
    string person_name=id==ids.end()?string(user):id->second;
    auto [person_it, inserted]=person_ids.try_emplace(person_name,static_cast<uint32_t>(people.size()));
    if(inserted)people.push_back(std::move(person_name));
    uint32_t cat_id=0;
    if(purchase){
      auto cat_it=categories.find(string(product));
      static const string unknown="unknown";
      cat_id=category_number(cat_it==categories.end()?unknown:cat_it->second);
    }
    events.push_back(Event{ts,cents,person_it->second,cat_id,purchase});
  }
  profile("event_validate");
  // Release the dedup payloads before sorting compact events.
  for(auto &shard:shards){shard.rows.clear();shard.rows.rehash(0);}
  // Assign each person a lexicographic rank so the global sort compares
  // integers, not UTF-8 strings, for every event comparison.
  vector<uint32_t> order(people.size());
  iota(order.begin(),order.end(),0U);
  sort(order.begin(),order.end(),[&](uint32_t a,uint32_t b){return people[a]<people[b];});
  vector<uint32_t> rank(people.size());
  vector<string> sorted_people(people.size());
  for(size_t i=0;i<order.size();++i){
    rank[order[i]]=static_cast<uint32_t>(i);
    sorted_people[i]=move(people[order[i]]);
  }
  for(auto &e:events)e.person=rank[e.person];
  vector<string>().swap(people);
  unordered_map<string,uint32_t>().swap(person_ids);
  // Counting-distribute by canonical person (already assigned lexicographic
  // ranks), then sort timestamps only inside each person's contiguous span.
  // This avoids millions of cross-person comparisons in the global sort.
  if(!events.empty()){
    vector<size_t> ends(sorted_people.size()+1,0);
    for(const Event &e:events)++ends[static_cast<size_t>(e.person)+1];
    for(size_t i=1;i<ends.size();++i)ends[i]+=ends[i-1];
    vector<size_t> write_pos=ends;
    vector<Event> grouped(events.size());
    for(const Event &e:events)grouped[write_pos[e.person]++]=e;
    events.swap(grouped);
    for(size_t i=0;i<sorted_people.size();++i){
      const size_t from=ends[i], to=ends[i+1];
      if(to-from>1)
        sort(events.begin()+from,events.begin()+to,
             [](const Event&a,const Event&b){return a.ts<b.ts;});
    }
  }
  profile("event_sort");
  ofstream so(argv[4],ios::binary);
  ofstream uo(argv[3],ios::binary);
  if(!so||!uo)throw runtime_error("output not writable");
  // Stream finished sessions and user-day rows immediately instead of retaining
  // millions of session objects. Input is ordered by canonical person + time,
  // so days are monotone within a person.
  Session cur;
  vector<native_campaign::CampaignSession> campaign_sessions;
  if(argc==6) campaign_sessions.reserve(events.size()/5);
  Daily daily;
  bool have_session=false, have_daily=false;
  i64 prev=0, session_count=0, daily_count=0;
  uint32_t last_person=UINT32_MAX;
  auto flush_daily=[&](){
    if(!have_daily)return;
    string best;bool has=false;i64 bestsum=0;
    for(const auto &kv:daily.categories)
      if(!has||kv.second>bestsum||(kv.second==bestsum&&kv.first<best)){
        best=kv.first;bestsum=kv.second;has=true;
      }
    uo<<"{\"user_id\":"<<json_escape(daily.person)<<",\"day\":"<<json_escape(daily.day)
      <<",\"sessions\":"<<daily.sessions<<",\"events\":"<<daily.events
      <<",\"revenue_cents\":"<<daily.revenue<<",\"top_category\":"
      <<(has?json_escape(best):"null")<<"}\n";
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
    if(argc==6) campaign_sessions.push_back(native_campaign::CampaignSession{cur.person,cur.day,cur.start,cur.revenue,cur.purchased});
    if(!have_daily||daily.person!=cur.person||daily.day!=cur.day){
      flush_daily();
      daily=Daily{cur.person,cur.day};
      have_daily=true;
    }
    daily.sessions++;daily.events+=cur.events;daily.revenue+=cur.revenue;
    for(const auto &c:cur.category_revenue)daily.categories[c.first]+=c.second;
  };
  for(const Event &e:events){
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
  flush_session();flush_daily();
  so.close();uo.close();
  profile("sessionize_and_json_output");
  if(argc==6){
    auto rows=native_campaign::compute_campaign_daily_native(data.string(),ids,lo,hi,campaign_sessions);
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
    profile("campaign_native");
  }
  cerr<<"native_events: clean="<<events.size()<<" sessions="<<session_count<<" user_days="<<daily_count<<"\n";
  return 0;
 }catch(const exception &e){cerr<<"native_events error: "<<e.what()<<"\n";return 1;}
}
