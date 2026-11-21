// Shopping Sessions: native event cleaning, deduplication, sessionization,
// and user_daily aggregation. Strictly follows the challenge CSV dialect.
// Compile: c++ -std=c++17 -O3 -DNDEBUG native_events.cpp -o native_events
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
using namespace std;
namespace fs = std::filesystem;
using i64 = int64_t;
static constexpr i64 DAY_MS=86400000, GAP_MS=1800000;

static bool digits(const string &s, size_t a, size_t b){
  if(a>=b || b>s.size())return false;
  for(size_t i=a;i<b;i++)if(s[i]<'0'||s[i]>'9')return false;
  return true;
}
static int number(const string &s,size_t a,size_t b){int n=0; for(size_t i=a;i<b;i++)n=n*10+(s[i]-'0'); return n;}
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
static bool parse_ts(const string &s,i64 &out){
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
struct Raw{string line, ingest;};
struct Event{string person,product; i64 ts=0, cents=0; bool purchase=false;};
struct Session{
  string person, day; i64 start=0,events=0,revenue=0;
  bool purchased=false;
  unordered_map<string,i64> category_revenue;
};
struct Daily{string person,day; i64 sessions=0,events=0,revenue=0;unordered_map<string,i64> categories;};
int main(int argc,char**argv){
 try{
  if(argc!=5){cerr<<"usage: native_events DATA_DIR ID_MAP_BIN USERS_JSONL SESSIONS_JSONL\n";return 2;}
  fs::path data=argv[1];unordered_map<string,string> ids;
  read_map(argv[2],ids);
  ifstream mi(data/"manifest.json",ios::binary);
  if(!mi)throw runtime_error("manifest missing");
  string manifest((istreambuf_iterator<char>(mi)),istreambuf_iterator<char>());
  i64 lo,hi;if(!parse_ts(json_property(manifest,"window_start"),lo)||
              !parse_ts(json_property(manifest,"window_end"),hi))throw runtime_error("bad window");
  unordered_map<string,string> categories;
  each_line(data/"products.csv",2,[&](const vector<string>&v){categories.emplace(v[0],v[1]);});
  // Store one compact raw line per event_id instead of six separate strings.
  // Dedup still precedes validation (including late/invalid timestamps).
  unordered_map<string,Raw> raw;
  for(const auto &p:parts(data/"events")){
    ifstream in(p,ios::binary);
    if(!in)throw runtime_error("can't open "+p.string());
    string line;getline(in,line);
    vector<string> fields;
    while(getline(in,line)){
      if(!parse_line(line,7,fields)||fields[0].empty())continue;
      auto it=raw.find(fields[0]);
      if(it==raw.end())raw.emplace(fields[0],Raw{line,fields[3]});
      else if(fields[3]<it->second.ingest)it->second=Raw{line,fields[3]};
    }
  }
  vector<Event> events;events.reserve(raw.size());
  vector<string> columns;
  for(const auto &entry:raw){
    const Raw &r=entry.second;
    // Every stored line was already checked as syntactically valid CSV.
    if(!parse_line(r.line,7,columns))throw runtime_error("internal CSV invariant");
    const string &user=columns[1], &tsstr=columns[2],
                 &event_type=columns[4], &product=columns[5], &price=columns[6];
    if(user.empty()||product.empty()||(event_type!="view"&&event_type!="cart"&&event_type!="purchase"))continue;
    i64 ts,ingest;
    if(!parse_ts(tsstr,ts)||ts<lo||ts>=hi||!parse_ts(r.ingest,ingest)||ingest-ts>DAY_MS)continue;
    i64 cents=0;
    const bool purchase=event_type=="purchase";
    if(purchase){
      if(!digits(price,0,price.size()))continue;
      for(char c:price){int x=c-'0';if(cents>10000000/10) {cents=10000001;break;}cents=cents*10+x;if(cents>10000000)break;}
      if(cents<1||cents>10000000)continue;
    }
    auto id=ids.find(user);
    events.push_back(Event{id==ids.end()?user:id->second,product,ts,cents,purchase});
  }
  raw.clear();raw.rehash(0);
  sort(events.begin(),events.end(),[](const Event&a,const Event&b){return a.person==b.person?a.ts<b.ts:a.person<b.person;});
  ofstream so(argv[4],ios::binary);
  ofstream uo(argv[3],ios::binary);
  if(!so||!uo)throw runtime_error("output not writable");
  // Stream finished sessions and user-day rows immediately instead of retaining
  // millions of session objects. Input is ordered by canonical person + time,
  // so days are monotone within a person.
  Session cur;
  Daily daily;
  bool have_session=false, have_daily=false;
  i64 prev=0, session_count=0, daily_count=0;
  string last_person;
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
    so<<"{\"session_id\":"<<session_count<<",\"person_id\":"<<json_escape(cur.person)
      <<",\"session_start_ms\":"<<cur.start<<",\"day\":"<<json_escape(cur.day)
      <<",\"events\":"<<cur.events<<",\"revenue_cents\":"<<cur.revenue
      <<",\"has_purchase\":"<<(cur.purchased?"true":"false")<<"}\n";
    ++session_count;
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
      cur=Session{e.person,day_string(e.ts),e.ts};
      have_session=true;
    }
    ++cur.events;
    if(e.purchase){
      cur.purchased=true;cur.revenue+=e.cents;
      auto it=categories.find(e.product);
      const string &cat=it==categories.end()?static_cast<const string&>("unknown"):it->second;
      cur.category_revenue[cat]+=e.cents;
    }
    prev=e.ts;last_person=e.person;
  }
  flush_session();flush_daily();
  so.close();uo.close();
  cerr<<"native_events: clean="<<events.size()<<" sessions="<<session_count<<" user_days="<<daily_count<<"\n";
  return 0;
 }catch(const exception &e){cerr<<"native_events error: "<<e.what()<<"\n";return 1;}
}
