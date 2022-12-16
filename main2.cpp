#include <iostream>
#include <string>
#include <chrono>
#include <fstream>
#include <functional>
#include "nlohmann/json.hpp"
#include "tiny-process-library/process.hpp"

using namespace std;
using namespace nlohmann;
using namespace TinyProcessLib;

void pruneJsonNulls( json &obj )
{
//  clog << "pruneJsonNulls: " << obj.dump() << endl;
    for( auto& v : obj )
        if( v.is_null()  )
            v = "";
//  clog << "pruneJsonNulls: " << obj.dump() << endl;
}

string g_c;

typedef struct MQTT2SQLite_stream_t
{
    json config;
    string name, topic;

    typedef function<bool(MQTT2SQLite_stream_t &,const json &)> filter_func_t;
    typedef function<json(MQTT2SQLite_stream_t &,const json &)> converter_func_t;

    filter_func_t    filter    = [](auto s,auto j){ return false; };
    converter_func_t converter = [](auto s,auto j){ return j; };

    json::json_pointer matchKey, valueKey;
    json matchValue;
    int valueTypeID = -1;

    MQTT2SQLite_stream_t(const json &deviceConfig, const json& valTypeConfig) {

    //  clog << "MQTT2SQLite_stream_t() d-enter: " << deviceConfig.dump() << endl;
    //  clog << "MQTT2SQLite_stream_t() v-enter: " << valTypeConfig.dump() << endl;

        config      = valTypeConfig;
        name        = deviceConfig.value("name","xxx"); + "." + valTypeConfig.value("name","yyy");
        topic       = deviceConfig.value("topic","" );
        matchKey    = json::json_pointer( valTypeConfig.value("matchKey","" ) );
        matchValue  = valTypeConfig.value("matchValue", json{} );
        valueKey    = json::json_pointer( valTypeConfig.value("valueKey","" ) );
        valueTypeID = valTypeConfig.value("ID", -2 );

    //  clog << "MQTT2SQLite_stream_t() exit: " << name << endl;
    };

} MQTT2SQLite_stream_t;

bool filterByJSONP( MQTT2SQLite_stream_t &s,const json &j)
{
//  std::cerr << "filterByJSONP: " << s.name  << ": " << j << endl;
    bool b1 = j.value("topic","zzz") == s.topic;
    bool b2 = j.contains(s.valueKey);
    bool b3 = ( s.matchKey == json::json_pointer("") || j.contains(s.matchKey) && j[s.matchKey] == s.matchValue);
//  std::cerr << "filterByJSONP: " << b1  << ", " << b2  << ", " << b3  << ", " << endl;
    return b1 && b2 && b3;
}

json generateFromJSONP( MQTT2SQLite_stream_t &s,const json &j)
{
    return json{
        {"value",    j[s.valueKey]  },
        {"timeStamp", j.value("tst","2000-01-01T00:00:00.000000Z+0100") },
        {"valueTypeID", s.valueTypeID }
    };
}


map<string,MQTT2SQLite_stream_t::filter_func_t> filters
{
};

map<string,MQTT2SQLite_stream_t::converter_func_t> converters
{
};

json g_config;
vector<MQTT2SQLite_stream_t> g_streams;

vector<MQTT2SQLite_stream_t>  makeStreams( json config )
{
    vector<MQTT2SQLite_stream_t> result;

    json devicesConfig;
    Process process1a("sqlite3 devices.sqlite '.mode json' 'select * from Devices'", "", [&devicesConfig](const char *bytes, size_t n) {
    //  clog << "Output from sqlite3 stdout: " << string(bytes, n) << endl;
        devicesConfig = json::parse( string(bytes, n) );
    //  clog << "devicesConfig: " << devicesConfig.dump() << endl;
    });
    process1a.get_exit_status();

    json valueTypesConfig;
    Process process1b("sqlite3 devices.sqlite '.mode json' 'select * from ValueTypes'", "", [&valueTypesConfig](const char *bytes, size_t n) {
    //  clog << "Output from sqlite3 stdout: " << string(bytes, n) << endl;
        valueTypesConfig = json::parse( string(bytes, n) );
    //  clog << "valueTypesConfig: " << valueTypesConfig.dump() << endl;
    });
    process1b.get_exit_status();

//  clog << "P1: " << endl;
    for( auto& dev : devicesConfig )
    {
        pruneJsonNulls( dev );
    //  clog << "P2: " << dev.dump() << endl;
        for( auto& valt: valueTypesConfig )
        {
            if( dev.value("ID",-1) == valt.value("DeviceID",-2) )
            {
            //  clog << "P3: " << valt.dump() << endl;
                pruneJsonNulls( valt );
            //  clog << "P4: " << valt.dump() << endl;

            //  clog.flush();
                MQTT2SQLite_stream_t entry(dev,valt);

                auto filter_it = filters.find(entry.name);
                auto filter_func = (filter_it != filters.end()) ? filter_it->second : filterByJSONP;
                entry.filter = filter_func;

                auto converter_it = converters.find(entry.name);
                auto converter_func = (converter_it != converters.end()) ? converter_it->second : generateFromJSONP;
                entry.converter = converter_func;

                result.push_back(entry);
            }
        } 
    }
    return result;
}


int main( int argc, char * argv[] )
{
	g_config = json::parse( ifstream("./config.json", ios::in ) );
    g_streams = makeStreams( g_config );

    while( cin )
    {
        string line;
        getline( cin,  line );
        if( line.size() > 0 )
        {
            clog << "Input     :" << line << endl;
            try {
                json parsed = json::parse( line );
                for( auto &s : g_streams )
                {
                    if( s.filter(s,parsed) )
                    {
                        clog << "Filtered  :" << parsed << endl;
                        auto converted = s.converter(s,parsed);
                        clog << "Converted :" << converted << endl;
                        cout << converted << endl;
                    }
                }
            }
            catch (std::exception e) {
                clog << "Exception: " << e.what() << endl;
            }
        }
    }
    return 0;
}
