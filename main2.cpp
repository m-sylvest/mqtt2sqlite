#include <iostream>
#include <string>
#include <chrono>
#include <fstream>
#include <functional>
#include "nlohmann/json.hpp"

using namespace std;
using namespace nlohmann;

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
        config      = valTypeConfig;
        name        = deviceConfig.value("name","xxx"); + "." + valTypeConfig.value("name","yyy");
        topic       = deviceConfig.value("topic","" );
        matchKey    = json::json_pointer( valTypeConfig.value("matchKey","" ) );
        matchValue  = valTypeConfig.value("matchValue", "" );
        valueKey    = json::json_pointer( valTypeConfig.value("valueKey","" ) );
        valueTypeID = valTypeConfig.value("ID", -2 );

        clog << "MQTT2SQLite_stream_t(): " << name << endl;
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

    for( auto dev : config.value("devices",json::array_t{}) )
    {
        for( auto valt: dev.value("valueTypes",json::array_t{}) )
        {
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
