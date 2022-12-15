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
    string name;

    typedef function<json(MQTT2SQLite_stream_t &,const json &)> converter_func_t;
    typedef function<bool(MQTT2SQLite_stream_t &,const json &)> filter_func_t;

    filter_func_t    filter    = [](auto s,auto j){ return true; };
    converter_func_t converter = [](auto s,auto j){ return j; };

    string topic;
    json config;
    int valueTypeID = 0;

} MQTT2SQLite_stream_t;

bool filterByJSONP( MQTT2SQLite_stream_t &s,const json &j)
{
    json::json_pointer jp( s.config.value("jsonp","") ); 
    return j.contains(jp);
}

json generateFromJSONP( MQTT2SQLite_stream_t &s,const json &j)
{
    json::json_pointer jp( s.config.value("jsonp","") ); 
    json result{
        {"value",j[jp] }
    };
    return result;
}


map<string,MQTT2SQLite_stream_t::filter_func_t> filters
{
    {"EvasKontor.Temperature", filterByJSONP  }
};

map<string,MQTT2SQLite_stream_t::converter_func_t> converters
{
    {"EvasKontor.Temperature", generateFromJSONP  }
};

json g_config;
vector<MQTT2SQLite_stream_t> g_streams;

vector<MQTT2SQLite_stream_t>  makeStreams( json config )
{
    vector<MQTT2SQLite_stream_t> result;

    for( const auto &dev : config.value("devices",json::array_t{}) )
    {
        for( const auto &valt: dev.value("valueTypes",json::array_t{}) )
        {
            auto name = dev.value("name","xxx") + "." + valt.value("name","yyy");
            auto filter_it = filters.find(name);
            auto filter_func = (filter_it != filters.end()) ? filter_it->second : [](auto s, auto j){ return true; };

            auto converter_it = converters.find(name);
            auto converter_func = (converter_it != converters.end()) ? converter_it->second : [](auto s, auto j){ return true; };

            MQTT2SQLite_stream_t entry{ name, filter_func, converter_func, "topic", valt, 0 };
            result.push_back(entry  );
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
            json parsed = json::parse( line );
            for( auto &s : g_streams )
            {
                if( s.filter(s,parsed) )
                {
                    clog << "Filtered  :" << parsed << endl;
                    auto converted = s.converter(s,parsed);
                    clog << "Converted :" << converted << endl;
                }
            }
        }
    }
    return 0;
}
