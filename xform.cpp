#include <iostream>
#include <string>
#include <chrono>
#include <fstream>
#include <functional>
#include "json/single_include/nlohmann/json.hpp"

using namespace std;
using namespace nlohmann;

typedef struct MQTT2SQLite_Source_t
{
    json config;

    MQTT2SQLite_Source_t(const json &c) {

        clog << "MQTT2SQLite_Source_t() enter: " << c.dump() << endl;
        config      = c;
    };

    bool   mqttFilter(  string line ) { 
        string filter = config.value( "mqttFilter","" );
        return filter == "" || line.find( filter ) != string::npos;
    };

    string mqttReplace( string line ) { 
        clog << "mqttReplace A: " << line << endl;
        json replJson = config["mqttReplace"];
        for( const auto &r : replJson )
        {
            if( r.contains("from") && r.contains("to") )
            {
                string from = r["from"], to = r["to"];
                size_t pos = 0;
                do {
                    pos = line.find(from, pos);
                    if( pos != string::npos )
                    {
                        line.replace( pos, from.size(), to );
                        pos += to.size();
                    }
                }
                while( pos != string::npos );
            }
        }
        clog << "mqttReplace Z: " << line << endl;

        return line; 
    };

    bool mqttMatchTopic( json j )
    {
        return config["mqttSample"]["topic"] == j["topic"];
    };

    json columnOutput( json j ) { return json{}; };

} MQTT2SQLite_Source_t;

json g_config;
vector<MQTT2SQLite_Source_t> g_sources;

vector<MQTT2SQLite_Source_t>  makeStreams()
{
    vector<MQTT2SQLite_Source_t> result;

    for( auto& src : g_config )
    {
        MQTT2SQLite_Source_t entry(src);

        result.push_back(entry);
    }
    return result;
}

void processInputLine( string line )
{
    if( line.size() > 0 )
    {
        try {
            clog << endl;
            clog << "Input: " << line << endl;
            for( auto &s : g_sources )
            {
                auto replaced = s.mqttReplace( line );
                if( s.mqttFilter(replaced) )
                {
                    json parsed = json::parse( replaced );
                    if( s.mqttMatchTopic(parsed))
                    {
                        clog << "Matched  :" << replaced << endl;
                    }
                }
            }
        }
        catch (json::exception e) {
            clog << "Exception: " << e.what() << endl;
        }
        catch (std::exception e) {
            clog << "Exception: " << e.what() << endl;
        }
    }
}

void runSampleTests()
{
    for( auto &s : g_sources )
    {
        clog << "runSampleTests(): " << s.config.dump() << endl;
        string sample = s.config["mqttSample"].dump();
        clog << "runSampleTests(): " << sample << endl;
        processInputLine( sample );
    }
}

int main( int argc, char * argv[] )
{
	g_config = json::parse( ifstream("./xform1.json" ) );
    clog << "Config: " << g_config << endl;
    g_sources = makeStreams();

    runSampleTests();

    while( cin )
    {
        string line;
        getline( cin,  line );
        processInputLine( line );
    }
    return 0;
}
