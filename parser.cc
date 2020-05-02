#include <iostream>     // cout, endl
#include <fstream>      // fstream
#include <vector>
#include <string>
#include <algorithm>    // copy
#include <iterator>     // ostream_operator
#include <boost/tokenizer.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <time.h>

using namespace std;
vector<string> headers;

int colIndex(string col) {
    return find(headers.begin(), headers.end(), col) - headers.begin();
}

class CarAccident {
    public:
        string date;
        string time;
        string zip_code;
        string lat;
        string lng;
        string location;
        string on_street_name;
        string cross_street_name;
        string off_street_name;
        int number_of_persons_injured;
        int number_of_persons_killed;
        int number_of_pedestrians_injured;
        int number_of_pedestrians_killed;
        int number_of_cyclist_injured;
        int number_of_cyclist_killed;
        int number_of_motorist_injured;
        int number_of_motorist_killed;
        string factor_1;
        string factor_2;
        string factor_3;
        string factor_4;
        string factor_5;
        int unique_key;

        int week_of_year
} car_accident;

class Query1 {
    int week_of_year;
    int lethal_accidents_num;
};

int main() {
    using namespace boost;
    using namespace boost::gregorian;
    string data("./sample.csv");

    ifstream in(data.c_str());
    if (!in.is_open()) return 1;

    typedef tokenizer <escaped_list_separator<char>> Tokenizer;
    vector<vector<string> > table;
    string line;

    while (getline(in, line)) {
        vector<string> vec;
        Tokenizer tok(line);
        vec.assign(tok.begin(), tok.end());

        // vector now contains strings from one row, output to cout here
        // copy(vec.begin(), vec.end(), ostream_iterator<string>(cout, "|"));

        // cout << "\n----------------------" << endl;
        if (headers.empty())
            headers = vec;
        else
            table.push_back(vec);
    }
    in.close();
    vector<vector<string> >::iterator it;
    vector<string>::iterator i;

    vector<CarAccident> car_accidents;
    for (long unsigned int j = 0; j < table.size(); j++) {
        // cout << (*(*it).begin()) << endl;
        for (long unsigned int i = 0; i < table[j].size(); i++){
            CarAccident c;
            c.date = table[j][0];
            c.time = table[j][1];
            c.zip_code = table[j][2];
            c.lat = table[j][3];
            c.lng = table[j][4];
            c.location = table[j][5];
            c.on_street_name = table[j][6];
            c.cross_street_name = table[j][7];
            c.off_street_name = table[j][8];
            c.number_of_persons_injured = std::atoi(table[j][9].c_str());
            c.number_of_persons_killed = std::atoi(table[j][10].c_str());
            c.number_of_pedestrians_injured = std::atoi(table[j][11].c_str());
            c.number_of_pedestrians_killed = std::atoi(table[j][12].c_str());
            c.number_of_cyclist_injured = std::atoi(table[j][13].c_str());
            c.number_of_cyclist_killed = std::atoi(table[j][14].c_str());
            c.number_of_motorist_injured = std::atoi(table[j][15].c_str());
            c.number_of_motorist_killed = std::atoi(table[j][16].c_str());
            c.factor_1 = table[j][17];
            c.factor_2 = table[j][18];
            c.factor_3 = table[j][19];
            c.factor_4 = table[j][20];
            c.factor_5 = table[j][21];
            c.unique_key = std::atoi(table[j][22].c_str());
            car_accidents.push_back(c);
        }
        // cout << "\n----------------------" << endl;
    }

    vector<Query1> query_1_results;

    for (long unsigned int i = 0; i < car_accidents.size(); i++){
        cout << car_accidents[i].date << endl;
        string formatted_date;
        std::string delimiter = "/";
        size_t pos = 0;
        std::string token;
        vector<string> split;
        std::string s = car_accidents[i].date + "/";
        while ((pos = s.find(delimiter)) != std::string::npos) {
            token = s.substr(0, pos);
            split.push_back(token);
            s.erase(0, pos + delimiter.length());
        }
        short unsigned int year = std::atoi(split[2].c_str());
        short unsigned int month = std::atoi(split[0].c_str());
        short unsigned int day = std::atoi(split[1].c_str());
        boost::gregorian::date d {year, month, day};
        int week_of_year = d.week_number();
        car_accidents[i].week_of_year = week_of_year;
        cout << week_of_year << endl;
        cout << "\n----------------------" << endl;
    }
}

