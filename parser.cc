#include <iostream>     // cout, endl
#include <fstream>      // fstream
#include <vector>
#include <string>
#include <algorithm>    // copy
#include <iterator>     // ostream_operator
#include <boost/tokenizer.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <time.h>
#include "boost/date_time/gregorian/gregorian.hpp"
#include <omp.h>

using namespace std;
vector<string> headers;

int NUM_THREADS = 16;
string CSV_FILE = "./NYPD_Motor_Vehicle_Collisions.csv";

int colIndex(string col) {
    return find(headers.begin(), headers.end(), col) - headers.begin();
}

ostream& operator<<(ostream& os, const std::pair<std::string, int>& p)
{
    os << p.first << ' ' << p.second << ' ';
    return os;
}

class CarAccident {
public:
    string date;
    string time;
    string borough;
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

    int week_of_year;
} car_accident;

class Query2 {
    public:
        int total_accidents;
        int lethal_accidents;
        float lethal_percentage;
};

class Query3{
    public:
        std::string borough;
        int week;
        int total_accidents;
        int lethal_accidents;
        float lethal_average;

};

bool in_array(const std::string &value, const std::vector<string> &array) {
    return std::find(array.begin(), array.end(), value) != array.end();
}

std::map<int, int> evaluateQuery1(vector<CarAccident> car_accidents) {
    std::map<int, int> query_results;
    omp_set_num_threads(NUM_THREADS);
    int chunkSize = car_accidents.size()/omp_get_max_threads();
    #pragma omp for schedule(dynamic, chunkSize)
    for (long unsigned int i = 0; i < car_accidents.size(); i++) {
        if (query_results.count(car_accidents[i].week_of_year) == false) {
            query_results[car_accidents[i].week_of_year] = car_accidents[i].number_of_persons_killed;
        } else {
            query_results[car_accidents[i].week_of_year] += car_accidents[i].number_of_persons_killed;
        }
    }
    //#pragma omp barrier

    cout << "----- QUERY 1 -----" << endl;
    for (const auto&[k, v] : query_results)
        std::cout << "week[" << k << "] = total_lethal_accidents(" << v << ") " << std::endl;
    cout << "-------------------" << endl;
    return query_results;
}

std::map<string, Query2> evaluateQuery2(vector<CarAccident> car_accidents) {
    std::map<string, Query2> query_results;
    omp_set_num_threads(NUM_THREADS);
    int chunkSize = car_accidents.size()/omp_get_max_threads();
    #pragma omp for schedule(dynamic, chunkSize)
    for (long unsigned int i = 0; i < car_accidents.size(); i++) {
        vector<string> already_processed_factors;
        // CHECK FACTOR 1
        if (car_accidents[i].factor_1 != ""){
            already_processed_factors.push_back(car_accidents[i].factor_1);
            if (query_results.count(car_accidents[i].factor_1) == false) {
                query_results[car_accidents[i].factor_1].total_accidents = 1;
                query_results[car_accidents[i].factor_1].lethal_accidents = car_accidents[i].number_of_persons_killed > 0 ? 1 : 0;
                query_results[car_accidents[i].factor_1].lethal_percentage =
                        (float)query_results[car_accidents[i].factor_1].lethal_accidents /
                                (float)query_results[car_accidents[i].factor_1].total_accidents;
            } else {
                query_results[car_accidents[i].factor_1].total_accidents += 1;
                query_results[car_accidents[i].factor_1].lethal_accidents += car_accidents[i].number_of_persons_killed > 0 ? 1 : 0;
                query_results[car_accidents[i].factor_1].lethal_percentage =
                        (float)query_results[car_accidents[i].factor_1].lethal_accidents /
                                (float)query_results[car_accidents[i].factor_1].total_accidents;
            }
        }
        // CHECK FACTOR 2
        if (!in_array(car_accidents[i].factor_2, already_processed_factors) && car_accidents[i].factor_2 != "") {
            already_processed_factors.push_back(car_accidents[i].factor_2);
            if (query_results.count(car_accidents[i].factor_2) == false) {
                query_results[car_accidents[i].factor_2].total_accidents = 1;
                query_results[car_accidents[i].factor_2].lethal_accidents = car_accidents[i].number_of_persons_killed > 0 ? 1 : 0;
                query_results[car_accidents[i].factor_2].lethal_percentage =
                        (float)query_results[car_accidents[i].factor_2].lethal_accidents /
                                (float)query_results[car_accidents[i].factor_2].total_accidents;
            } else {
                query_results[car_accidents[i].factor_2].total_accidents += 1;
                query_results[car_accidents[i].factor_2].lethal_accidents += car_accidents[i].number_of_persons_killed > 0 ? 1 : 0;
                query_results[car_accidents[i].factor_2].lethal_percentage =
                        (float)query_results[car_accidents[i].factor_2].lethal_accidents /
                                (float)query_results[car_accidents[i].factor_2].total_accidents;
            }
        }
        // CHECK FACTOR 3
        if (!in_array(car_accidents[i].factor_3, already_processed_factors) && car_accidents[i].factor_3 != "") {
            already_processed_factors.push_back(car_accidents[i].factor_3);
            if (query_results.count(car_accidents[i].factor_3) == false) {
                query_results[car_accidents[i].factor_3].total_accidents = 1;
                query_results[car_accidents[i].factor_3].lethal_accidents = car_accidents[i].number_of_persons_killed > 0 ? 1 : 0;
                query_results[car_accidents[i].factor_3].lethal_percentage =
                        (float)query_results[car_accidents[i].factor_3].lethal_accidents /
                                (float)query_results[car_accidents[i].factor_3].total_accidents;
            } else {
                query_results[car_accidents[i].factor_3].total_accidents += 1;
                query_results[car_accidents[i].factor_3].lethal_accidents += car_accidents[i].number_of_persons_killed > 0 ? 1 : 0;
                query_results[car_accidents[i].factor_3].lethal_percentage =
                        (float)query_results[car_accidents[i].factor_3].lethal_accidents /
                                (float)query_results[car_accidents[i].factor_3].total_accidents;
            }
        }
        // CHECK FACTOR 4
        if (!in_array(car_accidents[i].factor_4, already_processed_factors) && car_accidents[i].factor_4 != "") {
            already_processed_factors.push_back(car_accidents[i].factor_4);
            if (query_results.count(car_accidents[i].factor_4) == false) {
                query_results[car_accidents[i].factor_4].total_accidents = 1;
                query_results[car_accidents[i].factor_4].lethal_accidents = car_accidents[i].number_of_persons_killed > 0 ? 1 : 0;
                query_results[car_accidents[i].factor_4].lethal_percentage =
                        (float)query_results[car_accidents[i].factor_4].lethal_accidents /
                                (float)query_results[car_accidents[i].factor_4].total_accidents;
            } else {
                query_results[car_accidents[i].factor_4].total_accidents += 1;
                query_results[car_accidents[i].factor_4].lethal_accidents += car_accidents[i].number_of_persons_killed > 0 ? 1 : 0;
                query_results[car_accidents[i].factor_4].lethal_percentage =
                        (float)query_results[car_accidents[i].factor_4].lethal_accidents /
                                (float)query_results[car_accidents[i].factor_4].total_accidents;
            }
        }
        // CHECK FACTOR 5
        if (!in_array(car_accidents[i].factor_5, already_processed_factors) && car_accidents[i].factor_5 != "") {
            already_processed_factors.push_back(car_accidents[i].factor_5);
            if (query_results.count(car_accidents[i].factor_5) == false) {
                query_results[car_accidents[i].factor_5].total_accidents = 1;
                query_results[car_accidents[i].factor_5].lethal_accidents = car_accidents[i].number_of_persons_killed > 0 ? 1 : 0;
                query_results[car_accidents[i].factor_5].lethal_percentage =
                        (float)query_results[car_accidents[i].factor_5].lethal_accidents /
                                (float)query_results[car_accidents[i].factor_5].total_accidents;
            } else {
                query_results[car_accidents[i].factor_5].total_accidents += 1;
                query_results[car_accidents[i].factor_5].lethal_accidents += car_accidents[i].number_of_persons_killed > 0 ? 1 : 0;
                query_results[car_accidents[i].factor_5].lethal_percentage =
                        (float)query_results[car_accidents[i].factor_5].lethal_accidents /
                                (float)query_results[car_accidents[i].factor_5].total_accidents;
            }
        }

    }
    cout << "----- QUERY 2 -----" << endl;
    for (const auto&[k, v] : query_results)
        std::cout << "factor[" << k << "] = total_accidents(" << v.total_accidents << ") | lethal_accidents(" << v.lethal_accidents << ") | lethal_percentage(" << v.lethal_percentage << ") "<< std::endl;
    cout << "-------------------" << endl;
    return query_results;
}

std::map<std::pair<std::string, int>, Query3> evaluateQuery3(vector<CarAccident> car_accidents){
    std::map<std::pair<std::string, int>, Query3> query_results;
    omp_set_num_threads(NUM_THREADS);
    int chunkSize = car_accidents.size()/omp_get_max_threads();
    #pragma omp for schedule(dynamic, chunkSize)
    for (long unsigned int i = 0; i < car_accidents.size(); i++) {
        std::vector<std::pair<std::string, int>> boroughs_weeks;
        if (car_accidents[i].borough != ""){
            std::pair<std::string, int> b_w = std::make_pair(car_accidents[i].borough, car_accidents[i].week_of_year);
            boroughs_weeks.push_back(b_w);
            if (query_results.count(b_w) == false) {
                query_results[b_w].total_accidents = 1;
                query_results[b_w].week = car_accidents[i].week_of_year;
                query_results[b_w].lethal_accidents = car_accidents[i].number_of_persons_killed > 0 ? 1 : 0;
                query_results[b_w].lethal_average =
                        (float)query_results[b_w].lethal_accidents /
                                (float)query_results[b_w].total_accidents;
            } else {
                query_results[b_w].total_accidents += 1;
                query_results[b_w].lethal_accidents += car_accidents[i].number_of_persons_killed > 0 ? 1 : 0;
                query_results[b_w].lethal_average =
                        (float)query_results[b_w].lethal_accidents /
                                (float)query_results[b_w].total_accidents;
            }
        }
    }
    cout << "----- QUERY 3 -----" << endl;
    for (const auto&[k, v] : query_results)
        std::cout << "Borough[" << k << "] = total_accidents(" << v.total_accidents << ") | lethal_accidents(" << v.lethal_accidents << ") | average_lethal(" << v.lethal_average << ") "<< std::endl;
    cout << "-------------------" << endl;
    return query_results;
}

int main() {
    using namespace boost;
    using namespace boost::gregorian;
    string data(CSV_FILE);

    ifstream in(data.c_str());
    if (!in.is_open()) return 1;

    typedef tokenizer <escaped_list_separator<char>> Tokenizer;
    vector<vector<string> > table;
    string line;

    while (getline(in, line)) {
        vector<string> vec;
        Tokenizer tok(line);
        vec.assign(tok.begin(), tok.end());
        if (headers.empty())
            headers = vec;
        else
            table.push_back(vec);
    }
    in.close();
    vector<vector<string> >::iterator it;

    vector<CarAccident> car_accidents;

    omp_set_num_threads(NUM_THREADS);
    int chunkSize = table.size()/omp_get_max_threads();
    #pragma omp for schedule(dynamic, chunkSize)
    for (long unsigned int i = 0; i < table.size(); i++) {
        CarAccident c;
        c.date = table[i][0];
        c.time = table[i][1];
        c.borough = table[i][2];
        c.zip_code = table[i][3];
        c.lat = table[i][4];
        c.lng = table[i][5];
        c.location = table[i][6];
        c.on_street_name = table[i][7];
        c.cross_street_name = table[i][8];
        c.off_street_name = table[i][9];

        c.number_of_persons_injured = std::atoi(table[i][10].c_str());
        c.number_of_persons_killed = std::atoi(table[i][11].c_str());
        c.number_of_pedestrians_injured = std::atoi(table[i][12].c_str());
        c.number_of_pedestrians_killed = std::atoi(table[i][13].c_str());
        c.number_of_cyclist_injured = std::atoi(table[i][14].c_str());
        c.number_of_cyclist_killed = std::atoi(table[i][15].c_str());
        c.number_of_motorist_injured = std::atoi(table[i][16].c_str());
        c.number_of_motorist_killed = std::atoi(table[i][17].c_str());
        c.factor_1 = table[i][18];
        c.factor_2 = table[i][19];
        c.factor_3 = table[i][20];
        c.factor_4 = table[i][21];
        c.factor_5 = table[i][22];
        c.unique_key = std::atoi(table[i][23].c_str());
        car_accidents.push_back(c);
    }
    cout << "END";
    #pragma omp for schedule(dynamic, chunkSize)
    for (long unsigned int i = 0; i < car_accidents.size(); i++) {
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
        boost::gregorian::date d{year, month, day};
        int week_of_year = d.week_number();
        car_accidents[i].week_of_year = week_of_year;
    }

    // ---- QUERY 1 ------
    std::map<int, int> query_1_results = evaluateQuery1(car_accidents);

    // ---- QUERY 2 ------
    std::map<string, Query2> query_2_results = evaluateQuery2(car_accidents);

    //---- QUERY 3 ------
    std::map<std::pair<std::string, int>, Query3> query_3_results = evaluateQuery3(car_accidents);
}



