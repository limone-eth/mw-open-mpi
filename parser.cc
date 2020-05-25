#include <iostream>     // cout, endl
#include <fstream>      // fstream
#include <vector>
#include <string>
#include <time.h>
#include <omp.h>
#include <chrono>
#include <CarAccident.cpp>
#include <Query.cpp>
#include <Utils.cpp>

using namespace std;
vector<string> headers;

int NUM_THREADS = 16;
string CSV_FILE = "./files/NYPD_Motor_Vehicle_Collisions.csv";


int main() {
    using namespace boost::gregorian;
    using namespace std;
    using namespace std::chrono;

    string data(CSV_FILE);

    vector<CarAccident> car_accidents;

    ifstream in(data.c_str());
    if (!in.is_open()) return 1;

    vector< vector<string>> table;

    auto start_tokenizer = high_resolution_clock::now();
    bool stop = false;
    omp_set_num_threads(NUM_THREADS);
    #pragma omp parallel shared(in, stop)
    {
    string line;
    while (!stop) {
        #pragma omp critical (getline)
        {
            if(!getline(in, line))
                stop = true;
        }
        if (stop)
            break;
        vector<string> vec;
        std::string token;
        std::istringstream tokenStream(line);

        while (std::getline(tokenStream, token, ','))
            vec.push_back(token);

        if (headers.empty())
            headers = vec;
        else {
            CarAccident c;
            c.date = vec[0];
            c.borough = vec[2];
            c.factor_1 = vec[18];
            c.factor_2 = vec[19];
            c.factor_3 = vec[20];
            c.factor_4 = vec[21];
            c.factor_5 = vec[22];
            c.total_kills = std::atoi(vec[11].c_str()) + std::atoi(vec[13].c_str()) + std::atoi(vec[15].c_str()) +
                            std::atoi(vec[17].c_str());
            c.total_accidents =
                    std::atoi(vec[10].c_str()) + std::atoi(vec[12].c_str()) + std::atoi(vec[16].c_str()) +
                    std::atoi(vec[17].c_str());
            c.week_of_year = Utils::get_week(vec[0]);
            #pragma omp critical (mergeAccidents)
            {
                car_accidents.push_back(c);
            }
        }
    }
    }

    auto stop_tokenizer = high_resolution_clock::now();
    auto duration_tokenizer = duration_cast<microseconds>(stop_tokenizer - start_tokenizer);

    auto start_queries = high_resolution_clock::now();
    cout << "EVALUATING..." << endl;
    Query::evaluateQueries(car_accidents, NUM_THREADS);
    auto stop_queries = high_resolution_clock::now();
    auto duration_queries = duration_cast<microseconds>(stop_queries - start_queries);

    cout << "DURATION TOKENIZER - " << duration_tokenizer.count() << endl;
    cout << "DURATION Q1 - " << duration_queries.count() << endl;
}



