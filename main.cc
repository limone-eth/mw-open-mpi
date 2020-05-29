#include <iostream>     // cout, endl
#include <fstream>
#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <cstring>// fstream
#include <vector>
#include <string>
#include <time.h>
#include <omp.h>
#include <mpi.h>
#include <map>
#include "CarAccident.cpp"
#include <boost/date_time/gregorian/gregorian.hpp>
// #include "Query.cpp"
//#include "Utils.cpp"

using namespace std;
using namespace boost::gregorian;

int NUM_THREADS = 2;
string CSV_FILE = "./files/NYPD_Motor_Vehicle_Collisions.csv";

#define ROWS 955928
#define COLUMNS 29
#define MAX_CF_LENGHT 100
#define MAX_LINE_LENGHT 500

#define BOROUGHS 5
#define SECONDS_PER_WEEK 604800

#define NUMBER_OF_INDICATORS 10

void normalize(string *str_line);

time_t to_time_t(const string *date);

template<typename T> int freeMatrix(T ***set);

template<typename T> int allocateMatrix(T ***set, int rows, int columns, T value);

bool is_in_array(const std::string &value, const std::vector <string> &array) {
    return std::find(array.begin(), array.end(), value) != array.end();
}

int get_week(std::string date) {
    std::string formatted_date;
    std::string delimiter = "/";
    size_t pos = 0;
    std::string token;
    vector <string> split;
    std::string s = date + "/";
    while ((pos = s.find(delimiter)) != std::string::npos) {
        token = s.substr(0, pos);
        split.push_back(token);
        s.erase(0, pos + delimiter.length());
    }
    std::tm d={};
    if (split.size() > 0){
        try {
            d.tm_year=stoi(split[2].c_str());
            d.tm_mon=stoi(split[0].c_str());
            d.tm_mday=stoi(split[1].c_str());
        } catch (int e){
            cout << "ERROR DATE" << endl;
            throw e;
        }

    }
    std::mktime(&d);
    return (d.tm_yday-d.tm_wday+7)/7;
}

class Query1 {
public:
    std::string year;
    int total_accidents;
    std::string week;

    static void exec(std::map<std::string, Query1> query_results_1, vector<CarAccident> car_accidents, int NUM_THREADS) {
        // QUERY 1
        omp_set_num_threads(NUM_THREADS);
        int chunkSize = car_accidents.size() / NUM_THREADS;
#pragma omp parallel for schedule(dynamic, chunkSize)
        for (long unsigned int i = 0; i < car_accidents.size(); i++) {
            std::string s = car_accidents[i].date.substr(6) + " W:" + car_accidents[i].week_of_year;
            if (query_results_1.count(s) == false) {
                query_results_1[s].total_accidents = car_accidents[i].total_kills;
            } else {
#pragma omp atomic
                query_results_1[s].total_accidents += car_accidents[i].total_kills;
            }
        }
    }

};

int main() {
    using namespace std;
    using namespace std::chrono;

    int PROCESS_RANK;
    int SIZE;
    int THREAD_SUPPORT;

    MPI_Init_thread(nullptr, nullptr, MPI_THREAD_FUNNELED, &THREAD_SUPPORT);

    MPI_Comm_size(MPI_COMM_WORLD, &SIZE);
    MPI_Comm_rank(MPI_COMM_WORLD, &PROCESS_RANK);
    cout << "AVAILABLE PROCESSES: " << SIZE << endl;

    char * process_name = new char[1000];
    int process_name_len;

    MPI_Get_processor_name(process_name, &process_name_len);

    cout << "PROCESS NAME: " << process_name << endl;

    // Set local timezone
    setenv("TZ", "GMT0", 1);
    tzset();

    double local_timer_start = MPI_Wtime();

    // How many rows each process should process
    int ROWS_PER_PROCESS = ROWS / SIZE;

    // empty dataset
    char **car_accidents;
    char **scattered_car_accidents;

    allocateMatrix<char>(&car_accidents, ROWS, MAX_LINE_LENGHT, '\0');
    allocateMatrix<char>(&scattered_car_accidents, ROWS, MAX_LINE_LENGHT, '\0');

    string data(CSV_FILE);

    ifstream in(data.c_str());
    if (!in.is_open()) return 1;

    // LOADING DATASET
    string line;
    ifstream fin(CSV_FILE, ios::in);

    getline(fin, line); // reading header

    for (int i = 0; i < ROWS; ++i) {
        getline(fin, line);
        line.copy(car_accidents[i], line.size() + 1);
    }
    fin.close();

    // scatter data
    MPI_Scatter(&car_accidents[0][0], ROWS_PER_PROCESS * MAX_LINE_LENGHT, MPI_CHAR, &scattered_car_accidents[0][0], ROWS_PER_PROCESS * MAX_LINE_LENGHT, MPI_CHAR, 0, MPI_COMM_WORLD);
    freeMatrix<char>(&car_accidents);

    // Prepare dataset
    vector< vector<string> > local_dataset(ROWS, vector<string>(COLUMNS));

    int i = 0;
    string column;
    istringstream stream;
    // reading scattered_data into the local_dataset variable
    for(auto & l : local_dataset) {
        stream.str(scattered_car_accidents[i]);

        for(int j = 0; j < COLUMNS; ++j) {
            getline(stream, column, ',');
            l[j] = column;
        }

        stream.clear();
        i++;
    }
    freeMatrix<char>(&scattered_car_accidents);

    double local_timer_reading = MPI_Wtime();
    cout << PROCESS_RANK << " - " << local_timer_reading << endl;



    /*
     * @@@@@@@@
     *
     * QUERY 1
     *
     * @@@@@@@@
     */
    const int WEEKS = 500;

    int * local_lethal_accidents_per_week[WEEKS] = {0};
    vector<int> global_lethal_accidents_per_week(WEEKS, 0);
    std::string local_current_date;
    int threads = omp_get_max_threads();
    int w;
    // Compute number of lethal accidents per week

#pragma omp parallel for default(shared) private(i, w, local_current_date) reduction(+:local_lethal_accidents_per_week[:WEEKS])
    for(i = 0; i < ROWS_PER_PROCESS; ++i) {
        local_current_date = local_dataset[i][0];
        int sum;
        // if num of persons killed > 0
        w = get_week(local_current_date);
        if(local_dataset[i][11] != "0"){
            local_lethal_accidents_per_week[w]++;
        }
    }

    MPI_Reduce(local_lethal_accidents_per_week, &global_lethal_accidents_per_week[0], WEEKS, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    if (PROCESS_RANK == 0) {
        cout << "QUERY 1 completed -> " << MPI_Wtime() << endl;

        cout << "LETHAL ACCIDENTS PER WEEK" << endl;

        for (i = 0; i < WEEKS; ++i) {
            cout << "---- Week " << i << ": " << global_lethal_accidents_per_week[i] << endl;
            // lapw_sum += global_lethal_accidents_per_week[i];
        }

        // cout << "TOTAL LETHAL ACCIDENTS: " << lapw_sum << endl;
        cout << endl;
    }
    /*
     * @@@@@@@@
     *
     * QUERY 2
     *
     * @@@@@@@@
     */

    // Compute number of accidents per contributing factor
    /*int is_lethal;

    // create map to store for each global factors, how many persons have been killed
    map<string, int> global_factors = {};

    // local variables
    int * local_accidents_per_factor = new int[global_factors.size()] {0};
    int * local_lethal_accidents_per_factor  = new int[global_factors.size()] {0};
    vector<string> already_processed_factors;
#pragma omp parallel for default(shared) private(i, j, is_lethal, already_processed_factors) reduction(+: local_apcf[:global_factors.size()], local_lapcf[:global_factors.size()])
    for(i = 0; i < ROWS_PER_PROCESS; ++i) {
        is_lethal = 0;

        if(stoi(local_dataset[i][11]) + stoi(local_dataset[i][13]) + stoi(local_dataset[i][15]) + stoi(local_dataset[i][17]) > 0) {
            is_lethal = 1;
        }

        for(int j = 18; j < 23; j++) {
            if (!local_dataset[i][j].empty()) {
                // If the factor has been already analysed for that line, skip it
                if (is_in_array(local_dataset[i][j], already_processed_factors)) {
                    continue;
                }
                else {
                    local_accidents_per_factor[global_factors[local_dataset[i][j]]]++;
                    local_lethal_accidents_per_factor[global_factors[local_dataset[i][j]]] += is_lethal;

                    already_processed_factors.push_back(local_dataset[i][j]);
                }
            }
        }

        already_processed_factors.clear();
    }*/


    /*auto start_queries = high_resolution_clock::now();
    cout << "EVALUATING..." << endl;
    Query::evaluateQueries(car_accidents, NUM_THREADS);
    auto stop_queries = high_resolution_clock::now();
    auto duration_queries = duration_cast<microseconds>(stop_queries - start_queries);*/
    // cout << "DURATION Q1 - " << duration_queries.count() << endl;
    MPI_Finalize();
}

template<typename T> int freeMatrix(T ***data) {
    delete[] (*data);
    return 0;
}

template<typename T> int allocateMatrix(T ***data, int rows, int columns, T value) {
    T *p = nullptr;

    try {
        p = new T[rows * columns];    // Allocate rows*columns contiguous items
        (*data) = new T *[rows];         // Allocate row pointers
    } catch (exception &e) {
        cout << "Standard exception: " << e.what() << endl;
        return -1;
    }

    // Set up pointers into contiguous memory
    for (int i = 0; i < rows; ++i)
        (*data)[i] = &(p[i * columns]);

    return 0;
}