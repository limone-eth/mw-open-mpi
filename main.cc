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


#define SECONDS_PER_WEEK 604800

#define NUMBER_OF_INDICATORS 10

void normalize(string *str_line);

time_t to_time_t(const string *date);

template<typename T>
int freeMatrix(T ***set);

template<typename T>
int allocateMatrix(T ***set, int rows, int columns, T value);

bool is_in_array(string value, vector<string> array) {
    return find(array.begin(), array.end(), value) != array.end();
}

ostream &operator<<(ostream &os, const std::pair<std::string, std::string> &p) {
    os << p.first << ' ' << p.second << ' ';
    return os;
}

int get_week(std::string date) {
    std::string formatted_date;
    std::string delimiter = "/";
    size_t pos = 0;
    std::string token;
    vector<string> split;
    std::string s = date + "/";
    while ((pos = s.find(delimiter)) != std::string::npos) {
        token = s.substr(0, pos);
        split.push_back(token);
        s.erase(0, pos + delimiter.length());
    }
    std::tm d = {};
    if (!split.empty()) {
        try {
            d.tm_year = stoi(split[2]);
            d.tm_mon = stoi(split[0]);
            d.tm_mday = stoi(split[1]);
        } catch (int e) {
            cout << "ERROR DATE" << endl;
            throw e;
        }

    }
    std::mktime(&d);
    return (d.tm_yday - d.tm_wday + 7) / 7;
}

class Query1 {
public:
    std::string year;
    int total_accidents;
    std::string week;

    static void
    exec(std::map<std::string, Query1> query_results_1, vector<CarAccident> car_accidents, int NUM_THREADS) {
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

    char *process_name = new char[1000];
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
    MPI_Scatter(&car_accidents[0][0], ROWS_PER_PROCESS * MAX_LINE_LENGHT, MPI_CHAR, &scattered_car_accidents[0][0],
                ROWS_PER_PROCESS * MAX_LINE_LENGHT, MPI_CHAR, 0, MPI_COMM_WORLD);
    freeMatrix<char>(&car_accidents);

    // Prepare dataset
    vector<vector<string> > local_dataset(ROWS, vector<string>(COLUMNS));

    int i = 0;
    string column;
    istringstream stream;
    // reading scattered_data into the local_dataset variable
    for (auto &l : local_dataset) {
        stream.str(scattered_car_accidents[i]);

        for (int j = 0; j < COLUMNS; ++j) {
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
    const int WEEKS = 51;

    int *local_lethal_accidents_per_week[WEEKS] = {0};
    vector<int> global_lethal_accidents_per_week(WEEKS, 0);
    std::string local_current_date;
    int threads = omp_get_max_threads();
    int w;
    // Compute number of lethal accidents per week

#pragma omp parallel for default(shared) private(i, w, local_current_date) reduction(+:local_lethal_accidents_per_week[:WEEKS])
    for (i = 0; i < ROWS_PER_PROCESS; ++i) {
        local_current_date = local_dataset[i][0];

        // get week number from date
        w = get_week(local_current_date);
        // if num of persons killed > 0
        if (local_dataset[i][11] != "0") {
            local_lethal_accidents_per_week[w]++;
        }

    }

    MPI_Reduce(local_lethal_accidents_per_week, &global_lethal_accidents_per_week[0], WEEKS, MPI_INT, MPI_SUM, 0,
               MPI_COMM_WORLD);

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

    // storing local factors
    vector<string> factors;
    for (i = 0; i < ROWS_PER_PROCESS; ++i) {
        for (int j = 18; j < 23; ++j) {
            if (!is_in_array(local_dataset[i][j], factors)) {
                factors.push_back(local_dataset[i][j]);
            }
        }
    }

    int LOCAL_FACTORS_SIZE = factors.size();
    int MAX_FACTORS_SIZE = 0;


    MPI_Allreduce(&LOCAL_FACTORS_SIZE, &MAX_FACTORS_SIZE, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    // Convert factors to contiguous array
    char **local_factors;

    allocateMatrix(&local_factors, MAX_FACTORS_SIZE, MAX_CF_LENGHT, '\0');

    for (i = 0; i < LOCAL_FACTORS_SIZE; ++i)
        factors[i].copy(local_factors[i], factors[i].length() + 1);

    factors.clear();

    // Populate global factors variable
    char **global_factors_nn;

    allocateMatrix(&global_factors_nn, MAX_FACTORS_SIZE * SIZE, MAX_CF_LENGHT, '\0');

    MPI_Allgather(&local_factors[0][0], MAX_FACTORS_SIZE * MAX_CF_LENGHT, MPI_CHAR, &global_factors_nn[0][0],
                  MAX_FACTORS_SIZE * MAX_CF_LENGHT, MPI_CHAR, MPI_COMM_WORLD);

    // Create map to join all the local factors in a single one
    map<string, int> global_factors;

    for (i = 0; i < MAX_FACTORS_SIZE * SIZE; ++i) {
        if ((global_factors.find(global_factors_nn[i]) == global_factors.end()) && strlen(global_factors_nn[i])) {
            global_factors[global_factors_nn[i]] = 0;
        }
    }

    i = 0;

    // setting an integer index for each factor
    for (auto &f: global_factors) {
        f.second = f.second + i;
        i++;
    }

    // local variables
    int *local_accidents_per_factor[global_factors.size()];
    int *local_lethal_accidents_per_factor[global_factors.size()];
    for (i = 0; i < global_factors.size(); i++) {
        local_accidents_per_factor[i] = 0;
        local_lethal_accidents_per_factor[i] = 0;
    }
    vector<string> already_processed_factors;
#pragma omp parallel for default(shared) private(i, j, already_processed_factors) reduction(+: local_accidents_per_factor[:global_factors.size()], local_lethal_accidents_per_factor[:global_factors.size()])
    for (i = 0; i < ROWS_PER_PROCESS; ++i) {
        for (int j = 18; j < 23; j++) {
            if (!local_dataset[i][j].empty()) {
                // If the factor has not been already processed for that line, do the sum
                if (!is_in_array(local_dataset[i][j], already_processed_factors) && local_dataset[i][j] != "0") {
                    local_accidents_per_factor[global_factors[local_dataset[i][j]]]++;
                    local_lethal_accidents_per_factor[global_factors[local_dataset[i][j]]] +=
                            local_dataset[i][11] != "0" ? 1 : 0;
                    already_processed_factors.push_back(local_dataset[i][j]);
                }
            }
        }
        already_processed_factors.clear();
    }

    vector<int> global_accidents_per_factor(global_factors.size(), 0);
    vector<int> global_lethal_accidents_per_factor(global_factors.size(), 0);

    // Reduce local array to global correspondents
    MPI_Reduce(&local_accidents_per_factor[0], &global_accidents_per_factor[0], global_factors.size(), MPI_INT, MPI_SUM,
               0, MPI_COMM_WORLD);
    MPI_Reduce(&local_lethal_accidents_per_factor[0], &global_lethal_accidents_per_factor[0], global_factors.size(),
               MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

    if (PROCESS_RANK == 0) {
        cout << "QUERY 2 completed -> " << MPI_Wtime() << endl;

        cout << "ACCIDENTS AND PERCENTAGE OF L/NL ACCIDENTS PER CONTRIBUTING FACTOR" << endl;

        for (const auto &f: global_factors)
            cout << "-- FACTOR: " << f.first << ", " << global_accidents_per_factor[f.second] << ", "
                 << global_lethal_accidents_per_factor[f.second] << ", " << 100 *
                                                                            ((double) global_lethal_accidents_per_factor[f.second] /
                                                                             (double) global_accidents_per_factor[f.second])
                 << "%" << endl;
        cout << endl;
    }

    /*
     * @@@@@@@@
     *
     * QUERY 3
     *
     * @@@@@@@@
     */

    // storing local boroughs
    vector<string> boroughs;
    for (i = 0; i < ROWS_PER_PROCESS; ++i) {
        if (!is_in_array(local_dataset[i][2], boroughs) && !local_dataset[i][2].empty()) {
            boroughs.push_back(local_dataset[i][2]);
        }
    }

    int LOCAL_BOROUGHS_SIZE = boroughs.size();
    int MAX_BOROUGHS_SIZE = 0;

    MPI_Allreduce(&LOCAL_BOROUGHS_SIZE, &MAX_BOROUGHS_SIZE, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    // Convert boroughs to contiguous array
    char **local_boroughs;

    allocateMatrix(&local_boroughs, MAX_BOROUGHS_SIZE, MAX_CF_LENGHT, '\0');

    for (i = 0; i < LOCAL_BOROUGHS_SIZE; ++i)
        boroughs[i].copy(local_boroughs[i], boroughs[i].length() + 1);

    boroughs.clear();

    // Populate global boroughs variable
    char **global_boroughs_nn;

    allocateMatrix(&global_boroughs_nn, MAX_BOROUGHS_SIZE * SIZE, MAX_CF_LENGHT, '\0');

    MPI_Allgather(&local_boroughs[0][0], MAX_BOROUGHS_SIZE * MAX_CF_LENGHT, MPI_CHAR, &global_boroughs_nn[0][0],
                  MAX_BOROUGHS_SIZE * MAX_CF_LENGHT, MPI_CHAR, MPI_COMM_WORLD);

    map<string, int> global_boroughs;
    for (i = 0; i < MAX_BOROUGHS_SIZE * SIZE; ++i) {
        if ((global_boroughs.find(global_boroughs_nn[i]) == global_boroughs.end()) && strlen(global_boroughs_nn[i])) {
            global_boroughs[global_boroughs_nn[i]] = 0;
        }
    }

    i = 0;

    // setting an integer index for each factor
    for (auto &f: global_boroughs) {
        f.second = f.second + i;
        i++;
    }

    int *local_lethal_accidents_per_borough = new int[global_boroughs.size()]{0};
    int **local_accidents_per_borough_per_week;

    allocateMatrix(&local_accidents_per_borough_per_week, global_boroughs.size(), WEEKS, 0);

    for (i = 0; i < global_boroughs.size(); i++) {
        for (int j = 0; j < WEEKS; j++) {
            local_accidents_per_borough_per_week[i][j] = 0;
        }
    }

    // Compute number of lethal accidents per borough & accidents per borough per week
#pragma omp parallel for default(shared) private(i, w, local_current_date) reduction(+: local_lethal_accidents_per_borough[:global_boroughs.size()])
    for (i = 0; i < ROWS_PER_PROCESS; ++i) {

        // check if borough column is not empty
        if (!local_dataset[i][2].empty()) {
            local_current_date = local_dataset[i][0];

            // if is lethal, we add 1 otherwise 0
            local_lethal_accidents_per_borough[global_boroughs[local_dataset[i][2]]] +=
                    local_dataset[i][11] != "0" ? 1 : 0;
            w = get_week(local_current_date);
#pragma omp atomic
            local_accidents_per_borough_per_week[global_boroughs[local_dataset[i][2]]][w]++;
        }
    }


    vector<int> global_lethal_accidents_per_borough(global_boroughs.size(), 0);
    vector<vector<int>> global_accidents_per_borough_per_week(global_boroughs.size(), vector<int>(WEEKS, 0));

    MPI_Reduce(&local_lethal_accidents_per_borough[0], &global_lethal_accidents_per_borough[0], global_boroughs.size(),
               MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

    for (const auto &b : global_boroughs)
        MPI_Reduce(&local_accidents_per_borough_per_week[b.second][0],
                   &global_accidents_per_borough_per_week[b.second][0], WEEKS, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

    if (PROCESS_RANK == 0) {
        cout << "QUERY 3 completed -> " << MPI_Wtime() << endl;

        for (const auto &b: global_boroughs) {
            cout << "BOROUGH: " << b.first << " (Lethal Accidents: "
                 << global_lethal_accidents_per_borough[global_boroughs[b.first]] << ", Average: "
                 << ((double) global_lethal_accidents_per_borough[global_boroughs[b.first]] / (double) WEEKS) << ")"
                 << endl;
            for (w = 0; w < WEEKS; ++w) {
                cout << "---- Week " << w << ": " << global_accidents_per_borough_per_week[global_boroughs[b.first]][w]
                     << endl;
            }
        }

        cout << endl;
    }

    MPI_Finalize();
}

template<typename T>
int freeMatrix(T ***data) {
    delete[] (*data);
    return 0;
}

template<typename T>
int allocateMatrix(T ***data, int rows, int columns, T value) {
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