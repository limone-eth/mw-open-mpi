#include <boost/date_time/gregorian/gregorian.hpp>
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
#include <set>
#include "CarAccident.cpp"
// #include "Query.cpp"
//#include "Utils.cpp"

using namespace std;
using namespace boost::gregorian;

int NUM_THREADS = 4;
string CSV_FILE = "./files/NYPD_Motor_Vehicle_Collisions.csv";

#define ROWS 955928
#define COLUMNS 29
#define MAX_CF_LENGHT 100
#define MAX_LINE_LENGHT 500
#define PRINT_RESULTS false

void normalize(string *str_line);

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

int main() {
    using namespace std;
    // using namespace std::chrono;

    int PROCESS_RANK;
    int SIZE;
    int THREAD_SUPPORT;

    MPI_Init_thread(nullptr, nullptr, MPI_THREAD_FUNNELED, &THREAD_SUPPORT);
    // MPI_Init(nullptr, nullptr);

    MPI_Comm_size(MPI_COMM_WORLD, &SIZE);
    MPI_Comm_rank(MPI_COMM_WORLD, &PROCESS_RANK);
    if (PROCESS_RANK == 0){
        cout << "AVAILABLE PROCESSES: " << SIZE << endl;
    }

    char *process_name = new char[1000];
    int process_name_len;

    MPI_Get_processor_name(process_name, &process_name_len);

    cout << "PROCESS NAME: " << process_name << endl;

    // Set local timezone
    setenv("TZ", "GMT0", 1);
    tzset();

    // Local performance indicators will be then used to evaluate global performance
    auto * local_performance = new double[5]{0.0};

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

    // File reading ends here, get time
    local_performance[0] = MPI_Wtime();
    
    // scatter data
    MPI_Scatter(&car_accidents[0][0], ROWS_PER_PROCESS * MAX_LINE_LENGHT, MPI_CHAR, &scattered_car_accidents[0][0],
                ROWS_PER_PROCESS * MAX_LINE_LENGHT, MPI_CHAR, 0, MPI_COMM_WORLD);
    freeMatrix<char>(&car_accidents);
    
    // At this point, each process has a piece of the file on which to operate
    local_performance[1] = MPI_Wtime();

    // Prepare dataset
    vector<vector<string> > local_dataset(ROWS_PER_PROCESS, vector<string>(COLUMNS));

    int i = 0;
    int j = 0;
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
    
    // The dataset is prepared into the local variables
    local_performance[2] = MPI_Wtime();

    /*
     * @@@@@@@@
     *
     * QUERY 1
     *
     * @@@@@@@@
     */

    // Query 1 start
    //local_performance[3] = MPI_Wtime();

    const int WEEKS = 51;

    int *local_lethal_accidents_per_week = new int[WEEKS]{0}; // initializing array with all 0s
    vector<int> global_lethal_accidents_per_week(WEEKS, 0);
    std::string local_current_date;
    int threads = omp_get_max_threads();
    omp_set_num_threads(threads);
    
    int w;

    // Compute number of lethal accidents per week
#pragma omp parallel for default(shared) private(i, w, local_current_date) reduction(+:local_lethal_accidents_per_week[:WEEKS])
    for (i = 0; i < ROWS_PER_PROCESS; ++i) {
        local_current_date = local_dataset[i][0];

        // get week number from date
        w = get_week(local_current_date);

        // if num of persons killed > 0
        if (local_dataset[i][11] != "0") {
            local_lethal_accidents_per_week[w] += 1;
        }

    }

    MPI_Reduce(&local_lethal_accidents_per_week[0], &global_lethal_accidents_per_week[0], WEEKS, MPI_INT, MPI_SUM, 0,
               MPI_COMM_WORLD);

    if (PROCESS_RANK == 0 && PRINT_RESULTS == true) {
        cout << "QUERY 1 completed -> " << MPI_Wtime() << endl;

        cout << "LETHAL ACCIDENTS PER WEEK" << endl;

        for (i = 0; i < WEEKS; ++i) {
            cout << "---- Week " << i << ": " << global_lethal_accidents_per_week[i] << endl;
        }
        cout << endl;
    }

    /*
     * @@@@@@@@
     *
     * QUERY 2
     *
     * @@@@@@@@
     */
    // Query 2 start
    local_performance[3] = MPI_Wtime();

    // storing local factors
    set<string> factors;
    for (i = 0; i < ROWS_PER_PROCESS; ++i) {
        for (int j = 18; j < 23; ++j) {
            factors.insert(local_dataset[i][j]);
        }
    }

    int LOCAL_FACTORS_SIZE = factors.size();
    int MAX_FACTORS_SIZE = 0;

    MPI_Allreduce(&LOCAL_FACTORS_SIZE, &MAX_FACTORS_SIZE, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    cout << process_name << " - local_factors_size: " << LOCAL_FACTORS_SIZE << endl;
    cout << process_name << " - max_factors_size: " << MAX_FACTORS_SIZE << endl;
    // Convert factors to contiguous array
    char **local_factors;

    allocateMatrix(&local_factors, MAX_FACTORS_SIZE, MAX_CF_LENGHT, '\0');

    i = 0;
    for (auto elem : factors){
        if (elem.length > 1){
            cout << process_name << " - elem: " << elem << endl;
            elem.copy(local_factors[i], elem.length() + 1);
        }
        i++;
    }
    factors.clear();
    cout << "LOCAL FACTORS" << endl;
    for (i = 0; i < MAX_FACTORS_SIZE; i++){
        cout << process_name << " - " << local_factors[i] << endl;
    }

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
    int local_accidents_per_factor[global_factors.size()];
    int local_lethal_accidents_per_factor[global_factors.size()];
    for (i = 0; i < global_factors.size(); i++) {
        local_accidents_per_factor[i] = 0;
        local_lethal_accidents_per_factor[i] = 0;
    }
    set<string> already_processed_factors;
#pragma omp parallel for default(shared) private(i, j, already_processed_factors) reduction(+: local_accidents_per_factor[:global_factors.size()], local_lethal_accidents_per_factor[:global_factors.size()])
    for (i = 0; i < ROWS_PER_PROCESS; ++i) {
        for (int j = 18; j < 23; j++) {
            if (!local_dataset[i][j].empty()) {
                // If the factor has not been already processed for that line, do the sum
                if ( local_dataset[i][j] != "0") {
                    local_accidents_per_factor[global_factors[local_dataset[i][j]]]++;
                    local_lethal_accidents_per_factor[global_factors[local_dataset[i][j]]] +=
                            local_dataset[i][11] != "0" ? 1 : 0;
                    already_processed_factors.insert(local_dataset[i][j]);
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

    if (PROCESS_RANK == 0 && PRINT_RESULTS == true) {
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

    freeMatrix(&local_factors);
    freeMatrix(&global_factors_nn);

    /*
     * @@@@@@@@
     *
     * QUERY 3
     *
     * @@@@@@@@
     */

    // Query 3 start
    local_performance[4] = MPI_Wtime();


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
    //Query3 end

    local_performance[5] = MPI_Wtime();
    if (PROCESS_RANK == 0 && PRINT_RESULTS == true) {
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

    freeMatrix(&local_boroughs);
    freeMatrix(&local_accidents_per_borough_per_week);
    freeMatrix(&global_boroughs_nn);

    // Computation end
    double local_timer_end = MPI_Wtime();

    // Reduce timers to get global execution times
    auto * global_pi = new double[5]{0.0};

    double global_start = 0.0;
    double global_end = 0.0;

    MPI_Reduce(&local_timer_start, &global_start, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_timer_end, &global_end, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    MPI_Reduce(&local_performance[0], &global_pi[0], 5, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    cout << process_name << " - Printing results..." << endl;
    if(PROCESS_RANK == 0){
        cout << "Execution time: " << global_end - global_start << " s\n" << endl;

        for (i = 0; i < 5; ++i)
            global_pi[i] -= global_start;

        for (i = 4; i > 0; --i)
            global_pi[i] -= global_pi[i - 1];


        // Printing performance indicators as array object
        cout << "[" << process_name << ", " << SIZE << ", " << threads << ", ";

        for (i = 0; i < 5; ++i)
            cout << std::setprecision (5) << fixed << global_pi[i] << ", ";

        cout << global_end - global_start << "]" << endl;
    }
    cout << process_name <<" - MPI_Finalize()" << endl;
    #pragma omp barrier
    MPI_Finalize();
    cout << process_name <<" - Finalized" << endl;
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