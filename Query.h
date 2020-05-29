//
// Created by Simone Staffa on 25/05/2020.
//

#ifndef MW_OPEN_MPI_QUERY_H
#define MW_OPEN_MPI_QUERY_H

#include <iostream>     // cout, endl
#include <fstream>      // fstream
#include <vector>
#include <string>
#include <algorithm>    // copy
#include <iterator>     // ostream_operator
#include <time.h>
#include <omp.h>
#include <map>
#include "CarAccident.cpp"
#include "Utils.cpp"

using namespace std;

class Query1 {
public:
    std::string year;
    int total_accidents;
    std::string week;

    static void exec(vector<CarAccident> car_accidents, int NUM_THREADS) {
        // QUERY 1
        std::map<std::string, Query1> query_results_1;
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

class Query2 {
public:
    int total_accidents;
    int lethal_accidents;
    float lethal_percentage;

    static void exec(vector<CarAccident> car_accidents, int NUM_THREADS) {
        std::map<std::string, Query2> query_results_2;
        omp_set_num_threads(NUM_THREADS);
        int chunkSize = car_accidents.size() / NUM_THREADS;
#pragma omp parallel for schedule(dynamic, chunkSize)
        for (long unsigned int i = 0; i < car_accidents.size(); i++) {
            vector<string> already_processed_factors;
            // CHECK FACTOR 1
            if (car_accidents[i].factor_1 != "") {
                already_processed_factors.push_back(car_accidents[i].factor_1);
                if (query_results_2.count(car_accidents[i].factor_1) == false) {
                    query_results_2[car_accidents[i].factor_1].total_accidents = 1;
                    query_results_2[car_accidents[i].factor_1].lethal_accidents =
                            car_accidents[i].total_kills > 0 ? 1 : 0;
                    query_results_2[car_accidents[i].factor_1].lethal_percentage =
                            (float) query_results_2[car_accidents[i].factor_1].lethal_accidents /
                            (float) query_results_2[car_accidents[i].factor_1].total_accidents;
                } else {
#pragma omp critical (factor1)
                    {
                        query_results_2[car_accidents[i].factor_1].total_accidents += 1;
                        query_results_2[car_accidents[i].factor_1].lethal_accidents +=
                                car_accidents[i].total_kills > 0 ? 1 : 0;
                        query_results_2[car_accidents[i].factor_1].lethal_percentage =
                                (float) query_results_2[car_accidents[i].factor_1].lethal_accidents /
                                (float) query_results_2[car_accidents[i].factor_1].total_accidents;
                    }
                }
            }
            // CHECK FACTOR 2
            if (!Utils::in_array(car_accidents[i].factor_2, already_processed_factors) &&
                car_accidents[i].factor_2 != "") {
                already_processed_factors.push_back(car_accidents[i].factor_2);
                if (query_results_2.count(car_accidents[i].factor_2) == false) {
                    query_results_2[car_accidents[i].factor_2].total_accidents = 1;
                    query_results_2[car_accidents[i].factor_2].lethal_accidents =
                            car_accidents[i].total_kills > 0 ? 1 : 0;
                    query_results_2[car_accidents[i].factor_2].lethal_percentage =
                            (float) query_results_2[car_accidents[i].factor_2].lethal_accidents /
                            (float) query_results_2[car_accidents[i].factor_2].total_accidents;
                } else {
#pragma omp critical (factor2)
                    {
                        query_results_2[car_accidents[i].factor_2].total_accidents += 1;
                        query_results_2[car_accidents[i].factor_2].lethal_accidents +=
                                car_accidents[i].total_kills > 0 ? 1 : 0;
                        query_results_2[car_accidents[i].factor_2].lethal_percentage =
                                (float) query_results_2[car_accidents[i].factor_2].lethal_accidents /
                                (float) query_results_2[car_accidents[i].factor_2].total_accidents;
                    }
                }
            }
            // CHECK FACTOR 3
            if (!Utils::in_array(car_accidents[i].factor_3, already_processed_factors) &&
                car_accidents[i].factor_3 != "") {
                already_processed_factors.push_back(car_accidents[i].factor_3);
                if (query_results_2.count(car_accidents[i].factor_3) == false) {
                    query_results_2[car_accidents[i].factor_3].total_accidents = 1;
                    query_results_2[car_accidents[i].factor_3].lethal_accidents =
                            car_accidents[i].total_kills > 0 ? 1 : 0;
                    query_results_2[car_accidents[i].factor_3].lethal_percentage =
                            (float) query_results_2[car_accidents[i].factor_3].lethal_accidents /
                            (float) query_results_2[car_accidents[i].factor_3].total_accidents;
                } else {
#pragma omp critical (factor3)
                    {
                        query_results_2[car_accidents[i].factor_3].total_accidents += 1;
                        query_results_2[car_accidents[i].factor_3].lethal_accidents +=
                                car_accidents[i].total_kills > 0 ? 1 : 0;
                        query_results_2[car_accidents[i].factor_3].lethal_percentage =
                                (float) query_results_2[car_accidents[i].factor_3].lethal_accidents /
                                (float) query_results_2[car_accidents[i].factor_3].total_accidents;
                    }
                }
            }
            // CHECK FACTOR 4
            if (!Utils::in_array(car_accidents[i].factor_4, already_processed_factors) &&
                car_accidents[i].factor_4 != "") {
                already_processed_factors.push_back(car_accidents[i].factor_4);
                if (query_results_2.count(car_accidents[i].factor_4) == false) {
                    query_results_2[car_accidents[i].factor_4].total_accidents = 1;
                    query_results_2[car_accidents[i].factor_4].lethal_accidents =
                            car_accidents[i].total_kills > 0 ? 1 : 0;
                    query_results_2[car_accidents[i].factor_4].lethal_percentage =
                            (float) query_results_2[car_accidents[i].factor_4].lethal_accidents /
                            (float) query_results_2[car_accidents[i].factor_4].total_accidents;
                } else {
#pragma omp critical (factor4)
                    {
                        query_results_2[car_accidents[i].factor_4].total_accidents += 1;
                        query_results_2[car_accidents[i].factor_4].lethal_accidents +=
                                car_accidents[i].total_kills > 0 ? 1 : 0;
                        query_results_2[car_accidents[i].factor_4].lethal_percentage =
                                (float) query_results_2[car_accidents[i].factor_4].lethal_accidents /
                                (float) query_results_2[car_accidents[i].factor_4].total_accidents;
                    }
                }
            }
            // CHECK FACTOR 5
            if (!Utils::in_array(car_accidents[i].factor_5, already_processed_factors) &&
                car_accidents[i].factor_5 != "") {
                already_processed_factors.push_back(car_accidents[i].factor_5);
                if (query_results_2.count(car_accidents[i].factor_5) == false) {
                    query_results_2[car_accidents[i].factor_5].total_accidents = 1;
                    query_results_2[car_accidents[i].factor_5].lethal_accidents =
                            car_accidents[i].total_kills > 0 ? 1 : 0;
                    query_results_2[car_accidents[i].factor_5].lethal_percentage =
                            (float) query_results_2[car_accidents[i].factor_5].lethal_accidents /
                            (float) query_results_2[car_accidents[i].factor_5].total_accidents;
                } else {
#pragma omp critical (factor5)
                    {
                        query_results_2[car_accidents[i].factor_5].total_accidents += 1;
                        query_results_2[car_accidents[i].factor_5].lethal_accidents +=
                                car_accidents[i].total_kills > 0 ? 1 : 0;
                        query_results_2[car_accidents[i].factor_5].lethal_percentage =
                                (float) query_results_2[car_accidents[i].factor_5].lethal_accidents /
                                (float) query_results_2[car_accidents[i].factor_5].total_accidents;
                    }
                }
            }
        }
    }
};

class Query3 {
public:
    std::string borough;
    std::string week;
    int total_accidents;
    int lethal_accidents;
    float lethal_average;

    static void exec(vector<CarAccident> car_accidents, int NUM_THREADS) {
        std::map<std::pair<std::string, std::string>, Query3> query_results_3;
        omp_set_num_threads(NUM_THREADS);
        int chunkSize = car_accidents.size() / NUM_THREADS;
#pragma omp parallel for schedule(dynamic, chunkSize)
        for (long unsigned int i = 0; i < car_accidents.size(); i++) {
            std::vector< std::pair<std::string, std::string> > boroughs_weeks;
            if (car_accidents[i].borough != "") {
                std::pair<std::string, std::string> b_w = std::make_pair(car_accidents[i].borough,
                                                                         car_accidents[i].week_of_year);
                boroughs_weeks.push_back(b_w);
                if (query_results_3.count(b_w) == false) {
                    query_results_3[b_w].total_accidents = 1;
                    query_results_3[b_w].week = car_accidents[i].week_of_year;
                    query_results_3[b_w].lethal_accidents = car_accidents[i].total_kills > 0 ? 1 : 0;
                    query_results_3[b_w].lethal_average =
                            (float) query_results_3[b_w].lethal_accidents /
                            (float) query_results_3[b_w].total_accidents;
                } else {
#pragma omp critical (query3)
                    {
                        query_results_3[b_w].total_accidents += 1;
                        query_results_3[b_w].lethal_accidents += car_accidents[i].total_kills > 0 ? 1 : 0;
                        query_results_3[b_w].lethal_average =
                                (float) query_results_3[b_w].lethal_accidents /
                                (float) query_results_3[b_w].total_accidents;
                    }
                }
            }
        }
    }
};

ostream &operator<<(ostream &os, const std::pair<std::string, std::string> &p) {
    os << p.first << ' ' << p.second << ' ';
    return os;
}

class Query {
public:
    /*
     * EVALUATE QUERY 1,2,3 in a SINGLE CYCLE
     */
    static void evaluateQueries(vector<CarAccident> car_accidents, int NUM_THREADS) {
        std::map<std::string, Query1> query_results_1;
        std::map<std::string, Query2> query_results_2;
        std::map<std::pair<std::string, std::string>, Query3> query_results_3;
        omp_set_num_threads(NUM_THREADS);
        int chunkSize = car_accidents.size() / omp_get_max_threads();
#pragma omp parallel for schedule(dynamic, chunkSize)
        for (long unsigned int i = 0; i < car_accidents.size(); i++) {
            // QUERY 1
            std::string s = car_accidents[i].date.substr(6) + " W:" + car_accidents[i].week_of_year;
            if (query_results_1.count(s) == false) {
                query_results_1[s].total_accidents = car_accidents[i].total_kills;
            } else {
#pragma omp atomic
                query_results_1[s].total_accidents += car_accidents[i].total_kills;
            }
            // QUERY 2
            vector<string> already_processed_factors;
            // CHECK FACTOR 1
            if (car_accidents[i].factor_1 != "") {
                already_processed_factors.push_back(car_accidents[i].factor_1);
                if (query_results_2.count(car_accidents[i].factor_1) == false) {
                    query_results_2[car_accidents[i].factor_1].total_accidents = 1;
                    query_results_2[car_accidents[i].factor_1].lethal_accidents =
                            car_accidents[i].total_kills > 0 ? 1 : 0;
                    query_results_2[car_accidents[i].factor_1].lethal_percentage =
                            (float) query_results_2[car_accidents[i].factor_1].lethal_accidents /
                            (float) query_results_2[car_accidents[i].factor_1].total_accidents;
                } else {
#pragma omp critical (factor1)
                    {
                        query_results_2[car_accidents[i].factor_1].total_accidents += 1;
                        query_results_2[car_accidents[i].factor_1].lethal_accidents +=
                                car_accidents[i].total_kills > 0 ? 1 : 0;
                        query_results_2[car_accidents[i].factor_1].lethal_percentage =
                                (float) query_results_2[car_accidents[i].factor_1].lethal_accidents /
                                (float) query_results_2[car_accidents[i].factor_1].total_accidents;
                    }
                }
            }
            // CHECK FACTOR 2
            if (!Utils::in_array(car_accidents[i].factor_2, already_processed_factors) &&
                car_accidents[i].factor_2 != "") {
                already_processed_factors.push_back(car_accidents[i].factor_2);
                if (query_results_2.count(car_accidents[i].factor_2) == false) {
                    query_results_2[car_accidents[i].factor_2].total_accidents = 1;
                    query_results_2[car_accidents[i].factor_2].lethal_accidents =
                            car_accidents[i].total_kills > 0 ? 1 : 0;
                    query_results_2[car_accidents[i].factor_2].lethal_percentage =
                            (float) query_results_2[car_accidents[i].factor_2].lethal_accidents /
                            (float) query_results_2[car_accidents[i].factor_2].total_accidents;
                } else {
#pragma omp critical (factor2)
                    {
                        query_results_2[car_accidents[i].factor_2].total_accidents += 1;
                        query_results_2[car_accidents[i].factor_2].lethal_accidents +=
                                car_accidents[i].total_kills > 0 ? 1 : 0;
                        query_results_2[car_accidents[i].factor_2].lethal_percentage =
                                (float) query_results_2[car_accidents[i].factor_2].lethal_accidents /
                                (float) query_results_2[car_accidents[i].factor_2].total_accidents;
                    }
                }
            }
            // CHECK FACTOR 3
            if (!Utils::in_array(car_accidents[i].factor_3, already_processed_factors) &&
                car_accidents[i].factor_3 != "") {
                already_processed_factors.push_back(car_accidents[i].factor_3);
                if (query_results_2.count(car_accidents[i].factor_3) == false) {
                    query_results_2[car_accidents[i].factor_3].total_accidents = 1;
                    query_results_2[car_accidents[i].factor_3].lethal_accidents =
                            car_accidents[i].total_kills > 0 ? 1 : 0;
                    query_results_2[car_accidents[i].factor_3].lethal_percentage =
                            (float) query_results_2[car_accidents[i].factor_3].lethal_accidents /
                            (float) query_results_2[car_accidents[i].factor_3].total_accidents;
                } else {
#pragma omp critical (factor3)
                    {
                        query_results_2[car_accidents[i].factor_3].total_accidents += 1;
                        query_results_2[car_accidents[i].factor_3].lethal_accidents +=
                                car_accidents[i].total_kills > 0 ? 1 : 0;
                        query_results_2[car_accidents[i].factor_3].lethal_percentage =
                                (float) query_results_2[car_accidents[i].factor_3].lethal_accidents /
                                (float) query_results_2[car_accidents[i].factor_3].total_accidents;
                    }
                }
            }
            // CHECK FACTOR 4
            if (!Utils::in_array(car_accidents[i].factor_4, already_processed_factors) &&
                car_accidents[i].factor_4 != "") {
                already_processed_factors.push_back(car_accidents[i].factor_4);
                if (query_results_2.count(car_accidents[i].factor_4) == false) {
                    query_results_2[car_accidents[i].factor_4].total_accidents = 1;
                    query_results_2[car_accidents[i].factor_4].lethal_accidents =
                            car_accidents[i].total_kills > 0 ? 1 : 0;
                    query_results_2[car_accidents[i].factor_4].lethal_percentage =
                            (float) query_results_2[car_accidents[i].factor_4].lethal_accidents /
                            (float) query_results_2[car_accidents[i].factor_4].total_accidents;
                } else {
#pragma omp critical (factor4)
                    {
                        query_results_2[car_accidents[i].factor_4].total_accidents += 1;
                        query_results_2[car_accidents[i].factor_4].lethal_accidents +=
                                car_accidents[i].total_kills > 0 ? 1 : 0;
                        query_results_2[car_accidents[i].factor_4].lethal_percentage =
                                (float) query_results_2[car_accidents[i].factor_4].lethal_accidents /
                                (float) query_results_2[car_accidents[i].factor_4].total_accidents;
                    }
                }
            }
            // CHECK FACTOR 5
            if (!Utils::in_array(car_accidents[i].factor_5, already_processed_factors) &&
                car_accidents[i].factor_5 != "") {
                already_processed_factors.push_back(car_accidents[i].factor_5);
                if (query_results_2.count(car_accidents[i].factor_5) == false) {
                    query_results_2[car_accidents[i].factor_5].total_accidents = 1;
                    query_results_2[car_accidents[i].factor_5].lethal_accidents =
                            car_accidents[i].total_kills > 0 ? 1 : 0;
                    query_results_2[car_accidents[i].factor_5].lethal_percentage =
                            (float) query_results_2[car_accidents[i].factor_5].lethal_accidents /
                            (float) query_results_2[car_accidents[i].factor_5].total_accidents;
                } else {
#pragma omp critical (factor5)
                    {
                        query_results_2[car_accidents[i].factor_5].total_accidents += 1;
                        query_results_2[car_accidents[i].factor_5].lethal_accidents +=
                                car_accidents[i].total_kills > 0 ? 1 : 0;
                        query_results_2[car_accidents[i].factor_5].lethal_percentage =
                                (float) query_results_2[car_accidents[i].factor_5].lethal_accidents /
                                (float) query_results_2[car_accidents[i].factor_5].total_accidents;
                    }
                }
            }
            // QUERY 3
            std::vector< std::pair<std::string, std::string> > boroughs_weeks;
            if (car_accidents[i].borough != "") {
                std::pair<std::string, std::string> b_w = std::make_pair(car_accidents[i].borough,
                                                                         car_accidents[i].week_of_year);
                boroughs_weeks.push_back(b_w);
                if (query_results_3.count(b_w) == false) {
                    query_results_3[b_w].total_accidents = 1;
                    query_results_3[b_w].week = car_accidents[i].week_of_year;
                    query_results_3[b_w].lethal_accidents = car_accidents[i].total_kills > 0 ? 1 : 0;
                    query_results_3[b_w].lethal_average =
                            (float) query_results_3[b_w].lethal_accidents /
                            (float) query_results_3[b_w].total_accidents;
                } else {
#pragma omp critical (query3)
                    {
                        query_results_3[b_w].total_accidents += 1;
                        query_results_3[b_w].lethal_accidents += car_accidents[i].total_kills > 0 ? 1 : 0;
                        query_results_3[b_w].lethal_average =
                                (float) query_results_3[b_w].lethal_accidents /
                                (float) query_results_3[b_w].total_accidents;
                    }
                }
            }

        }
#pragma omp barrier
        cout << "----- QUERY 1 -----" << endl;
        for (const auto&[k, v] : query_results_1)
            std::cout << "week[" << k << "] = total_lethal_accidents(" << v.total_accidents << ") " << std::endl;
        cout << "-------------------" << endl;

        cout << "----- QUERY 2 -----" << endl;
        for (const auto&[k, v] : query_results_2)
            std::cout << "factor[" << k << "] = total_accidents(" << v.total_accidents << ") | lethal_accidents("
                      << v.lethal_accidents << ") | lethal_percentage(" << v.lethal_percentage << ") " << std::endl;
        cout << "-------------------" << endl;

        cout << "----- QUERY 3 -----" << endl;
        for (const auto&[k, v] : query_results_3)
            std::cout << "Borough[" << k << "] = total_accidents(" << v.total_accidents << ") | lethal_accidents("
                      << v.lethal_accidents << ") | average_lethal(" << v.lethal_average << ") " << std::endl;
        cout << "-------------------" << endl;
    }
};


#endif //MW_OPEN_MPI_QUERY_H
