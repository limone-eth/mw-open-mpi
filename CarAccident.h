//
// Created by Simone Staffa on 25/05/2020.
//

#ifndef MW_OPEN_MPI_CARACCIDENT_H
#define MW_OPEN_MPI_CARACCIDENT_H

#include <string>
using namespace std;

class CarAccident {
public:
    CarAccident() {}

    string date;
    string borough;

    string factor_1;
    string factor_2;
    string factor_3;
    string factor_4;
    string factor_5;

    int total_kills;
    int total_accidents;
    string week_of_year;
}car_accident;


#endif //MW_OPEN_MPI_CARACCIDENT_H
