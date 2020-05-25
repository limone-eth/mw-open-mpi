//
// Created by Simone Staffa on 25/05/2020.
//

#ifndef MW_OPEN_MPI_UTILS_H
#define MW_OPEN_MPI_UTILS_H

#include <vector>
#include <string> // copy
#include <boost/date_time/gregorian/gregorian.hpp>


using namespace std;
using namespace boost::gregorian;

class Utils {

public:
    /*
     * Returns WEEK OF YEAR given a string formatted date @date
     */
    static std::string get_week(std::string date) {
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
        short unsigned int year = std::atoi(split[2].c_str());
        short unsigned int month = std::atoi(split[0].c_str());
        short unsigned int day = std::atoi(split[1].c_str());
        boost::gregorian::date d{year, month, day};
        return to_string(d.week_number());
    }

    /*
     * Returns TRUE if elem @value is already existent in @array
     * otherwise it returns FALSE
     */
    static bool in_array(const std::string &value, const std::vector <string> &array) {
        return std::find(array.begin(), array.end(), value) != array.end();
    }
};


#endif //MW_OPEN_MPI_UTILS_H
