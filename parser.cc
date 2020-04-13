#include <iostream>     // cout, endl
#include <fstream>      // fstream
#include <vector>
#include <string>
#include <algorithm>    // copy
#include <iterator>     // ostream_operator
#include <boost/tokenizer.hpp>

using namespace std;
vector<string> headers;

int main()
{
    using namespace boost;
    string data("./sample.csv");

    ifstream in(data.c_str());
    if (!in.is_open()) return 1;

    typedef tokenizer< escaped_list_separator<char> > Tokenizer;
    vector< vector<string> > table;
    string line;

    while (getline(in,line))
    {   
        vector <string> vec;
        Tokenizer tok(line);
        vec.assign(tok.begin(),tok.end());

        // vector now contains strings from one row, output to cout here
        copy(vec.begin(), vec.end(), ostream_iterator<string>(cout, "|"));

        cout << "\n----------------------" << endl;
        if(headers.empty())
            headers = vec;
        else
            table.push_back(vec);
    }
    in.close();
}

int colIndex(string col){
    return find(headers.begin(), headers.end(), col) - headers.begin();
}