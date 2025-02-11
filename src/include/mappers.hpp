#ifndef MAPPERS_HPP
#define MAPPERS_HPP

#include <unordered_map>
#include <vector>
#include <string>
#include <tuple>

using namespace std;

// Function prototypes
void remove_unwanted_chars(string &word);
unordered_map<string, size_t> map_function(tuple<string, size_t> file);

#endif // MAPPERS_HPP
