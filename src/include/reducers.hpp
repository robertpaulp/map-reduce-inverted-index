#ifndef REDUCERS_HPP
#define REDUCERS_HPP

#include <unordered_map>
#include <vector>
#include <string>
#include <map>
#include <tuple>

using namespace std;

struct thread_data; // Forward declaration

// Function prototypes
bool sort_by_entry(const tuple<string, vector<size_t>> &a, const tuple<string, vector<size_t>> &b);
void reduce_function(thread_data *data);

#endif // REDUCERS_HPP
