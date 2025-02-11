#include "reducers.hpp"
#include "threading.hpp"
#include <algorithm>

// Sort by frequency in files, if equal, sort lexicographically
bool sort_by_entry(const tuple<string, vector<size_t>> &a, const tuple<string, vector<size_t>> &b) {
    if (get<1>(a).size() == get<1>(b).size()) {
        return get<0>(a) < get<0>(b);
    } else {
        return get<1>(a).size() > get<1>(b).size();
    }
}

// Each thread will work on a specific range and create a global grouped map
void reduce_function(thread_data *data) {
    int start = (data->thread_id - data->num_map_threads) * 26 / data->num_reduce_threads;
    int end = min((data->thread_id - data->num_map_threads + 1) * 26 / data->num_reduce_threads, 26);

    for (int i = start; i < end; i++) {
        char c = 'a' + i;
        vector<tuple<string, vector<size_t>>> group;

        for (auto &entry : *data->global_result) {
            if (entry.first[0] == c) {
                group.push_back(make_tuple(entry.first, vector<size_t>()));

                while (!entry.second.empty()) {
                    get<1>(group.back()).push_back(entry.second.top());
                    entry.second.pop();
                }
            }
        }

        sort(group.begin(), group.end(), sort_by_entry);

        (*data->group_result)[c] = group;
    }
}