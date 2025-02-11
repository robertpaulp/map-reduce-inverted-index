#ifndef THREADING_HPP
#define THREADING_HPP

#include <pthread.h>
#include <unordered_map>
#include <queue>
#include <vector>
#include <tuple>
#include <string>
#include <map>

using namespace std;

struct Compare {
    bool operator()(const tuple<string, size_t, size_t>& a, const tuple<string, size_t, size_t>& b) {
        return get<1>(a) > get<1>(b);
    }
};

struct thread_data {
    priority_queue<tuple<string, size_t, size_t>, vector<tuple<string, size_t, size_t>>, Compare> *file_queue;
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
    pthread_cond_t *cond;
    vector<tuple<size_t, unordered_map<string, vector<size_t>>>> *partial_results;
    unordered_map<string, priority_queue<size_t, vector<size_t>, greater<size_t>>> *global_result;
    map <char, vector<tuple<string, vector<size_t>>>> *group_result;
    bool *is_merged;
    int thread_id;
    int num_map_threads;
    int num_reduce_threads;
};

void *thread_function(void *arg);

#endif // THREADING_HPP
