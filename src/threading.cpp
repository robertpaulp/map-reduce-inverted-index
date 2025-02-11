#include "threading.hpp"
#include "mappers.hpp"
#include "reducers.hpp"
#include <iostream>

void *thread_function(void *arg) {
    thread_data *data = (thread_data *)arg;

    if (data->thread_id < data->num_map_threads) {
        unordered_map<string, vector<size_t>> thread_result;

        while (true) {
            tuple<string, size_t> file;

            pthread_mutex_lock(data->mutex);

            if (data->file_queue->empty()) {
                pthread_mutex_unlock(data->mutex);
                break;
            } else {
                tuple<string, size_t, size_t> file_tuple = data->file_queue->top();
                file = make_tuple(get<0>(file_tuple), get<2>(file_tuple));
                data->file_queue->pop();
                pthread_mutex_unlock(data->mutex);
            }

            unordered_map<string, size_t> local_result = map_function(file);

            // Add local results to thread's map
            for (auto &entry : local_result) {
                thread_result[entry.first].push_back(entry.second);
            }
        }

        // Save thread's results
        pthread_mutex_lock(data->mutex);
        data->partial_results->push_back(make_tuple(data->thread_id, thread_result));
        pthread_mutex_unlock(data->mutex);


        pthread_barrier_wait(data->barrier);
    } else {
        pthread_barrier_wait(data->barrier);

        // Merge results from map phase into global_result
        pthread_mutex_lock(data->mutex);

        if (data->thread_id == data->num_map_threads) {
            for (auto &entry : *data->partial_results) {
                for (auto &local_entry : get<1>(entry)) {
                    for (size_t file_id : local_entry.second) {
                        (*data->global_result)[local_entry.first].push(file_id);
                    }
                }
            }


            *data->is_merged = true;
            pthread_cond_broadcast(data->cond);
        } else {
            while (!(*data->is_merged)) {
                pthread_cond_wait(data->cond, data->mutex);
            }
        }

        pthread_mutex_unlock(data->mutex);

        // Perform reduce operation
        reduce_function(data);
    }

    return NULL;
}