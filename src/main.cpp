#include <pthread.h>
#include <vector>
#include <iostream>
#include <map>
#include <list>
#include <queue>
#include <tuple>
#include <string>
#include <fstream>
#include <algorithm>

using namespace std;

struct thread_data
{
    queue<string> *file_queue;
    pthread_mutex_t *mutex;
    map<string, vector<int>> *result;
    int thread_id;
    bool *is_done;
    int num_map_threads;
    int num_reduce_threads;
};

void map_function(string input_file) {
    //cout << "Mapping file: " << input_file << endl;
}

void reduce_function() {
    //cout << "Reducing" << endl;
}

void *thread_function(void *arg) {
    // map_function("input.txt");
    // reduce_function();
    thread_data *data = (thread_data *)arg;

    if (data->thread_id < data->num_map_threads) {
        cout << "Thread " << data->thread_id << " is mapping";
        // print queue
        while (!data->file_queue->empty()) {
            cout << " " << data->file_queue->front();
            data->file_queue->pop();
        }
        cout << endl;
    } else {
        cout << "Thread " << data->thread_id << " is reducing" << endl;
    }
    return NULL;
}

size_t getFileSize(const string& filename) {
    ifstream file(filename, ios::ate);
    if (!file.is_open()) {
        cout << "Cannot open file " << filename << endl;
        return 0;
    }
    return file.tellg();
}

bool sort_by_file_size(const tuple<string, size_t> &a, const tuple<string, size_t> &b) {
    return get<1>(a) > get<1>(b);
}

list<tuple<string, size_t>> get_files(string input_file) {
    list<tuple<string, size_t>> file_list;
    string line;
    ifstream fin(input_file);

    if (fin.is_open()) {
        getline(fin, line);
        while(getline(fin, line)) {
            file_list.push_back(make_tuple(line, getFileSize(line)));
        }
    }
    

    // sort the file list based on the file size
    file_list.sort(sort_by_file_size);

    fin.close();
    return file_list;
}

vector<queue<string>> distribute_files(list<tuple<string, size_t>> &file_list, int num_threads) {
    vector<queue<string>> thread_queues(num_threads);
    vector<size_t> thread_sizes(num_threads, 0);

    for (auto &file : file_list) {
        int smallest_thread = distance(thread_sizes.begin(), min_element(thread_sizes.begin(), thread_sizes.end()));
        thread_queues[smallest_thread].push(get<0>(file));
        thread_sizes[smallest_thread] += get<1>(file);
    }

    return thread_queues;
}

int main(int argc, char **argv)
{
    if (argc < 4)
    {
        cout << argv[0] << " <Map_num_threads> <Reduce_num_threads> <input_file>" << endl;
        return 1;
    }

    int num_map_threads = atoi(argv[1]);
    int num_reduce_threads = atoi(argv[2]);
    string input_file = argv[3];
    list<tuple<string, size_t>> file_list = get_files(input_file);

    vector<pthread_t> threads(num_map_threads + num_reduce_threads);
    vector<thread_data> thread_data_list(num_map_threads + num_reduce_threads);
    vector<queue<string>> thread_queues = distribute_files(file_list, num_map_threads);

    // Create threads
    for (int i = 0; i < num_map_threads + num_reduce_threads; i++) {

        thread_data_list[i].file_queue = &thread_queues[i];
        thread_data_list[i].mutex = NULL;
        thread_data_list[i].result = NULL;
        thread_data_list[i].thread_id = i;
        thread_data_list[i].is_done = NULL;
        thread_data_list[i].num_map_threads = num_map_threads;
        thread_data_list[i].num_reduce_threads = num_reduce_threads;

        pthread_create(&threads[i], NULL, thread_function, &thread_data_list[i]);
    }



    // Join threads
    for (int i = 0; i < num_map_threads + num_reduce_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    return 0;
}