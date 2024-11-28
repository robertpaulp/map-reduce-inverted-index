#include <pthread.h>
#include <vector>
#include <iostream>
#include <map>
#include <list>
#include <queue>
#include <tuple>
#include <string>
#include <fstream>
#include <sstream>
#include <algorithm>

using namespace std;

struct thread_data
{
    queue<tuple<string, size_t>> *file_queue;
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
    map<string, vector<size_t>> *result;
    int thread_id;
    int num_map_threads;
    int num_reduce_threads;
};

void remove_unwanted_chars(string &word) {
    string new_word = "";

    for (int i = 0; i < word.size(); i++) {
        if (isalpha(word[i])) {
            new_word += tolower(word[i]);
        }
    }

    word = new_word;
}

map<string, size_t>  map_function(tuple<string, size_t> file) {
    string line;
    map<string, size_t> local_result;

    size_t file_id = get<1>(file);
    string input_file = get<0>(file);

    ifstream fin(input_file);

    if (fin.is_open()) {
        while(getline(fin, line)) {
            // Process line

            stringstream ss(line);
            vector<string> words;
            string  word;

            while (ss >> word) {
                words.push_back(word);
            }

            for (string &word : words) {
                remove_unwanted_chars(word);

                if (word.size() > 0) {
                    if (local_result.find(word) == local_result.end()) {
                        local_result[word] = file_id;
                    }
                }
            }
        }
    }
    fin.close();

    return local_result;
}

bool sort_by_entry(const tuple<string, vector<size_t>> &a, const tuple<string, vector<size_t>> &b) {
    if (get<1>(a).size() == get<1>(b).size()) {
        return get<0>(a) < get<0>(b);
    } else {
        return get<1>(a).size() > get<1>(b).size();
    }
}

void reduce_function(thread_data *data) {
    
    map<char, vector<tuple<string, vector<size_t>>>> group_result;

    // Group result
    for (char c = 'a'; c <= 'z'; c++) {
        for (auto &entry : *data->result) {
            if (entry.first[0] == c) {
                if (group_result.find(c) == group_result.end()) {
                    group_result[c] = vector<tuple<string, vector<size_t>>>();
                }
                group_result[c].push_back(make_tuple(entry.first, entry.second));
            }
        }
    }

    // Sort group result
    for (char c = 'a'; c <= 'z'; c++) {
        if (group_result.find(c) != group_result.end()) {
            sort(group_result[c].begin(), group_result[c].end(), sort_by_entry);
        }
    }

    // Write result
    for (char c = 'a'; c <= 'z'; c++) {
        ofstream fout(string(1, c) + ".txt");

        if (group_result.find(c) != group_result.end()) {
            for (auto &entry : group_result[c]) {
                fout << get<0>(entry) << ":[";
                sort(get<1>(entry).begin(), get<1>(entry).end());
                for (size_t file_id : get<1>(entry)) {
                    if (file_id != get<1>(entry).back()) {
                        fout << file_id << " ";
                    } else {
                        fout << file_id;
                    }
                }
                fout << "]" << endl;
            }
        } else {
            fout << endl;
        }

        fout.close();
    }
}


void *thread_function(void *arg) {
    thread_data *data = (thread_data *)arg;

    if (data->thread_id < data->num_map_threads) {
        // Map

        while(true) {
            tuple<string, size_t> file;

            // Get file
            pthread_mutex_lock(data->mutex);

            if (data->file_queue->empty()) {
                pthread_mutex_unlock(data->mutex);
                break;
            } else {
                file = data->file_queue->front();
                data->file_queue->pop();
                pthread_mutex_unlock(data->mutex);
            }

            map<string, size_t> local_result = map_function(file);

            // Merge local result with global result
            pthread_mutex_lock(data->mutex);

            for (auto &entry : local_result) {
                if (data->result->find(entry.first) == data->result->end()) {
                    (*data->result)[entry.first] = vector<size_t>();
                }
                (*data->result)[entry.first].push_back(entry.second);
            }

            pthread_mutex_unlock(data->mutex);   
        }

        pthread_barrier_wait(data->barrier);
    } else {
        // Reduce
        pthread_barrier_wait(data->barrier);

        if (data->thread_id == data->num_map_threads) {
            reduce_function(data);
        }
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

bool sort_by_file_size(const tuple<string, size_t, size_t> &a, const tuple<string, size_t, size_t> &b) {
    return get<1>(a) < get<1>(b);
}

queue<tuple<string, size_t>> get_files(string input_file) {
    list<tuple<string, size_t, size_t>> file_list;
    string line;
    size_t id = 1;
    ifstream fin(input_file);

    if (fin.is_open()) {
        getline(fin, line);
        while(getline(fin, line)) {
            file_list.push_back(make_tuple(line, getFileSize(line), id));
            id++;
        }
    }
    fin.close();    

    file_list.sort(sort_by_file_size);

    queue<tuple<string, size_t>> file_queue;
    for (auto &file : file_list) {
        file_queue.push(make_tuple(get<0>(file), get<2>(file)));
    }

    return file_queue;
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
    queue<tuple<string, size_t>> file_queue = get_files(input_file);
    map<string, vector<size_t>> result;

    vector<pthread_t> threads(num_map_threads + num_reduce_threads);
    vector<thread_data> thread_data_list(num_map_threads + num_reduce_threads);

    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, NULL);

    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, num_map_threads + num_reduce_threads);

    // print file queue
    // cout << "File queue: " << endl;
    // queue<tuple<string, size_t>> file_queue_copy = file_queue;
    // while (!file_queue_copy.empty()) {
    //     cout << get<0>(file_queue_copy.front()) << " " << get<1>(file_queue_copy.front()) << endl;
    //     file_queue_copy.pop();
    // }

    // Create threads
    for (int i = 0; i < num_map_threads + num_reduce_threads; i++) {

        thread_data_list[i].file_queue = &file_queue;
        thread_data_list[i].mutex = &mutex;
        thread_data_list[i].barrier = &barrier;
        thread_data_list[i].result = &result;
        thread_data_list[i].thread_id = i;
        thread_data_list[i].num_map_threads = num_map_threads;
        thread_data_list[i].num_reduce_threads = num_reduce_threads;

        pthread_create(&threads[i], NULL, thread_function, &thread_data_list[i]);
    }

    // Join threads
    for (int i = 0; i < num_map_threads + num_reduce_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    pthread_mutex_destroy(&mutex);
    pthread_barrier_destroy(&barrier);

    return 0;
}