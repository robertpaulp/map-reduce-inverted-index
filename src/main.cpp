#include <pthread.h>
#include <vector>
#include <iostream>
#include <map>
#include <queue>
#include <tuple>
#include <string>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <algorithm>

using namespace std;

class Compare {
public:
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

// Clean word
void remove_unwanted_chars(string &word) {
    string new_word = "";

    for (int i = 0; i < word.size(); i++) {
        if (isalpha(word[i])) {
            new_word += tolower(word[i]);
        }
    }

    word = new_word;
}

// Each thread will process a file and create a map of words
unordered_map<string, size_t> map_function(tuple<string, size_t> file) {
    string line;
    unordered_map<string, size_t> local_result;

    size_t file_id = get<1>(file);
    string input_file = get<0>(file);

    ifstream fin(input_file);

    if (fin.is_open()) {
        while (getline(fin, line)) {
            stringstream ss(line);
            vector<string> words;
            string word;

            // Create a vector of words
            while (ss >> word) {
                words.push_back(word);
            }

            // Parse each word and if it's ok, add it to the map
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

size_t getFileSize(const string& filename) {
    ifstream file(filename, ios::ate);
    if (!file.is_open()) {
        cout << "Cannot open file " << filename << endl;
        return 0;
    }
    return file.tellg();
}

// Parse the input file and return a queue of files sorted in ascending order by size
priority_queue<tuple<string, size_t, size_t>, vector<tuple<string, size_t, size_t>>, Compare> get_files(string input_file) {
    priority_queue<tuple<string, size_t, size_t>, vector<tuple<string, size_t, size_t>>, Compare> file_list;
    string line;
    size_t id = 1;
    ifstream fin(input_file);

    if (fin.is_open()) {
        getline(fin, line); // Skip the first line
        while (getline(fin, line)) {
            file_list.push(make_tuple(line, getFileSize(line), id));
            id++;
        }
    }
    fin.close();    

    return file_list;
}

// Parse the grouped map and write the output to files
void write_output(map <char, vector<tuple<string, vector<size_t>>>> &group_result) {
    for (char c = 'a'; c <= 'z'; c++) {
        ofstream fout(string(1, c) + ".txt");

        if (group_result.find(c) != group_result.end()) {
            for (auto &entry : group_result[c]) {
                fout << get<0>(entry) << ":[";

                for (size_t file_id : get<1>(entry)) {
                    fout << file_id << " ";
                }
                fout << "]" << endl;
            }
        }

        fout.close();
    }
}

int main(int argc, char **argv)
{
    if (argc < 4)
    {
        cout << argv[0] << " <Map_num_threads> <Reduce_num_threads> <input_file>" << endl;
        return 1;
    }

    // Arguments
    int num_map_threads = atoi(argv[1]);
    int num_reduce_threads = atoi(argv[2]);
    string input_file = argv[3];

    // Get files
    priority_queue<tuple<string, size_t, size_t>, vector<tuple<string, size_t, size_t>>, Compare> file_queue = get_files(input_file);

    // Thread vars
    vector<tuple<size_t, unordered_map<string, vector<size_t>>>> partial_results;
    map <char, vector<tuple<string, vector<size_t>>>> group_result;
    unordered_map<string, priority_queue<size_t, vector<size_t>, greater<size_t>>> global_result;  
    vector<pthread_t> threads(num_map_threads + num_reduce_threads);
    vector<thread_data> thread_data_list(num_map_threads + num_reduce_threads);

    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, NULL);

    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, num_map_threads + num_reduce_threads);

    bool is_merged = false;
    pthread_cond_t cond;
    pthread_cond_init(&cond, NULL);

    // Create threads
    for (int i = 0; i < num_map_threads + num_reduce_threads; i++) {

        // Initialize thread data
        thread_data_list[i].file_queue = &file_queue;
        thread_data_list[i].mutex = &mutex;
        thread_data_list[i].barrier = &barrier;
        thread_data_list[i].cond = &cond;
        thread_data_list[i].partial_results = &partial_results;
        thread_data_list[i].group_result = &group_result;
        thread_data_list[i].global_result = &global_result;
        thread_data_list[i].thread_id = i;
        thread_data_list[i].is_merged = &is_merged;
        thread_data_list[i].num_map_threads = num_map_threads;
        thread_data_list[i].num_reduce_threads = num_reduce_threads;

        pthread_create(&threads[i], NULL, thread_function, &thread_data_list[i]);
    }

    // Join threads
    for (int i = 0; i < num_map_threads + num_reduce_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Write output
    write_output(group_result);

    pthread_mutex_destroy(&mutex);
    pthread_barrier_destroy(&barrier);

    return 0;
}
