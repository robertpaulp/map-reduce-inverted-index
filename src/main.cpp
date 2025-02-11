#include "threading.hpp"
#include "mappers.hpp"
#include "reducers.hpp"
#include <iostream>
#include <fstream>

using namespace std;

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
