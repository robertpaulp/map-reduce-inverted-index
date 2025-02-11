#include "mappers.hpp"
#include <fstream>
#include <sstream>

// Function to clean up words (remove punctuation, convert to lowercase)
void remove_unwanted_chars(string &word) {
    string new_word = "";

    for (int i = 0; i < (int) word.size(); i++) {
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