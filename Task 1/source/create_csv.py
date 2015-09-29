#!/usr/bin/python

import os;

def get_filepaths(directory):
    file_paths = []  # List which will store all of the full filepaths.

    # Walk the tree.
    for root, directories, files in os.walk(directory):
        for filename in files:
            # Join the two strings in order to form the full filepath.
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)  # Add it to the list.

    return file_paths

full_file_paths = get_filepaths("/Users/puneeth/Documents/mapreduce/Project/download/training_set_testing1")[0:]
# print full_file_paths;

with open('/Users/puneeth/Documents/mapreduce/Project/download/training_set_testing1/small_csv_ready.txt', 'w') as outfile:
    for fname in full_file_paths:
        with open(fname) as infile:
            movieID = 0;
            for line in infile:
              # Obtain the movie ID if the line ends in ":"
                if line[-2] == ":":
                    print "Found the movie ID:",line[0:-2]
                    movieID = line[0:-2]
                    continue
                else:
                    if movieID != 0:
                        outputStr = str(movieID)+","+line
                        outfile.write(outputStr)

