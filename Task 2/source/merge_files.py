#!/usr/bin/env python

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

full_file_paths = get_filepaths("/home/umang/Documents/sdata/")[1:]
print full_file_paths;

with open('/home/umang/Documents/combined_file.txt', 'w') as outfile:
    for fname in full_file_paths:
        with open(fname) as infile:
            for line in infile:
                outfile.write(line)
