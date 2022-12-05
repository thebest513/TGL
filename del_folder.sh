#!/bin/bash
# Input a text file containing one folder name in one line
#
# Taking `sys cmd`(backtick) output and assign it to variables
for folder in `cat folder_del.txt | tr "\r" " "`
do
# Redirecting both std output and std error, the former file size is smaller than the latter
# csh does not allow redirecting std error only
ctm deploy folder::delete CTM $folder >& $folder.json
done
