#!/bin/sh
#
# Generate some statistics about the project
######################

# Line of code
loc_java=$( (find .. -name '*.java' -print0 | xargs -0 cat ) | wc -l)

echo "Lines of java code: $loc_java"

