#!/bin/sh
#
# Generate some statistics about the project
######################

# Line of code
loc_java=$( (find .. -name '*.java' -print0 | xargs -0 cat ) | wc -l)
loc_shell=$( (find .. -name '*.sh' -print0 | xargs -0 cat ) | wc -l)
loc_xml=$( (find .. -name '*.xml' -print0 | xargs -0 cat ) | wc -l)
loc_yaml=$( (find .. -name '*.yaml' -print0 | xargs -0 cat ) | wc -l)

loc=$((loc_java + $loc_shell + $loc_xml + $loc_yaml))

printf "Lines of java code:\t %8d\n" $loc_java
printf "Lines of shell code:\t %8d\n" $loc_shell
printf "Lines of xml code:\t %8d\n" $loc_xml
printf "Lines of yaml code:\t %8d\n" $loc_yaml
printf "==================================\n"
printf "Total lines of code:\t %8d\n" $loc
printf "==================================\n"
printf "\n"

