#!/bin/sh
#*******************************************************************************
#
#    Copyright (C) 2015-2018 the BBoxDB project
#  
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#  
#      http://www.apache.org/licenses/LICENSE-2.0
#  
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License. 
#    
#*******************************************************************************
#
# Generate some statistics about the project
######################

# Is the environment configured?
if [ -z "$BBOXDB_HOME" ]; then
   echo "Your environment variable \$(BBOXDB_HOME) is empty. Please check your .bboxdbrc"
   exit -1
fi

# Lines of code
loc_java=$( (find $BBOXDB_HOME -name '*.java' -print0 | xargs -0 cat ) | wc -l)
loc_shell=$( (find $BBOXDB_HOME -name '*.sh' -print0 | xargs -0 cat ) | wc -l)
loc_python=$( (find $BBOXDB_HOME -name '*.py' -print0 | xargs -0 cat ) | wc -l)
loc_xml=$( (find $BBOXDB_HOME -name '*.xml' -print0 | xargs -0 cat ) | wc -l)
loc_yaml=$( (find $BBOXDB_HOME -name '*.yaml' -print0 | xargs -0 cat ) | wc -l)
loc_markdown=$( (find $BBOXDB_HOME -name '*.md' -print0 | xargs -0 cat ) | wc -l)

loc=$(($loc_java + $loc_shell + $loc_python + $loc_xml + $loc_yaml + $loc_markdown))

printf "Lines of java code:\t %8d\n" $loc_java
printf "Lines of shell code:\t %8d\n" $loc_shell
printf "Lines of python code:\t %8d\n" $loc_python
printf "Lines of xml code:\t %8d\n" $loc_xml
printf "Lines of yaml code:\t %8d\n" $loc_yaml
printf "Lines of markdown code:\t %8d\n" $loc_markdown
printf "==================================\n"
printf "Total lines of code:\t %8d\n" $loc
printf "==================================\n"
printf "\n"

