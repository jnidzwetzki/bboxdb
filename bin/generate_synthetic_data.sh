#!/bin/bash
#
# Generate synthetic test data
#
#
#########################################

# Is the environment configured?
if [ -z "$BBOXDB_HOME" ]; then
   echo "Your environment variable \$(BBOXDB_HOME) is empty. Please check your .bboxdbrc"
   exit -1
fi

# Load all required functions and variables
source $BBOXDB_HOME/bin/bootstrap.sh

java $jvm_ops_tools -cp $classpath org.bboxdb.tools.generator.SyntheticDataGenerator "$@"

