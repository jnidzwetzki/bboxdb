#!/bin/bash

$BBOXDB_HOME/bin/cli.sh -action create_dgroup -dgroup osmgroup -replicationfactor 1 -dimensions 2 -maxregionsize 10485760

$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_lightrail
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_buses
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_metro
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_nswtrains
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_ferries
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_trains

$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_adsb


