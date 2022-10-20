#!/bin/bash

REL_DIR=`dirname "$0"`


python $REL_DIR/data_generator.py --timestamp_period=$1
