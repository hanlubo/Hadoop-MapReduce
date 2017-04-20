#! /bin/bash

# if (( $# < 2 ))
if [ $# -lt 2 ]
then
    # echo "$0, $1, $2, $# , $@ , $*"
    echo "Please provide input_dir and output_dir for Word Count to run."
    echo "eg:\n  wordCountRun.sh input_dir output_dir"
    exit
fi
if [ "$2\/" != 'x' ] 
then
    hadoop fs -rm ./$2/*
    hadoop fs -rmdir ./$2
    hadoop jar wordcount.jar hadoop.WordCount $1 $2
fi
