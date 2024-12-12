#!/bin/bash

suffix="test1"

rm -rf ./tmpout/result${suffix}*
mkdir ./tmpout
mkdir ./tmpout/result${suffix}*

# test
for i in {1..15}; do 
    go test -run TestConcurrent3_5B -v >> "./tmpout/result${suffix}_1.txt";
done  &

echo $!

# test
for i in {1..15}; do 
    go test -run TestConcurrent3_5B -v >> "./tmpout/result${suffix}_2.txt";
done  &

echo $!

# test
for i in {1..15}; do 
    go test -run TestConcurrent3_5B -v >> "./tmpout/result${suffix}_3.txt";
done  &

echo $!

# test
for i in {1..15}; do 
    go test -run TestConcurrent3_5B -v >> "./tmpout/result${suffix}_4.txt";
done  &

echo $!

echo 'pgrep -f batch_test | xargs kill -9'

wait
echo 'finish'