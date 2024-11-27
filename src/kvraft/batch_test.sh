#!/bin/bash

rm -rf ./tmpout
mkdir ./tmpout

# Lab4A
for i in {1..10}; do 
    go test -run 4A -v >> ./tmpout/result4A_1.txt;
done  &

echo $!

# Lab4A
for i in {1..10}; do 
    go test -run 4A -v >> ./tmpout/result4A_2.txt;
done  &

echo $!

# Lab4A
for i in {1..10}; do 
    go test -run 4A -v >> ./tmpout/result4A_3.txt;
done  &

echo $!

# Lab4A
for i in {1..10}; do 
    go test -run 4A -v >> ./tmpout/result4A_4.txt;
done  &

echo $!

echo 'pgrep -f batch_test | xargs kill -9'

wait
echo 'finish'