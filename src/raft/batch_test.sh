#!/bin/bash

rm -rf ./results
mkdir ./results

# Lab3A
for i in {1..50}; do
    go test -run 3A >> ./results/result3A.txt;
done  &

echo $!

# Lab3B
for i in {1..50}; do 
    go test -run 3B >> ./results/result3B.txt;
done  &

echo $!

# Lab3C
for i in {1..50}; do 
    go test -run 3C >> ./results/result3C.txt;
done  &

echo $!

# Lab3D
for i in {1..50}; do 
    go test -run 3D >> ./results/result3D.txt;
done  &

echo $!

echo 'pgrep -f batch_test | xargs kill -9'

wait
echo 'finish'