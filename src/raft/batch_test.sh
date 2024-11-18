#!/bin/bash

rm -rf ./results
mkdir ./results

# Lab3A
for i in {1..20}; do
    go test -run 3A >> ./results/result3A.txt;
done  &

# Lab3B
for i in {1..20}; do 
    go test -run 3B >> ./results/result3B.txt;
done  &

# Lab3C
for i in {1..20}; do 
    go test -run 3C >> ./results/result3C.txt;
done  &

wait
echo 'finish'