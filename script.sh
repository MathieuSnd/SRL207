#!/bin/bash

for COMPUTERS in $(seq 3 23); do

    rm -f servers.txt
    head -n $COMPUTERS servers_all.txt > servers.txt

    make > log.txt
    
    
    grep "Finished" < log.txt >> results
done