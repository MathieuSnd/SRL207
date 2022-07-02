#!/bin/bash

for COMPUTERS in $(seq 1 23); do

    rm -f servers.txt
    head -n $COMPUTERS servers_all.txt > servers.txt

    make > log.txt
    cat servers.txt
    grep Results fetched! < log.txt >> results
done