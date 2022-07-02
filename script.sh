#!/bin/bash

for COMPUTERS in $(seq 1 23); do

    rm -f servers.txt
    head -c $COMPUTERS servers_all.txt > servers.txt

    make > log.txt

    grep Results fetched! < log.txt >> results
done