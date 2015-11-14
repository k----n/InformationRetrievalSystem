#!/bin/bash

sort -u pterms.txt | perl break.pl | db_load -T -t btree pt.idx
sort -u rterms.txt | perl break.pl | db_load -T -t btree rt.idx
sort -u scores.txt | perl break.pl | db_load -T -t btree sc.idx
db_load -f reviews.txt -T -t hash rw.idx
