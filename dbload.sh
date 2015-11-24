#!/bin/bash

sort -u pterms.txt | perl break.pl | db_load -T -t btree -c duplicates=1 pt.idx
sort -u rterms.txt | perl break.pl | db_load -T -t btree -c duplicates=1 rt.idx
sort -u scores.txt | perl break.pl | db_load -T -t btree -c duplicates=1 sc.idx
cat reviews.txt | perl break.pl | db_load -f -T -t hash rw.idx
