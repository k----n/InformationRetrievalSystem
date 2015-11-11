#!/bin/bash

sort -u -o pterms.txt pterms.txt
sort -u -o rterms.txt rterms.txt
sort -u -o scores.txt scores.txt

cat scores.txt | perl break.pl > scores_db.txt
cat pterms.txt | perl break.pl > pterms_db.txt
cat rterms.txt | perl break.pl > rterms_db.txt

db_load -f reviews.txt -T -t hash rw.idx
db_load -f scores_db.txt -T -t btree sc.idx
db_load -f pterms_db.txt -T -t btree pt.idx
db_load -f rterms_db.txt -T -t btree rt.idx

rm scores_db.txt -f
rm pterms_db.txt -f
rm rterms_db.txt -f
