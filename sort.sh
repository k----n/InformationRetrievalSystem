#!/bin/bash

sort -u -o pterms.txt pterms.txt
sort -u -o pterms.txt rterms.txt
sort -u -o pterms.txt scores.txt

echo "Successfully sorted pterms.txt, rterms.txt, scores.txt"
