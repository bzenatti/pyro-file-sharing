#!/usr/bin/env bash
# Creates 50 files named file1.txt … file50.txt, each containing its own name.

for i in {1..50}; do
  filename="file${i}.txt"
  echo "${filename}" > "${filename}"
done

echo "Created files file1.txt through file50.txt in $(pwd)"
