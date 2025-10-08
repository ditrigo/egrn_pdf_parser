#!/bin/bash
set -e

BASE_DIR="./extracted"

for dir in "$BASE_DIR"/*/; do
  echo ">>> Обрабатываю папку: $dir"

  python main.py \
    --db_type sqlite \
    --sqlite_path "egrn_database.sqlite" \
    --xml_directory "$dir" \
    --output_csv "$dir/restrict_records.csv" \
    --output_xlsx "$dir/restrict_records.xlsx" \
    --log_file "parser.log"
done
