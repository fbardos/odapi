#!/bin/bash
# Script to scan models/models/intermediate for SQL files without "__" in their name
# and generate corresponding __measure.sql, __typecast.sql, and mart_*.sql files.

BASE_DIR="models/models/intermediate"
MARTS_DIR="models/models/marts"

# Ensure marts directory exists
mkdir -p "$MARTS_DIR"

# Find .sql files not containing "__" in the filename
find "$BASE_DIR" -type f -name "*.sql" ! -name "*__*.sql" | while read -r file; do
    # Remove .sql extension
    base="${file%.sql}"

    # Define new filenames in intermediate
    measure_file="${base}__measure.sql"
    typecast_file="${base}__typecast.sql"

    # Create files with the requested content
    echo "{{ intm_measures() }}" > "$measure_file"
    echo "{{ intm_typecast() }}" > "$typecast_file"

    echo "Created: $measure_file"
    echo "Created: $typecast_file"

    # Create mart file
    filename=$(basename "$file")       # e.g. intm_orders.sql
    mart_filename="${filename/intm_/mart_}"  # replace prefix
    mart_file="$MARTS_DIR/$mart_filename"

    echo "{{ mart_partition() }}" > "$mart_file"
    echo "Created: $mart_file"
done

