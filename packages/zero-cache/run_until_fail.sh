#!/bin/bash

count=0

echo "Running 'npm run test -- change-source.pg-test.ts' repeatedly until failure..."

while true; do
    # Start timing
    start_time=$(date +%s.%N)
    
    # Capture both stdout and stderr
    output=$(npm run test -- change-source.pg-test.ts 2>&1)
    exit_code=$?
    
    # Calculate duration
    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc)
    
    if [ $exit_code -eq 0 ]; then
        count=$((count + 1))
        printf "Successful run #%d (%.2fs)\n" $count $duration
    else
        echo ""
        printf "Test failed after %d successful attempts (last run: %.2fs)\n" $count $duration
        echo ""
        echo "=== Test output (stdout/stderr) ==="
        echo "$output"
        echo "=== End test output ==="
        exit 0
    fi
done
