#!/bin/bash

set -e
BASE_URL="https://rocinante-dev.s3.us-east-1.amazonaws.com/"
FILES=(
    "comments_1.csv"
    "comments_2.csv"
    "comments_3.csv"
    "comments_4.csv"
    "comments_5.csv"
    "comments_6.csv"
    "comments.csv"
    "issue_labels_1.csv"
    "issue_labels_2.csv"
    "issue_labels_3.csv"
    "issue_labels_4.csv"
    "issue_labels_5.csv"
    "issue_labels_6.csv"
    "issue_labels.csv"
    "issues_2.csv"
    "issues_3.csv"
    "issues_4.csv"
    "issues_5.csv"
    "issues_6.csv"
    "issues.csv"
    "labels.csv"
    "users.csv"
)

mkdir -p data/1gb

for file in "${FILES[@]}"; do
    echo "Downloading $file..."
    curl -L -o "data/1gb/$file" "$BASE_URL/$file"
done

echo "All files downloaded successfully!"
