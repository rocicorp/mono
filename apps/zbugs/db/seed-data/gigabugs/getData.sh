set -e
BASE_URL="https://rocinante-dev.s3.us-east-1.amazonaws.com"
FILES=(
    "user_000.csv"
    "label_000.csv"
    "comment_000.csv"
    "comment_001.csv"
    "comment_002.csv"
    "comment_003.csv"
    "comment_004.csv"
    "comment_005.csv"
    "comment_006.csv"
    "comment_007.csv"
    "issueLabel_000.csv"
    "issueLabel_001.csv"
    "issueLabel_002.csv"
    "issueLabel_003.csv"
    "issueLabel_004.csv"
    "issueLabel_005.csv"
    "issueLabel_006.csv"
    "issueLabel_007.csv"
    "issue_000.csv"
    "issue_001.csv"
    "issue_002.csv"
    "issue_003.csv"
    "issue_004.csv"
    "issue_005.csv"
    "issue_006.csv"
    "issue_007.csv"
)

for file in "${FILES[@]}"; do
    echo "Downloading $file..."
    curl -L -o "$file" "$BASE_URL/$file"
done

echo "All files downloaded successfully!"