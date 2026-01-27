set -e
BASE_URL="https://rocinante-dev.s3.us-east-1.amazonaws.com/synthetic_v1"

echo "Downloading synthetic seed data manifest..."
MANIFEST=$(curl -s "$BASE_URL/manifest.txt")

if [ -z "$MANIFEST" ]; then
    echo "Error: Could not fetch manifest from $BASE_URL/manifest.txt"
    echo "The synthetic data may not have been uploaded yet."
    echo ""
    echo "To generate synthetic data locally instead:"
    echo "  1. npm run generate-templates   # Requires ANTHROPIC_API_KEY"
    echo "  2. npm run generate-synthetic"
    exit 1
fi

echo "$MANIFEST" | while read -r file; do
    if [ -n "$file" ]; then
        echo "Downloading $file..."
        curl -L -o "$file" "$BASE_URL/$file"
    fi
done

echo "All files downloaded successfully!"
