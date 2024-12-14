#!/bin/sh

# Function to log all running Chromium processes
log_chromium_processes() {
  echo "Logging Chromium processes:"
  pgrep -fl chromium || echo "No Chromium processes running"
}

# Function to kill all Chromium processes on exit
cleanup() {
  echo "Chromium processes after running Playwright:"
  log_chromium_processes
  echo "Cleaning up Chromium processes..."
  pkill -f chromium
  echo "Chromium processes after cleanup:"
  log_chromium_processes
}

# Trap the EXIT signal to ensure cleanup happens on script exit
trap cleanup EXIT

# Run Playwright tests and capture the exit code
npx playwright test
exit_code=$?
cleanup

# Exit with the same exit code as npx playwright test
exit $exit_code
