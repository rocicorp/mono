import fs from 'node:fs';
const filename = './out';
// Function to initialize a log file (truncates if exists)
function initLogFile() {
  // Open with 'w' flag to truncate if exists or create if doesn't exist
  fs.writeFileSync(filename, '', 'utf8');
}

// Function to append to the log file (like console.log)
export function log(...args: unknown[]) {
  const logEntry = `[${new Date().toISOString()}] ${JSON.stringify(
    args,
    null,
    2,
  )}\n`;
  fs.appendFileSync(filename, logEntry, 'utf8');
}

initLogFile();
