import {writeFileSync, readFileSync, existsSync, copyFileSync} from 'node:fs';
import dotenv from 'dotenv';
import {execSync} from 'node:child_process';
import process from 'node:process';

dotenv.config();

if (process.env.SKIP_ENSURE_ENV && process.env.SKIP_ENSURE_ENV === 'true') {
  console.log(
    'SKIP_ENSURE_ENV was set to "true". Not automatically setting: VITE_SUPA_ANON_KEY, VITE_SUPABASE_URL, UPSTREAM_URI',
  );
  process.exit(0);
}

try {
  if (!existsSync('.env')) {
    copyFileSync('.env.example', '.env');
  }
  const envContent = readFileSync('.env', 'utf-8');

  const supabaseStatus = JSON.parse(
    execSync('supabase status -o json', {encoding: 'utf-8'}),
  );

  // Function to update a specific key in the .env content
  const updateEnvVariable = (content, key, value) => {
    const regex = new RegExp(`^${key}[\\s]+=[\\s]+".*"$`, 'm');
    if (regex.test(content)) {
      return content.replace(regex, `${key} = "${value}"`);
    } else {
      return `${content}\n${key} = "${value}"`;
    }
  };

  let updatedEnvContent = updateEnvVariable(
    envContent,
    'VITE_SUPABASE_ANON_KEY',
    supabaseStatus.ANON_KEY,
  );
  updatedEnvContent = updateEnvVariable(
    updatedEnvContent,
    'VITE_SUPABASE_URL',
    supabaseStatus.API_URL,
  );
  updatedEnvContent = updateEnvVariable(
    updatedEnvContent,
    'UPSTREAM_URI',
    supabaseStatus.DB_URL,
  );
  updatedEnvContent = updateEnvVariable(
    updatedEnvContent,
    'CVR_DB_URI',
    updateDatabaseUrl(supabaseStatus.DB_URL, 'zero_cvr'),
  );
  updatedEnvContent = updateEnvVariable(
    updatedEnvContent,
    'CHANGE_DB_URI',
    updateDatabaseUrl(supabaseStatus.DB_URL, 'zero_changelog'),
  );

  writeFileSync('.env', updatedEnvContent);

  console.log('Successfully updated .env file');
} catch (error) {
  console.error('Error updating .env file:', error.message);
}

function updateDatabaseUrl(originalUrl, newDbName) {
  const url = new URL(originalUrl);
  url.username = 'postgres.pooler-dev';
  url.port = '54329';
  url.pathname = `/${newDbName}`;
  return url.toString();
}
