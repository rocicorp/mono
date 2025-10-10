import {readdir, readFile, writeFile} from 'fs/promises';
import {dirname, join, relative} from 'path';

interface PackageDependency {
  source: string;
  target: string;
  importPath: string;
  file: string;
}

interface PackageJson {
  name?: string;
  version?: string;
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
}

async function findTypeScriptFiles(dir: string): Promise<string[]> {
  const files: string[] = [];

  try {
    const entries = await readdir(dir, {withFileTypes: true});

    for (const entry of entries) {
      const fullPath = join(dir, entry.name);

      if (entry.isDirectory()) {
        // Skip common directories we don't want to analyze
        if (
          ![
            'node_modules',
            '.git',
            'dist',
            'build',
            '__tests__',
            'test',
            'tests',
          ].includes(entry.name)
        ) {
          files.push(...(await findTypeScriptFiles(fullPath)));
        }
      } else if (entry.isFile()) {
        // Include TypeScript files, excluding test files
        if (
          entry.name.endsWith('.ts') &&
          !entry.name.includes('.test.') &&
          !entry.name.includes('.pg-test.') &&
          !entry.name.includes('.spec.') &&
          !entry.name.endsWith('.d.ts')
        ) {
          files.push(fullPath);
        }
      }
    }
  } catch (error) {
    // Ignore directories we can't read
  }

  return files;
}

function getPackageName(filePath: string): string | null {
  const parts = filePath.split('/');
  if (parts.length >= 2 && (parts[0] === 'packages' || parts[0] === 'apps')) {
    return `${parts[0]}/${parts[1]}`;
  }
  return null;
}

function extractImports(
  content: string,
): Array<{path: string; line: number; ignored: boolean}> {
  const imports: Array<{path: string; line: number; ignored: boolean}> = [];
  const lines = content.split('\n');

  // Match import statements and export-from statements
  const importRegex =
    /(?:import\s+(?:(?:\{[^}]*\}|\*\s+as\s+\w+|\w+)(?:\s*,\s*(?:\{[^}]*\}|\*\s+as\s+\w+|\w+))*\s+from\s+)?|export\s+(?:\{[^}]*\}|\*)\s+from\s+)['"]([^'"]+)['"]/g;

  let match;
  while ((match = importRegex.exec(content)) !== null) {
    // Calculate line number by counting newlines up to this point
    const lineNumber = content.substring(0, match.index).split('\n').length;

    // Check if the line or previous line has an ignore comment
    const currentLine = lines[lineNumber - 1] || '';
    const previousLine = lines[lineNumber - 2] || '';

    // Current line: inline comment is fine
    const currentLineIgnored = currentLine.includes('@circular-dep-ignore');

    // Previous line: must be a comment-only line (not code with inline comment)
    const previousLineIgnored =
      previousLine.includes('@circular-dep-ignore') &&
      previousLine.trim().startsWith('//');

    const ignored = currentLineIgnored || previousLineIgnored;

    imports.push({path: match[1], line: lineNumber, ignored});
  }

  return imports;
}

async function analyzePackageDependencies(): Promise<{
  packageDeps: Map<string, Set<string>>;
  exampleFiles: Map<
    string,
    {source: string; sourceLine: number; target: string}
  >;
  importLocations: Map<
    string,
    Map<string, Array<{file: string; line: number}>>
  >;
}> {
  console.log('Finding TypeScript files...');

  const packagesFiles = await findTypeScriptFiles('packages');
  const appsFiles = await findTypeScriptFiles('apps');
  const allFiles = [...packagesFiles, ...appsFiles];

  console.log(`Found ${allFiles.length} TypeScript files to analyze`);

  const packageDeps = new Map<string, Set<string>>();
  // Map from "sourcePackage -> targetPackage" to an example file pair (source:line -> target)
  const exampleFiles = new Map<
    string,
    {source: string; sourceLine: number; target: string}
  >();
  // Map from sourcePackage to targetWorkspaceName to list of import locations
  const importLocations = new Map<
    string,
    Map<string, Array<{file: string; line: number}>>
  >();

  for (const filePath of allFiles) {
    const sourcePackage = getPackageName(filePath);
    if (!sourcePackage) continue;

    try {
      const content = await readFile(filePath, 'utf-8');
      const imports = extractImports(content);

      for (const importInfo of imports) {
        // Skip imports with @circular-dep-ignore comment
        if (importInfo.ignored) {
          continue;
        }

        // Check if this is a relative import that goes to another package
        if (importInfo.path.startsWith('../')) {
          // Resolve the relative path
          const fileDir = dirname(filePath);
          const resolvedPath = join(fileDir, importInfo.path);
          const normalizedPath = relative('.', resolvedPath).replace(
            /\\/g,
            '/',
          );

          const targetPackage = getPackageName(normalizedPath);
          if (targetPackage && targetPackage !== sourcePackage) {
            if (!packageDeps.has(sourcePackage)) {
              packageDeps.set(sourcePackage, new Set());
            }
            packageDeps.get(sourcePackage)!.add(targetPackage);

            // Store an example file pair for this dependency edge
            const edgeKey = `${sourcePackage} -> ${targetPackage}`;
            if (!exampleFiles.has(edgeKey)) {
              // Use the resolved import path as-is (imports always have full extension)
              exampleFiles.set(edgeKey, {
                source: filePath,
                sourceLine: importInfo.line,
                target: normalizedPath,
              });
            }

            // Track import location by target package path (not workspace name yet)
            if (!importLocations.has(sourcePackage)) {
              importLocations.set(sourcePackage, new Map());
            }
            const packageImports = importLocations.get(sourcePackage)!;
            if (!packageImports.has(targetPackage)) {
              packageImports.set(targetPackage, []);
            }
            packageImports.get(targetPackage)!.push({
              file: filePath,
              line: importInfo.line,
            });
          }
        }
      }
    } catch (error) {
      console.error(`Error reading file ${filePath}:`, error);
    }
  }

  return {packageDeps, exampleFiles, importLocations};
}

function findCircularDependencies(
  packageDeps: Map<string, Set<string>>,
): Array<string[]> {
  const cycles: Array<string[]> = [];
  const visited = new Set<string>();
  const recursionStack = new Set<string>();

  function dfs(pkg: string, path: string[]): void {
    if (recursionStack.has(pkg)) {
      // Found a cycle - extract it from the path
      const cycleStart = path.indexOf(pkg);
      const cycle = [...path.slice(cycleStart), pkg];

      // Normalize cycle (start with smallest package name to avoid duplicates)
      const normalized = normalizeCycle(cycle);

      // Check if we already found this cycle
      const cycleStr = normalized.join(' -> ');
      if (!cycles.some(c => c.join(' -> ') === cycleStr)) {
        cycles.push(normalized);
      }
      return;
    }

    if (visited.has(pkg)) {
      return;
    }

    recursionStack.add(pkg);
    path.push(pkg);

    const deps = packageDeps.get(pkg);
    if (deps) {
      for (const dep of deps) {
        dfs(dep, path);
      }
    }

    path.pop();
    recursionStack.delete(pkg);
    visited.add(pkg);
  }

  function normalizeCycle(cycle: string[]): string[] {
    // Remove the duplicate last element
    const cleanCycle = cycle.slice(0, -1);

    // Find the lexicographically smallest element
    let minIndex = 0;
    for (let i = 1; i < cleanCycle.length; i++) {
      if (cleanCycle[i] < cleanCycle[minIndex]) {
        minIndex = i;
      }
    }

    // Rotate the cycle to start with the smallest element
    const rotated = [
      ...cleanCycle.slice(minIndex),
      ...cleanCycle.slice(0, minIndex),
    ];

    // Add back the starting element at the end to show the cycle
    return [...rotated, rotated[0]];
  }

  for (const pkg of packageDeps.keys()) {
    if (!visited.has(pkg)) {
      dfs(pkg, []);
    }
  }

  return cycles;
}

async function getPackageJson(
  packagePath: string,
): Promise<PackageJson | null> {
  try {
    const pkgJsonPath = join(packagePath, 'package.json');
    const content = await readFile(pkgJsonPath, 'utf-8');
    return JSON.parse(content);
  } catch {
    return null;
  }
}

async function getWorkspaceName(packagePath: string): Promise<string | null> {
  // Read the actual package name from package.json
  const pkgJson = await getPackageJson(packagePath);
  return pkgJson?.name || null;
}

async function fixPackageJson(
  packagePath: string,
  missingDeps: Array<{name: string; version: string}>,
): Promise<void> {
  const pkgJsonPath = join(packagePath, 'package.json');
  const content = await readFile(pkgJsonPath, 'utf-8');
  const pkgJson = JSON.parse(content);

  // Initialize devDependencies if it doesn't exist
  if (!pkgJson.devDependencies) {
    pkgJson.devDependencies = {};
  }

  // Add missing dependencies with their actual versions
  for (const dep of missingDeps) {
    pkgJson.devDependencies[dep.name] = dep.version;
  }

  // Sort devDependencies alphabetically
  const sortedDevDeps: Record<string, string> = {};
  for (const key of Object.keys(pkgJson.devDependencies).sort()) {
    sortedDevDeps[key] = pkgJson.devDependencies[key];
  }
  pkgJson.devDependencies = sortedDevDeps;

  // Write back with proper formatting
  await writeFile(pkgJsonPath, JSON.stringify(pkgJson, null, 2) + '\n');
}

async function verifyPackageJsonDependencies(fix: boolean) {
  console.log('\nVerifying package.json dependencies...\n');

  const {packageDeps, exampleFiles, importLocations} =
    await analyzePackageDependencies();

  // Check for circular dependencies first
  const circularDeps = findCircularDependencies(packageDeps);
  if (circularDeps.length > 0) {
    console.log('⚠️  Found circular dependencies:\n');
    for (const cycle of circularDeps) {
      console.log(`  ${cycle.join(' -> ')}`);
      // Show example files for each edge in cycle
      for (let i = 0; i < cycle.length - 1; i++) {
        const edgeKey = `${cycle[i]} -> ${cycle[i + 1]}`;
        const exampleFilePair = exampleFiles.get(edgeKey);
        if (exampleFilePair) {
          console.log(
            `    ${exampleFilePair.source}:${exampleFilePair.sourceLine} -> ${exampleFilePair.target}`,
          );
        }
      }
    }
    console.log();
  }

  const missingDeps: Array<{
    package: string;
    missing: Array<{name: string; version: string}>;
  }> = [];
  const versionMismatches: Array<{
    package: string;
    dependency: string;
    expected: string;
    actual: string;
  }> = [];
  const extraDeps: Array<{
    package: string;
    extra: Array<string>;
  }> = [];

  // Build a map of all workspace package names
  const allWorkspacePackages = new Set<string>();
  for (const pkg of packageDeps.keys()) {
    const pkgJson = await getPackageJson(pkg);
    if (pkgJson?.name) {
      allWorkspacePackages.add(pkgJson.name);
    }
  }

  for (const [sourcePackage, deps] of packageDeps) {
    const pkgJson = await getPackageJson(sourcePackage);

    if (!pkgJson) {
      console.log(`⚠️  ${sourcePackage}: No package.json found`);
      continue;
    }

    const allDeclaredDeps = {
      ...pkgJson.dependencies,
      ...pkgJson.devDependencies,
    };

    const missing: Array<{name: string; version: string}> = [];

    // Build a map from targetPackage to workspace name for this source package
    const targetPackageToWorkspaceName = new Map<string, string>();
    for (const targetPackage of deps) {
      const targetPkgJson = await getPackageJson(targetPackage);
      const targetWorkspaceName = targetPkgJson?.name;
      if (targetWorkspaceName) {
        targetPackageToWorkspaceName.set(targetPackage, targetWorkspaceName);
      }
    }

    // Convert importLocations from targetPackage to targetWorkspaceName
    const workspaceImportLocations = new Map<
      string,
      Array<{file: string; line: number}>
    >();
    const packageImportLocs = importLocations.get(sourcePackage);
    if (packageImportLocs) {
      for (const [targetPackage, locations] of packageImportLocs) {
        const workspaceName = targetPackageToWorkspaceName.get(targetPackage);
        if (workspaceName) {
          if (!workspaceImportLocations.has(workspaceName)) {
            workspaceImportLocations.set(workspaceName, []);
          }
          workspaceImportLocations.get(workspaceName)!.push(...locations);
        }
      }
    }

    // Collect the workspace names of actual dependencies
    const actualWorkspaceDeps = new Set<string>();
    for (const targetPackage of deps) {
      const targetPkgJson = await getPackageJson(targetPackage);
      const targetWorkspaceName = targetPkgJson?.name;
      const targetVersion = targetPkgJson?.version || '0.0.0';

      if (!targetWorkspaceName) continue;

      actualWorkspaceDeps.add(targetWorkspaceName);

      // Check if the dependency is declared
      if (!allDeclaredDeps[targetWorkspaceName]) {
        missing.push({name: targetWorkspaceName, version: targetVersion});
      } else {
        // Check if version matches
        const declaredVersion = allDeclaredDeps[targetWorkspaceName];
        if (declaredVersion !== targetVersion) {
          versionMismatches.push({
            package: sourcePackage,
            dependency: targetWorkspaceName,
            expected: targetVersion,
            actual: declaredVersion,
          });
        }
      }
    }

    if (missing.length > 0) {
      missingDeps.push({
        package: sourcePackage,
        missing: missing.sort((a, b) => a.name.localeCompare(b.name)),
      });
    }

    // Check for extra workspace dependencies (declared but not used)
    const extra: Array<string> = [];
    for (const declaredDep of Object.keys(allDeclaredDeps)) {
      // Only check workspace packages
      if (
        allWorkspacePackages.has(declaredDep) &&
        !actualWorkspaceDeps.has(declaredDep)
      ) {
        extra.push(declaredDep);
      }
    }

    if (extra.length > 0) {
      extraDeps.push({
        package: sourcePackage,
        extra: extra.sort(),
      });
    }
  }

  const hasIssues =
    missingDeps.length > 0 ||
    versionMismatches.length > 0 ||
    extraDeps.length > 0;

  if (!hasIssues) {
    console.log(
      '✅ All internal dependencies are properly declared in package.json files!',
    );
    return true;
  }

  if (fix) {
    if (missingDeps.length > 0) {
      console.log('🔧 Fixing missing dependencies in package.json files...\n');

      for (const {package: pkg, missing} of missingDeps) {
        console.log(`${pkg}:`);
        console.log(`  Adding devDependencies:`);
        for (const dep of missing) {
          console.log(`    + ${dep.name}@${dep.version}`);
        }
        await fixPackageJson(pkg, missing);
        console.log();
      }
    }

    if (versionMismatches.length > 0) {
      console.log('🔧 Fixing version mismatches...\n');

      // Group mismatches by package
      const mismatchesByPackage = new Map<
        string,
        Array<{name: string; version: string}>
      >();
      for (const mismatch of versionMismatches) {
        if (!mismatchesByPackage.has(mismatch.package)) {
          mismatchesByPackage.set(mismatch.package, []);
        }
        mismatchesByPackage.get(mismatch.package)!.push({
          name: mismatch.dependency,
          version: mismatch.expected,
        });
      }

      for (const [pkg, deps] of mismatchesByPackage) {
        console.log(`${pkg}:`);
        console.log(`  Updating devDependencies:`);
        for (const dep of deps) {
          console.log(`    ~ ${dep.name}@${dep.version}`);
        }
        await fixPackageJson(pkg, deps);
        console.log();
      }
    }

    if (extraDeps.length > 0) {
      console.log(
        '⚠️  Extra dependencies detected (not automatically removed):\n',
      );

      for (const {package: pkg, extra} of extraDeps) {
        console.log(`${pkg}:`);
        console.log(`  Unused workspace dependencies:`);
        for (const dep of extra) {
          console.log(`    ? ${dep}`);
        }
        console.log();
      }
    }

    const total = missingDeps.length + versionMismatches.length;
    console.log(
      `✅ Fixed ${total} issue(s). Run 'npm install' to update lockfile.`,
    );
    if (extraDeps.length > 0) {
      console.log(
        `⚠️  ${extraDeps.length} package(s) with extra dependencies (review manually)`,
      );
    }
    return extraDeps.length === 0;
  } else {
    let hasErrors = false;

    if (missingDeps.length > 0) {
      console.log('❌ Found missing dependencies in package.json files:\n');

      for (const {package: pkg, missing} of missingDeps) {
        console.log(`${pkg}:`);
        console.log(`  Missing devDependencies:`);

        // Get import locations for this package
        const packageImportLocs = importLocations.get(pkg);
        const pkgWorkspaceImportLocations = new Map<
          string,
          Array<{file: string; line: number}>
        >();

        if (packageImportLocs) {
          // Get package.json for each target to map to workspace names
          for (const targetPackage of packageDeps.get(pkg) || []) {
            const targetPkgJson = await getPackageJson(targetPackage);
            const targetWorkspaceName = targetPkgJson?.name;
            if (targetWorkspaceName && packageImportLocs.has(targetPackage)) {
              if (!pkgWorkspaceImportLocations.has(targetWorkspaceName)) {
                pkgWorkspaceImportLocations.set(targetWorkspaceName, []);
              }
              pkgWorkspaceImportLocations
                .get(targetWorkspaceName)!
                .push(...packageImportLocs.get(targetPackage)!);
            }
          }
        }

        for (const dep of missing) {
          console.log(`    - ${dep.name}@${dep.version}`);

          // Show import locations
          const locations = pkgWorkspaceImportLocations.get(dep.name);
          if (locations && locations.length > 0) {
            // Show first 3 locations
            for (const loc of locations.slice(0, 3)) {
              console.log(`      ${loc.file}:${loc.line}`);
            }
            if (locations.length > 3) {
              console.log(`      ... and ${locations.length - 3} more`);
            }
          }
        }
        console.log();
      }
      hasErrors = true;
    }

    if (versionMismatches.length > 0) {
      console.log('❌ Found version mismatches in package.json files:\n');

      for (const mismatch of versionMismatches) {
        console.log(`${mismatch.package}:`);
        console.log(
          `  ${mismatch.dependency}: expected ${mismatch.expected}, got ${mismatch.actual}`,
        );
      }
      console.log();
      hasErrors = true;
    }

    if (extraDeps.length > 0) {
      console.log('❌ Found extra dependencies in package.json files:\n');

      for (const {package: pkg, extra} of extraDeps) {
        console.log(`${pkg}:`);
        console.log(`  Unused workspace dependencies:`);
        for (const dep of extra) {
          console.log(`    ? ${dep}`);
        }
        console.log();
      }
      hasErrors = true;
    }

    if (hasErrors) {
      console.log(
        `Summary: ${missingDeps.length} package(s) with missing dependencies, ${versionMismatches.length} version mismatch(es), ${extraDeps.length} package(s) with extra dependencies`,
      );
      console.log('\nRun with --fix to automatically fix these issues');
    }
    return false;
  }
}

async function main() {
  try {
    const fix = process.argv.includes('--fix');
    const isValid = await verifyPackageJsonDependencies(fix);
    process.exit(isValid ? 0 : 1);
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

void main();
