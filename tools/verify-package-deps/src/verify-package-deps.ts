/* eslint-disable no-console */
import {readdir, readFile, writeFile} from 'node:fs/promises';
import {dirname, join, relative} from 'node:path';
import {fileURLToPath} from 'node:url';
import {parseSync} from 'oxc-parser';

const __dirname = dirname(fileURLToPath(import.meta.url));
const WORKSPACE_ROOT = join(__dirname, '../../..');

type PackageJson = {
  name?: string;
  version?: string;
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
};

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
            'out',
          ].includes(entry.name)
        ) {
          files.push(...(await findTypeScriptFiles(fullPath)));
        }
      } else if (entry.isFile()) {
        // Include TypeScript files, excluding test files
        if (
          entry.name.endsWith('.ts') &&
          !entry.name.includes('.test.') &&
          !entry.name.includes('.spec.') &&
          !entry.name.endsWith('.d.ts')
        ) {
          files.push(fullPath);
        }
      }
    }
  } catch {
    // Ignore directories we can't read
  }

  return files;
}

function getPackageName(filePath: string): string | undefined {
  // Convert to relative path from workspace root
  const relativePath = relative(WORKSPACE_ROOT, filePath);
  const parts = relativePath.split('/');
  if (
    parts.length >= 2 &&
    (parts[0] === 'packages' || parts[0] === 'apps' || parts[0] === 'tools')
  ) {
    return `${parts[0]}/${parts[1]}`;
  }
  return undefined;
}

function extractImports(
  content: string,
  filePath: string,
): {path: string; line: number; ignored: boolean}[] {
  const imports: {path: string; line: number; ignored: boolean}[] = [];
  const lines = content.split('\n');

  try {
    const result = parseSync(filePath, content);

    // Extract static imports from the module info
    for (const imp of result.module.staticImports) {
      const lineNumber = getLineNumber(content, imp.start);

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

      imports.push({
        path: imp.moduleRequest.value,
        line: lineNumber,
        ignored,
      });
    }

    // Extract export-from statements
    for (const exp of result.module.staticExports) {
      for (const entry of exp.entries) {
        if (entry.moduleRequest) {
          const lineNumber = getLineNumber(content, exp.start);

          // Check if the line or previous line has an ignore comment
          const currentLine = lines[lineNumber - 1] || '';
          const previousLine = lines[lineNumber - 2] || '';

          const currentLineIgnored = currentLine.includes(
            '@circular-dep-ignore',
          );
          const previousLineIgnored =
            previousLine.includes('@circular-dep-ignore') &&
            previousLine.trim().startsWith('//');

          const ignored = currentLineIgnored || previousLineIgnored;

          imports.push({
            path: entry.moduleRequest.value,
            line: lineNumber,
            ignored,
          });
          break; // Only add once per export statement
        }
      }
    }
  } catch (_error) {
    // If parsing fails, silently skip this file
    // This can happen for files with syntax errors or unsupported syntax
  }

  return imports;
}

function getLineNumber(content: string, offset: number): number {
  return content.substring(0, offset).split('\n').length;
}

type AnalyzeResult = {
  packageDeps: Map<string, Set<string>>;
  exampleFiles: Map<
    string,
    {source: string; sourceLine: number; target: string}
  >;
  importLocations: Map<string, Map<string, {file: string; line: number}[]>>;
};

async function analyzePackageDependencies(): Promise<AnalyzeResult> {
  console.log('Finding TypeScript files...');

  const [packagesFiles, appsFiles, toolsFiles] = await Promise.all([
    findTypeScriptFiles(join(WORKSPACE_ROOT, 'packages')),
    findTypeScriptFiles(join(WORKSPACE_ROOT, 'apps')),
    findTypeScriptFiles(join(WORKSPACE_ROOT, 'tools')),
  ]);
  const allFiles = [...packagesFiles, ...appsFiles, ...toolsFiles];

  console.log(`Found ${allFiles.length} TypeScript files to analyze`);

  // Build a map from package name to package path
  const packageNameToPath = new Map<string, string>();
  for (const dir of ['packages', 'apps', 'tools']) {
    const fullDir = join(WORKSPACE_ROOT, dir);
    const entries = await readdir(fullDir, {withFileTypes: true});
    for (const entry of entries) {
      if (entry.isDirectory()) {
        const pkgPath = join(dir, entry.name);
        const pkgJson = await getPackageJson(join(WORKSPACE_ROOT, pkgPath));
        if (pkgJson?.name) {
          packageNameToPath.set(pkgJson.name, pkgPath);
        }
      }
    }
  }

  const packageDeps = new Map<string, Set<string>>();
  // Map from "sourcePackage -> targetPackage" to an example file pair (source:line -> target)
  const exampleFiles = new Map<
    string,
    {source: string; sourceLine: number; target: string}
  >();
  // Map from sourcePackage to targetWorkspaceName to list of import locations
  const importLocations = new Map<
    string,
    Map<string, {file: string; line: number}[]>
  >();

  // Process all files in parallel
  await Promise.all(
    allFiles.map(async filePath => {
      const sourcePackage = getPackageName(filePath);
      if (!sourcePackage) return;

      try {
        const content = await readFile(filePath, 'utf-8');
        const imports = extractImports(content, filePath);

        for (const importInfo of imports) {
          // Skip imports with @circular-dep-ignore comment
          if (importInfo.ignored) {
            continue;
          }

          let targetPackage: string | undefined;
          let normalizedPath: string | undefined;

          // Check if this is a relative import that goes to another package
          if (importInfo.path.startsWith('../')) {
            // Resolve the relative path
            const fileDir = dirname(filePath);
            const resolvedPath = join(fileDir, importInfo.path);
            normalizedPath = relative(WORKSPACE_ROOT, resolvedPath).replace(
              /\\/g,
              '/',
            );
            targetPackage = getPackageName(resolvedPath);
          } else {
            // Check if this is a non-relative import that matches a workspace package
            // Extract the package name (before any subpath)
            const packageName = importInfo.path
              .split('/')
              .slice(0, 2)
              .join('/');
            const targetPath = packageNameToPath.get(packageName);
            if (targetPath) {
              targetPackage = targetPath;
              normalizedPath = targetPath;
            }
          }

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
                target: normalizedPath!,
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
      } catch (error) {
        console.error(`Error reading file ${filePath}:`, error);
      }
    }),
  );

  return {packageDeps, exampleFiles, importLocations};
}

function findCircularDependencies(
  packageDeps: Map<string, Set<string>>,
): string[][] {
  const cycles: string[][] = [];
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
    return (await readPackageJsonFile(packagePath)).json;
  } catch {
    return null;
  }
}

async function readPackageJsonFile(packagePath: string): Promise<{
  path: string;
  content: string;
  json: PackageJson;
}> {
  const pkgJsonPath = join(packagePath, 'package.json');
  const content = await readFile(pkgJsonPath, 'utf-8');
  const json = JSON.parse(content) as PackageJson;
  return {path: pkgJsonPath, content, json};
}

function sortDependencies(
  deps: Record<string, string>,
): Record<string, string> {
  const sorted: Record<string, string> = {};
  for (const key of Object.keys(deps).sort()) {
    sorted[key] = deps[key];
  }
  return sorted;
}

async function writePackageJsonFile(
  pkgJsonPath: string,
  pkgJson: PackageJson,
): Promise<void> {
  await writeFile(pkgJsonPath, JSON.stringify(pkgJson, null, 2) + '\n');
}

async function fixPackageJson(
  packagePath: string,
  missingDeps: {name: string; version: string}[],
): Promise<void> {
  const {path: pkgJsonPath, json: pkgJson} =
    await readPackageJsonFile(packagePath);

  // Initialize devDependencies if it doesn't exist
  if (!pkgJson.devDependencies) {
    pkgJson.devDependencies = {};
  }

  // Add missing dependencies with their actual versions
  for (const dep of missingDeps) {
    pkgJson.devDependencies[dep.name] = dep.version;
  }

  // Sort devDependencies alphabetically
  pkgJson.devDependencies = sortDependencies(pkgJson.devDependencies);

  // Write back with proper formatting
  await writePackageJsonFile(pkgJsonPath, pkgJson);
}

async function removePackageJsonDeps(
  packagePath: string,
  depsToRemove: string[],
): Promise<void> {
  const {path: pkgJsonPath, json: pkgJson} =
    await readPackageJsonFile(packagePath);

  // Remove from both dependencies and devDependencies
  for (const dep of depsToRemove) {
    if (pkgJson.dependencies?.[dep]) {
      delete pkgJson.dependencies[dep];
    }
    if (pkgJson.devDependencies?.[dep]) {
      delete pkgJson.devDependencies[dep];
    }
  }

  // Sort dependencies alphabetically if they exist
  if (pkgJson.dependencies) {
    pkgJson.dependencies = sortDependencies(pkgJson.dependencies);
  }

  if (pkgJson.devDependencies) {
    pkgJson.devDependencies = sortDependencies(pkgJson.devDependencies);
  }

  // Write back with proper formatting
  await writePackageJsonFile(pkgJsonPath, pkgJson);
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

  const missingDeps: {
    package: string;
    missing: {name: string; version: string}[];
  }[] = [];
  const versionMismatches: {
    package: string;
    dependency: string;
    expected: string;
    actual: string;
  }[] = [];
  const extraDeps: {
    package: string;
    extra: string[];
  }[] = [];

  // Build a map of all workspace package names
  const allWorkspacePackages = new Set<string>();
  for (const pkg of packageDeps.keys()) {
    const pkgJson = await getPackageJson(join(WORKSPACE_ROOT, pkg));
    if (pkgJson?.name) {
      allWorkspacePackages.add(pkgJson.name);
    }
  }

  for (const [sourcePackage, deps] of packageDeps) {
    const pkgJson = await getPackageJson(join(WORKSPACE_ROOT, sourcePackage));

    if (!pkgJson) {
      console.log(`⚠️  ${sourcePackage}: No package.json found`);
      continue;
    }

    const allDeclaredDeps = {
      ...pkgJson.dependencies,
      ...pkgJson.devDependencies,
    };

    const missing: {name: string; version: string}[] = [];

    // Build a map from targetPackage to workspace name for this source package
    const targetPackageToWorkspaceName = new Map<string, string>();
    for (const targetPackage of deps) {
      const targetPkgJson = await getPackageJson(
        join(WORKSPACE_ROOT, targetPackage),
      );
      const targetWorkspaceName = targetPkgJson?.name;
      if (targetWorkspaceName) {
        targetPackageToWorkspaceName.set(targetPackage, targetWorkspaceName);
      }
    }

    // Convert importLocations from targetPackage to targetWorkspaceName
    const workspaceImportLocations = new Map<
      string,
      {file: string; line: number}[]
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
      const targetPkgJson = await getPackageJson(
        join(WORKSPACE_ROOT, targetPackage),
      );
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
    const extra: string[] = [];
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
        await fixPackageJson(join(WORKSPACE_ROOT, pkg), missing);
        console.log();
      }
    }

    if (versionMismatches.length > 0) {
      console.log('🔧 Fixing version mismatches...\n');

      // Group mismatches by package
      const mismatchesByPackage = new Map<
        string,
        {name: string; version: string}[]
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
        await fixPackageJson(join(WORKSPACE_ROOT, pkg), deps);
        console.log();
      }
    }

    if (extraDeps.length > 0) {
      console.log(
        '🔧 Removing extra dependencies from package.json files...\n',
      );

      for (const {package: pkg, extra} of extraDeps) {
        console.log(`${pkg}:`);
        console.log(`  Removing devDependencies:`);
        for (const dep of extra) {
          console.log(`    - ${dep}`);
        }
        await removePackageJsonDeps(join(WORKSPACE_ROOT, pkg), extra);
        console.log();
      }
    }

    const total =
      missingDeps.length + versionMismatches.length + extraDeps.length;
    console.log(
      `✅ Fixed ${total} issue(s). Run 'npm install' to update lockfile.`,
    );
    return true;
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
          {file: string; line: number}[]
        >();

        if (packageImportLocs) {
          // Get package.json for each target to map to workspace names
          for (const targetPackage of packageDeps.get(pkg) || []) {
            const targetPkgJson = await getPackageJson(
              join(WORKSPACE_ROOT, targetPackage),
            );
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
