import {readdir, readFile} from 'fs/promises';
import {dirname, join, relative} from 'path';

interface PackageDependency {
  source: string;
  target: string;
  importPath: string;
  file: string;
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

function extractImports(content: string): string[] {
  const imports: string[] = [];

  // Match import statements
  const importRegex =
    /import\s+(?:(?:\{[^}]*\}|\*\s+as\s+\w+|\w+)(?:\s*,\s*(?:\{[^}]*\}|\*\s+as\s+\w+|\w+))*\s+from\s+)?['"]([^'"]+)['"]/g;

  let match;
  while ((match = importRegex.exec(content)) !== null) {
    imports.push(match[1]);
  }

  return imports;
}

async function analyzePackageDependencies(): Promise<Map<string, Set<string>>> {
  console.log('Finding TypeScript files...');

  const packagesFiles = await findTypeScriptFiles('packages');
  const appsFiles = await findTypeScriptFiles('apps');
  const allFiles = [...packagesFiles, ...appsFiles];

  console.log(`Found ${allFiles.length} TypeScript files to analyze`);

  const packageDeps = new Map<string, Set<string>>();
  const dependencies: PackageDependency[] = [];

  for (const filePath of allFiles) {
    const sourcePackage = getPackageName(filePath);
    if (!sourcePackage) continue;

    try {
      const content = await readFile(filePath, 'utf-8');
      const imports = extractImports(content);

      for (const importPath of imports) {
        // Check if this is a relative import that goes to another package
        if (importPath.startsWith('../')) {
          // Resolve the relative path
          const fileDir = dirname(filePath);
          const resolvedPath = join(fileDir, importPath);
          const normalizedPath = relative('.', resolvedPath).replace(
            /\\/g,
            '/',
          );

          const targetPackage = getPackageName(normalizedPath);
          if (targetPackage && targetPackage !== sourcePackage) {
            dependencies.push({
              source: sourcePackage,
              target: targetPackage,
              importPath,
              file: filePath,
            });

            if (!packageDeps.has(sourcePackage)) {
              packageDeps.set(sourcePackage, new Set());
            }
            packageDeps.get(sourcePackage)!.add(targetPackage);
          }
        }
      }
    } catch (error) {
      console.error(`Error reading file ${filePath}:`, error);
    }
  }

  return packageDeps;
}

async function main() {
  try {
    const packageDeps = await analyzePackageDependencies();

    console.log('\nPackage Dependencies (excluding test files):');
    console.log('==========================================\n');

    const sortedPackages = Array.from(packageDeps.keys()).sort();

    for (const pkg of sortedPackages) {
      const deps = Array.from(packageDeps.get(pkg) || []).sort();
      if (deps.length > 0) {
        console.log(`${pkg} depends on:`);
        for (const dep of deps) {
          console.log(`  → ${dep}`);
        }
        console.log();
      }
    }

    const totalPackages = sortedPackages.length;
    const packagesWithDeps = sortedPackages.filter(
      pkg => (packageDeps.get(pkg)?.size || 0) > 0,
    ).length;
    const totalDeps = Array.from(packageDeps.values()).reduce(
      (sum, deps) => sum + deps.size,
      0,
    );

    console.log('Summary:');
    console.log('========');
    console.log(`Total packages analyzed: ${totalPackages}`);
    console.log(`Packages with dependencies: ${packagesWithDeps}`);
    console.log(`Total dependency relationships: ${totalDeps}`);
  } catch (error) {
    console.error('Error:', error);
  }
}

main();
