import { default as skott } from 'skott';import { default as skott } from 'skott';#!/usr/bin/env node



interface PackageDependencies {

  [packageName: string]: string[];

}interface PackageDependencies {import { default as skott } from 'skott';



async function analyzePackageDependencies(): Promise<PackageDependencies> {  [packageName: string]: string[];import { fileURLToPath } from 'url';

  console.log('Analyzing package dependencies...\n');

}import { dirname } from 'path';

  const { getStructure } = await skott({

    entrypoint: 'packages/**/*.ts,apps/**/*.ts',

    ignorePatterns: [

      '**/*.test.ts',async function analyzePackageDependencies(): Promise<PackageDependencies> {const __filename = fileURLToPath(import.meta.url);

      '**/*.pg-test.ts',

      '**/*.spec.ts',  /* eslint-disable no-console */const __dirname = dirname(__filename);

      '**/test/**',

      '**/tests/**',  console.log('Analyzing package dependencies...\n');

      '**/__tests__/**',

      '**/node_modules/**',interface PackageDependencies {

      '**/dist/**',

      '**/build/**',  const { getStructure } = await skott({  [packageName: string]: string[];

      '**/*.d.ts',

    ],    entrypoint: 'packages/**/*.ts,apps/**/*.ts',}

    includeBaseDir: true,

  });    ignorePatterns: [



  const structure = getStructure();      '**/*.test.ts',interface FileInfo {

  const packageDependencies = new Map<string, Set<string>>();

      '**/*.pg-test.ts',  adjacentTo: string[];

  function getPackageName(filePath: string): string | null {

    const parts = filePath.split('/');      '**/*.spec.ts',  [key: string]: unknown;

    if (parts[0] === 'packages' || parts[0] === 'apps') {

      return `${parts[0]}/${parts[1]}`;      '**/test/**',}

    }

    return null;      '**/tests/**',

  }

      '**/__tests__/**',interface SkottStructure {

  for (const [filePath, adjacentFiles] of Object.entries(structure)) {

    const sourcePackage = getPackageName(filePath);      '**/node_modules/**',  files: Map<string, FileInfo>;

    if (!sourcePackage) continue;

      '**/dist/**',}

    if (!packageDependencies.has(sourcePackage)) {

      packageDependencies.set(sourcePackage, new Set<string>());      '**/build/**',

    }

      '**/*.d.ts',async function analyzePackageDependencies(): Promise<PackageDependencies> {

    const dependencies = Array.isArray(adjacentFiles) ? adjacentFiles : [];

    for (const depPath of dependencies) {    ],  // eslint-disable-next-line no-console

      const targetPackage = getPackageName(depPath);

    includeBaseDir: true,  console.log('Analyzing package dependencies...\n');

      if (targetPackage && targetPackage !== sourcePackage) {

        packageDependencies.get(sourcePackage)?.add(targetPackage);  });  

      }

    }  const { getStructure } = await skott({

  }

  const structure = getStructure();    entrypoint: 'packages/**/*.ts,apps/**/*.ts',

  const result: PackageDependencies = {};

  for (const [pkg, deps] of packageDependencies) {  const packageDependencies = new Map<string, Set<string>>();    ignorePatterns: [

    result[pkg] = Array.from(deps).sort();

  }      '**/*.test.ts',



  return result;  // Helper to get package name from file path      '**/*.pg-test.ts', 

}

  function getPackageName(filePath: string): string | null {      '**/*.spec.ts',

async function main(): Promise<void> {

  try {    const parts = filePath.split('/');      '**/test/**',

    const dependencies = await analyzePackageDependencies();

    if (parts[0] === 'packages' || parts[0] === 'apps') {      '**/tests/**',

    console.log('Package Dependencies (excluding test files):');

    console.log('=========================================\n');      return `${parts[0]}/${parts[1]}`;      '**/__tests__/**',



    const sortedPackages = Object.keys(dependencies).sort();    }      '**/node_modules/**',



    for (const pkg of sortedPackages) {    return null;      '**/dist/**',

      const deps = dependencies[pkg];

      if (deps && deps.length > 0) {  }      '**/build/**',

        console.log(`${pkg} depends on:`);

        for (const dep of deps) {      '**/*.d.ts'

          console.log(`  → ${dep}`);

        }  // Analyze each file's dependencies    ],

        console.log();

      }  for (const [filePath, adjacentFiles] of Object.entries(structure)) {    includeBaseDir: true,

    }

    const sourcePackage = getPackageName(filePath);    displayMode: 'raw'

    const totalPackages = sortedPackages.length;

    const packagesWithDeps = sortedPackages.filter(    if (!sourcePackage) continue;  });

      (pkg) => dependencies[pkg] && dependencies[pkg].length > 0,

    ).length;

    const totalDependencyRelationships = sortedPackages.reduce(

      (sum, pkg) => sum + (dependencies[pkg]?.length ?? 0),    if (!packageDependencies.has(sourcePackage)) {  const structure = getStructure() as SkottStructure;

      0,

    );      packageDependencies.set(sourcePackage, new Set<string>());  const packageDependencies = new Map<string, Set<string>>();



    console.log('Summary:');    }

    console.log('========');

    console.log(`Total packages analyzed: ${totalPackages}`);  // Helper to get package name from file path

    console.log(`Packages with dependencies: ${packagesWithDeps}`);

    console.log(`Total dependency relationships: ${totalDependencyRelationships}`);    // Check each dependency of this file  function getPackageName(filePath: string): string | null {

  } catch (error) {

    console.error('Error analyzing dependencies:', error);    const dependencies = Array.isArray(adjacentFiles) ? adjacentFiles : [];    const parts = filePath.split('/');

    process.exit(1);

  }    for (const depPath of dependencies) {    if (parts[0] === 'packages' || parts[0] === 'apps') {

}

      const targetPackage = getPackageName(depPath);      return `${parts[0]}/${parts[1]}`;

void main();
    }

      // Only track cross-package dependencies    return null;

      if (targetPackage && targetPackage !== sourcePackage) {  }

        packageDependencies.get(sourcePackage)?.add(targetPackage);

      }  // Analyze each file's dependencies

    }  for (const [filePath, fileInfo] of structure.files) {

  }    const sourcePackage = getPackageName(filePath);

    if (!sourcePackage) continue;

  // Convert Sets to Arrays and sort

  const result: PackageDependencies = {};    if (!packageDependencies.has(sourcePackage)) {

  for (const [pkg, deps] of packageDependencies) {      packageDependencies.set(sourcePackage, new Set<string>());

    result[pkg] = Array.from(deps).sort();    }

  }

    // Check each dependency of this file

  return result;    for (const depPath of fileInfo.adjacentTo) {

}      const targetPackage = getPackageName(depPath);

      

async function main(): Promise<void> {      // Only track cross-package dependencies

  try {      if (targetPackage && targetPackage !== sourcePackage) {

    const dependencies = await analyzePackageDependencies();        packageDependencies.get(sourcePackage)?.add(targetPackage);

      }

    console.log('Package Dependencies (excluding test files):');    }

    console.log('=========================================\n');  }



    // Sort packages for consistent output  // Convert Sets to Arrays and sort

    const sortedPackages = Object.keys(dependencies).sort();  const result: PackageDependencies = {};

  for (const [pkg, deps] of packageDependencies) {

    for (const pkg of sortedPackages) {    result[pkg] = Array.from(deps).sort();

      const deps = dependencies[pkg];  }

      if (deps && deps.length > 0) {

        console.log(`${pkg} depends on:`);  return result;

        for (const dep of deps) {}

          console.log(`  → ${dep}`);

        }async function main() {

        console.log();  try {

      }    const dependencies = await analyzePackageDependencies();

    }    

    console.log('Package Dependencies (excluding test files):');

    // Summary statistics    console.log('=========================================\n');

    const totalPackages = sortedPackages.length;    

    const packagesWithDeps = sortedPackages.filter(    // Sort packages for consistent output

      (pkg) => dependencies[pkg] && dependencies[pkg].length > 0,    const sortedPackages = Object.keys(dependencies).sort();

    ).length;    

    const totalDependencyRelationships = sortedPackages.reduce(    for (const pkg of sortedPackages) {

      (sum, pkg) => sum + (dependencies[pkg]?.length ?? 0),      const deps = dependencies[pkg];

      0,      if (deps.length > 0) {

    );        console.log(`${pkg} depends on:`);

        for (const dep of deps) {

    console.log('Summary:');          console.log(`  → ${dep}`);

    console.log('========');        }

    console.log(`Total packages analyzed: ${totalPackages}`);        console.log();

    console.log(`Packages with dependencies: ${packagesWithDeps}`);      }

    console.log(`Total dependency relationships: ${totalDependencyRelationships}`);    }

  } catch (error) {    

    console.error('Error analyzing dependencies:', error);    // Summary statistics

    process.exit(1);    const totalPackages = sortedPackages.length;

  }    const packagesWithDeps = sortedPackages.filter(pkg => dependencies[pkg].length > 0).length;

}    const totalDependencyRelationships = sortedPackages.reduce((sum, pkg) => sum + dependencies[pkg].length, 0);

    

void main();    console.log('Summary:');
    console.log('========');
    console.log(`Total packages analyzed: ${totalPackages}`);
    console.log(`Packages with dependencies: ${packagesWithDeps}`);
    console.log(`Total dependency relationships: ${totalDependencyRelationships}`);
    
  } catch (error) {
    console.error('Error analyzing dependencies:', error);
    process.exit(1);
  }
}

main();