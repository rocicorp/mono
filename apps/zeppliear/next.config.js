import {readFileSync} from 'node:fs';
import webpack from 'webpack';

/** @type {import('next').NextConfig} */
const nextConfig = {
  eslint: {
    // Warning: This allows production builds to successfully complete even if
    // your project has ESLint errors.
    ignoreDuringBuilds: true,
    dirs: ['pages', 'frontend', 'backend', 'util'],
  },
  webpack: config => {
    config.module.rules.push({
      test: /\.svg$/i,
      issuer: /\.[jt]sx?$/,
      use: ['@svgr/webpack'],
    });
    config.module.rules.push({
      test: /\.gz$/,
      enforce: 'pre',
      use: 'gzip-loader',
    });

    config.resolve.extensionAlias = {
      ...config.resolve.extensionAlias,
      '.js': ['.js', '.ts'],
      '.jsx': ['.jsx', '.tsx'],
    };

    const s = readFileSync('../../packages/replicache/package.json', 'utf8');
    const REPLICACHE_VERSION = JSON.parse(s).version;
    config.plugins.push(
      new webpack.DefinePlugin({
        REPLICACHE_VERSION: JSON.stringify(REPLICACHE_VERSION),
      }),
    );

    return config;
  },
  transpilePackages: ['shared', '@rocicorp/zql'],
};

export default nextConfig;
