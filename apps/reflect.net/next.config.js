import {readFileSync} from 'node:fs';
import webpack from 'webpack';

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  webpack(config) {
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
  transpilePackages: ['shared'],
};
export default nextConfig;
