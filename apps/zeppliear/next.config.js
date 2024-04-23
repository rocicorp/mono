import webpack from 'webpack';
import {makeDefine} from '../../packages/shared/src/build.js';

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

    config.plugins.push(new webpack.DefinePlugin(makeDefine()));

    return config;
  },
  transpilePackages: ['shared', '@rocicorp/zql'],
};

export default nextConfig;
