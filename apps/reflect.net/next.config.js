import webpack from 'webpack';
import {makeDefine} from '../../packages/shared/src/build.js';

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  webpack(config) {
    config.resolve.extensionAlias = {
      ...config.resolve.extensionAlias,
      '.js': ['.js', '.ts'],
      '.jsx': ['.jsx', '.tsx'],
    };

    config.plugins.push(new webpack.DefinePlugin(makeDefine()));

    return config;
  },
  transpilePackages: ['shared'],
};
export default nextConfig;
