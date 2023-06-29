/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  webpack(config) {
    config.resolve.extensionAlias = {
      ...config.resolve.extensionAlias,
      '.js': ['.js', '.ts'],
      '.jsx': ['.jsx', '.tsx'],
    };
    return config;
  },
  transpilePackages: ['shared'],
};
export default nextConfig;
