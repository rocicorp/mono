/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  webpack(config) {
    // config.output.webassemblyModuleFilename = 'static/wasm/[modulehash].wasm';
    config.experiments = {asyncWebAssembly: true, layers: true};
    return config;
  },
};
export default nextConfig;
