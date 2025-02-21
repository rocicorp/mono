import { execSync } from "node:child_process";

export const deploy = () =>
  execSync("npx zero-deploy-permissions", { stdio: "inherit" });
