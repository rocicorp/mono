// oxlint-disable no-console

import {
  assertGitTagExists,
  assertMainWorkflowRef,
  assertNpmZeroVersionExists,
  assertStableZeroVersion,
  defaultExec,
  mustEnv,
  writeGithubOutput,
  zeroTag,
  type Exec,
  type StableZeroVersion,
  type ZeroTag,
} from '../shared.ts';

export type PromotionValidation = {
  version: StableZeroVersion;
  tag: ZeroTag<StableZeroVersion>;
};

export type ValidatePromotionOptions = {
  exec?: Exec | undefined;
  version: string;
  workflowRefName: string;
};

export function runPromoteValidateCli() {
  const validation = validatePromotion({
    version: mustEnv('VERSION'),
    workflowRefName: mustEnv('WORKFLOW_REF_NAME'),
  });

  writeGithubOutput(validation);
  console.log(
    `Validated Zero ${validation.version} is ready for latest promotion`,
  );
}

export function validatePromotion({
  exec = defaultExec,
  version,
  workflowRefName,
}: ValidatePromotionOptions): PromotionValidation {
  assertMainWorkflowRef('Promote', workflowRefName);
  assertStableZeroVersion(
    version,
    `Promotion requires a stable version, got ${version}`,
  );

  const tag = zeroTag(version);
  assertGitTagExists(tag, exec);
  assertNpmZeroVersionExists(version, exec);
  assertDockerImageExists(exec, `docker.io/rocicorp/zero:${version}`);
  assertDockerImageExists(exec, `ghcr.io/rocicorp/zero:${version}`);

  return {version, tag};
}

function assertDockerImageExists(exec: Exec, image: string) {
  exec('docker', ['buildx', 'imagetools', 'inspect', image], {
    stdio: 'inherit',
  });
}
