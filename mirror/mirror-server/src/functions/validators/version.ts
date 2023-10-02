import {logger} from 'firebase-functions';
import {HttpsError} from 'firebase-functions/v2/https';
import type {BaseRequest} from 'mirror-protocol/src/base.js';
import type {RequestContextValidator} from './types.js';
import {SemVer, gt} from 'semver';
import {
  DistTag,
  DistTagMap,
  lookupDistTags,
} from 'mirror-protocol/src/version.js';

export type DistTags = DistTagMap<SemVer>;

export type ReleaseVersions = {
  distTags: DistTags;
};

/**
 * Validator that checks the UserAgent and version against the `@sup` (min supported)
 * dist-tag and rejects the request if the agent is no longer supported. The
 * `distTags` are propagated in the Context for the application logic to use
 * if necessary. Note, however, that the fetching of the DistTags is best effort;
 * the request will proceed if the npm registry is unavailable.
 */
export function userAgentVersion<Request extends BaseRequest, Context>(
  testDistTags?: DistTags,
): RequestContextValidator<Request, Context, Context & ReleaseVersions> {
  return async (req, context) => {
    const {
      requester: {
        userAgent: {type: agent, version},
      },
    } = req;

    let distTags;
    try {
      distTags = testDistTags
        ? testDistTags
        : await lookupDistTags(SemVer, 5000);
    } catch (e) {
      logger.warn(
        'Error fetching dist-tags. Proceeding without version check.',
        e,
      );
      return {...context, distTags: {}};
    }

    let semver;
    try {
      semver = new SemVer(version);
    } catch (e) {
      throw new HttpsError(
        'invalid-argument',
        `Invalid version value ${version}`,
      );
    }
    checkAgent(agent, semver, distTags);
    return {...context, distTags};
  };
}

function checkAgent(
  agent: string,
  version: SemVer,
  distTags: DistTagMap<SemVer>,
) {
  if (agent !== 'reflect-cli') {
    // e.g. The auth-ui sends "web"
    logger.debug(`Unregulated agent ${agent}. Allowing request to proceed.`);
    return;
  }
  const minSupported = distTags[DistTag.MinSupported];
  if (minSupported && gt(minSupported, version)) {
    throw new HttpsError(
      'unavailable',
      'This version of Reflect is no longer supported. Please update to @rocicorp/reflect@latest.',
    );
  }
}
