/**
 * Returns true if the property name is accessed by React 19's dev mode
 * component render logging and should be silently ignored.
 *
 * React 19's `addObjectDiffToProperties` in ReactPerformanceTrackProperties.js
 * inspects props to diff them between renders, accessing `$$typeof` to identify
 * React elements. This breaks Proxy-based objects that throw on unknown access.
 *
 * @see https://github.com/facebook/react/issues/35126
 * @see https://github.com/facebook/react/blob/main/packages/shared/ReactPerformanceTrackProperties.js
 */
export function isIntrospectionProperty(prop: string): boolean {
  // React element type marker - the main culprit from React 19's addObjectDiffToProperties
  // It checks `$$typeof` to identify React elements when diffing props
  // Note: We saw `$typeof` in practice, but React uses `$$typeof` internally
  if (prop === '$$typeof' || prop === '$typeof') {
    if (process.env.NODE_ENV !== 'production') {
      console.warn(
        `[Zero] Ignoring access to "${prop}" on schema proxy. ` +
          `This is likely React 19 dev mode inspecting props. ` +
          `See https://github.com/facebook/react/issues/35126`,
      );
    }
    return true;
  }

  return false;
}
