import '../global.css';
import { ZeroProvider } from '@rocicorp/zero/expo'; // Import from the path alias defined in tsconfig.json
import { Stack } from 'expo-router';
import { useSyncExternalStore, useCallback } from 'react';

import { zeroRef } from '@/lib/zero-setup';

export const unstable_settings = {
  // Ensure that reloading on `/modal` keeps a back button present.
  initialRouteName: '(tabs)',
};

export default function RootLayout() {
  const z = useSyncExternalStore(
    zeroRef.onChange,
    useCallback(() => zeroRef.value, [])
  );

  if (!z) {
    return null; // or add a fallback spinner/message
  }

  return (
    <ZeroProvider zero={z}>
      <Stack>
        <Stack.Screen name="(tabs)" options={{ headerShown: false }} />
        <Stack.Screen name="modal" options={{ presentation: 'modal' }} />
      </Stack>
    </ZeroProvider>
  );
}
