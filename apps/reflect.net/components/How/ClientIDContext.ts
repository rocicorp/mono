import React from 'react';

interface ClientIDContextValue {
  client1ID: string;
  client2ID: string;
}

// eslint-disable-next-line @typescript-eslint/naming-convention
export const ClientIDContext = React.createContext<ClientIDContextValue>({
  client1ID: '',
  client2ID: '',
});
