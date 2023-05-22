import React from 'react';

interface ClientIDContextValue {
  client1ID: string;
  client2ID: string;
}

export const ClientIDContext = React.createContext<ClientIDContextValue>({
  client1ID: '',
  client2ID: '',
});
