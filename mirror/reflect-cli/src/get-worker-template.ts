export function getWorkerTemplate(app: string, reflectServer: string) {
  return `
  import {default as makeOptions} from './${app}';
  import {createReflectServer} from './${reflectServer}';
  const {worker, RoomDO, AuthDO} = createReflectServer(makeOptions);
  export {AuthDO, RoomDO, worker as default};
`;
}
