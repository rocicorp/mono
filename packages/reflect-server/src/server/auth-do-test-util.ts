import {AuthDOOptions, BaseAuthDO} from './auth-do.js';

// https://community.cloudflare.com/t/durable-objects-larger-storage-limit-per-object-time-invalidated-storage/222156/17
// There is only one alarm per object, so each call to setAlarm() replaces the previous alarm with the new alarm time
type Alarm = {
  timeoutID: ReturnType<typeof setTimeout>;
  scheduledTime: number;
  options: DurableObjectSetAlarmOptions | undefined;
};

const storageToDOMap = new WeakMap<DurableObjectStorage, DurableObject>();
const durableObjectToAlarmMap = new WeakMap<DurableObject, Alarm>();

function getDurableObject(storage: DurableObjectStorage): DurableObject {
  const durableObject = storageToDOMap.get(storage);
  if (!durableObject) {
    const s =
      "Invalid test. Make sure you associate the storage with the durable object using 'storageToDOMap.set(storage, this)'";
    console.error(s);
    throw new Error(s);
  }
  return durableObject;
}

function getAlarm(
  this: DurableObjectStorage,
  _options?: DurableObjectGetAlarmOptions,
): Promise<number | null> {
  const durableObject = getDurableObject(this);
  const alarm = durableObjectToAlarmMap.get(durableObject);
  return Promise.resolve(alarm?.scheduledTime ?? null);
}

function setAlarm(
  this: DurableObjectStorage,
  scheduledTime: number | Date,
  options?: DurableObjectSetAlarmOptions,
) {
  const durableObject = getDurableObject(this);
  const oldAlarm = durableObjectToAlarmMap.get(durableObject);
  if (oldAlarm) {
    clearTimeout(oldAlarm.timeoutID);
    durableObjectToAlarmMap.delete(durableObject);
  }

  if (typeof scheduledTime !== 'number') {
    scheduledTime = scheduledTime.getTime();
  }

  const timeout = scheduledTime - Date.now();
  const alarm = {
    scheduledTime,
    options,
    timeoutID: setTimeout(() => {
      durableObjectToAlarmMap.delete(durableObject);
      void durableObject.alarm?.();
    }, timeout),
  };
  durableObjectToAlarmMap.set(durableObject, alarm);
  return Promise.resolve();
}
export class TestAuthDO extends BaseAuthDO {
  constructor(options: AuthDOOptions) {
    super(options);
    storageToDOMap.set(options.state.storage, this);
    options.state.storage.setAlarm = setAlarm;
    options.state.storage.getAlarm = getAlarm;
  }
}
