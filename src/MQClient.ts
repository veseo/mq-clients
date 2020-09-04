/* eslint-disable no-undef */

type DataPayload = any;
type CallbackFunc = (data: DataPayload) => void;

interface MQClient {
  connect(): Promise<void>;
  publish(namespace: string, data: DataPayload): void;
  subscribe(namespace: string, callback: CallbackFunc): Promise<void>;
  unsubscribe(): Promise<void>;
}

export {
  MQClient,
  DataPayload,
  CallbackFunc,
};
