export interface StorageAdapter {
  loadKey(key: string): Promise<string | undefined>;
  saveKey(key: string, encrypted: string): Promise<void>;
  deleteKey(key: string): Promise<void>;
}

export interface EncryptionAdapter {
  encrypt(value: any): string;
  decrypt(encrypted: string): any;
}
