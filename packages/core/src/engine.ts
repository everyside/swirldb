import { StorageAdapter, EncryptionAdapter } from '@swirldb/types';

export class SwirlEngine {
  private cache = new Map<string, any>(); // MRU ordering
  private maxSize = 1000;

  constructor(
    private storage: StorageAdapter,
    private encryption: EncryptionAdapter,
  ) {}

  async findById(key: string): Promise<any> {
    if (this.cache.has(key)) {
      const val = this.cache.get(key);
      this.touch(key, val);
      return val;
    }

    const encrypted = await this.storage.loadKey(key);
    if (!encrypted) return undefined;

    const decrypted = this.encryption.decrypt(encrypted);
    this.setInCache(key, decrypted);
    return decrypted;
  }

  async createOrUpdate(key: string, value: any): Promise<void> {
    this.setInCache(key, value);
    const encrypted = this.encryption.encrypt(value);
    await this.storage.saveKey(key, encrypted);
  }

  async delete(key: string): Promise<void> {
    this.cache.delete(key);
    await this.storage.deleteKey(key);
  }

  private setInCache(key: string, value: any): void {
    this.cache.delete(key);
    this.cache.set(key, value);
    if (this.cache.size > this.maxSize) {
      const oldest = this.cache.keys().next().value;
      if (oldest !== undefined) {
        this.cache.delete(oldest);
      }
    }
  }

  private touch(key: string, value: any): void {
    this.cache.delete(key);
    this.cache.set(key, value);
  }
}
