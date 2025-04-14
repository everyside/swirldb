type Listener = (val: any) => void;

export class SwirlDB {
  private store = new Map<string, any>();
  private listeners = new Map<string, Set<Listener>>();

  createOrUpdate(key: string, value: any): void {
    this.store.set(key, value);
    this.emit(key, value);
  }

  delete(key: string): void {
    this.store.delete(key);
    this.emit(key, null);
  }

  findById(key: string): any {
    return this.store.get(key);
  }

  findAll(prefix: string = ''): Record<string, any> {
    const result: Record<string, any> = {};
    for (const [k, v] of this.store.entries()) {
      if (k.startsWith(prefix)) {
        result[k] = v;
      }
    }
    return result;
  }

  subscribe(key: string, listener: Listener): void {
    if (!this.listeners.has(key)) {
      this.listeners.set(key, new Set());
    }
    this.listeners.get(key)!.add(listener);
  }

  unsubscribe(key: string, listener: Listener): void {
    this.listeners.get(key)?.delete(listener);
  }

  private emit(key: string, value: any): void {
    for (const listener of this.listeners.get(key) ?? []) {
      listener(value);
    }
  }
}
