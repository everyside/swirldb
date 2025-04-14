import { describe, it, expect } from 'vitest';
import { SwirlDB } from '../src';

describe('SwirlDB', () => {
  it('should set and get a value', () => {
    const db = new SwirlDB();
    db.createOrUpdate('foo', 123);
    expect(db.findById('foo')).toBe(123);
  });

  it('should delete a value', () => {
    const db = new SwirlDB();
    db.createOrUpdate('foo', 123);
    db.delete('foo');
    expect(db.findById('foo')).toBeUndefined();
  });

  it('should return all matching keys with findAll()', () => {
    const db = new SwirlDB();
    db.createOrUpdate('user:1', 'A');
    db.createOrUpdate('user:2', 'B');
    db.createOrUpdate('post:1', 'X');
    const result = db.findAll('user:');
    expect(result).toEqual({ 'user:1': 'A', 'user:2': 'B' });
  });

  it('should notify subscribers on change', () => {
    const db = new SwirlDB();
    const changes: any[] = [];
    const listener = (val: any) => changes.push(val);
    db.subscribe('foo', listener);
    db.createOrUpdate('foo', 'bar');
    db.delete('foo');
    expect(changes).toEqual(['bar', null]);
  });

  it('should unsubscribe properly', () => {
    const db = new SwirlDB();
    const changes: any[] = [];
    const listener = (val: any) => changes.push(val);
    db.subscribe('foo', listener);
    db.unsubscribe('foo', listener);
    db.createOrUpdate('foo', 'bar');
    expect(changes).toEqual([]);
  });
});
