import { test, expect } from 'vitest';
import { encrypt, decrypt } from '../src';

test('basic encryption', () => {
  const enc = encrypt('hello');
  expect(decrypt(enc)).toBe('hello');
});
