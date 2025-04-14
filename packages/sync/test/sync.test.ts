import { test, expect } from 'vitest';
import { merge } from '../src';

test('merge wins b', () => {
  expect(merge(1, 2)).toBe(2);
});
