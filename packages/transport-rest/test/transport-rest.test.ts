import { test, expect } from 'vitest';
import { createServer } from '../src';

test('rest', () => {
  expect(createServer()).toBe('rest-server');
});
