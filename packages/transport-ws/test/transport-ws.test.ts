import { test, expect } from 'vitest';
import { createSocket } from '../src';

test('websocket', () => {
  expect(createSocket()).toBe('ws-connection');
});
