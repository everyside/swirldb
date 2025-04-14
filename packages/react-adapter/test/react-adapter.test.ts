import { test, expect } from 'vitest';
import { useSwirl } from '../src';

test('hook returns array', () => {
  const [v] = useSwirl();
  expect(typeof v).toBe('string');
});
