import { encrypt, decrypt } from '../src';
test('noop encryption', () => {
  expect(encrypt('x')).toBe('x');
  expect(decrypt('x')).toBe('x');
});
