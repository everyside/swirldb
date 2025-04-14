import eslintPluginTs from '@typescript-eslint/eslint-plugin';
import eslintParserTs from '@typescript-eslint/parser';
import eslintPluginImport from 'eslint-plugin-import';

export default [
  {
    files: ['**/*.ts'],
    ignores: ['**/jest.config.ts', '**/*.d.ts'],
    languageOptions: {
      parser: eslintParserTs,
      parserOptions: {
        tsconfigRootDir: process.cwd(),
        project: ['./packages/*/tsconfig.json'],
      },
    },
    plugins: {
      '@typescript-eslint': eslintPluginTs,
      import: eslintPluginImport,
    },
    rules: {
      ...eslintPluginTs.configs.recommended.rules,
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-unused-vars': [
        'warn',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_' },
      ],
    },
  },
];
