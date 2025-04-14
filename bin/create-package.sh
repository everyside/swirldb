#!/bin/bash
set -e

NAME=$1

if [ -z "$NAME" ]; then
  echo "Usage: ./scripts/create-package.sh <package-name>"
  exit 1
fi

PKG_DIR="packages/$NAME"
SRC_DIR="$PKG_DIR/src"
TEST_DIR="$PKG_DIR/test"

mkdir -p "$SRC_DIR" "$TEST_DIR"

# package.json
cat > "$PKG_DIR/package.json" <<EOF
{
  "name": "@swirldb/$NAME",
  "version": "0.1.0",
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "scripts": {
    "build": "node esbuild.config.mjs",
    "test": "vitest"
  }
}
EOF

# tsconfig.json
cat > "$PKG_DIR/tsconfig.json" <<EOF
{
  "extends": "../../tsconfig.base.json",
  "include": ["src", "test"]
}
EOF

# project.json
cat > "$PKG_DIR/project.json" <<EOF
{
  "name": "$NAME",
  "root": "packages/$NAME",
  "sourceRoot": "packages/$NAME/src",
  "projectType": "library",
  "targets": {
    "build": {
      "executor": "nx:run-commands",
      "options": {
        "command": "npm run build",
        "cwd": "packages/$NAME"
      }
    },
    "test": {
      "executor": "nx:run-commands",
      "options": {
        "command": "npm run test",
        "cwd": "packages/$NAME"
      }
    }
  }
}
EOF

# esbuild.config.mjs
cat > "$PKG_DIR/esbuild.config.mjs" <<EOF
import { build } from 'esbuild';

build({
  entryPoints: ['src/index.ts'],
  outfile: 'dist/index.js',
  bundle: true,
  platform: 'node',
  target: ['node18'],
  format: 'cjs',
  sourcemap: true,
  logLevel: 'info'
}).catch(() => process.exit(1));
EOF

# src/index.ts
echo "// $NAME package" > "$SRC_DIR/index.ts"

# test file
cat > "$TEST_DIR/$NAME.test.ts" <<EOF
import { describe, it, expect } from 'vitest';

describe('$NAME', () => {
  it('should work', () => {
    expect(true).toBe(true);
  });
});
EOF

echo "✅ Created package @swirldb/$NAME in $PKG_DIR"
