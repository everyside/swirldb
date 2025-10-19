#!/bin/bash
# SwirlDB Build Script
# Builds all components in the correct order with proper flags

set -e  # Exit on error

echo "🔨 SwirlDB Build Script"
echo "======================="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
BUILD_WASM=true
BUILD_SERVER=true
BUILD_ADMIN=false
BUILD_DOCS=false
TARGET=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --wasm-only)
      BUILD_SERVER=false
      shift
      ;;
    --server-only)
      BUILD_WASM=false
      shift
      ;;
    --admin)
      BUILD_ADMIN=true
      shift
      ;;
    --docs)
      BUILD_DOCS=true
      shift
      ;;
    --target)
      TARGET="$2"
      shift
      shift
      ;;
    --help)
      echo "Usage: ./build.sh [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --wasm-only      Build only WASM (skip server)"
      echo "  --server-only    Build only server (skip WASM)"
      echo "  --admin          Deploy WASM to admin site"
      echo "  --docs           Deploy WASM to docs site"
      echo "  --target DIR     Specify custom WASM output directory"
      echo "  --help           Show this help message"
      echo ""
      echo "Examples:"
      echo "  ./build.sh                    # Build both WASM and server"
      echo "  ./build.sh --wasm-only        # Build WASM only"
      echo "  ./build.sh --admin            # Build WASM and deploy to admin"
      echo "  ./build.sh --admin --docs     # Build WASM and deploy to both sites"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Function to build WASM
build_wasm() {
  echo -e "${BLUE}📦 Building WASM (Browser Target)${NC}"
  echo "Directory: native/swirldb-core"
  echo "Features: wasm"
  echo "Target: wasm32-unknown-unknown"
  echo ""

  cd native/swirldb-core

  # CRITICAL: Must use --features wasm or browser bindings won't be included!
  wasm-pack build --target web --features wasm

  if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ WASM build successful${NC}"
    echo "Output: native/swirldb-core/pkg/"
    echo ""
  else
    echo -e "${YELLOW}❌ WASM build failed${NC}"
    exit 1
  fi

  cd ../..
}

# Function to deploy WASM to a target
deploy_wasm() {
  local target_dir=$1
  local site_name=$2

  echo -e "${BLUE}📋 Deploying WASM to ${site_name}${NC}"
  echo "Source: native/swirldb-core/pkg/"
  echo "Target: ${target_dir}"
  echo ""

  mkdir -p "${target_dir}"
  cp -r native/swirldb-core/pkg/* "${target_dir}/"

  if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ WASM deployed to ${site_name}${NC}"
    echo ""
  else
    echo -e "${YELLOW}❌ WASM deployment failed${NC}"
    exit 1
  fi
}

# Function to build server
build_server() {
  echo -e "${BLUE}🚀 Building Sync Server (Native Rust)${NC}"
  echo "Directory: native/swirldb-server"
  echo "Profile: release"
  echo "Features: none (uses default native deps)"
  echo ""

  cd native/swirldb-server

  cargo build --release

  if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Server build successful${NC}"
    echo "Binary: native/swirldb-server/target/release/swirldb-server"
    echo ""
  else
    echo -e "${YELLOW}❌ Server build failed${NC}"
    exit 1
  fi

  cd ../..
}

# Execute builds
echo "Build Configuration:"
echo "  WASM:   ${BUILD_WASM}"
echo "  Server: ${BUILD_SERVER}"
echo "  Admin:  ${BUILD_ADMIN}"
echo "  Docs:   ${BUILD_DOCS}"
echo ""

if [ "$BUILD_WASM" = true ]; then
  build_wasm
fi

if [ "$BUILD_SERVER" = true ]; then
  build_server
fi

# Deploy WASM if requested
if [ "$BUILD_WASM" = true ]; then
  if [ "$BUILD_ADMIN" = true ]; then
    deploy_wasm "admin/public/wasm" "Admin Site"
  fi

  if [ "$BUILD_DOCS" = true ]; then
    deploy_wasm "docs/public/wasm" "Docs Site"
  fi

  if [ -n "$TARGET" ]; then
    deploy_wasm "$TARGET" "Custom Target"
  fi
fi

echo -e "${GREEN}🎉 Build complete!${NC}"
echo ""
echo "Next steps:"
if [ "$BUILD_SERVER" = true ]; then
  echo "  • Run server: cd native/swirldb-server && cargo run --release"
fi
if [ "$BUILD_ADMIN" = true ]; then
  echo "  • Run admin:  cd admin && npm run dev"
fi
if [ "$BUILD_DOCS" = true ]; then
  echo "  • Run docs:   cd docs && npm run dev"
fi
