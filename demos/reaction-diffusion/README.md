# Reaction-Diffusion Demo

A standalone demonstration of the **Gray-Scott reaction-diffusion** system built with React, TypeScript, and Vite.

## Overview

This demo simulates pattern formation through chemical reactions and diffusion. Users can:
- **Paint chemicals** by clicking/dragging on the canvas
- **Switch between presets** to see different patterns (spots, stripes, coral, fingerprints, worms)
- **Watch patterns evolve** in real-time at 30 FPS
- **See color attribution** - each user's contributions maintain their color as patterns grow

## Gray-Scott Equations

The simulation implements the Gray-Scott model:

```
A + 2B → 3B       (reaction: B is autocatalytic)
A → A + feed      (A is replenished)
B → ∅ + kill      (B decays)
```

Different feed/kill rates produce different patterns:
- **Spots** (F=0.014, k=0.054): Stable circular spots
- **Stripes** (F=0.026, k=0.051): Parallel stripe patterns
- **Coral** (F=0.022, k=0.051): Branching maze-like structures
- **Fingerprints** (F=0.030, k=0.055): Swirling patterns
- **Worms** (F=0.062, k=0.061): Moving worm-like structures

## Running Locally

```bash
# Install dependencies
pnpm install

# Start dev server
pnpm dev

# Build for production
pnpm build
```

## Architecture

- **Simulation Engine** (`lib/ReactionDiffusion.ts`): Pure TypeScript implementation of Gray-Scott equations
  - Uses typed arrays (Float32Array, Uint8Array) for performance
  - 9-point stencil Laplacian for diffusion computation
  - Delta tracking for efficient CRDT sync (future)

- **React App** (`App.tsx`): Canvas rendering and user interaction
  - 30 FPS animation loop using requestAnimationFrame
  - Brush-based painting for chemical injection
  - Real-time FPS counter

- **Controls** (`components/Controls.tsx`): Pattern presets, playback controls, brush size

## Data Model (Designed for CRDT Sync)

Each cell in the 256×256 grid contains:

```typescript
{
  id: number,      // Linear index (y * width + x)
  A: number,       // Chemical A concentration (0.0 - 1.0)
  B: number,       // Chemical B concentration (0.0 - 1.0)
  r: number,       // Red (0-255) - user color
  g: number,       // Green (0-255)
  b: number        // Blue (0-255)
}
```

The `ReactionDiffusion` class includes `getChangedCells()` method which returns only cells with significant changes (threshold=0.01), enabling efficient delta sync for collaborative real-time updates.

## Future: SwirlDB Integration

This demo is designed to be extended with SwirlDB for real-time collaborative pattern generation:

1. Multiple users paint simultaneously
2. Each user has their own color
3. Colors diffuse with chemical B
4. Patterns blend where users' contributions meet
5. Server-side simulation broadcasts updates to all clients

See `WIP/conway-life-demo.md` (now reaction-diffusion spec) for full collaborative architecture.

## Performance

- **Grid**: 256×256 cells (65,536 total)
- **Canvas**: 512×512 pixels (2px per cell)
- **Target FPS**: 30 (33ms per frame)
- **Memory**: ~2.5 MB for simulation state
- **Render time**: ~3-5ms per frame on modern hardware

## References

- [Gray-Scott Reaction-Diffusion](https://en.wikipedia.org/wiki/Reaction%E2%80%93diffusion_system)
- [Karl Sims - Reaction-Diffusion Tutorial](https://www.karlsims.com/rd.html)
- [Pearson's Classification](http://mrob.com/pub/comp/xmorphia/)
