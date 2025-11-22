export interface Preset {
  name: string;
  feed: number;
  kill: number;
  description: string;
}

export const PRESETS: Record<string, Preset> = {
  spots: {
    name: 'Spots',
    feed: 0.0367,
    kill: 0.0649,
    description: 'Small stable circular spots'
  },
  stripes: {
    name: 'Stripes',
    feed: 0.035,
    kill: 0.060,
    description: 'Parallel stripe patterns'
  },
  waves: {
    name: 'Waves',
    feed: 0.014,
    kill: 0.054,
    description: 'Expanding circular waves'
  },
  coral: {
    name: 'Coral',
    feed: 0.0545,
    kill: 0.062,
    description: 'Branching coral-like maze'
  },
  spirals: {
    name: 'Spirals',
    feed: 0.0118,
    kill: 0.0475,
    description: 'Spiral wave patterns'
  },
  chaos: {
    name: 'Chaos',
    feed: 0.026,
    kill: 0.051,
    description: 'Chaotic turbulent patterns'
  },
  worms: {
    name: 'Worms',
    feed: 0.078,
    kill: 0.061,
    description: 'Squirming worm-like structures'
  },
  holes: {
    name: 'Holes',
    feed: 0.039,
    kill: 0.058,
    description: 'Negative space holes'
  }
};

export const DEFAULT_PRESET = 'coral';
