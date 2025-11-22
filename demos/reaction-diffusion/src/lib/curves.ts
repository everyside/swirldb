// Automation curve functions
// All curves take time and offset, return value in [-1, 1] range

export type CurveName =
  | 'smooth'
  | 'sine'
  | 'triangle'
  | 'sawtooth'
  | 'reverseSaw'
  | 'square'
  | 'easeIn'
  | 'easeOut'
  | 'bounce'
  | 'random';

export const CURVE_NAMES: CurveName[] = [
  'smooth',
  'sine',
  'triangle',
  'sawtooth',
  'reverseSaw',
  'square',
  'easeIn',
  'easeOut',
  'bounce',
  'random',
];

export const CURVE_DISPLAY_NAMES: Record<CurveName, string> = {
  smooth: 'Smooth',
  sine: 'Sine',
  triangle: 'Triangle',
  sawtooth: 'Sawtooth',
  reverseSaw: 'Rev Saw',
  square: 'Square',
  easeIn: 'Ease In',
  easeOut: 'Ease Out',
  bounce: 'Bounce',
  random: 'Random',
};

type CurveFunction = (time: number, offset: number) => number;

// Smooth multi-sine wave (original smoothNoise)
function smooth(time: number, offset: number): number {
  const t = time * 0.0001 + offset;
  return (
    Math.sin(t * 0.5) * 0.4 +
    Math.sin(t * 0.7 + 1.2) * 0.3 +
    Math.sin(t * 1.1 + 2.4) * 0.2 +
    Math.sin(t * 1.3 + 3.6) * 0.1
  );
}

// Pure sine wave
function sine(time: number, offset: number): number {
  const t = time * 0.0001 + offset;
  return Math.sin(t);
}

// Triangle wave (linear up and down)
function triangle(time: number, offset: number): number {
  const t = (time * 0.0001 + offset) / (2 * Math.PI);
  const phase = t - Math.floor(t); // [0, 1]
  return phase < 0.5
    ? -1 + 4 * phase      // Rising from -1 to 1
    : 3 - 4 * phase;      // Falling from 1 to -1
}

// Sawtooth wave (ramp up, snap down)
function sawtooth(time: number, offset: number): number {
  const t = (time * 0.0001 + offset) / (2 * Math.PI);
  const phase = t - Math.floor(t); // [0, 1]
  return -1 + 2 * phase; // Linear from -1 to 1
}

// Reverse sawtooth (snap up, ramp down)
function reverseSaw(time: number, offset: number): number {
  const t = (time * 0.0001 + offset) / (2 * Math.PI);
  const phase = t - Math.floor(t); // [0, 1]
  return 1 - 2 * phase; // Linear from 1 to -1
}

// Square wave (hard toggle)
function square(time: number, offset: number): number {
  const t = (time * 0.0001 + offset) / (2 * Math.PI);
  const phase = t - Math.floor(t); // [0, 1]
  return phase < 0.5 ? -1 : 1;
}

// Exponential ease in (slow start, fast end)
function easeIn(time: number, offset: number): number {
  const t = (time * 0.0001 + offset) / (2 * Math.PI);
  const phase = t - Math.floor(t); // [0, 1]
  const eased = phase < 0.5
    ? Math.pow(2 * phase, 2) / 2           // Ease in from 0 to 0.5
    : 1 - Math.pow(2 * (1 - phase), 2) / 2; // Ease in from 0.5 to 1
  return -1 + 2 * eased;
}

// Exponential ease out (fast start, slow end)
function easeOut(time: number, offset: number): number {
  const t = (time * 0.0001 + offset) / (2 * Math.PI);
  const phase = t - Math.floor(t); // [0, 1]
  const eased = phase < 0.5
    ? 1 - Math.pow(1 - 2 * phase, 2) / 2     // Ease out from 0 to 0.5
    : 0.5 + Math.pow(2 * phase - 1, 2) / 2;  // Ease out from 0.5 to 1
  return -1 + 2 * eased;
}

// Bounce effect
function bounce(time: number, offset: number): number {
  const t = (time * 0.0001 + offset) / (2 * Math.PI);
  const phase = t - Math.floor(t); // [0, 1]

  // Elastic bounce effect
  const bouncePhase = phase < 0.5 ? 2 * phase : 2 * (1 - phase); // [0, 1, 0]
  const bounced = Math.abs(Math.sin(bouncePhase * Math.PI * 4)) * Math.pow(1 - bouncePhase, 1.5);

  return -1 + 2 * bounced;
}

// Random walk (smooth perlin-like noise)
let randomState = 12345;
function seededRandom(): number {
  randomState = (randomState * 1103515245 + 12345) & 0x7fffffff;
  return randomState / 0x7fffffff;
}

function random(time: number, offset: number): number {
  // Use time+offset as seed for deterministic randomness
  const seed = Math.floor(time * 0.00001 + offset);
  randomState = seed;

  // Generate several octaves of random values
  const r1 = seededRandom() * 2 - 1;
  const r2 = seededRandom() * 2 - 1;
  const r3 = seededRandom() * 2 - 1;

  // Blend for smoother random walk
  return (r1 * 0.5 + r2 * 0.3 + r3 * 0.2);
}

const CURVE_FUNCTIONS: Record<CurveName, CurveFunction> = {
  smooth,
  sine,
  triangle,
  sawtooth,
  reverseSaw,
  square,
  easeIn,
  easeOut,
  bounce,
  random,
};

export function getCurveValue(curveName: CurveName, time: number, offset: number): number {
  const curveFunc = CURVE_FUNCTIONS[curveName];
  if (!curveFunc) {
    return smooth(time, offset); // Fallback to smooth
  }
  return curveFunc(time, offset);
}
