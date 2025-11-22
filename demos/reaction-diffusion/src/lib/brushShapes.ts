// Brush shape functions with caching for performance
// Each function takes size and returns an array of {dx, dy, intensity} offsets from center

export type BrushPoint = { dx: number; dy: number; intensity: number };

export type BrushShape = (size: number) => BrushPoint[];

// Cache for pre-calculated brush shapes: key is "shapeName_size"
const shapeCache = new Map<string, BrushPoint[]>();

// Get cached brush shape or calculate and cache it
export function getCachedBrushShape(shapeName: string, size: number): BrushPoint[] {
  const cacheKey = `${shapeName}_${size}`;
  let cached = shapeCache.get(cacheKey);

  if (!cached) {
    const shapeFunc = BRUSH_SHAPES[shapeName];
    if (shapeFunc) {
      cached = shapeFunc(size);
      shapeCache.set(cacheKey, cached);

      // Limit cache size to prevent memory issues
      if (shapeCache.size > 1000) {
        const firstKey = shapeCache.keys().next().value;
        if (firstKey) shapeCache.delete(firstKey);
      }
    } else {
      // Fallback to circle
      cached = circle(size);
      shapeCache.set(cacheKey, cached);
    }
  }

  return cached;
}

// 1. Circle (default)
function circle(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const radius = size / 2;
  for (let dy = -radius; dy <= radius; dy++) {
    for (let dx = -radius; dx <= radius; dx++) {
      const dist = Math.sqrt(dx * dx + dy * dy);
      if (dist <= radius) {
        const intensity = 1 - (dist / radius); // Soft falloff
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

// 2. Square
function square(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const half = Math.floor(size / 2);
  for (let dy = -half; dy <= half; dy++) {
    for (let dx = -half; dx <= half; dx++) {
      const distFromEdge = Math.min(
        half - Math.abs(dx),
        half - Math.abs(dy)
      );
      const intensity = Math.min(1, distFromEdge / (half * 0.3));
      points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
    }
  }
  return points;
}

// 3. Triangle
function triangle(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const height = size;
  const half = Math.floor(size / 2);

  for (let dy = -half; dy <= half; dy++) {
    const rowWidth = (half + dy) * (size / height);
    for (let dx = -rowWidth; dx <= rowWidth; dx++) {
      const dist = Math.abs(dx) / (rowWidth || 1);
      const intensity = 1 - dist * 0.5;
      points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
    }
  }
  return points;
}

// 4. Diamond
function diamond(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const half = Math.floor(size / 2);

  for (let dy = -half; dy <= half; dy++) {
    for (let dx = -half; dx <= half; dx++) {
      const manhattanDist = Math.abs(dx) + Math.abs(dy);
      if (manhattanDist <= half) {
        const intensity = 1 - (manhattanDist / half);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

// 5. Star (5-pointed)
function star5(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const radius = size / 2;
  const innerRadius = radius * 0.4;

  for (let dy = -radius; dy <= radius; dy++) {
    for (let dx = -radius; dx <= radius; dx++) {
      const angle = Math.atan2(dy, dx);
      const dist = Math.sqrt(dx * dx + dy * dy);

      // Star shape with 5 points
      const starAngle = ((angle + Math.PI) % (Math.PI * 2 / 5)) - (Math.PI / 5);
      const maxRadius = innerRadius + (radius - innerRadius) * (1 - Math.abs(starAngle) / (Math.PI / 5));

      if (dist <= maxRadius) {
        const intensity = 1 - (dist / maxRadius);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

// 6. Hexagon
function hexagon(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const radius = size / 2;

  for (let dy = -radius; dy <= radius; dy++) {
    for (let dx = -radius; dx <= radius; dx++) {
      const angle = Math.atan2(dy, dx);
      const dist = Math.sqrt(dx * dx + dy * dy);

      // Hexagon boundary
      const sextant = Math.floor((angle + Math.PI + Math.PI / 6) / (Math.PI / 3));
      const hexAngle = angle - sextant * (Math.PI / 3) + Math.PI / 6;
      const maxRadius = radius / Math.cos(hexAngle);

      if (dist <= maxRadius) {
        const intensity = 1 - (dist / maxRadius);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

// 7. Cross/Plus
function cross(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const half = Math.floor(size / 2);
  const thickness = Math.max(2, Math.floor(size / 6));

  for (let dy = -half; dy <= half; dy++) {
    for (let dx = -half; dx <= half; dx++) {
      if (Math.abs(dx) <= thickness || Math.abs(dy) <= thickness) {
        const distFromCenter = Math.min(
          Math.abs(dx) / (thickness || 1),
          Math.abs(dy) / (thickness || 1)
        );
        const intensity = 1 - distFromCenter * 0.5;
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

// 8. Ring/Donut
function ring(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const outerRadius = size / 2;
  const innerRadius = outerRadius * 0.5;

  for (let dy = -outerRadius; dy <= outerRadius; dy++) {
    for (let dx = -outerRadius; dx <= outerRadius; dx++) {
      const dist = Math.sqrt(dx * dx + dy * dy);
      if (dist >= innerRadius && dist <= outerRadius) {
        const intensity = 1 - Math.abs(dist - (innerRadius + outerRadius) / 2) / ((outerRadius - innerRadius) / 2);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

// 9. Heart
function heart(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const scale = size / 16;

  for (let dy = -size/2; dy <= size/2; dy++) {
    for (let dx = -size/2; dx <= size/2; dx++) {
      const x = dx / scale;
      const y = -dy / scale;

      // Heart equation
      const left = (x * x + y * y - 1);
      const heartValue = left * left * left - x * x * y * y * y;

      if (heartValue <= 0) {
        const dist = Math.sqrt(dx * dx + dy * dy);
        const intensity = 1 - (dist / (size / 2));
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity: Math.max(0, intensity) });
      }
    }
  }
  return points;
}

// 10. Spiral
function spiral(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const maxRadius = size / 2;
  const thickness = Math.max(2, size / 12);

  for (let dy = -maxRadius; dy <= maxRadius; dy++) {
    for (let dx = -maxRadius; dx <= maxRadius; dx++) {
      const dist = Math.sqrt(dx * dx + dy * dy);
      const angle = Math.atan2(dy, dx);

      // Archimedean spiral
      const spiralRadius = (angle + Math.PI) * maxRadius / (4 * Math.PI);
      const distFromSpiral = Math.abs(dist - spiralRadius);

      if (distFromSpiral <= thickness && dist <= maxRadius) {
        const intensity = 1 - (distFromSpiral / thickness);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

// 11. Crescent/Moon
function crescent(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const radius = size / 2;
  const offset = radius * 0.3;

  for (let dy = -radius; dy <= radius; dy++) {
    for (let dx = -radius; dx <= radius; dx++) {
      const dist1 = Math.sqrt(dx * dx + dy * dy);
      const dist2 = Math.sqrt((dx - offset) * (dx - offset) + dy * dy);

      if (dist1 <= radius && dist2 > radius * 0.7) {
        const intensity = 1 - (dist1 / radius);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

// 12. Flower (8 petals)
function flower(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const radius = size / 2;

  for (let dy = -radius; dy <= radius; dy++) {
    for (let dx = -radius; dx <= radius; dx++) {
      const angle = Math.atan2(dy, dx);
      const dist = Math.sqrt(dx * dx + dy * dy);

      // 8-petal flower shape
      const petalRadius = radius * (0.5 + 0.5 * Math.abs(Math.sin(angle * 4)));

      if (dist <= petalRadius) {
        const intensity = 1 - (dist / petalRadius);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

// 13-24: Additional creative shapes
function octagon(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const radius = size / 2;

  for (let dy = -radius; dy <= radius; dy++) {
    for (let dx = -radius; dx <= radius; dx++) {
      const angle = Math.atan2(dy, dx);
      const dist = Math.sqrt(dx * dx + dy * dy);

      const octant = Math.floor((angle + Math.PI + Math.PI / 8) / (Math.PI / 4));
      const octAngle = angle - octant * (Math.PI / 4) + Math.PI / 8;
      const maxRadius = radius / Math.cos(octAngle);

      if (dist <= maxRadius) {
        const intensity = 1 - (dist / maxRadius);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

function gear(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const radius = size / 2;
  const teethCount = 12;

  for (let dy = -radius; dy <= radius; dy++) {
    for (let dx = -radius; dx <= radius; dx++) {
      const angle = Math.atan2(dy, dx);
      const dist = Math.sqrt(dx * dx + dy * dy);

      const toothAngle = (angle + Math.PI) % (2 * Math.PI / teethCount);
      const isInTooth = toothAngle < Math.PI / teethCount;
      const maxRadius = isInTooth ? radius : radius * 0.8;

      if (dist <= maxRadius) {
        const intensity = 1 - (dist / maxRadius);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

function burst(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const radius = size / 2;
  const rays = 16;

  for (let dy = -radius; dy <= radius; dy++) {
    for (let dx = -radius; dx <= radius; dx++) {
      const angle = Math.atan2(dy, dx);
      const dist = Math.sqrt(dx * dx + dy * dy);

      const rayAngle = ((angle + Math.PI) % (2 * Math.PI / rays)) - (Math.PI / rays);
      const maxRadius = radius * (0.6 + 0.4 * (1 - Math.abs(rayAngle) / (Math.PI / rays)));

      if (dist <= maxRadius) {
        const intensity = 1 - (dist / maxRadius);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

function lightning(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const half = Math.floor(size / 2);

  // Create a jagged lightning bolt pattern
  for (let dy = -half; dy <= half; dy++) {
    const zigzag = Math.sin(dy * 0.5) * (size / 6);
    const thickness = Math.max(2, size / 8);

    for (let dx = -half; dx <= half; dx++) {
      const distFromBolt = Math.abs(dx - zigzag);
      if (distFromBolt <= thickness) {
        const intensity = 1 - (distFromBolt / thickness);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

function waves(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const half = Math.floor(size / 2);

  for (let dy = -half; dy <= half; dy++) {
    const wave = Math.sin(dy * 0.4) * (size / 8);
    const thickness = Math.max(2, size / 10);

    for (let dx = -half; dx <= half; dx++) {
      const distFromWave = Math.abs(dx - wave);
      if (distFromWave <= thickness) {
        const intensity = 1 - (distFromWave / thickness);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

function gridPattern(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const half = Math.floor(size / 2);
  const gridSize = Math.max(3, Math.floor(size / 6));

  for (let dy = -half; dy <= half; dy++) {
    for (let dx = -half; dx <= half; dx++) {
      const inGridLine = (Math.abs(dx) % gridSize <= 1) || (Math.abs(dy) % gridSize <= 1);
      if (inGridLine) {
        const dist = Math.sqrt(dx * dx + dy * dy);
        const intensity = 1 - (dist / half);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity: Math.max(0, intensity) });
      }
    }
  }
  return points;
}

function dots(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const half = Math.floor(size / 2);
  const dotSpacing = Math.max(4, Math.floor(size / 8));
  const dotRadius = Math.max(1, Math.floor(dotSpacing / 2));

  for (let dy = -half; dy <= half; dy += dotSpacing) {
    for (let dx = -half; dx <= half; dx += dotSpacing) {
      for (let ddy = -dotRadius; ddy <= dotRadius; ddy++) {
        for (let ddx = -dotRadius; ddx <= dotRadius; ddx++) {
          const dist = Math.sqrt(ddx * ddx + ddy * ddy);
          if (dist <= dotRadius) {
            const intensity = 1 - (dist / dotRadius);
            points.push({
              dx: Math.floor(dx + ddx),
              dy: Math.floor(dy + ddy),
              intensity
            });
          }
        }
      }
    }
  }
  return points;
}

function butterfly(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const radius = size / 2;

  for (let dy = -radius; dy <= radius; dy++) {
    for (let dx = -radius; dx <= radius; dx++) {
      const angle = Math.atan2(dy, dx);
      const dist = Math.sqrt(dx * dx + dy * dy);

      // Butterfly polar equation
      const r = radius * Math.abs(Math.sin(angle)) * (Math.exp(Math.cos(angle)) - 2 * Math.cos(4 * angle) + Math.pow(Math.sin(angle / 12), 5));

      if (dist <= r && dist <= radius) {
        const intensity = 1 - (dist / radius);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity: Math.max(0, intensity) });
      }
    }
  }
  return points;
}

function clover(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const radius = size / 2;

  for (let dy = -radius; dy <= radius; dy++) {
    for (let dx = -radius; dx <= radius; dx++) {
      const angle = Math.atan2(dy, dx);
      const dist = Math.sqrt(dx * dx + dy * dy);

      // 4-leaf clover shape
      const cloverRadius = radius * (0.5 + 0.5 * Math.abs(Math.cos(2 * angle)));

      if (dist <= cloverRadius) {
        const intensity = 1 - (dist / cloverRadius);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

function eye(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const half = Math.floor(size / 2);
  const aspectRatio = 2.5;

  for (let dy = -half; dy <= half; dy++) {
    for (let dx = -half; dx <= half; dx++) {
      const scaledX = dx / aspectRatio;
      const dist = Math.sqrt(scaledX * scaledX + dy * dy);

      if (dist <= half / aspectRatio) {
        // Pupil in the center
        const centerDist = Math.sqrt(dx * dx + dy * dy);
        const isPupil = centerDist <= size / 8;
        const intensity = isPupil ? 1 : 1 - (dist / (half / aspectRatio));
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

function arrow(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const half = Math.floor(size / 2);
  const thickness = Math.max(2, size / 8);
  const headSize = size / 3;

  for (let dy = -half; dy <= half; dy++) {
    for (let dx = -half; dx <= half; dx++) {
      // Arrow shaft
      const inShaft = Math.abs(dy) <= thickness && dx < half - headSize;
      // Arrow head
      const inHead = dx >= half - headSize && Math.abs(dy) <= (half - dx) * 2;

      if (inShaft || inHead) {
        const dist = Math.sqrt(dx * dx + dy * dy);
        const intensity = 1 - (dist / half);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity: Math.max(0, intensity) });
      }
    }
  }
  return points;
}

function snowflake(size: number): BrushPoint[] {
  const points: BrushPoint[] = [];
  const radius = size / 2;
  const branches = 6;
  const thickness = Math.max(1, size / 16);

  for (let dy = -radius; dy <= radius; dy++) {
    for (let dx = -radius; dx <= radius; dx++) {
      const angle = Math.atan2(dy, dx);
      const dist = Math.sqrt(dx * dx + dy * dy);

      // Check if point is near any of the 6 branches
      let nearBranch = false;
      for (let i = 0; i < branches; i++) {
        const branchAngle = (i * 2 * Math.PI) / branches;
        const angleDiff = Math.abs(((angle - branchAngle + Math.PI) % (2 * Math.PI)) - Math.PI);

        if (angleDiff <= thickness / dist && dist <= radius) {
          nearBranch = true;
          break;
        }
      }

      if (nearBranch) {
        const intensity = 1 - (dist / radius);
        points.push({ dx: Math.floor(dx), dy: Math.floor(dy), intensity });
      }
    }
  }
  return points;
}

export const BRUSH_SHAPES: Record<string, BrushShape> = {
  circle,
  square,
  triangle,
  diamond,
  star5,
  hexagon,
  cross,
  ring,
  heart,
  spiral,
  crescent,
  flower,
  octagon,
  gear,
  burst,
  lightning,
  waves,
  gridPattern,
  dots,
  butterfly,
  clover,
  eye,
  arrow,
  snowflake,
  vesicaPiscis: circle, // Placeholder - implemented in GPU
  mandala: circle, // Placeholder - implemented in GPU
  yantra: circle, // Placeholder - implemented in GPU
  torus: circle, // Placeholder - implemented in GPU
  metatron: circle, // Placeholder - implemented in GPU
  fibonacci: circle, // Placeholder - implemented in GPU
  seedOfLife: circle, // Placeholder - implemented in GPU
  flowerOfLife: circle, // Placeholder - implemented in GPU
  lotusOfLife: circle, // Placeholder - implemented in GPU
};

export const BRUSH_SHAPE_NAMES = Object.keys(BRUSH_SHAPES);

export const BRUSH_SHAPE_DISPLAY_NAMES: Record<string, string> = {
  circle: 'Circle',
  square: 'Square',
  triangle: 'Triangle',
  diamond: 'Diamond',
  star5: 'Star',
  hexagon: 'Hexagon',
  cross: 'Cross',
  ring: 'Ring',
  heart: 'Heart',
  spiral: 'Spiral',
  crescent: 'Moon',
  flower: 'Flower',
  octagon: 'Octagon',
  gear: 'Gear',
  burst: 'Burst',
  lightning: 'Bolt',
  waves: 'Waves',
  gridPattern: 'Grid',
  dots: 'Dots',
  butterfly: 'Butterfly',
  clover: 'Clover',
  eye: 'Eye',
  arrow: 'Arrow',
  snowflake: 'Snow',
  vesicaPiscis: 'Vesica',
  mandala: 'Mandala',
  yantra: 'Yantra',
  torus: 'Torus',
  metatron: 'Metatron',
  fibonacci: 'Fibo',
  seedOfLife: 'Seed',
  flowerOfLife: 'Flower⚘',
  lotusOfLife: 'Lotus',
};
