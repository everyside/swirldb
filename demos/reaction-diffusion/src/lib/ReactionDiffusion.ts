export interface SimulationConfig {
  width: number;
  height: number;
  feed: number;
  kill: number;
  diffA: number;
  diffB: number;
  dt: number;
}

export interface CellData {
  id: number;
  A: number;
  B: number;
  r: number;
  g: number;
  b: number;
}

export class ReactionDiffusion {
  private width: number;
  private height: number;
  private size: number;

  // Simulation parameters
  private feed: number;
  private kill: number;
  private diffA: number;
  private diffB: number;
  private dt: number;

  // Working buffers for fast computation
  private A: Float32Array;
  private B: Float32Array;
  private R: Uint8Array;
  private G: Uint8Array;
  private B_color: Uint8Array;

  // Temporary buffers (avoid allocation)
  private nextA: Float32Array;
  private nextB: Float32Array;
  private nextR: Uint8Array;
  private nextG: Uint8Array;
  private nextB_color: Uint8Array;

  // Previous state for delta detection
  private previousA: Float32Array;
  private previousB: Float32Array;

  // Pre-computed rainbow color lookup table (360 hues)
  private rainbowLUT: Uint8Array;
  private rainbowSaturation: number = 0.7;

  public paused: boolean = false;
  public frame: number = 0;

  constructor(config: SimulationConfig) {
    this.width = config.width;
    this.height = config.height;
    this.size = this.width * this.height;

    this.feed = config.feed;
    this.kill = config.kill;
    this.diffA = config.diffA;
    this.diffB = config.diffB;
    this.dt = config.dt;

    // Initialize buffers
    this.A = new Float32Array(this.size);
    this.B = new Float32Array(this.size);
    this.R = new Uint8Array(this.size);
    this.G = new Uint8Array(this.size);
    this.B_color = new Uint8Array(this.size);

    this.nextA = new Float32Array(this.size);
    this.nextB = new Float32Array(this.size);
    this.nextR = new Uint8Array(this.size);
    this.nextG = new Uint8Array(this.size);
    this.nextB_color = new Uint8Array(this.size);

    this.previousA = new Float32Array(this.size);
    this.previousB = new Float32Array(this.size);

    // Pre-compute rainbow lookup table (360 hues × 3 RGB channels)
    this.rainbowLUT = new Uint8Array(360 * 3);
    this.initRainbowLUT();

    // Initialize to equilibrium (all A, no B)
    this.reset();
  }

  private initRainbowLUT(): void {
    for (let hue = 0; hue < 360; hue++) {
      const [r, g, b] = this.hslToRgb(hue, this.rainbowSaturation, 0.5);
      const idx = hue * 3;
      this.rainbowLUT[idx] = r;
      this.rainbowLUT[idx + 1] = g;
      this.rainbowLUT[idx + 2] = b;
    }
  }

  setSaturation(saturation: number): void {
    this.rainbowSaturation = saturation;
    this.initRainbowLUT();
  }

  private hslToRgb(h: number, s: number, l: number): [number, number, number] {
    h /= 360;
    const k = (n: number) => (n + h * 12) % 12;
    const a = s * Math.min(l, 1 - l);
    const f = (n: number) => l - a * Math.max(-1, Math.min(k(n) - 3, Math.min(9 - k(n), 1)));
    return [
      Math.round(255 * f(0)),
      Math.round(255 * f(8)),
      Math.round(255 * f(4))
    ];
  }

  reset(): void {
    for (let i = 0; i < this.size; i++) {
      this.A[i] = 1.0;
      this.B[i] = 0.0;
      this.R[i] = 0;
      this.G[i] = 0;
      this.B_color[i] = 0;
      this.previousA[i] = 1.0;
      this.previousB[i] = 0.0;
    }
    this.frame = 0;
  }

  updateConfig(config: Partial<SimulationConfig>): void {
    if (config.feed !== undefined) this.feed = config.feed;
    if (config.kill !== undefined) this.kill = config.kill;
    if (config.diffA !== undefined) this.diffA = config.diffA;
    if (config.diffB !== undefined) this.diffB = config.diffB;
    if (config.dt !== undefined) this.dt = config.dt;
  }

  /**
   * Inject chemical B at a position with user color
   */
  inject(x: number, y: number, brushSize: number, color: { r: number; g: number; b: number }, _brushShapeIndex?: number, flowRate: number = 1.0): void {
    // Note: CPU version doesn't support brush shapes yet (only GPU version does)
    for (let dx = -brushSize; dx <= brushSize; dx++) {
      for (let dy = -brushSize; dy <= brushSize; dy++) {
        if (dx * dx + dy * dy <= brushSize * brushSize) {
          const nx = x + dx;
          const ny = y + dy;

          if (nx >= 0 && nx < this.width && ny >= 0 && ny < this.height) {
            const idx = ny * this.width + nx;
            this.A[idx] = 0.5;  // Deplete A
            this.B[idx] = 0.8 * flowRate;  // Inject B based on flow rate
            this.R[idx] = color.r;
            this.G[idx] = color.g;
            this.B_color[idx] = color.b;
          }
        }
      }
    }
  }

  /**
   * Load state from cell data array
   */
  loadCells(cells: CellData[]): void {
    for (const cell of cells) {
      if (cell && cell.id !== undefined && cell.id < this.size) {
        const i = cell.id;
        this.A[i] = cell.A ?? 1.0;
        this.B[i] = cell.B ?? 0.0;
        this.R[i] = cell.r ?? 0;
        this.G[i] = cell.g ?? 0;
        this.B_color[i] = cell.b ?? 0;
        this.previousA[i] = this.A[i];
        this.previousB[i] = this.B[i];
      }
    }
  }

  /**
   * Compute one simulation step using Gray-Scott equations
   */
  step(): void {
    if (this.paused) return;

    // Gray-Scott reaction-diffusion equations
    for (let i = 0; i < this.size; i++) {
      const x = i % this.width;
      const y = Math.floor(i / this.width);

      // Compute Laplacian (diffusion kernel)
      const lapA = this.laplacian(this.A, x, y);
      const lapB = this.laplacian(this.B, x, y);
      const lapR = this.laplacian(this.R, x, y);
      const lapG = this.laplacian(this.G, x, y);
      const lapB_color = this.laplacian(this.B_color, x, y);

      const A = this.A[i];
      const B = this.B[i];

      // Reaction term: A + 2B → 3B
      const reaction = A * B * B;

      // Update chemical concentrations
      this.nextA[i] = A + (this.diffA * lapA - reaction + this.feed * (1 - A)) * this.dt;
      this.nextB[i] = B + (this.diffB * lapB + reaction - (this.kill + this.feed) * B) * this.dt;

      // Clamp to [0, 1]
      this.nextA[i] = Math.max(0, Math.min(1, this.nextA[i]));
      this.nextB[i] = Math.max(0, Math.min(1, this.nextB[i]));

      // Colors barely diffuse - they stick to patterns
      // Only slight diffusion where B is very active
      const colorDiffusion = B > 0.1 ? this.diffB * 0.02 : 0;  // 2% of chemical diffusion, only where pattern is strong
      this.nextR[i] = Math.max(0, Math.min(255, this.R[i] + colorDiffusion * lapR * this.dt));
      this.nextG[i] = Math.max(0, Math.min(255, this.G[i] + colorDiffusion * lapG * this.dt));
      this.nextB_color[i] = Math.max(0, Math.min(255, this.B_color[i] + colorDiffusion * lapB_color * this.dt));
    }

    // Swap buffers
    [this.A, this.nextA] = [this.nextA, this.A];
    [this.B, this.nextB] = [this.nextB, this.B];
    [this.R, this.nextR] = [this.nextR, this.R];
    [this.G, this.nextG] = [this.nextG, this.G];
    [this.B_color, this.nextB_color] = [this.nextB_color, this.B_color];

    this.frame++;
  }

  /**
   * Compute Laplacian using 9-point stencil
   */
  private laplacian(grid: Float32Array | Uint8Array, x: number, y: number): number {
    const idx = y * this.width + x;
    const center = grid[idx];
    let sum = center * -1.0;

    // Adjacent cells (top, bottom, left, right)
    if (y > 0) sum += grid[idx - this.width] * 0.2;
    if (y < this.height - 1) sum += grid[idx + this.width] * 0.2;
    if (x > 0) sum += grid[idx - 1] * 0.2;
    if (x < this.width - 1) sum += grid[idx + 1] * 0.2;

    // Diagonal cells
    if (x > 0 && y > 0) sum += grid[idx - this.width - 1] * 0.05;
    if (x < this.width - 1 && y > 0) sum += grid[idx - this.width + 1] * 0.05;
    if (x > 0 && y < this.height - 1) sum += grid[idx + this.width - 1] * 0.05;
    if (x < this.width - 1 && y < this.height - 1) sum += grid[idx + this.width + 1] * 0.05;

    return sum;
  }

  /**
   * Get cells that have changed significantly (for CRDT sync)
   */
  getChangedCells(threshold: number = 0.01): CellData[] {
    const changes: CellData[] = [];

    for (let i = 0; i < this.size; i++) {
      const deltaA = Math.abs(this.A[i] - this.previousA[i]);
      const deltaB = Math.abs(this.B[i] - this.previousB[i]);

      if (deltaA > threshold || deltaB > threshold) {
        changes.push({
          id: i,
          A: parseFloat(this.A[i].toFixed(3)),  // Round to 3 decimals
          B: parseFloat(this.B[i].toFixed(3)),
          r: this.R[i],
          g: this.G[i],
          b: this.B_color[i]
        });

        this.previousA[i] = this.A[i];
        this.previousB[i] = this.B[i];
      }
    }

    return changes;
  }

  /**
   * Get all cells as array (for initial sync or full state)
   */
  getAllCells(): CellData[] {
    const cells: CellData[] = [];
    for (let i = 0; i < this.size; i++) {
      cells.push({
        id: i,
        A: parseFloat(this.A[i].toFixed(3)),
        B: parseFloat(this.B[i].toFixed(3)),
        r: this.R[i],
        g: this.G[i],
        b: this.B_color[i]
      });
    }
    return cells;
  }

  /**
   * Render to canvas using ImageData (much faster than fillRect)
   */
  render(ctx: CanvasRenderingContext2D, cellSize: number = 2): void {
    if (cellSize === 1) {
      // Direct 1:1 rendering - fastest path
      this.renderDirect(ctx);
    } else {
      // Scaled rendering for cellSize > 1
      this.renderScaled(ctx, cellSize);
    }
  }

  private renderDirect(ctx: CanvasRenderingContext2D): void {
    const imageData = ctx.createImageData(this.width, this.height);
    const data = imageData.data;

    for (let i = 0; i < this.size; i++) {
      // Smooth intensity using both A and B to avoid harsh black edges
      const B = this.B[i];
      const A = this.A[i];

      // Use a smooth transfer function that fills in the dark valleys
      const baseIntensity = B * 2.5;
      const ambientFill = (1.0 - A) * 0.8;
      const rawIntensity = Math.max(baseIntensity, ambientFill);
      const intensity = Math.pow(Math.min(1.0, rawIntensity), 0.6);

      const pixelIndex = i * 4;

      // Check if user color is present and still vibrant
      const colorMagnitude = Math.sqrt(this.R[i] * this.R[i] + this.G[i] * this.G[i] + this.B_color[i] * this.B_color[i]);
      const hasColor = colorMagnitude > 20;

      if (hasColor) {
        // Boost user color saturation to prevent fading
        const saturationBoost = 1.3;
        data[pixelIndex] = Math.min(255, this.R[i] * intensity * saturationBoost);
        data[pixelIndex + 1] = Math.min(255, this.G[i] * intensity * saturationBoost);
        data[pixelIndex + 2] = Math.min(255, this.B_color[i] * intensity * saturationBoost);
      } else {
        // Rainbow gradient using pre-computed LUT - slowly cycling clockwise
        const x = i % this.width;
        const y = Math.floor(i / this.width);
        const hue = Math.floor((B * 360 + this.frame * 0.3 + (x + y) * 0.2)) % 360;
        const lutIdx = hue * 3;

        data[pixelIndex] = this.rainbowLUT[lutIdx] * intensity;
        data[pixelIndex + 1] = this.rainbowLUT[lutIdx + 1] * intensity;
        data[pixelIndex + 2] = this.rainbowLUT[lutIdx + 2] * intensity;
      }
      data[pixelIndex + 3] = 255;
    }

    ctx.putImageData(imageData, 0, 0);
  }

  private renderScaled(ctx: CanvasRenderingContext2D, cellSize: number): void {
    // Create a small image at grid resolution
    const imageData = ctx.createImageData(this.width, this.height);
    const data = imageData.data;

    for (let i = 0; i < this.size; i++) {
      const B = this.B[i];
      const A = this.A[i];

      // Use same smooth transfer function as renderDirect
      const baseIntensity = B * 2.5;
      const ambientFill = (1.0 - A) * 0.8;
      const rawIntensity = Math.max(baseIntensity, ambientFill);
      const intensity = Math.pow(Math.min(1.0, rawIntensity), 0.6);

      const pixelIndex = i * 4;
      const colorMagnitude = Math.sqrt(this.R[i] * this.R[i] + this.G[i] * this.G[i] + this.B_color[i] * this.B_color[i]);
      const hasColor = colorMagnitude > 20;

      if (hasColor) {
        const saturationBoost = 1.3;
        data[pixelIndex] = Math.min(255, this.R[i] * intensity * saturationBoost);
        data[pixelIndex + 1] = Math.min(255, this.G[i] * intensity * saturationBoost);
        data[pixelIndex + 2] = Math.min(255, this.B_color[i] * intensity * saturationBoost);
      } else {
        // Rainbow gradient using pre-computed LUT - slowly cycling clockwise
        const x = i % this.width;
        const y = Math.floor(i / this.width);
        const hue = Math.floor((B * 360 + this.frame * 0.3 + (x + y) * 0.2)) % 360;
        const lutIdx = hue * 3;

        data[pixelIndex] = this.rainbowLUT[lutIdx] * intensity;
        data[pixelIndex + 1] = this.rainbowLUT[lutIdx + 1] * intensity;
        data[pixelIndex + 2] = this.rainbowLUT[lutIdx + 2] * intensity;
      }
      data[pixelIndex + 3] = 255;
    }

    // Draw small image to temporary canvas
    const tempCanvas = document.createElement('canvas');
    tempCanvas.width = this.width;
    tempCanvas.height = this.height;
    const tempCtx = tempCanvas.getContext('2d')!;
    tempCtx.putImageData(imageData, 0, 0);

    // Scale up using drawImage with smoothing for less jagged edges
    ctx.imageSmoothingEnabled = true;
    ctx.imageSmoothingQuality = 'high';
    ctx.drawImage(tempCanvas, 0, 0, this.width, this.height,
                  0, 0, this.width * cellSize, this.height * cellSize);
  }
}
