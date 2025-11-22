export interface SimulationConfig {
  width: number;
  height: number;
  feed: number;
  kill: number;
  diffA: number;
  diffB: number;
  dt: number;
  swirlSpeed?: number;
  gpuNoiseStrength?: number;
}

export interface CellData {
  id: number;
  A: number;
  B: number;
  r: number;
  g: number;
  b: number;
}

export class ReactionDiffusionWebGL {
  private canvas: HTMLCanvasElement;
  private gl: WebGL2RenderingContext;
  private width: number;
  private height: number;

  // Simulation parameters
  private feed: number;
  private kill: number;
  private diffA: number;
  private diffB: number;
  private dt: number;
  private swirlSpeed: number;
  private gpuNoiseStrength: number;
  private rotation: number;

  // WebGL resources
  private stateProgram: WebGLProgram;      // Updates A and B
  private colorProgram: WebGLProgram;      // Updates color
  private renderProgram: WebGLProgram;     // Final render to canvas
  private brushProgram: WebGLProgram;      // Draws brush strokes
  private fadeProgram: WebGLProgram | null = null;  // Gradual fade effect
  private quadBuffer: WebGLBuffer;

  // Cached uniform locations for brush program (performance)
  private brushUniforms: {
    brushCenter: WebGLUniformLocation | null;
    brushRadius: WebGLUniformLocation | null;
    resolution: WebGLUniformLocation | null;
    color: WebGLUniformLocation | null;
    time: WebGLUniformLocation | null;
    target: WebGLUniformLocation | null;
    brushShape: WebGLUniformLocation | null;
    flowRate: WebGLUniformLocation | null;
  };

  // Framebuffers and textures (ping-pong)
  private fbState: [WebGLFramebuffer, WebGLFramebuffer]; // For A and B (RG channels)
  private fbColor: [WebGLFramebuffer, WebGLFramebuffer]; // For RGB color (ping-pong)

  private texState: [WebGLTexture, WebGLTexture]; // RG = (A, B)
  private texColor: [WebGLTexture, WebGLTexture]; // RGB color (ping-pong)

  private pingpong: number = 0;

  public paused: boolean = false;
  public frame: number = 0;

  constructor(canvas: HTMLCanvasElement, config: SimulationConfig) {
    this.canvas = canvas;
    this.width = config.width;
    this.height = config.height;
    this.feed = config.feed;
    this.kill = config.kill;
    this.diffA = config.diffA;
    this.diffB = config.diffB;
    this.dt = config.dt;
    this.swirlSpeed = config.swirlSpeed ?? 0.5;
    this.gpuNoiseStrength = config.gpuNoiseStrength ?? 1.0;
    this.rotation = 0;

    // Get WebGL2 context
    const gl = canvas.getContext('webgl2', {
      alpha: false,
      antialias: false,
      preserveDrawingBuffer: false,
      powerPreference: 'high-performance'
    });

    if (!gl) {
      throw new Error('WebGL2 not supported');
    }
    this.gl = gl;

    // Create shaders and programs
    this.stateProgram = this.createProgram(
      this.copyVertexShaderSource(),  // Use non-flipped vertex shader
      this.stateShaderSource()
    );
    this.colorProgram = this.createProgram(
      this.copyVertexShaderSource(),  // Use non-flipped vertex shader
      this.colorShaderSource()
    );
    this.renderProgram = this.createProgram(
      this.vertexShaderSource(),
      this.renderShaderSource()
    );
    this.brushProgram = this.createProgram(
      this.brushVertexShaderSource(),
      this.brushFragmentShaderSource()
    );

    // Cache brush program uniform locations (massive performance improvement)
    this.brushUniforms = {
      brushCenter: gl.getUniformLocation(this.brushProgram, 'u_brushCenter'),
      brushRadius: gl.getUniformLocation(this.brushProgram, 'u_brushRadius'),
      resolution: gl.getUniformLocation(this.brushProgram, 'u_resolution'),
      color: gl.getUniformLocation(this.brushProgram, 'u_color'),
      time: gl.getUniformLocation(this.brushProgram, 'u_time'),
      target: gl.getUniformLocation(this.brushProgram, 'u_target'),
      brushShape: gl.getUniformLocation(this.brushProgram, 'u_brushShape'),
      flowRate: gl.getUniformLocation(this.brushProgram, 'u_flowRate')
    };

    // Create quad for full-screen rendering
    this.quadBuffer = this.createQuadBuffer();

    // Create textures and framebuffers (use RGBA8 - guaranteed color-renderable in WebGL2)
    this.texState = [this.createTexture(gl.RGBA8, gl.RGBA), this.createTexture(gl.RGBA8, gl.RGBA)];
    this.texColor = [this.createTexture(gl.RGBA8, gl.RGBA), this.createTexture(gl.RGBA8, gl.RGBA)];

    this.fbState = [this.createFramebuffer(this.texState[0]), this.createFramebuffer(this.texState[1])];
    this.fbColor = [this.createFramebuffer(this.texColor[0]), this.createFramebuffer(this.texColor[1])];

    // Initialize state
    this.reset();
  }

  private vertexShaderSource(): string {
    return `#version 300 es
    in vec2 a_position;
    out vec2 v_texCoord;

    void main() {
      // Flip y-coordinate to match canvas coordinate system (origin at top-left)
      v_texCoord = vec2(a_position.x * 0.5 + 0.5, -a_position.y * 0.5 + 0.5);
      gl_Position = vec4(a_position, 0.0, 1.0);
    }`;
  }

  private stateShaderSource(): string {
    return `#version 300 es
    precision highp float;

    in vec2 v_texCoord;

    uniform sampler2D u_texState;  // RG = (A, B), BA unused
    uniform vec2 u_resolution;
    uniform float u_feed;
    uniform float u_kill;
    uniform float u_diffA;
    uniform float u_diffB;
    uniform float u_dt;
    uniform float u_frame;
    uniform float u_swirlSpeed;
    uniform float u_gpuNoiseStrength;  // 0-1: strength of GPU noise

    out vec4 outState;  // RG = (nextA, nextB), BA unused

    // Classic Perlin Noise
    vec2 fade(vec2 t) { return t*t*t*(t*(t*6.0-15.0)+10.0); }
    vec4 permute(vec4 x) { return mod(((x*34.0)+1.0)*x, 289.0); }

    float pnoise(vec2 P) {
      vec4 Pi = floor(P.xyxy) + vec4(0.0, 0.0, 1.0, 1.0);
      vec4 Pf = fract(P.xyxy) - vec4(0.0, 0.0, 1.0, 1.0);
      Pi = mod(Pi, 289.0);
      vec4 ix = Pi.xzxz;
      vec4 iy = Pi.yyww;
      vec4 fx = Pf.xzxz;
      vec4 fy = Pf.yyww;
      vec4 i = permute(permute(ix) + iy);
      vec4 gx = 2.0 * fract(i / 41.0) - 1.0;
      vec4 gy = abs(gx) - 0.5;
      vec4 tx = floor(gx + 0.5);
      gx = gx - tx;
      vec2 g00 = vec2(gx.x,gy.x);
      vec2 g10 = vec2(gx.y,gy.y);
      vec2 g01 = vec2(gx.z,gy.z);
      vec2 g11 = vec2(gx.w,gy.w);
      vec4 norm = 1.79284291400159 - 0.85373472095314 *
        vec4(dot(g00, g00), dot(g01, g01), dot(g10, g10), dot(g11, g11));
      g00 *= norm.x;
      g01 *= norm.y;
      g10 *= norm.z;
      g11 *= norm.w;
      float n00 = dot(g00, vec2(fx.x, fy.x));
      float n10 = dot(g10, vec2(fx.y, fy.y));
      float n01 = dot(g01, vec2(fx.z, fy.z));
      float n11 = dot(g11, vec2(fx.w, fy.w));
      vec2 fade_xy = fade(Pf.xy);
      vec2 n_x = mix(vec2(n00, n01), vec2(n10, n11), fade_xy.x);
      float n_xy = mix(n_x.x, n_x.y, fade_xy.y);
      return 2.3 * n_xy;
    }

    // SIMPLEX (commented out - switch back by uncommenting and commenting Perlin above)
    // vec3 permute(vec3 x) { return mod(((x*34.0)+1.0)*x, 289.0); }
    // float snoise(vec2 v) {
    //   const vec4 C = vec4(0.211324865405187, 0.366025403784439,
    //             -0.577350269189626, 0.024390243902439);
    //   vec2 i  = floor(v + dot(v, C.yy));
    //   vec2 x0 = v -   i + dot(i, C.xx);
    //   vec2 i1;
    //   i1 = (x0.x > x0.y) ? vec2(1.0, 0.0) : vec2(0.0, 1.0);
    //   vec4 x12 = x0.xyxy + C.xxzz;
    //   x12.xy -= i1;
    //   i = mod(i, 289.0);
    //   vec3 p = permute(permute(i.y + vec3(0.0, i1.y, 1.0))
    //     + i.x + vec3(0.0, i1.x, 1.0));
    //   vec3 m = max(0.5 - vec3(dot(x0,x0), dot(x12.xy,x12.xy),
    //     dot(x12.zw,x12.zw)), 0.0);
    //   m = m*m;
    //   m = m*m;
    //   vec3 x = 2.0 * fract(p * C.www) - 1.0;
    //   vec3 h = abs(x) - 0.5;
    //   vec3 ox = floor(x + 0.5);
    //   vec3 a0 = x - ox;
    //   m *= 1.79284291400159 - 0.85373472095314 * (a0*a0 + h*h);
    //   vec3 g;
    //   g.x  = a0.x  * x0.x  + h.x  * x0.y;
    //   g.yz = a0.yz * x12.xz + h.yz * x12.yw;
    //   return 130.0 * dot(m, g);
    // }

    // Multi-octave noise (using Perlin temporarily)
    float multiOctaveNoise(vec2 pos) {
      if (u_gpuNoiseStrength <= 0.0) {
        return 0.0;
      }

      // Perlin noise with lower frequencies for smoother result (returns [-1, 1])
      float result = 0.0;
      result += pnoise(pos * 2.0) * 1.0;    // Very large, smooth swirls
      result += pnoise(pos * 5.0) * 0.5;    // Medium details
      result += pnoise(pos * 12.0) * 0.25;  // Fine details
      result /= 1.75; // Normalize

      // SIMPLEX version (commented out)
      // result += snoise(pos * 2.0) * 1.0;    // Very large, smooth swirls
      // result += snoise(pos * 5.0) * 0.5;    // Medium details
      // result += snoise(pos * 12.0) * 0.25;  // Fine details

      return result * u_gpuNoiseStrength;
    }

    vec2 laplacian(sampler2D tex, vec2 uv, vec2 texelSize) {
      vec2 center = texture(tex, uv).rg;
      vec2 sum = center * -1.0;

      // 9-point stencil
      sum += texture(tex, uv + vec2(0.0, texelSize.y)).rg * 0.2;
      sum += texture(tex, uv - vec2(0.0, texelSize.y)).rg * 0.2;
      sum += texture(tex, uv + vec2(texelSize.x, 0.0)).rg * 0.2;
      sum += texture(tex, uv - vec2(texelSize.x, 0.0)).rg * 0.2;

      sum += texture(tex, uv + vec2(texelSize.x, texelSize.y)).rg * 0.05;
      sum += texture(tex, uv + vec2(-texelSize.x, texelSize.y)).rg * 0.05;
      sum += texture(tex, uv + vec2(texelSize.x, -texelSize.y)).rg * 0.05;
      sum += texture(tex, uv + vec2(-texelSize.x, -texelSize.y)).rg * 0.05;

      return sum;
    }

    void main() {
      vec2 texelSize = 1.0 / u_resolution;

      vec2 warpedCoord = v_texCoord;

      // Only apply domain warping if swirl speed > 0
      if (u_swirlSpeed > 0.0) {
        // Domain warping: add subtle swirling/rippling displacement
        // Use much slower time scale for gentle motion
        float time = u_frame * 0.00002 * u_swirlSpeed;

        // Sample GPU noise at multiple scales with time offset
        vec2 offset1 = vec2(time * 0.015, time * 0.012);
        vec2 offset2 = vec2(-time * 0.008, time * 0.011);

        // Multi-octave noise returns [-1, 1], map to [0, 1]
        float noise1 = (multiOctaveNoise(v_texCoord * 1.5 + offset1) + 1.0) * 0.5;
        float noise2 = (multiOctaveNoise(v_texCoord * 2.3 + offset2) + 1.0) * 0.5;
        float noise3 = (multiOctaveNoise(v_texCoord * 0.8 + offset1 * 0.5) + 1.0) * 0.5;

        // Combine noise samples for smooth, non-directional displacement
        vec2 displacement = vec2(
          (noise1 - 0.5) * 0.002 + (noise3 - 0.5) * 0.001,
          (noise2 - 0.5) * 0.002 + (noise3 - 0.5) * 0.001
        ) * u_swirlSpeed;

        warpedCoord = v_texCoord + displacement;
      }
      vec2 state = texture(u_texState, warpedCoord).rg;
      float A = state.r;
      float B = state.g;

      // Sample GPU multi-octave noise for spatial variation
      float noise = (multiOctaveNoise(v_texCoord * 2.0) + 1.0) * 0.5; // Map [-1,1] to [0,1]

      // Add variation to feed/kill parameters (-0.002 to +0.002)
      float feedVar = (noise - 0.5) * 0.004;
      float killVar = (noise - 0.5) * 0.004;
      float localFeed = u_feed + feedVar;
      float localKill = u_kill + killVar;

      // Compute Laplacians with warped coordinates
      vec2 lap = laplacian(u_texState, warpedCoord, texelSize);
      float lapA = lap.r;
      float lapB = lap.g;

      // Gray-Scott reaction
      float reaction = A * B * B;

      // Update chemicals with spatially-varying parameters
      float nextA = A + (u_diffA * lapA - reaction + localFeed * (1.0 - A)) * u_dt;
      float nextB = B + (u_diffB * lapB + reaction - (localKill + localFeed) * B) * u_dt;

      // Clamp and output (RG = state, BA unused)
      outState = vec4(clamp(vec2(nextA, nextB), 0.0, 1.0), 0.0, 1.0);
    }`;
  }

  private colorShaderSource(): string {
    return `#version 300 es
    precision highp float;

    in vec2 v_texCoord;

    uniform sampler2D u_texState;  // RG = (A, B)
    uniform sampler2D u_texColor;  // RGB
    uniform vec2 u_resolution;
    uniform float u_diffB;
    uniform float u_dt;
    uniform float u_frame;
    uniform float u_swirlSpeed;
    uniform float u_gpuNoiseStrength;  // 0-1: strength of GPU noise

    out vec4 outColor;  // RGB

    // Classic Perlin Noise
    vec2 fade(vec2 t) { return t*t*t*(t*(t*6.0-15.0)+10.0); }
    vec4 permute(vec4 x) { return mod(((x*34.0)+1.0)*x, 289.0); }

    float pnoise(vec2 P) {
      vec4 Pi = floor(P.xyxy) + vec4(0.0, 0.0, 1.0, 1.0);
      vec4 Pf = fract(P.xyxy) - vec4(0.0, 0.0, 1.0, 1.0);
      Pi = mod(Pi, 289.0);
      vec4 ix = Pi.xzxz;
      vec4 iy = Pi.yyww;
      vec4 fx = Pf.xzxz;
      vec4 fy = Pf.yyww;
      vec4 i = permute(permute(ix) + iy);
      vec4 gx = 2.0 * fract(i / 41.0) - 1.0;
      vec4 gy = abs(gx) - 0.5;
      vec4 tx = floor(gx + 0.5);
      gx = gx - tx;
      vec2 g00 = vec2(gx.x,gy.x);
      vec2 g10 = vec2(gx.y,gy.y);
      vec2 g01 = vec2(gx.z,gy.z);
      vec2 g11 = vec2(gx.w,gy.w);
      vec4 norm = 1.79284291400159 - 0.85373472095314 *
        vec4(dot(g00, g00), dot(g01, g01), dot(g10, g10), dot(g11, g11));
      g00 *= norm.x;
      g01 *= norm.y;
      g10 *= norm.z;
      g11 *= norm.w;
      float n00 = dot(g00, vec2(fx.x, fy.x));
      float n10 = dot(g10, vec2(fx.y, fy.y));
      float n01 = dot(g01, vec2(fx.z, fy.z));
      float n11 = dot(g11, vec2(fx.w, fy.w));
      vec2 fade_xy = fade(Pf.xy);
      vec2 n_x = mix(vec2(n00, n01), vec2(n10, n11), fade_xy.x);
      float n_xy = mix(n_x.x, n_x.y, fade_xy.y);
      return 2.3 * n_xy;
    }

    // SIMPLEX (commented out - switch back by uncommenting)
    // vec3 permute(vec3 x) { return mod(((x*34.0)+1.0)*x, 289.0); }
    // float snoise(vec2 v) {
    //   const vec4 C = vec4(0.211324865405187, 0.366025403784439,
    //             -0.577350269189626, 0.024390243902439);
    //   vec2 i  = floor(v + dot(v, C.yy));
    //   vec2 x0 = v -   i + dot(i, C.xx);
    //   vec2 i1;
    //   i1 = (x0.x > x0.y) ? vec2(1.0, 0.0) : vec2(0.0, 1.0);
    //   vec4 x12 = x0.xyxy + C.xxzz;
    //   x12.xy -= i1;
    //   i = mod(i, 289.0);
    //   vec3 p = permute(permute(i.y + vec3(0.0, i1.y, 1.0))
    //     + i.x + vec3(0.0, i1.x, 1.0));
    //   vec3 m = max(0.5 - vec3(dot(x0,x0), dot(x12.xy,x12.xy),
    //     dot(x12.zw,x12.zw)), 0.0);
    //   m = m*m;
    //   m = m*m;
    //   vec3 x = 2.0 * fract(p * C.www) - 1.0;
    //   vec3 h = abs(x) - 0.5;
    //   vec3 ox = floor(x + 0.5);
    //   vec3 a0 = x - ox;
    //   m *= 1.79284291400159 - 0.85373472095314 * (a0*a0 + h*h);
    //   vec3 g;
    //   g.x  = a0.x  * x0.x  + h.x  * x0.y;
    //   g.yz = a0.yz * x12.xz + h.yz * x12.yw;
    //   return 130.0 * dot(m, g);
    // }

    // Multi-octave noise (using Perlin temporarily)
    float multiOctaveNoise(vec2 pos) {
      if (u_gpuNoiseStrength <= 0.0) {
        return 0.0;
      }

      // Perlin noise with lower frequencies for smoother result (returns [-1, 1])
      float result = 0.0;
      result += pnoise(pos * 2.0) * 1.0;    // Very large, smooth swirls
      result += pnoise(pos * 5.0) * 0.5;    // Medium details
      result += pnoise(pos * 12.0) * 0.25;  // Fine details
      result /= 1.75; // Normalize

      // SIMPLEX version (commented out)
      // result += snoise(pos * 2.0) * 1.0;    // Very large, smooth swirls
      // result += snoise(pos * 5.0) * 0.5;    // Medium details
      // result += snoise(pos * 12.0) * 0.25;  // Fine details

      return result * u_gpuNoiseStrength;
    }

    vec3 laplacianColor(sampler2D tex, vec2 uv, vec2 texelSize) {
      vec3 center = texture(tex, uv).rgb;
      vec3 sum = center * -1.0;

      sum += texture(tex, uv + vec2(0.0, texelSize.y)).rgb * 0.2;
      sum += texture(tex, uv - vec2(0.0, texelSize.y)).rgb * 0.2;
      sum += texture(tex, uv + vec2(texelSize.x, 0.0)).rgb * 0.2;
      sum += texture(tex, uv - vec2(texelSize.x, 0.0)).rgb * 0.2;

      sum += texture(tex, uv + vec2(texelSize.x, texelSize.y)).rgb * 0.05;
      sum += texture(tex, uv + vec2(-texelSize.x, texelSize.y)).rgb * 0.05;
      sum += texture(tex, uv + vec2(texelSize.x, -texelSize.y)).rgb * 0.05;
      sum += texture(tex, uv + vec2(-texelSize.x, -texelSize.y)).rgb * 0.05;

      return sum;
    }

    void main() {
      vec2 texelSize = 1.0 / u_resolution;

      vec2 warpedCoord = v_texCoord;

      // Only apply domain warping if swirl speed > 0 (same as state shader)
      if (u_swirlSpeed > 0.0) {
        // Domain warping: add subtle swirling/rippling displacement
        // Use much slower time scale for gentle motion
        float time = u_frame * 0.00002 * u_swirlSpeed;

        // Sample GPU noise at multiple scales with time offset
        vec2 offset1 = vec2(time * 0.015, time * 0.012);
        vec2 offset2 = vec2(-time * 0.008, time * 0.011);

        // pnoise returns [-1, 1], map to [0, 1]
        float noise1 = (pnoise((v_texCoord * 1.5 + offset1) * 100.0) + 1.0) * 0.5;
        float noise2 = (pnoise((v_texCoord * 2.3 + offset2) * 100.0) + 1.0) * 0.5;
        float noise3 = (pnoise((v_texCoord * 0.8 + offset1 * 0.5) * 100.0) + 1.0) * 0.5;

        // SIMPLEX version (commented out)
        // float noise1 = (snoise((v_texCoord * 1.5 + offset1) * 100.0) + 1.0) * 0.5;
        // float noise2 = (snoise((v_texCoord * 2.3 + offset2) * 100.0) + 1.0) * 0.5;
        // float noise3 = (snoise((v_texCoord * 0.8 + offset1 * 0.5) * 100.0) + 1.0) * 0.5;

        // Combine noise samples for smooth, non-directional displacement
        vec2 displacement = vec2(
          (noise1 - 0.5) * 0.002 + (noise3 - 0.5) * 0.001,
          (noise2 - 0.5) * 0.002 + (noise3 - 0.5) * 0.001
        ) * u_swirlSpeed;

        warpedCoord = v_texCoord + displacement;
      }

      float B = texture(u_texState, warpedCoord).g;
      vec3 color = texture(u_texColor, warpedCoord).rgb;

      // Color diffusion (very slight, only where B is high)
      float colorDiffusion = B > 0.1 ? u_diffB * 0.02 : 0.0;
      vec3 lapColor = laplacianColor(u_texColor, warpedCoord, texelSize);
      vec3 nextColor = color + colorDiffusion * lapColor * u_dt;

      outColor = vec4(clamp(nextColor, 0.0, 1.0), 1.0);
    }`;
  }

  private renderShaderSource(): string {
    return `#version 300 es
    precision highp float;

    in vec2 v_texCoord;

    uniform sampler2D u_texState;  // RG = (A, B)
    uniform sampler2D u_texColor;  // RGB
    uniform float u_frame;
    uniform float u_rotation;  // Rotation angle in radians

    out vec4 outColor;

    vec3 hslToRgb(float h, float s, float l) {
      h /= 360.0;
      float k0 = mod(0.0 + h * 12.0, 12.0);
      float k8 = mod(8.0 + h * 12.0, 12.0);
      float k4 = mod(4.0 + h * 12.0, 12.0);

      float a = s * min(l, 1.0 - l);

      float f0 = l - a * max(-1.0, min(k0 - 3.0, min(9.0 - k0, 1.0)));
      float f8 = l - a * max(-1.0, min(k8 - 3.0, min(9.0 - k8, 1.0)));
      float f4 = l - a * max(-1.0, min(k4 - 3.0, min(9.0 - k4, 1.0)));

      return vec3(f0, f8, f4);
    }

    void main() {
      // Apply rotation around center
      vec2 centered = v_texCoord - 0.5;
      float cosA = cos(u_rotation);
      float sinA = sin(u_rotation);
      vec2 rotated = vec2(
        centered.x * cosA - centered.y * sinA,
        centered.x * sinA + centered.y * cosA
      );
      vec2 rotatedCoord = rotated + 0.5;

      vec2 state = texture(u_texState, rotatedCoord).rg;
      float A = state.r;
      float B = state.g;
      vec3 color = texture(u_texColor, rotatedCoord).rgb;

      // Smooth intensity
      float baseIntensity = B * 2.5;
      float ambientFill = (1.0 - A) * 0.8;
      float rawIntensity = max(baseIntensity, ambientFill);
      float intensity = pow(min(1.0, rawIntensity), 0.6);

      // Check if user color is present
      float colorMagnitude = length(color * 255.0);
      bool hasColor = colorMagnitude > 20.0;

      vec3 finalColor;
      if (hasColor) {
        // User color with saturation boost
        finalColor = color * intensity * 1.3;
      } else {
        // Rainbow gradient
        float hue = mod(B * 360.0 + u_frame * 0.3 + (v_texCoord.x + v_texCoord.y) * 72.0, 360.0);
        vec3 rainbow = hslToRgb(hue, 0.7, 0.5);
        finalColor = rainbow * intensity;
      }

      outColor = vec4(finalColor, 1.0);
    }`;
  }

  private brushVertexShaderSource(): string {
    return `#version 300 es
    in vec2 a_position;

    uniform vec2 u_brushCenter;  // In normalized coordinates [0,1]
    uniform float u_brushRadius; // In pixels
    uniform vec2 u_resolution;   // Grid resolution

    out vec2 v_position;
    out vec2 v_worldPos;  // Absolute position for noise sampling

    void main() {
      // Convert from normalized [0,1] to clip space [-1,1]
      vec2 clipCenter = u_brushCenter * 2.0 - 1.0;

      // Convert radius to clip space
      vec2 radiusClip = (u_brushRadius / u_resolution) * 2.0;

      // Position vertices around brush center
      vec2 pos = clipCenter + a_position * radiusClip;
      gl_Position = vec4(pos, 0.0, 1.0);

      v_position = a_position; // -1 to 1 relative to brush center

      // Calculate world position for noise sampling
      v_worldPos = u_brushCenter + a_position * (u_brushRadius / u_resolution);
    }`;
  }

  private brushFragmentShaderSource(): string {
    return `#version 300 es
    precision highp float;

    in vec2 v_position;
    in vec2 v_worldPos;

    uniform vec3 u_color;         // RGB color [0,1]
    uniform int u_target;         // 0 = state, 1 = color
    uniform float u_time;         // Time for animated noise
    uniform float u_gpuNoiseStrength;  // 0-1: strength of GPU noise
    uniform int u_brushShape;     // Brush shape: 0=circle, 1=square, 2=triangle, etc.
    uniform float u_flowRate;     // Flow rate: how much chemical B is deposited (0-2)

    out vec4 outColor;

    // Classic Perlin Noise
    vec2 fade(vec2 t) { return t*t*t*(t*(t*6.0-15.0)+10.0); }
    vec4 permute(vec4 x) { return mod(((x*34.0)+1.0)*x, 289.0); }

    float pnoise(vec2 P) {
      vec4 Pi = floor(P.xyxy) + vec4(0.0, 0.0, 1.0, 1.0);
      vec4 Pf = fract(P.xyxy) - vec4(0.0, 0.0, 1.0, 1.0);
      Pi = mod(Pi, 289.0);
      vec4 ix = Pi.xzxz;
      vec4 iy = Pi.yyww;
      vec4 fx = Pf.xzxz;
      vec4 fy = Pf.yyww;
      vec4 i = permute(permute(ix) + iy);
      vec4 gx = 2.0 * fract(i / 41.0) - 1.0;
      vec4 gy = abs(gx) - 0.5;
      vec4 tx = floor(gx + 0.5);
      gx = gx - tx;
      vec2 g00 = vec2(gx.x,gy.x);
      vec2 g10 = vec2(gx.y,gy.y);
      vec2 g01 = vec2(gx.z,gy.z);
      vec2 g11 = vec2(gx.w,gy.w);
      vec4 norm = 1.79284291400159 - 0.85373472095314 *
        vec4(dot(g00, g00), dot(g01, g01), dot(g10, g10), dot(g11, g11));
      g00 *= norm.x;
      g01 *= norm.y;
      g10 *= norm.z;
      g11 *= norm.w;
      float n00 = dot(g00, vec2(fx.x, fy.x));
      float n10 = dot(g10, vec2(fx.y, fy.y));
      float n01 = dot(g01, vec2(fx.z, fy.z));
      float n11 = dot(g11, vec2(fx.w, fy.w));
      vec2 fade_xy = fade(Pf.xy);
      vec2 n_x = mix(vec2(n00, n01), vec2(n10, n11), fade_xy.x);
      float n_xy = mix(n_x.x, n_x.y, fade_xy.y);
      return 2.3 * n_xy;
    }

    // SIMPLEX (commented out - switch back by uncommenting)
    // vec3 permute(vec3 x) { return mod(((x*34.0)+1.0)*x, 289.0); }
    // float snoise(vec2 v) {
    //   const vec4 C = vec4(0.211324865405187, 0.366025403784439,
    //             -0.577350269189626, 0.024390243902439);
    //   vec2 i  = floor(v + dot(v, C.yy));
    //   vec2 x0 = v -   i + dot(i, C.xx);
    //   vec2 i1;
    //   i1 = (x0.x > x0.y) ? vec2(1.0, 0.0) : vec2(0.0, 1.0);
    //   vec4 x12 = x0.xyxy + C.xxzz;
    //   x12.xy -= i1;
    //   i = mod(i, 289.0);
    //   vec3 p = permute(permute(i.y + vec3(0.0, i1.y, 1.0))
    //     + i.x + vec3(0.0, i1.x, 1.0));
    //   vec3 m = max(0.5 - vec3(dot(x0,x0), dot(x12.xy,x12.xy),
    //     dot(x12.zw,x12.zw)), 0.0);
    //   m = m*m;
    //   m = m*m;
    //   vec3 x = 2.0 * fract(p * C.www) - 1.0;
    //   vec3 h = abs(x) - 0.5;
    //   vec3 ox = floor(x + 0.5);
    //   vec3 a0 = x - ox;
    //   m *= 1.79284291400159 - 0.85373472095314 * (a0*a0 + h*h);
    //   vec3 g;
    //   g.x  = a0.x  * x0.x  + h.x  * x0.y;
    //   g.yz = a0.yz * x12.xz + h.yz * x12.yw;
    //   return 130.0 * dot(m, g);
    // }

    // GPU noise function (using Perlin temporarily)
    float getNoise(vec2 pos) {
      if (u_gpuNoiseStrength <= 0.0) {
        return 0.0;
      }

      // Perlin noise (returns [-1, 1])
      return pnoise(pos) * u_gpuNoiseStrength;

      // SIMPLEX version (commented out)
      // return snoise(pos) * u_gpuNoiseStrength;
    }

    // HSL to RGB conversion
    vec3 hslToRgb(float h, float s, float l) {
      h /= 360.0;
      float k0 = mod(0.0 + h * 12.0, 12.0);
      float k8 = mod(8.0 + h * 12.0, 12.0);
      float k4 = mod(4.0 + h * 12.0, 12.0);
      float a = s * min(l, 1.0 - l);
      float f0 = l - a * max(-1.0, min(k0 - 3.0, min(9.0 - k0, 1.0)));
      float f8 = l - a * max(-1.0, min(k8 - 3.0, min(9.0 - k8, 1.0)));
      float f4 = l - a * max(-1.0, min(k4 - 3.0, min(9.0 - k4, 1.0)));
      return vec3(f0, f8, f4);
    }

    // Brush shape functions - return (distance, intensity) pair
    // distance < 1.0 means inside shape, intensity is for anti-aliasing
    vec2 getShapeDistanceAndIntensity(vec2 pos, int shape) {
      float dist = 0.0;
      float intensity = 1.0;

      if (shape == 0) {
        // Circle
        dist = length(pos);
      } else if (shape == 1) {
        // Square
        vec2 d = abs(pos);
        dist = max(d.x, d.y);
      } else if (shape == 2) {
        // Triangle (pointing up)
        const float k = sqrt(3.0);
        vec2 p = vec2(abs(pos.x), pos.y);
        p -= vec2(-1.0, -0.5);
        float d1 = dot(vec2(k/2.0, 0.5), p);
        float d2 = p.y;
        dist = max(d1, d2) / k + 0.5;
      } else if (shape == 3) {
        // Diamond (rotated square)
        float d = (abs(pos.x) + abs(pos.y)) / 1.414;
        dist = d;
      } else if (shape == 4) {
        // Star (5-pointed)
        const float an = 3.141593 / 5.0;
        const float en = 3.141593 / 10.0;
        vec2 acs = vec2(sin(en), cos(en));
        vec2 ecs = vec2(sin(an), cos(an));

        float bn = mod(atan(pos.x, pos.y), 2.0*an) - an;
        vec2 p2 = length(pos) * vec2(cos(bn), abs(sin(bn)));
        p2 -= acs;
        p2 += ecs * clamp(-dot(p2, ecs), 0.0, acs.y/ecs.y);
        dist = length(p2) * sign(p2.x);
        dist = dist * 1.5 + 0.5; // Normalize to [0,1] range
      } else if (shape == 5) {
        // Hexagon
        const vec3 k = vec3(-0.866025404, 0.5, 0.577350269);
        vec2 p = abs(pos);
        p -= 2.0*min(dot(k.xy, p), 0.0)*k.xy;
        p -= vec2(clamp(p.x, -k.z, k.z), 1.0);
        dist = length(p) * sign(p.y);
        dist = dist * 2.0 + 0.5;
      } else if (shape == 6) {
        // Cross/Plus
        vec2 d = abs(pos) - vec2(0.35, 1.0);
        vec2 d2 = abs(pos) - vec2(1.0, 0.35);
        dist = min(
          length(max(d, 0.0)) + min(max(d.x, d.y), 0.0),
          length(max(d2, 0.0)) + min(max(d2.x, d2.y), 0.0)
        );
        dist = dist * 2.0 + 0.5;
      } else if (shape == 7) {
        // Ring/Donut
        float r = length(pos);
        dist = abs(r - 0.65) * 4.0;
      } else if (shape == 8) {
        // Heart
        vec2 p = pos;
        p.y -= 0.25;
        p.x = abs(p.x);

        if (p.y + p.x > 1.0) {
          dist = sqrt(dot(p - vec2(0.25, 0.75), p - vec2(0.25, 0.75)));
        } else {
          dist = sqrt(min(dot(p - vec2(0.0, 1.0), p - vec2(0.0, 1.0)),
                         dot(p - 0.5*max(p.x+p.y, 0.0), p - 0.5*max(p.x+p.y, 0.0))));
        }
        dist = dist * 2.0;
      } else if (shape == 9) {
        // Spiral
        float angle = atan(pos.y, pos.x);
        float r = length(pos);
        float spiralR = (angle + 3.14159) * 0.15;
        dist = abs(r - spiralR) * 6.0;
      } else if (shape == 10) {
        // Crescent/Moon
        float r1 = length(pos);
        float r2 = length(pos - vec2(0.4, 0.0));
        dist = max(r1, -(r2 - 0.6));
      } else if (shape == 11) {
        // Flower (8 petals)
        float angle = atan(pos.y, pos.x);
        float r = length(pos);
        float petalRadius = 0.6 + 0.4 * cos(angle * 4.0);
        dist = r / petalRadius;
      } else if (shape == 12) {
        // Octagon
        const float pi = 3.141593;
        float a = atan(pos.y, pos.x) + pi;
        float r = length(pos);
        float sides = 8.0;
        float segment = 2.0 * pi / sides;
        dist = r * cos(mod(a, segment) - segment/2.0);
      } else if (shape == 13) {
        // Gear
        float angle = atan(pos.y, pos.x);
        float r = length(pos);
        float teeth = 12.0;
        float toothAngle = mod((angle + 3.14159) * teeth / (2.0 * 3.14159), 1.0);
        float toothRadius = (toothAngle < 0.5) ? 1.0 : 0.8;
        dist = r / toothRadius;
      } else if (shape == 14) {
        // Burst (sunburst with 16 rays)
        float angle = atan(pos.y, pos.x);
        float r = length(pos);
        float rays = 16.0;
        float rayPattern = abs(sin(angle * rays / 2.0));
        float rayRadius = 0.6 + 0.4 * rayPattern;
        dist = r / rayRadius;
      } else if (shape == 15) {
        // Lightning bolt
        vec2 p = pos;
        float zigzag = sin(p.y * 8.0) * 0.2;
        dist = abs(p.x - zigzag) * 3.0 + abs(p.y);
      } else if (shape == 16) {
        // Waves
        vec2 p = pos;
        float wave = sin(p.y * 6.0) * 0.15;
        dist = abs(p.x - wave) * 4.0 + abs(p.y) * 0.5;
      } else if (shape == 17) {
        // Grid pattern
        vec2 p = abs(pos);
        float gridSize = 0.2;
        vec2 grid = mod(p, gridSize);
        float gridDist = min(grid.x, grid.y);
        dist = (gridDist < gridSize * 0.15) ? length(pos) * 0.5 : 999.0;
      } else if (shape == 18) {
        // Dots pattern
        vec2 p = pos;
        vec2 dotGrid = fract(p * 5.0) - 0.5;
        dist = length(dotGrid) * 4.0 + length(pos) * 0.3;
      } else if (shape == 19) {
        // Butterfly
        vec2 p = pos;
        p.y = abs(p.y);
        float angle = atan(p.y, p.x);
        float r = length(p);
        float butterfly = sin(angle) * exp(cos(angle)) - 2.0*cos(4.0*angle) + pow(sin(angle/12.0), 5.0);
        dist = r / (0.5 + 0.3 * butterfly);
      } else if (shape == 20) {
        // Clover (4-leaf)
        float angle = atan(pos.y, pos.x);
        float r = length(pos);
        float cloverRadius = 0.6 + 0.4 * abs(cos(angle * 2.0));
        dist = r / cloverRadius;
      } else if (shape == 21) {
        // Eye shape
        vec2 p = pos;
        p.x *= 2.5; // Elongate
        float eyeDist = length(p);
        float pupil = length(pos) * 4.0;
        dist = min(eyeDist, pupil);
      } else if (shape == 22) {
        // Arrow
        vec2 p = pos;
        // Arrow shaft
        float shaft = abs(p.y) - 0.15;
        // Arrow head
        float head = max(abs(p.x) * 1.5 + p.y - 0.5, -p.y - 0.8);
        dist = min(max(shaft, abs(p.x + 0.3) - 0.7), head);
        dist = dist * 3.0 + 0.5;
      } else if (shape == 23) {
        // Snowflake (6 branches)
        float angle = atan(pos.y, pos.x);
        float r = length(pos);
        float branches = 6.0;
        float branchAngle = mod(angle + 3.14159, 2.0 * 3.14159 / branches);
        float nearBranch = abs(branchAngle - 3.14159 / branches);
        dist = (nearBranch < 0.2) ? r : r * 3.0;
      } else if (shape == 24) {
        // Vesica Piscis (two overlapping circles forming almond/mandorla)
        vec2 p = pos;
        float r1 = length(p - vec2(0.3, 0.0));
        float r2 = length(p + vec2(0.3, 0.0));
        // Intersection of two circles
        dist = max(r1, r2);
      } else if (shape == 25) {
        // Mandala (complex circular pattern with multiple layers)
        float angle = atan(pos.y, pos.x);
        float r = length(pos);
        // Multiple layers of petals at different scales
        float layer1 = 0.7 + 0.3 * cos(angle * 8.0);
        float layer2 = 0.5 + 0.2 * cos(angle * 16.0 + 1.0);
        float layer3 = 0.3 + 0.1 * cos(angle * 32.0 + 2.0);
        float mandalaDist = (r / layer1) * (r / layer2) * (r / layer3) * 3.0;
        dist = mandalaDist;
      } else if (shape == 26) {
        // Yantra (Sri Yantra inspired - triangles and sacred geometry)
        vec2 p = abs(pos);
        // Upward triangle
        float t1 = max((p.x * 0.866025 + p.y * 0.5), -p.y);
        // Downward triangle (interlaced)
        float t2 = max((p.x * 0.866025 - p.y * 0.5), p.y - 0.7);
        // Combine triangles
        dist = min(t1, t2);
        // Add circular boundary
        float circle = length(pos) - 0.9;
        dist = max(dist, -circle) * 2.0;
      } else if (shape == 27) {
        // Torus (fuller donut shape)
        vec2 p = pos;
        float r = length(p);
        float torusR = 0.6;
        float thickness = 0.3;
        dist = abs(r - torusR) / thickness;
      } else if (shape == 28) {
        // Metatron's Cube (13 circles with connecting lines)
        vec2 p = pos;
        float r = length(p);
        // Central circle
        float centerCircle = abs(r - 0.0) * 4.0;
        // 6 circles around center in hexagonal pattern
        float minDist = centerCircle;
        for (float i = 0.0; i < 6.0; i++) {
          float a = i * 3.14159 * 2.0 / 6.0;
          vec2 offset = vec2(cos(a), sin(a)) * 0.4;
          float circleDist = abs(length(p - offset) - 0.15) * 8.0;
          minDist = min(minDist, circleDist);
        }
        // Add connecting lines
        float hexPattern = abs(sin(atan(p.y, p.x) * 3.0)) * r;
        minDist = min(minDist, hexPattern * 4.0);
        dist = minDist;
      } else if (shape == 29) {
        // Fibonacci Spiral (golden ratio logarithmic spiral)
        float angle = atan(pos.y, pos.x);
        float r = length(pos);
        // Golden ratio: phi = 1.618
        float phi = 1.618034;
        // Logarithmic spiral: r = a * e^(b*theta)
        float expectedR = 0.1 * pow(phi, angle / 3.14159);
        dist = abs(r - expectedR) * 6.0;
      } else if (shape == 30) {
        // Seed of Life (7 circles: 1 center + 6 hexagonal)
        vec2 p = pos;
        float r = length(p);
        // Central circle
        float centerCircle = abs(r - 0.25);
        float minDist = centerCircle;
        // 6 circles in hexagonal pattern
        for (float i = 0.0; i < 6.0; i++) {
          float a = i * 3.14159 * 2.0 / 6.0;
          vec2 offset = vec2(cos(a), sin(a)) * 0.25;
          float circleDist = abs(length(p - offset) - 0.25);
          minDist = min(minDist, circleDist);
        }
        dist = minDist * 8.0;
      } else if (shape == 31) {
        // Flower of Life (19 circles in overlapping pattern)
        vec2 p = pos;
        float r = length(p);
        float radius = 0.2;
        // Central circle
        float minDist = abs(r - radius);
        // 6 circles around center
        for (float i = 0.0; i < 6.0; i++) {
          float a = i * 3.14159 * 2.0 / 6.0;
          vec2 offset = vec2(cos(a), sin(a)) * radius;
          float circleDist = abs(length(p - offset) - radius);
          minDist = min(minDist, circleDist);
        }
        // 12 outer circles (second ring)
        for (float i = 0.0; i < 6.0; i++) {
          float a = i * 3.14159 * 2.0 / 6.0;
          vec2 offset1 = vec2(cos(a), sin(a)) * radius * 2.0;
          vec2 offset2 = vec2(cos(a + 3.14159 / 6.0), sin(a + 3.14159 / 6.0)) * radius * 1.732;
          minDist = min(minDist, abs(length(p - offset1) - radius));
          minDist = min(minDist, abs(length(p - offset2) - radius));
        }
        dist = minDist * 8.0;
      } else if (shape == 32) {
        // Lotus of Life (variant with 8-fold symmetry)
        vec2 p = pos;
        float r = length(p);
        float radius = 0.22;
        // Central circle
        float minDist = abs(r - radius);
        // 8 circles around center (octagonal pattern)
        for (float i = 0.0; i < 8.0; i++) {
          float a = i * 3.14159 * 2.0 / 8.0;
          vec2 offset = vec2(cos(a), sin(a)) * radius * 1.3;
          float circleDist = abs(length(p - offset) - radius);
          minDist = min(minDist, circleDist);
        }
        dist = minDist * 8.0;
      } else {
        // Default to circle for unknown shapes
        dist = length(pos);
      }

      return vec2(dist, intensity);
    }

    // RGB to HSL conversion (approximate)
    vec3 rgbToHsl(vec3 rgb) {
      float maxC = max(max(rgb.r, rgb.g), rgb.b);
      float minC = min(min(rgb.r, rgb.g), rgb.b);
      float l = (maxC + minC) / 2.0;

      if (maxC == minC) {
        return vec3(0.0, 0.0, l);
      }

      float d = maxC - minC;
      float s = l > 0.5 ? d / (2.0 - maxC - minC) : d / (maxC + minC);

      float h;
      if (maxC == rgb.r) {
        h = (rgb.g - rgb.b) / d + (rgb.g < rgb.b ? 6.0 : 0.0);
      } else if (maxC == rgb.g) {
        h = (rgb.b - rgb.r) / d + 2.0;
      } else {
        h = (rgb.r - rgb.g) / d + 4.0;
      }
      h /= 6.0;

      return vec3(h * 360.0, s, l);
    }

    void main() {
      // Get distance and intensity for selected brush shape
      vec2 shapeData = getShapeDistanceAndIntensity(v_position, u_brushShape);
      float dist = shapeData.x;
      float shapeIntensity = shapeData.y;

      if (dist > 1.0) discard;

      // Gaussian falloff for smooth edges
      float sigma = 0.4;
      float alpha = exp(-(dist * dist) / (2.0 * sigma * sigma)) * shapeIntensity;

      if (u_target == 0) {
        // State texture: set A=0.2, B=flowRate with smooth falloff (higher B = more active patterns)
        outColor = vec4(0.2, u_flowRate, 0.0, alpha);
      } else {
        // Faster, more turbulent swirling motion
        float timeScale = u_time * 0.0001;

        // Create swirling vortex-like motion
        vec2 center = v_worldPos - 0.5;
        float angle = atan(center.y, center.x);
        float radius = length(center);

        // Rotating flow with turbulence
        vec2 flow = vec2(
          sin(timeScale * 1.2 + angle * 2.0) * 0.08,
          cos(timeScale * 0.9 - angle * 1.5) * 0.08
        );

        // Add vortex rotation
        float rotation = timeScale * 0.3;
        vec2 rotatedPos = vec2(
          v_worldPos.x * cos(rotation) - v_worldPos.y * sin(rotation),
          v_worldPos.x * sin(rotation) + v_worldPos.y * cos(rotation)
        );

        // Multi-octave noise for cloud-like, tie-dye effect
        // Layer multiple frequencies for organic, flowing patterns
        vec2 noisePos = rotatedPos + flow;
        float noise = 0.0;
        noise += getNoise(noisePos * 0.8) * 0.5;      // Very large, smooth swirls
        noise += getNoise(noisePos * 2.0) * 0.3;      // Medium details
        noise += getNoise(noisePos * 6.0) * 0.2;      // Fine texture
        noise = (noise + 1.0) * 0.5; // Map to [0, 1]

        // Convert to HSL for better color variation
        vec3 hsl = rgbToHsl(u_color);

        // Vary hue more dramatically (±30 degrees) with smoother transitions
        float hueVar = (noise - 0.5) * 60.0;
        hsl.x = mod(hsl.x + hueVar, 360.0);

        // Vary saturation (±15%)
        float satVar = (noise - 0.5) * 0.3;
        hsl.y = clamp(hsl.y + satVar, 0.0, 1.0);

        // Vary lightness (±12%)
        float lightVar = (noise - 0.5) * 0.24;
        hsl.z = clamp(hsl.z + lightVar, 0.0, 1.0);

        // Convert back to RGB
        vec3 variedColor = hslToRgb(hsl.x, hsl.y, hsl.z);

        outColor = vec4(variedColor, alpha);
      }
    }`;
  }

  private copyVertexShaderSource(): string {
    return `#version 300 es
    in vec2 a_position;
    out vec2 v_texCoord;

    void main() {
      // No Y-flip - direct copy
      v_texCoord = a_position * 0.5 + 0.5;
      gl_Position = vec4(a_position, 0.0, 1.0);
    }`;
  }

  private createShader(type: number, source: string): WebGLShader {
    const gl = this.gl;
    const shader = gl.createShader(type)!;
    gl.shaderSource(shader, source);
    gl.compileShader(shader);

    if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
      const info = gl.getShaderInfoLog(shader);
      gl.deleteShader(shader);
      throw new Error('Shader compilation error: ' + info);
    }

    return shader;
  }

  private createProgram(vertexSource: string, fragmentSource: string): WebGLProgram {
    const gl = this.gl;
    const vertexShader = this.createShader(gl.VERTEX_SHADER, vertexSource);
    const fragmentShader = this.createShader(gl.FRAGMENT_SHADER, fragmentSource);

    const program = gl.createProgram()!;
    gl.attachShader(program, vertexShader);
    gl.attachShader(program, fragmentShader);
    gl.linkProgram(program);

    if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
      const info = gl.getProgramInfoLog(program);
      gl.deleteProgram(program);
      throw new Error('Program linking error: ' + info);
    }

    return program;
  }

  private createQuadBuffer(): WebGLBuffer {
    const gl = this.gl;
    const buffer = gl.createBuffer()!;
    gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([
      -1, -1,
       1, -1,
      -1,  1,
       1,  1
    ]), gl.STATIC_DRAW);
    return buffer;
  }

  private createTexture(internalFormat: number, format: number): WebGLTexture {
    const gl = this.gl;
    const texture = gl.createTexture()!;
    gl.bindTexture(gl.TEXTURE_2D, texture);
    // RGBA8 requires UNSIGNED_BYTE, not FLOAT
    gl.texImage2D(gl.TEXTURE_2D, 0, internalFormat, this.width, this.height, 0, format, gl.UNSIGNED_BYTE, null);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
    return texture;
  }

  private createFramebuffer(texture: WebGLTexture): WebGLFramebuffer {
    const gl = this.gl;
    const fb = gl.createFramebuffer()!;
    gl.bindFramebuffer(gl.FRAMEBUFFER, fb);
    gl.framebufferTexture2D(gl.FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, texture, 0);
    return fb;
  }

  fade(amount: number = 0.95): void {
    const gl = this.gl;

    // Create a simple fade shader program if not already created
    if (!this.fadeProgram) {
      const fadeFragmentShader = `#version 300 es
        precision highp float;
        in vec2 v_texCoord;
        uniform sampler2D u_texture;
        uniform float u_fadeAmount;
        out vec4 outColor;

        void main() {
          vec4 current = texture(u_texture, v_texCoord);
          outColor = vec4(current.rgb, 1.0) * u_fadeAmount;
        }
      `;
      this.fadeProgram = this.createProgram(this.copyVertexShaderSource(), fadeFragmentShader);
    }

    // Fade both state and color textures
    gl.useProgram(this.fadeProgram);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.quadBuffer);

    const posLoc = gl.getAttribLocation(this.fadeProgram, 'a_position');
    gl.enableVertexAttribArray(posLoc);
    gl.vertexAttribPointer(posLoc, 2, gl.FLOAT, false, 0, 0);

    const fadeAmountLoc = gl.getUniformLocation(this.fadeProgram, 'u_fadeAmount');
    gl.uniform1f(fadeAmountLoc, amount);

    // Fade state texture
    gl.activeTexture(gl.TEXTURE0);
    gl.bindTexture(gl.TEXTURE_2D, this.texState[0]);
    gl.uniform1i(gl.getUniformLocation(this.fadeProgram, 'u_texture'), 0);
    gl.bindFramebuffer(gl.FRAMEBUFFER, this.fbState[1]);
    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);

    // Swap state buffers
    [this.texState[0], this.texState[1]] = [this.texState[1], this.texState[0]];
    [this.fbState[0], this.fbState[1]] = [this.fbState[1], this.fbState[0]];

    // Fade color texture
    gl.bindTexture(gl.TEXTURE_2D, this.texColor[0]);
    gl.uniform1i(gl.getUniformLocation(this.fadeProgram, 'u_texture'), 0);
    gl.bindFramebuffer(gl.FRAMEBUFFER, this.fbColor[1]);
    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);

    // Swap color buffers
    [this.texColor[0], this.texColor[1]] = [this.texColor[1], this.texColor[0]];
    [this.fbColor[0], this.fbColor[1]] = [this.fbColor[1], this.fbColor[0]];
  }

  reset(): void {
    const gl = this.gl;

    // Initialize state: A = 1.0, B = 0.0 (RGBA8 format, use RG channels, values 0-255)
    const dataState = new Uint8Array(this.width * this.height * 4);
    for (let i = 0; i < this.width * this.height; i++) {
      dataState[i * 4] = 255;  // A (R channel) = 1.0 * 255
      dataState[i * 4 + 1] = 0;   // B (G channel) = 0.0 * 255
      dataState[i * 4 + 2] = 0;   // unused
      dataState[i * 4 + 3] = 255; // unused
    }

    // Initialize color to black (RGBA8 format, values 0-255)
    const dataColor = new Uint8Array(this.width * this.height * 4);
    for (let i = 0; i < this.width * this.height; i++) {
      dataColor[i * 4] = 0;     // R
      dataColor[i * 4 + 1] = 0; // G
      dataColor[i * 4 + 2] = 0; // B
      dataColor[i * 4 + 3] = 255; // A
    }

    gl.bindTexture(gl.TEXTURE_2D, this.texState[0]);
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, this.width, this.height, 0, gl.RGBA, gl.UNSIGNED_BYTE, dataState);
    gl.bindTexture(gl.TEXTURE_2D, this.texState[1]);
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, this.width, this.height, 0, gl.RGBA, gl.UNSIGNED_BYTE, dataState);

    gl.bindTexture(gl.TEXTURE_2D, this.texColor[0]);
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, this.width, this.height, 0, gl.RGBA, gl.UNSIGNED_BYTE, dataColor);
    gl.bindTexture(gl.TEXTURE_2D, this.texColor[1]);
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, this.width, this.height, 0, gl.RGBA, gl.UNSIGNED_BYTE, dataColor);

    this.frame = 0;
    this.pingpong = 0;
  }

  updateConfig(config: Partial<SimulationConfig>): void {
    if (config.feed !== undefined) this.feed = config.feed;
    if (config.kill !== undefined) this.kill = config.kill;
    if (config.diffA !== undefined) this.diffA = config.diffA;
    if (config.diffB !== undefined) this.diffB = config.diffB;
    if (config.dt !== undefined) this.dt = config.dt;
    if (config.swirlSpeed !== undefined) this.swirlSpeed = config.swirlSpeed;
  }

  setRotation(rotation: number): void {
    this.rotation = rotation;
  }

  inject(x: number, y: number, brushSize: number, color: { r: number; g: number; b: number }, brushShapeIndex: number = 0, flowRate: number = 1.0): void {
    const gl = this.gl;

    // Convert coordinates to normalized [0,1]
    const centerX = x / this.width;
    const centerY = y / this.height;

    gl.useProgram(this.brushProgram);
    gl.viewport(0, 0, this.width, this.height);

    // Unbind all framebuffer textures to prevent feedback loop
    // (We might have texState or texColor bound from previous step() call)
    gl.activeTexture(gl.TEXTURE0);
    gl.bindTexture(gl.TEXTURE_2D, null);
    gl.activeTexture(gl.TEXTURE1);
    gl.bindTexture(gl.TEXTURE_2D, null);
    gl.activeTexture(gl.TEXTURE2);
    gl.bindTexture(gl.TEXTURE_2D, null);

    // Set brush uniforms using cached locations
    gl.uniform2f(this.brushUniforms.brushCenter, centerX, centerY);
    gl.uniform1f(this.brushUniforms.brushRadius, brushSize);
    gl.uniform2f(this.brushUniforms.resolution, this.width, this.height);
    gl.uniform3f(this.brushUniforms.color, color.r / 255, color.g / 255, color.b / 255);
    gl.uniform1f(this.brushUniforms.time, this.frame);
    gl.uniform1i(this.brushUniforms.brushShape, brushShapeIndex);
    gl.uniform1f(this.brushUniforms.flowRate, flowRate);

    // Set GPU noise strength uniform
    gl.uniform1f(gl.getUniformLocation(this.brushProgram, 'u_gpuNoiseStrength'), this.gpuNoiseStrength);

    // Enable blending for smooth painting with anti-aliased edges
    gl.enable(gl.BLEND);
    gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA); // Alpha blending

    // Draw to CURRENT buffer only (the source buffer that step() will read from)
    const currentBuffer = this.pingpong;

    // Draw state
    gl.uniform1i(this.brushUniforms.target, 0);
    gl.bindFramebuffer(gl.FRAMEBUFFER, this.fbState[currentBuffer]);
    this.drawQuad();

    // Draw color
    gl.uniform1i(this.brushUniforms.target, 1);
    gl.bindFramebuffer(gl.FRAMEBUFFER, this.fbColor[currentBuffer]);
    this.drawQuad();

    gl.disable(gl.BLEND);
    gl.bindFramebuffer(gl.FRAMEBUFFER, null);
  }

  step(): void {
    if (this.paused) return;

    const gl = this.gl;
    gl.viewport(0, 0, this.width, this.height);

    // For numerical stability, cap individual sub-steps at dt=1.0
    // Run multiple sub-steps if needed to achieve higher animation speeds
    const maxStableDt = 1.0;
    const numSubSteps = Math.max(1, Math.ceil(this.dt / maxStableDt));
    const dt_sub = this.dt / numSubSteps;

    for (let i = 0; i < numSubSteps; i++) {
      const src = this.pingpong;
      const dst = 1 - this.pingpong;

      // Update state (A and B) using Gray-Scott reaction-diffusion
      gl.useProgram(this.stateProgram);
      gl.activeTexture(gl.TEXTURE0);
      gl.bindTexture(gl.TEXTURE_2D, this.texState[src]);
      gl.uniform1i(gl.getUniformLocation(this.stateProgram, 'u_texState'), 0);

      gl.uniform2f(gl.getUniformLocation(this.stateProgram, 'u_resolution'), this.width, this.height);
      gl.uniform1f(gl.getUniformLocation(this.stateProgram, 'u_feed'), this.feed);
      gl.uniform1f(gl.getUniformLocation(this.stateProgram, 'u_kill'), this.kill);
      gl.uniform1f(gl.getUniformLocation(this.stateProgram, 'u_diffA'), this.diffA);
      gl.uniform1f(gl.getUniformLocation(this.stateProgram, 'u_diffB'), this.diffB);
      gl.uniform1f(gl.getUniformLocation(this.stateProgram, 'u_dt'), dt_sub);
      gl.uniform1f(gl.getUniformLocation(this.stateProgram, 'u_frame'), this.frame);
      gl.uniform1f(gl.getUniformLocation(this.stateProgram, 'u_swirlSpeed'), this.swirlSpeed);
      gl.uniform1f(gl.getUniformLocation(this.stateProgram, 'u_gpuNoiseStrength'), this.gpuNoiseStrength);
      gl.bindFramebuffer(gl.FRAMEBUFFER, this.fbState[dst]);
      this.drawQuad();

      // Update color with diffusion
      gl.useProgram(this.colorProgram);
      gl.activeTexture(gl.TEXTURE0);
      gl.bindTexture(gl.TEXTURE_2D, this.texState[dst]);
      gl.uniform1i(gl.getUniformLocation(this.colorProgram, 'u_texState'), 0);
      gl.activeTexture(gl.TEXTURE1);
      gl.bindTexture(gl.TEXTURE_2D, this.texColor[src]);
      gl.uniform1i(gl.getUniformLocation(this.colorProgram, 'u_texColor'), 1);

      gl.uniform2f(gl.getUniformLocation(this.colorProgram, 'u_resolution'), this.width, this.height);
      gl.uniform1f(gl.getUniformLocation(this.colorProgram, 'u_diffB'), this.diffB);
      gl.uniform1f(gl.getUniformLocation(this.colorProgram, 'u_dt'), dt_sub);
      gl.uniform1f(gl.getUniformLocation(this.colorProgram, 'u_frame'), this.frame);
      gl.uniform1f(gl.getUniformLocation(this.colorProgram, 'u_swirlSpeed'), this.swirlSpeed);
      gl.uniform1f(gl.getUniformLocation(this.colorProgram, 'u_gpuNoiseStrength'), this.gpuNoiseStrength);
      gl.bindFramebuffer(gl.FRAMEBUFFER, this.fbColor[dst]);
      this.drawQuad();

      // Swap buffers for next sub-step
      this.pingpong = dst;
    }

    this.frame++;
  }

  render(): void {
    const gl = this.gl;

    // Render to canvas
    gl.bindFramebuffer(gl.FRAMEBUFFER, null);
    gl.viewport(0, 0, this.canvas.width, this.canvas.height);
    gl.useProgram(this.renderProgram);

    // Bind textures (use current pingpong buffers)
    gl.activeTexture(gl.TEXTURE0);
    gl.bindTexture(gl.TEXTURE_2D, this.texState[this.pingpong]);
    gl.uniform1i(gl.getUniformLocation(this.renderProgram, 'u_texState'), 0);

    gl.activeTexture(gl.TEXTURE1);
    gl.bindTexture(gl.TEXTURE_2D, this.texColor[this.pingpong]);
    gl.uniform1i(gl.getUniformLocation(this.renderProgram, 'u_texColor'), 1);

    gl.uniform1f(gl.getUniformLocation(this.renderProgram, 'u_frame'), this.frame);
    gl.uniform1f(gl.getUniformLocation(this.renderProgram, 'u_rotation'), this.rotation);

    this.drawQuad();
  }

  private drawQuad(): void {
    const gl = this.gl;
    const posLoc = gl.getAttribLocation(gl.getParameter(gl.CURRENT_PROGRAM) as WebGLProgram, 'a_position');

    gl.bindBuffer(gl.ARRAY_BUFFER, this.quadBuffer);
    gl.enableVertexAttribArray(posLoc);
    gl.vertexAttribPointer(posLoc, 2, gl.FLOAT, false, 0, 0);

    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
  }

  // Stub methods for compatibility
  loadCells(_cells: CellData[]): void {
    // Not implemented for WebGL version
  }

  getChangedCells(_threshold?: number): CellData[] {
    return [];
  }

  getAllCells(): CellData[] {
    return [];
  }
}
