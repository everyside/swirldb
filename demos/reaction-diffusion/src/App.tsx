import { useEffect, useRef, useState } from 'react';
import { ReactionDiffusion } from './lib/ReactionDiffusion';
import { ReactionDiffusionWebGL } from './lib/ReactionDiffusionWebGL';
import { PRESETS, DEFAULT_PRESET } from './lib/presets';
import { Controls } from './components/Controls';
import { getCurveValue, CurveName } from './lib/curves';
import { BRUSH_SHAPE_NAMES } from './lib/brushShapes';
import './App.css';

const CELL_SIZE = 1;
const TARGET_FPS = 60;
const STORAGE_KEY = 'reaction-diffusion-settings';

// Default settings
const DEFAULT_SETTINGS = {
  gridSize: 4096,
  brushSize: 96,
  brushShape: 'circle',
  currentPreset: DEFAULT_PRESET,
  baseFeed: PRESETS[DEFAULT_PRESET].feed,
  baseKill: 0.04,
  taperMin: 0.1,
  taperSensitivity: 0.7,
  smoothing: 0.9,
  symmetry: 4,
  baseSwirlSpeed: 0.5,
  gpuNoiseStrength: 1.0,
  userColor: { r: 100, g: 150, b: 200 },
  colorOpacity: 1.0, // Color injection opacity (0-1)
  flowRate: 10.0, // Flow rate: how much chemical B is deposited (0-20)
  automationSpeed: 1.0, // Overall speed multiplier for all automations
  animationSpeed: 1.0, // Speed multiplier for reaction-diffusion simulation
  automationCurve: 'smooth' as CurveName, // Automation curve type
  rotationSpeed: 0.5, // Rotation speed: 0.5 = stopped, >0.5 = CW, <0.5 = CCW
  feedAutomationEnabled: true,
  feedAutomationMin: 0,
  feedAutomationMax: 0.1,
  killAutomationEnabled: true,
  killAutomationMin: 0,
  killAutomationMax: 0.1,
  swirlAutomationEnabled: true,
  swirlAutomationMin: 0,
  swirlAutomationMax: 1,
  symmetryAutomationEnabled: true,
  symmetryAutomationMin: 4,
  symmetryAutomationMax: 8,
  presetAutomationEnabled: true,
  presetAutomationMin: 0,
  presetAutomationMax: Object.keys(PRESETS).length - 1,
  rotationAutomationEnabled: false,
  rotationAutomationMin: 0,
  rotationAutomationMax: 1,
};

// Load settings from localStorage
function loadSettings(): typeof DEFAULT_SETTINGS {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) {
      const parsed = JSON.parse(saved);
      return { ...DEFAULT_SETTINGS, ...parsed };
    }
  } catch (error) {
    console.error('Failed to load settings:', error);
  }
  return DEFAULT_SETTINGS;
}

// Save settings to localStorage
function saveSettings(settings: Partial<typeof DEFAULT_SETTINGS>) {
  try {
    const current = loadSettings();
    const updated = { ...current, ...settings };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(updated));
  } catch (error) {
    console.error('Failed to save settings:', error);
  }
}

function App() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const simRef = useRef<ReactionDiffusion | ReactionDiffusionWebGL | null>(null);
  const animationFrameRef = useRef<number | null>(null);
  const lastFrameTimeRef = useRef<number>(0);
  const frameCountRef = useRef<number>(0);
  const lastFpsUpdateRef = useRef<number>(Date.now());

  // Load initial settings from localStorage
  const initialSettings = loadSettings();

  const [gridSize, setGridSize] = useState(initialSettings.gridSize);
  const isPaused = false; // Always running
  const useWebGL = true; // Always use WebGL for better performance
  const [currentPreset, setCurrentPreset] = useState(initialSettings.currentPreset);
  const [brushSize, setBrushSize] = useState(initialSettings.brushSize);
  const [brushShape, setBrushShape] = useState(initialSettings.brushShape);
  const [colorOpacity, setColorOpacity] = useState(initialSettings.colorOpacity);
  const [flowRate, setFlowRate] = useState(initialSettings.flowRate);

  // Adjust brush size when grid size changes
  useEffect(() => {
    const minBrush = Math.max(1, Math.floor(gridSize / 256));
    const maxBrush = 4096; // Match the max from Controls
    if (brushSize < minBrush) {
      setBrushSize(minBrush);
    } else if (brushSize > maxBrush) {
      setBrushSize(maxBrush);
    }
  }, [gridSize, brushSize]);
  const [baseFeed, setBaseFeed] = useState(initialSettings.baseFeed);
  const [baseKill, setBaseKill] = useState(initialSettings.baseKill);
  const [feed, setFeed] = useState(initialSettings.baseFeed);
  const [kill, setKill] = useState(initialSettings.baseKill);
  const autoModulate = true; // Auto-modulation always enabled

  // Overall automation speed multiplier
  const [automationSpeed, setAutomationSpeed] = useState(initialSettings.automationSpeed);

  // Animation speed multiplier (affects simulation dt)
  const [animationSpeed, setAnimationSpeed] = useState(initialSettings.animationSpeed);

  // Automation curve type
  const [automationCurve, setAutomationCurve] = useState<CurveName>(initialSettings.automationCurve);

  // Rotation speed (0.5 = stopped, >0.5 = CW, <0.5 = CCW)
  const [rotationSpeed, setRotationSpeed] = useState(initialSettings.rotationSpeed);
  const [rotation, setRotation] = useState(0); // Accumulated rotation in radians

  // Automation settings for feed
  const [feedAutomationEnabled, setFeedAutomationEnabled] = useState(initialSettings.feedAutomationEnabled);
  const [feedAutomationMin, setFeedAutomationMin] = useState(initialSettings.feedAutomationMin);
  const [feedAutomationMax, setFeedAutomationMax] = useState(initialSettings.feedAutomationMax);

  // Automation settings for kill
  const [killAutomationEnabled, setKillAutomationEnabled] = useState(initialSettings.killAutomationEnabled);
  const [killAutomationMin, setKillAutomationMin] = useState(initialSettings.killAutomationMin);
  const [killAutomationMax, setKillAutomationMax] = useState(initialSettings.killAutomationMax);

  // Automation settings for swirl
  const [swirlAutomationEnabled, setSwirlAutomationEnabled] = useState(initialSettings.swirlAutomationEnabled);
  const [swirlAutomationMin, setSwirlAutomationMin] = useState(initialSettings.swirlAutomationMin);
  const [swirlAutomationMax, setSwirlAutomationMax] = useState(initialSettings.swirlAutomationMax);

  // Automation settings for symmetry
  const [symmetryAutomationEnabled, setSymmetryAutomationEnabled] = useState(initialSettings.symmetryAutomationEnabled);
  const [symmetryAutomationMin, setSymmetryAutomationMin] = useState(initialSettings.symmetryAutomationMin);
  const [symmetryAutomationMax, setSymmetryAutomationMax] = useState(initialSettings.symmetryAutomationMax);

  // Automation settings for preset
  const [presetAutomationEnabled, setPresetAutomationEnabled] = useState(initialSettings.presetAutomationEnabled);
  const [presetAutomationMin, setPresetAutomationMin] = useState(initialSettings.presetAutomationMin);
  const [presetAutomationMax, setPresetAutomationMax] = useState(initialSettings.presetAutomationMax);

  // Automation settings for rotation
  const [rotationAutomationEnabled, setRotationAutomationEnabled] = useState(initialSettings.rotationAutomationEnabled);
  const [rotationAutomationMin, setRotationAutomationMin] = useState(initialSettings.rotationAutomationMin);
  const [rotationAutomationMax, setRotationAutomationMax] = useState(initialSettings.rotationAutomationMax);

  const [taperMin, setTaperMin] = useState(initialSettings.taperMin); // Minimum brush size at max speed (0-1)
  const [taperSensitivity, setTaperSensitivity] = useState(initialSettings.taperSensitivity); // How much velocity affects taper
  const [smoothing, setSmoothing] = useState(initialSettings.smoothing); // Position smoothing (0 = no smoothing, 1 = max smoothing)
  const [symmetry, setSymmetry] = useState(initialSettings.symmetry); // Symmetry: 1, 2, 4, or 8
  const [baseSwirlSpeed, setBaseSwirlSpeed] = useState(initialSettings.baseSwirlSpeed); // Base swirl speed
  const [swirlSpeed, setSwirlSpeed] = useState(initialSettings.baseSwirlSpeed); // Ripple/swirl speed (0 = none, 1 = max)
  const [gpuNoiseStrength, setGpuNoiseStrength] = useState(initialSettings.gpuNoiseStrength); // GPU noise strength (0-1)
  const [userColor, setUserColor] = useState(initialSettings.userColor);
  const [fps, setFps] = useState(0);
  const [isDrawing, setIsDrawing] = useState(false);
  const [cursorPos, setCursorPos] = useState<{ x: number; y: number } | null>(null);
  const lastPaintPosRef = useRef<{ x: number; y: number; timestamp: number; brushSize: number } | null>(null);
  const smoothedVelocityRef = useRef(0);
  const smoothedPosRef = useRef<{ x: number; y: number } | null>(null);
  const paintIntervalRef = useRef<number | null>(null);
  const poolStartTimeRef = useRef<number>(0);
  const poolingSizeRef = useRef<number>(0);
  const strokeStartTimeRef = useRef<number>(0); // Track stroke start for ramp-up curve

  const GRID_WIDTH = gridSize;
  const GRID_HEIGHT = gridSize;
  const CANVAS_WIDTH = GRID_WIDTH * CELL_SIZE;
  const CANVAS_HEIGHT = GRID_HEIGHT * CELL_SIZE;

  // Initialize simulation (recreate when switching renderer or changing grid size)
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    if (useWebGL) {
      simRef.current = new ReactionDiffusionWebGL(canvas, {
        width: GRID_WIDTH,
        height: GRID_HEIGHT,
        feed: feed,
        kill: kill,
        diffA: 1.0,
        diffB: 0.3,  // Lower diffB for tighter, more defined patterns
        dt: 1.0,
        swirlSpeed: swirlSpeed * animationSpeed,
        gpuNoiseStrength: gpuNoiseStrength
      });
    } else {
      simRef.current = new ReactionDiffusion({
        width: GRID_WIDTH,
        height: GRID_HEIGHT,
        feed: feed,
        kill: kill,
        diffA: 1.0,
        diffB: 0.3,  // Lower diffB for tighter, more defined patterns
        dt: 1.0
      });
    }

    // Start with blank canvas - no initial patterns
  }, [useWebGL, gridSize, GRID_WIDTH, GRID_HEIGHT]); // Recreate when switching renderer or changing size

  // Update simulation swirl when animation speed changes
  useEffect(() => {
    if (simRef.current) {
      simRef.current.updateConfig({ swirlSpeed: swirlSpeed * animationSpeed });
    }
  }, [animationSpeed, swirlSpeed]);

  // Auto-save settings to localStorage whenever they change
  useEffect(() => {
    saveSettings({
      gridSize,
      brushSize,
      brushShape,
      colorOpacity,
      flowRate,
      currentPreset,
      baseFeed,
      baseKill,
      taperMin,
      taperSensitivity,
      smoothing,
      symmetry,
      baseSwirlSpeed,
      gpuNoiseStrength,
      userColor,
      automationSpeed,
      animationSpeed,
      automationCurve,
      rotationSpeed,
      feedAutomationEnabled,
      feedAutomationMin,
      feedAutomationMax,
      killAutomationEnabled,
      killAutomationMin,
      killAutomationMax,
      swirlAutomationEnabled,
      swirlAutomationMin,
      swirlAutomationMax,
      symmetryAutomationEnabled,
      symmetryAutomationMin,
      symmetryAutomationMax,
      presetAutomationEnabled,
      presetAutomationMin,
      presetAutomationMax,
      rotationAutomationEnabled,
      rotationAutomationMin,
      rotationAutomationMax,
    });
  }, [
    gridSize, brushSize, brushShape, colorOpacity, flowRate, currentPreset, baseFeed, baseKill, taperMin, taperSensitivity,
    smoothing, symmetry, baseSwirlSpeed, gpuNoiseStrength, userColor, automationSpeed, animationSpeed, automationCurve, rotationSpeed,
    feedAutomationEnabled, feedAutomationMin, feedAutomationMax,
    killAutomationEnabled, killAutomationMin, killAutomationMax,
    swirlAutomationEnabled, swirlAutomationMin, swirlAutomationMax,
    symmetryAutomationEnabled, symmetryAutomationMin, symmetryAutomationMax,
    presetAutomationEnabled, presetAutomationMin, presetAutomationMax,
    rotationAutomationEnabled, rotationAutomationMin, rotationAutomationMax,
  ]);

  // Animation loop
  useEffect(() => {
    const canvas = canvasRef.current;
    const sim = simRef.current;
    if (!canvas || !sim) return;

    const ctx = useWebGL ? null : canvas.getContext('2d');

    const animate = (timestamp: number) => {
      const deltaTime = timestamp - lastFrameTimeRef.current;
      const targetFrameTime = 1000 / TARGET_FPS;

      if (deltaTime >= targetFrameTime) {
        // Auto-modulate feed, kill, and swirl with slow noise (independent variations)
        if (autoModulate && !isPaused) {
          // Feed automation
          if (feedAutomationEnabled) {
            const feedNoise = getCurveValue(automationCurve, timestamp * 3 * automationSpeed, 0); // 3x faster movement
            // Map noise from [-1, 1] to automation range
            const modulatedFeed = feedAutomationMin + ((feedNoise + 1) * 0.5) * (feedAutomationMax - feedAutomationMin);

            if (Math.abs(modulatedFeed - feed) > 0.0001) {
              setFeed(modulatedFeed);
              sim.updateConfig({ feed: modulatedFeed });
            }
          }

          // Kill automation
          if (killAutomationEnabled) {
            const killNoise = getCurveValue(automationCurve, timestamp * 3 * automationSpeed, 100); // 3x faster, different offset for independence
            // Map noise from [-1, 1] to automation range
            const modulatedKill = killAutomationMin + ((killNoise + 1) * 0.5) * (killAutomationMax - killAutomationMin);

            if (Math.abs(modulatedKill - kill) > 0.0001) {
              setKill(modulatedKill);
              sim.updateConfig({ kill: modulatedKill });
            }
          }

          // Swirl automation
          if (swirlAutomationEnabled) {
            const swirlNoise = getCurveValue(automationCurve, timestamp * 3 * automationSpeed, 200); // 3x faster, different offset for swirl
            // Map noise from [-1, 1] to automation range
            const modulatedSwirl = swirlAutomationMin + ((swirlNoise + 1) * 0.5) * (swirlAutomationMax - swirlAutomationMin);

            if (Math.abs(modulatedSwirl - swirlSpeed) > 0.01) {
              setSwirlSpeed(modulatedSwirl);
              sim.updateConfig({ swirlSpeed: modulatedSwirl * animationSpeed });
            }
          }

          // Symmetry automation
          if (symmetryAutomationEnabled) {
            const symmetryNoise = getCurveValue(automationCurve, timestamp * 3 * automationSpeed, 300); // 3x faster, different offset
            // Map noise from [-1, 1] to automation range
            // For discrete values, pick the closest valid symmetry value
            const normalizedNoise = (symmetryNoise + 1) * 0.5; // [0, 1]
            const continuousValue = symmetryAutomationMin + normalizedNoise * (symmetryAutomationMax - symmetryAutomationMin);

            // Snap to valid symmetry values: 1, 2, 4, 8
            const validValues = [1, 2, 4, 8].filter(v => v >= symmetryAutomationMin && v <= symmetryAutomationMax);
            if (validValues.length > 0) {
              const newSymmetry = validValues.reduce((prev, curr) =>
                Math.abs(curr - continuousValue) < Math.abs(prev - continuousValue) ? curr : prev
              );
              if (newSymmetry !== symmetry) {
                setSymmetry(newSymmetry);
              }
            }
          }

          // Preset automation (1/3 speed of symmetry)
          if (presetAutomationEnabled) {
            const presetNoise = getCurveValue(automationCurve, timestamp * 1 * automationSpeed, 400); // 1/3 speed of symmetry, different offset
            // Map noise from [-1, 1] to automation range
            const normalizedNoise = (presetNoise + 1) * 0.5; // [0, 1]
            const continuousValue = presetAutomationMin + normalizedNoise * (presetAutomationMax - presetAutomationMin);

            // Map to preset indices and snap to integer
            const presetKeys = Object.keys(PRESETS);
            const newPresetIndex = Math.round(continuousValue);
            const clampedIndex = Math.max(presetAutomationMin, Math.min(presetAutomationMax, newPresetIndex));
            const newPreset = presetKeys[clampedIndex];

            if (newPreset && newPreset !== currentPreset) {
              setCurrentPreset(newPreset);
            }
          }

          // Rotation automation
          if (rotationAutomationEnabled) {
            const rotationNoise = getCurveValue(automationCurve, timestamp * 3 * automationSpeed, 500); // 3x faster, different offset
            // Map noise from [-1, 1] to automation range
            const modulatedRotation = rotationAutomationMin + ((rotationNoise + 1) * 0.5) * (rotationAutomationMax - rotationAutomationMin);

            if (Math.abs(modulatedRotation - rotationSpeed) > 0.01) {
              setRotationSpeed(modulatedRotation);
            }
          }
        }

        // Update rotation based on rotation speed
        const rotationDelta = (rotationSpeed - 0.5) * 0.02; // Map 0-1 to rotation speed
        const newRotation = rotation + rotationDelta;
        setRotation(newRotation);

        // Step simulation
        if (!isPaused) {
          sim.step();
        }

        // Render
        if (useWebGL) {
          (sim as ReactionDiffusionWebGL).setRotation(newRotation);
          (sim as ReactionDiffusionWebGL).render();
        } else {
          (sim as ReactionDiffusion).render(ctx!, CELL_SIZE);
        }

        // Update FPS counter
        frameCountRef.current++;
        const now = Date.now();
        if (now - lastFpsUpdateRef.current >= 1000) {
          setFps(frameCountRef.current);
          frameCountRef.current = 0;
          lastFpsUpdateRef.current = now;
        }

        lastFrameTimeRef.current = timestamp;
      }

      animationFrameRef.current = requestAnimationFrame(animate);
    };

    animationFrameRef.current = requestAnimationFrame(animate);

    return () => {
      if (animationFrameRef.current) {
        cancelAnimationFrame(animationFrameRef.current);
      }
    };
  }, [isPaused, useWebGL, gridSize, GRID_WIDTH, GRID_HEIGHT, autoModulate, baseFeed, baseKill, baseSwirlSpeed, feed, kill, swirlSpeed, symmetry, currentPreset, feedAutomationEnabled, feedAutomationMin, feedAutomationMax, killAutomationEnabled, killAutomationMin, killAutomationMax, swirlAutomationEnabled, swirlAutomationMin, swirlAutomationMax, symmetryAutomationEnabled, symmetryAutomationMin, symmetryAutomationMax, presetAutomationEnabled, presetAutomationMin, presetAutomationMax]);

  // Update simulation config when preset changes (continue from current state)
  useEffect(() => {
    if (simRef.current) {
      const preset = PRESETS[currentPreset];
      const newFeed = preset.feed;
      const newKill = preset.kill;
      setBaseFeed(newFeed);
      setBaseKill(newKill);
      setFeed(newFeed);
      setKill(newKill);
      simRef.current.updateConfig({
        feed: newFeed,
        kill: newKill
      });
      // Don't reset - let the pattern continue evolving with new parameters
    }
  }, [currentPreset]);

  // Update simulation when feed/kill change manually
  useEffect(() => {
    if (simRef.current) {
      simRef.current.updateConfig({ feed, kill });
    }
  }, [feed, kill]);

  // Update pause state in simulation
  useEffect(() => {
    if (simRef.current) {
      simRef.current.paused = isPaused;
    }
  }, [isPaused]);

  // Mouse/touch handlers for painting
  const handlePointerDown = (e: React.PointerEvent<HTMLCanvasElement>) => {
    setIsDrawing(true);
    smoothedVelocityRef.current = 0; // Reset velocity on new stroke
    const pos = getCanvasPosition(e);
    if (pos) {
      smoothedPosRef.current = { ...pos }; // Initialize smoothed position
      const timestamp = Date.now();
      strokeStartTimeRef.current = timestamp; // Track stroke start for ramp-up curve
      lastPaintPosRef.current = { ...pos, timestamp, brushSize };
      poolStartTimeRef.current = timestamp;
      poolingSizeRef.current = brushSize;
      paintAt(pos.x, pos.y, brushSize);

      // Start continuous interval that pools when stationary
      // Pooling starts from the current tapered size and grows gradually
      let poolingBaseSize = brushSize; // Track the size when pooling started
      paintIntervalRef.current = window.setInterval(() => {
        const currentPos = smoothedPosRef.current;
        const lastPaint = lastPaintPosRef.current;
        if (currentPos && lastPaint) {
          const timeSinceLastMove = Date.now() - lastPaint.timestamp;
          // Only pool if we haven't moved in 50ms (user is stationary)
          if (timeSinceLastMove > 50) {
            const elapsed = Date.now() - poolStartTimeRef.current;
            // First time pooling starts, record the current tapered size
            if (elapsed < 100) {
              poolingBaseSize = poolingSizeRef.current;
            }
            // Grow from the tapered size: add 30% of brush size per second
            const growthRate = brushSize * 0.3 / 1000; // per millisecond
            poolingSizeRef.current = poolingBaseSize + (elapsed * growthRate);
            paintAt(currentPos.x, currentPos.y, poolingSizeRef.current);
          }
        }
      }, 30); // Check every 30ms for smoother pooling
    }
    // Update cursor position
    setCursorPos({ x: e.clientX, y: e.clientY });
  };

  const handlePointerMove = (e: React.PointerEvent<HTMLCanvasElement>) => {
    // Always update cursor position
    setCursorPos({ x: e.clientX, y: e.clientY });

    if (!isDrawing) return;

    const rawPos = getCanvasPosition(e);
    if (!rawPos) return;

    // Use raw position directly to avoid lag - smoothing comes from velocity-based tapering
    const pos = rawPos;
    smoothedPosRef.current = pos;

    const timestamp = Date.now();
    const lastPaintPos = lastPaintPosRef.current;

    // Calculate velocity-based brush taper with smoothing
    let currentBrushSize = brushSize;
    if (lastPaintPos) {
      const dx = pos.x - lastPaintPos.x;
      const dy = pos.y - lastPaintPos.y;
      const dt = Math.max(1, timestamp - lastPaintPos.timestamp);
      const distance = Math.sqrt(dx * dx + dy * dy);
      const instantVelocity = distance / dt; // pixels per millisecond

      // Exponential moving average for smooth velocity (very high smoothing for gel pen)
      const newSmoothedVelocity = smoothedVelocityRef.current * 0.92 + instantVelocity * 0.08;
      smoothedVelocityRef.current = newSmoothedVelocity;

      // Taper: fast = thin, slow = thick (like a gel pen with gradual response)
      const taperFactor = Math.max(taperMin, Math.min(1.0, 1.0 - newSmoothedVelocity * taperSensitivity));
      const targetSize = Math.max(2, brushSize * taperFactor);

      // Use extremely heavy smoothing for very gradual size transitions
      // Make it super smooth to prevent any abrupt un-tapering
      const sizeSmoothingFactor = 0.97; // Constant heavy smoothing

      poolingSizeRef.current = poolingSizeRef.current * sizeSmoothingFactor + targetSize * (1 - sizeSmoothingFactor);
      currentBrushSize = poolingSizeRef.current;

      // Reset pooling timer when moving (growth will restart from scratch when stopped)
      if (distance > 1) {
        poolStartTimeRef.current = timestamp;
      }

      // Interpolate between last position and current with gradual size change
      interpolatePaintSmooth(
        lastPaintPos.x, lastPaintPos.y, lastPaintPos.brushSize,
        pos.x, pos.y, currentBrushSize
      );
    } else {
      paintAt(pos.x, pos.y, currentBrushSize);
    }

    lastPaintPosRef.current = { ...pos, timestamp, brushSize: currentBrushSize };
  };

  const handlePointerUp = () => {
    setIsDrawing(false);
    lastPaintPosRef.current = null;
    smoothedPosRef.current = null;

    // Stop continuous painting
    if (paintIntervalRef.current) {
      clearInterval(paintIntervalRef.current);
      paintIntervalRef.current = null;
    }
  };

  const handlePointerEnter = (e: React.PointerEvent<HTMLCanvasElement>) => {
    setCursorPos({ x: e.clientX, y: e.clientY });
  };

  const handlePointerLeave = () => {
    setCursorPos(null);

    // Stop continuous painting when leaving canvas
    if (paintIntervalRef.current) {
      clearInterval(paintIntervalRef.current);
      paintIntervalRef.current = null;
    }
  };

  const getCanvasPosition = (e: React.PointerEvent<HTMLCanvasElement>): { x: number; y: number } | null => {
    const canvas = canvasRef.current;
    if (!canvas) return null;

    const rect = canvas.getBoundingClientRect();

    // Simple mapping since object-fit: fill stretches to fill container
    const x = Math.floor(((e.clientX - rect.left) / rect.width) * GRID_WIDTH);
    const y = Math.floor(((e.clientY - rect.top) / rect.height) * GRID_HEIGHT);

    if (x >= 0 && x < GRID_WIDTH && y >= 0 && y < GRID_HEIGHT) {
      return { x, y };
    }
    return null;
  };

  // Calculate smooth ramp-up curve for brush size at stroke start
  const getRampUpMultiplier = (): number => {
    const elapsed = Date.now() - strokeStartTimeRef.current;
    const rampDuration = 200; // Ramp up over 200ms

    if (elapsed >= rampDuration) return 1.0;

    // Ease-out cubic curve: starts fast, slows at end
    const t = elapsed / rampDuration;
    return 1 - Math.pow(1 - t, 3);
  };

  // Rotate a point around the center (inverse rotation for mouse input)
  const rotatePoint = (x: number, y: number): { x: number, y: number } => {
    const centerX = GRID_WIDTH / 2;
    const centerY = GRID_HEIGHT / 2;

    // Translate to origin
    const dx = x - centerX;
    const dy = y - centerY;

    // Apply inverse rotation to map screen coordinates back to simulation space
    const cos = Math.cos(rotation);
    const sin = Math.sin(rotation);
    const rotatedX = dx * cos - dy * sin;
    const rotatedY = dx * sin + dy * cos;

    // Translate back
    return {
      x: rotatedX + centerX,
      y: rotatedY + centerY
    };
  };

  const paintAt = (x: number, y: number, size: number) => {
    const sim = simRef.current;
    if (!sim) return;

    // Apply ramp-up curve to size
    const rampMultiplier = getRampUpMultiplier();
    const adjustedSize = size * rampMultiplier;

    // Rotate input coordinates to match the rotated view
    const rotated = rotatePoint(x, y);
    const rx = rotated.x;
    const ry = rotated.y;

    const centerX = GRID_WIDTH / 2;
    const centerY = GRID_HEIGHT / 2;

    // Get brush shape index
    const brushShapeIndex = BRUSH_SHAPE_NAMES.indexOf(brushShape);

    // Apply color opacity
    const opacityColor = {
      r: userColor.r * colorOpacity,
      g: userColor.g * colorOpacity,
      b: userColor.b * colorOpacity
    };

    if (symmetry === 1) {
      // No symmetry
      sim.inject(rx, ry, adjustedSize, opacityColor, brushShapeIndex, flowRate);
    } else {
      // Radial symmetry (2-12 fold)
      const dx = rx - centerX;
      const dy = ry - centerY;
      const angle = Math.atan2(dy, dx);
      const radius = Math.sqrt(dx * dx + dy * dy);

      for (let i = 0; i < symmetry; i++) {
        const newAngle = angle + (i * 2 * Math.PI / symmetry);
        const newX = centerX + radius * Math.cos(newAngle);
        const newY = centerY + radius * Math.sin(newAngle);
        sim.inject(Math.floor(newX), Math.floor(newY), adjustedSize, opacityColor, brushShapeIndex, flowRate);
      }
    }
  };

  // Interpolate painting with gradual brush size transition
  const interpolatePaintSmooth = (
    x0: number, y0: number, size0: number,
    x1: number, y1: number, size1: number
  ) => {
    const dx = x1 - x0;
    const dy = y1 - y0;
    const distance = Math.sqrt(dx * dx + dy * dy);

    // Calculate steps based on average brush size for proper overlap
    // Use 1/3 of brush size as step distance - balanced performance and smoothness
    const avgSize = (size0 + size1) / 2;
    const stepDistance = avgSize / 3;
    const steps = Math.max(1, Math.ceil(distance / stepDistance));

    for (let i = 0; i <= steps; i++) {
      const t = i / steps;
      const x = Math.round(x0 + dx * t);
      const y = Math.round(y0 + dy * t);
      // Smoothly interpolate brush size
      const size = Math.round(size0 + (size1 - size0) * t);
      paintAt(x, y, size);
    }
  };

  const handleFeedChange = (newFeed: number) => {
    setBaseFeed(newFeed);
    setFeed(newFeed);
    setFeedAutomationEnabled(false); // Disable automation on manual change
    if (simRef.current) {
      simRef.current.updateConfig({ feed: newFeed });
    }
  };

  const handleKillChange = (newKill: number) => {
    setBaseKill(newKill);
    setKill(newKill);
    setKillAutomationEnabled(false); // Disable automation on manual change
    if (simRef.current) {
      simRef.current.updateConfig({ kill: newKill });
    }
  };

  const handleSwirlSpeedChange = (newSwirl: number) => {
    setBaseSwirlSpeed(newSwirl);
    setSwirlSpeed(newSwirl);
    setSwirlAutomationEnabled(false); // Disable automation on manual change
    if (simRef.current) {
      simRef.current.updateConfig({ swirlSpeed: newSwirl * animationSpeed });
    }
  };

  const handleSymmetryChange = (newSymmetry: number) => {
    setSymmetry(newSymmetry);
    setSymmetryAutomationEnabled(false); // Disable automation on manual change
  };

  const handleRotationSpeedChange = (newRotationSpeed: number) => {
    setRotationSpeed(newRotationSpeed);
    setRotationAutomationEnabled(false); // Disable automation on manual change
  };

  const handlePresetChange = (newPreset: string) => {
    setCurrentPreset(newPreset);
    setPresetAutomationEnabled(false); // Disable automation on manual change
  };

  const handleClearCanvas = () => {
    const sim = simRef.current;
    if (!sim || !('fade' in sim)) return;

    // Gradually fade the canvas over 1 second (60 frames)
    let fadeCount = 0;
    const totalFades = 60;
    const fadeInterval = setInterval(() => {
      if (sim && 'fade' in sim) {
        sim.fade(0.92); // Fade by 8% each frame
        fadeCount++;
        if (fadeCount >= totalFades) {
          clearInterval(fadeInterval);
          // Final reset to ensure clean state
          sim.reset();
        }
      } else {
        clearInterval(fadeInterval);
      }
    }, 16); // ~60fps
  };

  const handleResetSettings = () => {
    // Reset all settings to defaults
    setGridSize(DEFAULT_SETTINGS.gridSize);
    setBrushSize(DEFAULT_SETTINGS.brushSize);
    setBrushShape(DEFAULT_SETTINGS.brushShape);
    setCurrentPreset(DEFAULT_SETTINGS.currentPreset);
    setBaseFeed(DEFAULT_SETTINGS.baseFeed);
    setBaseKill(DEFAULT_SETTINGS.baseKill);
    setTaperMin(DEFAULT_SETTINGS.taperMin);
    setTaperSensitivity(DEFAULT_SETTINGS.taperSensitivity);
    setSmoothing(DEFAULT_SETTINGS.smoothing);
    setSymmetry(DEFAULT_SETTINGS.symmetry);
    setBaseSwirlSpeed(DEFAULT_SETTINGS.baseSwirlSpeed);
    setSwirlSpeed(DEFAULT_SETTINGS.baseSwirlSpeed);
    setGpuNoiseStrength(DEFAULT_SETTINGS.gpuNoiseStrength);
    setUserColor(DEFAULT_SETTINGS.userColor);
    setColorOpacity(DEFAULT_SETTINGS.colorOpacity);
    setFlowRate(DEFAULT_SETTINGS.flowRate);
    setAutomationSpeed(DEFAULT_SETTINGS.automationSpeed);
    setAnimationSpeed(DEFAULT_SETTINGS.animationSpeed);
    setAutomationCurve(DEFAULT_SETTINGS.automationCurve);
    setFeedAutomationEnabled(DEFAULT_SETTINGS.feedAutomationEnabled);
    setFeedAutomationMin(DEFAULT_SETTINGS.feedAutomationMin);
    setFeedAutomationMax(DEFAULT_SETTINGS.feedAutomationMax);
    setKillAutomationEnabled(DEFAULT_SETTINGS.killAutomationEnabled);
    setKillAutomationMin(DEFAULT_SETTINGS.killAutomationMin);
    setKillAutomationMax(DEFAULT_SETTINGS.killAutomationMax);
    setSwirlAutomationEnabled(DEFAULT_SETTINGS.swirlAutomationEnabled);
    setSwirlAutomationMin(DEFAULT_SETTINGS.swirlAutomationMin);
    setSwirlAutomationMax(DEFAULT_SETTINGS.swirlAutomationMax);
    setSymmetryAutomationEnabled(DEFAULT_SETTINGS.symmetryAutomationEnabled);
    setSymmetryAutomationMin(DEFAULT_SETTINGS.symmetryAutomationMin);
    setSymmetryAutomationMax(DEFAULT_SETTINGS.symmetryAutomationMax);
    setPresetAutomationEnabled(DEFAULT_SETTINGS.presetAutomationEnabled);
    setPresetAutomationMin(DEFAULT_SETTINGS.presetAutomationMin);
    setPresetAutomationMax(DEFAULT_SETTINGS.presetAutomationMax);

    // Update simulation config
    if (simRef.current) {
      simRef.current.updateConfig({
        feed: DEFAULT_SETTINGS.baseFeed,
        kill: DEFAULT_SETTINGS.baseKill,
        swirlSpeed: DEFAULT_SETTINGS.baseSwirlSpeed * DEFAULT_SETTINGS.animationSpeed,
        gpuNoiseStrength: DEFAULT_SETTINGS.gpuNoiseStrength,
      });
    }
  };

  return (
    <div className="app">
      <div className="canvas-container">
        <canvas
          key={`${useWebGL ? 'webgl' : 'cpu'}-${gridSize}`}
          ref={canvasRef}
          width={CANVAS_WIDTH}
          height={CANVAS_HEIGHT}
          className="simulation-canvas"
          onPointerDown={handlePointerDown}
          onPointerMove={handlePointerMove}
          onPointerUp={handlePointerUp}
          onPointerEnter={handlePointerEnter}
          onPointerLeave={() => { handlePointerUp(); handlePointerLeave(); }}
          style={{ touchAction: 'none', cursor: 'none' }}
        />
        {cursorPos && canvasRef.current && (() => {
          const rect = canvasRef.current.getBoundingClientRect();
          const scaleX = rect.width / GRID_WIDTH;
          const scaleY = rect.height / GRID_HEIGHT;
          const scale = Math.min(scaleX, scaleY); // Use min to keep it circular
          const size = brushSize * scale;

          return (
            <div
              className="custom-cursor"
              style={{
                left: cursorPos.x,
                top: cursorPos.y,
                width: size,
                height: size,
                borderColor: `rgb(${userColor.r}, ${userColor.g}, ${userColor.b})`,
                backgroundColor: `rgba(${userColor.r}, ${userColor.g}, ${userColor.b}, 0.7)`,
              }}
            />
          );
        })()}
      </div>

      <div className="sidebar">
        <Controls
          currentPreset={currentPreset}
          brushSize={brushSize}
          gridSize={gridSize}
          feed={feed}
          kill={kill}
          taperMin={taperMin}
          taperSensitivity={taperSensitivity}
          smoothing={smoothing}
          symmetry={symmetry}
          fps={fps}
          userColor={userColor}
          feedAutomationEnabled={feedAutomationEnabled}
          feedAutomationMin={feedAutomationMin}
          feedAutomationMax={feedAutomationMax}
          killAutomationEnabled={killAutomationEnabled}
          killAutomationMin={killAutomationMin}
          killAutomationMax={killAutomationMax}
          swirlAutomationEnabled={swirlAutomationEnabled}
          swirlAutomationMin={swirlAutomationMin}
          swirlAutomationMax={swirlAutomationMax}
          symmetryAutomationEnabled={symmetryAutomationEnabled}
          symmetryAutomationMin={symmetryAutomationMin}
          symmetryAutomationMax={symmetryAutomationMax}
          presetAutomationEnabled={presetAutomationEnabled}
          presetAutomationMin={presetAutomationMin}
          presetAutomationMax={presetAutomationMax}
          rotationSpeed={rotationSpeed}
          rotationAutomationEnabled={rotationAutomationEnabled}
          rotationAutomationMin={rotationAutomationMin}
          rotationAutomationMax={rotationAutomationMax}
          onColorChange={setUserColor}
          onPresetChange={handlePresetChange}
          onBrushSizeChange={setBrushSize}
          brushShape={brushShape}
          onBrushShapeChange={setBrushShape}
          colorOpacity={colorOpacity}
          onColorOpacityChange={setColorOpacity}
          flowRate={flowRate}
          onFlowRateChange={setFlowRate}
          onGridSizeChange={setGridSize}
          onFeedChange={handleFeedChange}
          onKillChange={handleKillChange}
          onTaperMinChange={setTaperMin}
          onTaperSensitivityChange={setTaperSensitivity}
          onSmoothingChange={setSmoothing}
          onSymmetryChange={handleSymmetryChange}
          swirlSpeed={swirlSpeed}
          onSwirlSpeedChange={handleSwirlSpeedChange}
          gpuNoiseStrength={gpuNoiseStrength}
          onGpuNoiseStrengthChange={setGpuNoiseStrength}
          onRotationSpeedChange={handleRotationSpeedChange}
          automationSpeed={automationSpeed}
          onAutomationSpeedChange={setAutomationSpeed}
          animationSpeed={animationSpeed}
          onAnimationSpeedChange={setAnimationSpeed}
          automationCurve={automationCurve}
          onAutomationCurveChange={setAutomationCurve}
          onFeedAutomationToggle={() => setFeedAutomationEnabled(!feedAutomationEnabled)}
          onFeedAutomationRangeChange={(min, max) => {
            setFeedAutomationMin(min);
            setFeedAutomationMax(max);
          }}
          onKillAutomationToggle={() => setKillAutomationEnabled(!killAutomationEnabled)}
          onKillAutomationRangeChange={(min, max) => {
            setKillAutomationMin(min);
            setKillAutomationMax(max);
          }}
          onSwirlAutomationToggle={() => setSwirlAutomationEnabled(!swirlAutomationEnabled)}
          onSwirlAutomationRangeChange={(min, max) => {
            setSwirlAutomationMin(min);
            setSwirlAutomationMax(max);
          }}
          onSymmetryAutomationToggle={() => setSymmetryAutomationEnabled(!symmetryAutomationEnabled)}
          onSymmetryAutomationRangeChange={(min, max) => {
            setSymmetryAutomationMin(min);
            setSymmetryAutomationMax(max);
          }}
          onPresetAutomationToggle={() => setPresetAutomationEnabled(!presetAutomationEnabled)}
          onPresetAutomationRangeChange={(min, max) => {
            setPresetAutomationMin(min);
            setPresetAutomationMax(max);
          }}
          onRotationAutomationToggle={() => setRotationAutomationEnabled(!rotationAutomationEnabled)}
          onRotationAutomationRangeChange={(min, max) => {
            setRotationAutomationMin(min);
            setRotationAutomationMax(max);
          }}
          onClearCanvas={handleClearCanvas}
          onResetSettings={handleResetSettings}
        />
      </div>
    </div>
  );
}

export default App;
