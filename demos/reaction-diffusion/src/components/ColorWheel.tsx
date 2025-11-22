import { useRef, useEffect, useState } from 'react';
import './ColorWheel.css';

interface ColorWheelProps {
  color: { r: number; g: number; b: number };
  onChange: (color: { r: number; g: number; b: number }) => void;
}

export function ColorWheel({ color, onChange }: ColorWheelProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [isDragging, setIsDragging] = useState(false);
  const size = 200;
  const center = size / 2;
  const ringThickness = 48; // 1.25x thicker visual ring (accounts for doubled border)
  const innerRadius = center - ringThickness - 5;

  // Convert RGB to HSV
  const rgbToHsv = (r: number, g: number, b: number): [number, number, number] => {
    r /= 255;
    g /= 255;
    b /= 255;
    const max = Math.max(r, g, b);
    const min = Math.min(r, g, b);
    const d = max - min;
    const s = max === 0 ? 0 : d / max;
    const v = max;
    let h = 0;

    if (max !== min) {
      switch (max) {
        case r: h = ((g - b) / d + (g < b ? 6 : 0)) / 6; break;
        case g: h = ((b - r) / d + 2) / 6; break;
        case b: h = ((r - g) / d + 4) / 6; break;
      }
    }

    return [h * 360, s, v];
  };

  // Convert HSV to RGB
  const hsvToRgb = (h: number, s: number, v: number): [number, number, number] => {
    h /= 360;
    const i = Math.floor(h * 6);
    const f = h * 6 - i;
    const p = v * (1 - s);
    const q = v * (1 - f * s);
    const t = v * (1 - (1 - f) * s);

    let r = 0, g = 0, b = 0;
    switch (i % 6) {
      case 0: r = v; g = t; b = p; break;
      case 1: r = q; g = v; b = p; break;
      case 2: r = p; g = v; b = t; break;
      case 3: r = p; g = q; b = v; break;
      case 4: r = t; g = p; b = v; break;
      case 5: r = v; g = p; b = q; break;
    }

    return [Math.round(r * 255), Math.round(g * 255), Math.round(b * 255)];
  };

  // Draw color wheel
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext('2d', { willReadFrequently: true });
    if (!ctx) return;

    ctx.clearRect(0, 0, size, size);

    const [currentHue] = rgbToHsv(color.r, color.g, color.b);

    // Draw outer hue ring using conic gradient for smooth, aliasing-free rendering
    // Start at 240°
    // Hues go counter-clockwise: as we go clockwise visually, hues decrease
    const startAngle = (240 * Math.PI) / 180;
    const gradient = ctx.createConicGradient(startAngle, center, center);

    // Add color stops with adjusted spacing - balanced oranges
    // Custom hue mapping: compress greens (60-180°), moderate reds/oranges
    const hueStops = [
      60,   // Yellow
      40,   // Yellow-orange
      20,   // Orange
      0,    // Red
      330,  // Red-magenta
      300,  // Magenta
      270,  // Blue-magenta
      240,  // Blue
      200,  // Blue-cyan (compressed)
      160,  // Cyan-green (compressed)
      120,  // Green (compressed)
      60    // Back to yellow
    ];

    for (let i = 0; i < hueStops.length; i++) {
      const hue = hueStops[i];
      const [r, g, b] = hsvToRgb(hue, 1, 1);
      gradient.addColorStop(i / (hueStops.length - 1), `rgb(${r}, ${g}, ${b})`);
    }

    // Draw the ring with thicker border
    ctx.save();
    ctx.beginPath();
    ctx.arc(center, center, center - 12, 0, Math.PI * 2);
    ctx.arc(center, center, innerRadius + 12, 0, Math.PI * 2, true);
    ctx.closePath();
    ctx.fillStyle = gradient;
    ctx.fill();
    ctx.restore();

    // Draw center saturation/value square
    // First, clip to circle
    ctx.save();
    ctx.beginPath();
    ctx.arc(center, center, innerRadius, 0, Math.PI * 2);
    ctx.clip();

    // Get the full saturated color for current hue
    const [fullR, fullG, fullB] = hsvToRgb(currentHue, 1, 1);

    // Draw horizontal gradient: white to full color (saturation)
    const satGradient = ctx.createLinearGradient(
      center - innerRadius, center,
      center + innerRadius, center
    );
    satGradient.addColorStop(0, 'white');
    satGradient.addColorStop(1, `rgb(${fullR}, ${fullG}, ${fullB})`);

    ctx.fillStyle = satGradient;
    ctx.fillRect(center - innerRadius, center - innerRadius, innerRadius * 2, innerRadius * 2);

    // Draw vertical gradient: transparent to black (value)
    const valGradient = ctx.createLinearGradient(
      center, center - innerRadius,
      center, center + innerRadius
    );
    valGradient.addColorStop(0, 'rgba(0, 0, 0, 0)');
    valGradient.addColorStop(1, 'rgba(0, 0, 0, 1)');

    ctx.fillStyle = valGradient;
    ctx.fillRect(center - innerRadius, center - innerRadius, innerRadius * 2, innerRadius * 2);

    ctx.restore();

    // Draw current color indicator
    const [, s, v] = rgbToHsv(color.r, color.g, color.b);

    // Calculate indicator position in center circle
    // X position based on saturation (left = 0, right = 1)
    const indicatorX = center - innerRadius + (s * innerRadius * 2);
    // Y position based on value (top = 1, bottom = 0)
    const indicatorY = center - innerRadius + ((1 - v) * innerRadius * 2);

    // Draw indicator with ring style
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.4)';
    ctx.lineWidth = 2;
    ctx.shadowColor = 'rgba(0, 0, 0, 0.5)';
    ctx.shadowBlur = 4;
    ctx.beginPath();
    ctx.arc(indicatorX, indicatorY, 7, 0, Math.PI * 2);
    ctx.stroke();

    ctx.strokeStyle = 'rgba(0, 0, 0, 0.5)';
    ctx.lineWidth = 1;
    ctx.shadowBlur = 0;
    ctx.beginPath();
    ctx.arc(indicatorX, indicatorY, 8, 0, Math.PI * 2);
    ctx.stroke();
  }, [color]);

  const handlePointerEvent = (e: React.PointerEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const rect = canvas.getBoundingClientRect();
    // Scale mouse coordinates to match canvas internal resolution
    const scaleX = canvas.width / rect.width;
    const scaleY = canvas.height / rect.height;
    const x = (e.clientX - rect.left) * scaleX;
    const y = (e.clientY - rect.top) * scaleY;
    const dx = x - center;
    const dy = y - center;
    const distance = Math.sqrt(dx * dx + dy * dy);

    const [currentHue, currentSat, currentVal] = rgbToHsv(color.r, color.g, color.b);

    if (distance <= innerRadius) {
      // Clicking in center circle: change saturation and value
      // X-axis: saturation (left = 0, right = 1)
      const normalizedX = (dx + innerRadius) / (innerRadius * 2);
      const saturation = Math.max(0, Math.min(1, normalizedX));

      // Y-axis: value (top = 1, bottom = 0)
      const normalizedY = (dy + innerRadius) / (innerRadius * 2);
      const value = Math.max(0, Math.min(1, 1.0 - normalizedY));

      const [r, g, b] = hsvToRgb(currentHue, saturation, value);
      onChange({ r, g, b });
    } else if (distance > innerRadius + 12 && distance <= center - 12) {
      // Clicking in outer ring: change hue
      // Calculate angle from center (0° = right, increases counter-clockwise)
      const angle = Math.atan2(dy, dx);
      // Convert to degrees
      const angleDegrees = (angle * 180 / Math.PI + 360) % 360;

      // Since gradient starts at 240°,
      // calculate how far clockwise from the start we are
      const offsetFromStart = (angleDegrees - 240 + 360) % 360;

      // Map the angle to hue using the same custom distribution
      const hueStops = [60, 40, 20, 0, 330, 300, 270, 240, 200, 160, 120, 60];
      const normalizedPosition = offsetFromStart / 360;
      const segmentIndex = normalizedPosition * (hueStops.length - 1);
      const lowerIndex = Math.floor(segmentIndex);
      const upperIndex = Math.ceil(segmentIndex);
      const t = segmentIndex - lowerIndex;

      // Interpolate between the two nearest hue stops
      const lowerHue = hueStops[Math.min(lowerIndex, hueStops.length - 1)];
      const upperHue = hueStops[Math.min(upperIndex, hueStops.length - 1)];
      let hue = lowerHue + (upperHue - lowerHue) * t;

      // Handle wrap-around at red (0°/360°)
      if (lowerHue > 300 && upperHue < 100) {
        hue = lowerHue + ((upperHue + 360 - lowerHue) * t);
      }
      hue = (hue + 360) % 360;

      const [r, g, b] = hsvToRgb(hue, currentSat, currentVal);
      onChange({ r, g, b });
    }
  };

  const handlePointerDown = (e: React.PointerEvent<HTMLCanvasElement>) => {
    setIsDragging(true);
    (e.target as HTMLElement).setPointerCapture(e.pointerId);
    handlePointerEvent(e);
  };

  const handlePointerMove = (e: React.PointerEvent<HTMLCanvasElement>) => {
    if (isDragging) {
      handlePointerEvent(e);
    }
  };

  const handlePointerUp = (e: React.PointerEvent<HTMLCanvasElement>) => {
    setIsDragging(false);
    (e.target as HTMLElement).releasePointerCapture(e.pointerId);
  };

  return (
    <div className="color-wheel-popup">
      <canvas
        ref={canvasRef}
        width={size}
        height={size}
        className="color-wheel-canvas"
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        style={{ cursor: isDragging ? 'grabbing' : 'crosshair' }}
      />
    </div>
  );
}
