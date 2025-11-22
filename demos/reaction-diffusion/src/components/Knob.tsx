import { useRef, useState } from 'react';
import './Knob.css';

interface KnobProps {
  value: number;
  min: number;
  max: number;
  step?: number;
  onChange: (value: number) => void;
  label: string;
  size?: number;
  displayValue?: string;
  automationMin?: number;
  automationMax?: number;
  automationEnabled?: boolean;
  onAutomationRangeChange?: (min: number, max: number) => void;
  onAutomationToggle?: () => void;
}

export function Knob({ value, min, max, step = 0.01, onChange, label, size = 50, displayValue, automationMin, automationMax, automationEnabled, onAutomationRangeChange, onAutomationToggle }: KnobProps) {
  const [isDragging, setIsDragging] = useState(false);
  const [isDraggingRange, setIsDraggingRange] = useState<'min' | 'max' | null>(null);
  const [hasMoved, setHasMoved] = useState(false);
  const startYRef = useRef(0);
  const startValueRef = useRef(0);
  const startAngleRef = useRef(0);

  const normalizedValue = (value - min) / (max - min);
  const rotation = -135 + normalizedValue * 270; // -135 to +135 degrees

  const hasAutomation = automationMin !== undefined && automationMax !== undefined && onAutomationRangeChange && onAutomationToggle;

  const handlePointerDown = (e: React.PointerEvent) => {
    e.preventDefault();
    setIsDragging(true);
    setHasMoved(false);
    startYRef.current = e.clientY;
    startValueRef.current = value;
    (e.target as HTMLElement).setPointerCapture(e.pointerId);
  };

  const handlePointerMove = (e: React.PointerEvent) => {
    if (!isDragging) return;

    const deltaY = startYRef.current - e.clientY;

    // Track if mouse moved more than 3 pixels
    if (Math.abs(deltaY) > 3) {
      setHasMoved(true);
    }

    const range = max - min;
    const sensitivity = range / 150; // 150 pixels for full range
    const newValue = startValueRef.current + deltaY * sensitivity;
    const clampedValue = Math.max(min, Math.min(max, newValue));

    // Apply step
    const steppedValue = Math.round(clampedValue / step) * step;
    onChange(steppedValue);
  };

  const handlePointerUp = (e: React.PointerEvent) => {
    // If didn't move, it's a click - toggle automation
    if (!hasMoved && hasAutomation) {
      onAutomationToggle?.();
    }

    setIsDragging(false);
    setIsDraggingRange(null);
    setHasMoved(false);
    (e.target as HTMLElement).releasePointerCapture(e.pointerId);
  };

  const handleRangePointerDown = (e: React.PointerEvent, type: 'min' | 'max') => {
    if (!hasAutomation) return;
    e.stopPropagation();
    e.preventDefault();
    setIsDraggingRange(type);

    const svg = e.currentTarget.closest('svg');
    if (!svg) return;
    const rect = svg.getBoundingClientRect();
    const centerX = rect.left + rect.width / 2;
    const centerY = rect.top + rect.height / 2;
    const angle = Math.atan2(e.clientY - centerY, e.clientX - centerX) * 180 / Math.PI;
    startAngleRef.current = angle;

    (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
  };

  const handleRangePointerMove = (e: React.PointerEvent) => {
    if (!isDraggingRange || !hasAutomation) return;

    const svg = e.currentTarget.closest('svg');
    if (!svg) return;
    const rect = svg.getBoundingClientRect();
    const centerX = rect.left + rect.width / 2;
    const centerY = rect.top + rect.height / 2;

    // Calculate angle from center
    let angle = Math.atan2(e.clientY - centerY, e.clientX - centerX) * 180 / Math.PI;
    // Convert to knob range (-135 to +135)
    angle = angle + 90; // Adjust so top is 0
    if (angle < -180) angle += 360;
    if (angle > 180) angle -= 360;

    // Clamp to -135 to +135
    angle = Math.max(-135, Math.min(135, angle));

    // Convert to normalized value
    const normalizedAngle = (angle + 135) / 270;
    const newValue = min + normalizedAngle * (max - min);

    if (isDraggingRange === 'min' && automationMax !== undefined) {
      onAutomationRangeChange?.(Math.min(newValue, automationMax), automationMax);
    } else if (isDraggingRange === 'max' && automationMin !== undefined) {
      onAutomationRangeChange?.(automationMin, Math.max(newValue, automationMin));
    }
  };

  // Calculate automation range positions if automation is available
  const normalizedAutomationMin = hasAutomation && automationMin !== undefined
    ? (automationMin - min) / (max - min)
    : 0;
  const normalizedAutomationMax = hasAutomation && automationMax !== undefined
    ? (automationMax - min) / (max - min)
    : 1;
  const automationMinRotation = -135 + normalizedAutomationMin * 270;
  const automationMaxRotation = -135 + normalizedAutomationMax * 270;
  const automationRangeAngle = automationMaxRotation - automationMinRotation;

  // Calculate handle positions
  const handleRadius = size / 2 - 2;
  const getHandlePosition = (angle: number) => {
    const rad = (angle - 90) * Math.PI / 180;
    return {
      x: size / 2 + handleRadius * Math.cos(rad),
      y: size / 2 + handleRadius * Math.sin(rad)
    };
  };
  const minHandle = getHandlePosition(automationMinRotation);
  const maxHandle = getHandlePosition(automationMaxRotation);

  return (
    <div className="knob-container">
      <div className="knob-label">{label}</div>
      <svg
        width={size}
        height={size}
        className={`knob ${isDragging ? 'dragging' : ''} ${automationEnabled ? 'automated' : ''}`}
        onPointerDown={handlePointerDown}
        onPointerMove={isDraggingRange ? handleRangePointerMove : handlePointerMove}
        onPointerUp={handlePointerUp}
        style={{ cursor: isDragging ? 'grabbing' : 'grab' }}
      >
        {/* Automation range arc (outer ring) */}
        {hasAutomation && (
          <>
            {/* Background arc (inactive area) */}
            <path
              d={`M ${size/2 + (size/2 - 2) * Math.cos((-135 - 90) * Math.PI / 180)} ${size/2 + (size/2 - 2) * Math.sin((-135 - 90) * Math.PI / 180)} A ${size/2 - 2} ${size/2 - 2} 0 1 1 ${size/2 + (size/2 - 2) * Math.cos((135 - 90) * Math.PI / 180)} ${size/2 + (size/2 - 2) * Math.sin((135 - 90) * Math.PI / 180)}`}
              fill="none"
              stroke="rgba(255, 255, 255, 0.1)"
              strokeWidth="3"
              strokeLinecap="round"
            />

            {/* Active automation range arc */}
            <path
              d={`M ${size/2 + (size/2 - 2) * Math.cos((automationMinRotation - 90) * Math.PI / 180)} ${size/2 + (size/2 - 2) * Math.sin((automationMinRotation - 90) * Math.PI / 180)} A ${size/2 - 2} ${size/2 - 2} 0 ${automationRangeAngle > 180 ? 1 : 0} 1 ${size/2 + (size/2 - 2) * Math.cos((automationMaxRotation - 90) * Math.PI / 180)} ${size/2 + (size/2 - 2) * Math.sin((automationMaxRotation - 90) * Math.PI / 180)}`}
              fill="none"
              stroke={automationEnabled ? "rgba(102, 126, 234, 0.6)" : "rgba(255, 255, 255, 0.3)"}
              strokeWidth="3"
              strokeLinecap="round"
            />

            {/* Min handle */}
            <circle
              cx={minHandle.x}
              cy={minHandle.y}
              r="4"
              fill={automationEnabled ? "#667eea" : "rgba(255, 255, 255, 0.5)"}
              stroke="rgba(0, 0, 0, 0.5)"
              strokeWidth="1"
              style={{ cursor: 'grab' }}
              onPointerDown={(e) => handleRangePointerDown(e, 'min')}
            />

            {/* Max handle */}
            <circle
              cx={maxHandle.x}
              cy={maxHandle.y}
              r="4"
              fill={automationEnabled ? "#667eea" : "rgba(255, 255, 255, 0.5)"}
              stroke="rgba(0, 0, 0, 0.5)"
              strokeWidth="1"
              style={{ cursor: 'grab' }}
              onPointerDown={(e) => handleRangePointerDown(e, 'max')}
            />
          </>
        )}

        {/* Outer ring (for non-automated knobs) */}
        {!hasAutomation && (
          <circle
            cx={size / 2}
            cy={size / 2}
            r={size / 2 - 2}
            fill="rgba(255, 255, 255, 0.05)"
            stroke="rgba(255, 255, 255, 0.2)"
            strokeWidth="2"
          />
        )}

        {/* Value arc */}
        <circle
          cx={size / 2}
          cy={size / 2}
          r={size / 2 - 8}
          fill="none"
          stroke="url(#knobGradient)"
          strokeWidth="3"
          strokeDasharray={`${normalizedValue * 270 * Math.PI * (size / 2 - 8) / 180} ${1000}`}
          strokeDashoffset={-135 * Math.PI * (size / 2 - 8) / 180}
          strokeLinecap="round"
        />

        {/* Center circle */}
        <circle
          cx={size / 2}
          cy={size / 2}
          r={size / 2 - 12}
          fill="rgba(0, 0, 0, 0.5)"
        />

        {/* Indicator line */}
        <line
          x1={size / 2}
          y1={size / 2}
          x2={size / 2}
          y2={size / 2 - (size / 2 - 16)}
          stroke="white"
          strokeWidth="2"
          strokeLinecap="round"
          transform={`rotate(${rotation} ${size / 2} ${size / 2})`}
        />

        <defs>
          <linearGradient id="knobGradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#667eea" />
            <stop offset="100%" stopColor="#764ba2" />
          </linearGradient>
        </defs>
      </svg>
      <div className="knob-value">{displayValue || value.toFixed(step >= 1 ? 0 : step >= 0.1 ? 1 : step >= 0.01 ? 2 : 4)}</div>
    </div>
  );
}
