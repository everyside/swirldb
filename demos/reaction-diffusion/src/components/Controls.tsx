import { PRESETS } from '../lib/presets';
import { Knob } from './Knob';
import { ColorWheel } from './ColorWheel';
import { CURVE_NAMES, CURVE_DISPLAY_NAMES, CurveName } from '../lib/curves';
import { BRUSH_SHAPE_NAMES, BRUSH_SHAPE_DISPLAY_NAMES } from '../lib/brushShapes';

interface ControlsProps {
  currentPreset: string;
  brushSize: number;
  brushShape: string;
  colorOpacity: number;
  flowRate: number;
  gridSize: number;
  feed: number;
  kill: number;
  taperMin: number;
  taperSensitivity: number;
  smoothing: number;
  symmetry: number;
  swirlSpeed: number;
  gpuNoiseStrength: number;
  rotationSpeed: number;
  fps: number;
  userColor: { r: number; g: number; b: number };

  // Automation settings
  feedAutomationEnabled: boolean;
  feedAutomationMin: number;
  feedAutomationMax: number;
  killAutomationEnabled: boolean;
  killAutomationMin: number;
  killAutomationMax: number;
  swirlAutomationEnabled: boolean;
  swirlAutomationMin: number;
  swirlAutomationMax: number;
  symmetryAutomationEnabled: boolean;
  symmetryAutomationMin: number;
  symmetryAutomationMax: number;
  presetAutomationEnabled: boolean;
  presetAutomationMin: number;
  presetAutomationMax: number;
  rotationAutomationEnabled: boolean;
  rotationAutomationMin: number;
  rotationAutomationMax: number;

  onColorChange: (color: { r: number; g: number; b: number }) => void;
  onPresetChange: (preset: string) => void;
  onBrushSizeChange: (size: number) => void;
  onBrushShapeChange: (shape: string) => void;
  onColorOpacityChange: (opacity: number) => void;
  onFlowRateChange: (flowRate: number) => void;
  onGridSizeChange: (size: number) => void;
  onFeedChange: (feed: number) => void;
  onKillChange: (kill: number) => void;
  onTaperMinChange: (value: number) => void;
  onTaperSensitivityChange: (value: number) => void;
  onSmoothingChange: (value: number) => void;
  onSymmetryChange: (value: number) => void;
  onSwirlSpeedChange: (value: number) => void;
  onGpuNoiseStrengthChange: (value: number) => void;
  onRotationSpeedChange: (value: number) => void;
  automationSpeed: number;
  onAutomationSpeedChange: (value: number) => void;
  animationSpeed: number;
  onAnimationSpeedChange: (value: number) => void;
  automationCurve: CurveName;
  onAutomationCurveChange: (curve: CurveName) => void;

  // Automation callbacks
  onFeedAutomationToggle: () => void;
  onFeedAutomationRangeChange: (min: number, max: number) => void;
  onKillAutomationToggle: () => void;
  onKillAutomationRangeChange: (min: number, max: number) => void;
  onSwirlAutomationToggle: () => void;
  onSwirlAutomationRangeChange: (min: number, max: number) => void;
  onSymmetryAutomationToggle: () => void;
  onSymmetryAutomationRangeChange: (min: number, max: number) => void;
  onPresetAutomationToggle: () => void;
  onPresetAutomationRangeChange: (min: number, max: number) => void;
  onRotationAutomationToggle: () => void;
  onRotationAutomationRangeChange: (min: number, max: number) => void;

  onClearCanvas: () => void;
  onResetSettings: () => void;
}

export function Controls({
  currentPreset,
  brushSize,
  brushShape,
  colorOpacity,
  flowRate,
  gridSize,
  feed,
  kill,
  taperMin,
  taperSensitivity,
  smoothing,
  symmetry,
  swirlSpeed,
  gpuNoiseStrength,
  rotationSpeed,
  fps,
  userColor,
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
  onColorChange,
  onPresetChange,
  onBrushSizeChange,
  onBrushShapeChange,
  onColorOpacityChange,
  onFlowRateChange,
  onGridSizeChange,
  onFeedChange,
  onKillChange,
  onTaperMinChange,
  onTaperSensitivityChange,
  onSmoothingChange,
  onSymmetryChange,
  onSwirlSpeedChange,
  onGpuNoiseStrengthChange,
  onRotationSpeedChange,
  automationSpeed,
  onAutomationSpeedChange,
  animationSpeed,
  onAnimationSpeedChange,
  automationCurve,
  onAutomationCurveChange,
  onFeedAutomationToggle,
  onFeedAutomationRangeChange,
  onKillAutomationToggle,
  onKillAutomationRangeChange,
  onSwirlAutomationToggle,
  onSwirlAutomationRangeChange,
  onSymmetryAutomationToggle,
  onSymmetryAutomationRangeChange,
  onPresetAutomationToggle,
  onPresetAutomationRangeChange,
  onRotationAutomationToggle,
  onRotationAutomationRangeChange,
  onClearCanvas,
  onResetSettings
}: ControlsProps) {
  // Map preset keys to indices
  const presetKeys = Object.keys(PRESETS);
  const presetIndex = presetKeys.indexOf(currentPreset);

  // Map curve name to index
  const curveIndex = CURVE_NAMES.indexOf(automationCurve);

  // Map brush shape name to index
  const brushShapeIndex = BRUSH_SHAPE_NAMES.indexOf(brushShape);

  return (
    <div className="controls">
      <div className="control-section">
        <ColorWheel
          color={userColor}
          onChange={onColorChange}
        />
      </div>

      <div className="control-section">
        <div className="knobs-grid">
          <Knob
            label="Pattern"
            value={presetIndex}
            min={0}
            max={presetKeys.length - 1}
            step={1}
            onChange={(index) => {
              const roundedIndex = Math.round(index);
              onPresetChange(presetKeys[roundedIndex]);
            }}
            size={45}
            displayValue={PRESETS[currentPreset]?.name || currentPreset}
            automationMin={presetAutomationMin}
            automationMax={presetAutomationMax}
            automationEnabled={presetAutomationEnabled}
            onAutomationRangeChange={onPresetAutomationRangeChange}
            onAutomationToggle={onPresetAutomationToggle}
          />
          <Knob
            label="Feed"
            value={feed || 0}
            min={0}
            max={0.1}
            step={0.0001}
            onChange={onFeedChange}
            size={45}
            automationMin={feedAutomationMin}
            automationMax={feedAutomationMax}
            automationEnabled={feedAutomationEnabled}
            onAutomationRangeChange={onFeedAutomationRangeChange}
            onAutomationToggle={onFeedAutomationToggle}
          />
          <Knob
            label="Decay"
            value={kill || 0}
            min={0}
            max={0.1}
            step={0.0001}
            onChange={onKillChange}
            size={45}
            automationMin={killAutomationMin}
            automationMax={killAutomationMax}
            automationEnabled={killAutomationEnabled}
            onAutomationRangeChange={onKillAutomationRangeChange}
            onAutomationToggle={onKillAutomationToggle}
          />
        </div>
      </div>

      <div className="control-section">
        <div className="knobs-grid">
          <Knob
            label="Brush Size"
            value={brushSize}
            min={Math.max(1, Math.floor(gridSize / 256))}
            max={4096}
            step={Math.max(1, Math.floor(gridSize / 512))}
            onChange={onBrushSizeChange}
            size={45}
          />
          <Knob
            label="Brush Shape"
            value={brushShapeIndex}
            min={0}
            max={BRUSH_SHAPE_NAMES.length - 1}
            step={1}
            onChange={(index) => {
              const roundedIndex = Math.round(index);
              onBrushShapeChange(BRUSH_SHAPE_NAMES[roundedIndex]);
            }}
            size={45}
            displayValue={BRUSH_SHAPE_DISPLAY_NAMES[brushShape] || brushShape}
          />
          <Knob
            label="Opacity"
            value={colorOpacity}
            min={0}
            max={1}
            step={0.01}
            onChange={onColorOpacityChange}
            size={45}
            displayValue={`${(colorOpacity * 100).toFixed(0)}%`}
          />
          <Knob
            label="Flow"
            value={flowRate}
            min={0.0001}
            max={20}
            step={0.1}
            onChange={onFlowRateChange}
            size={45}
            displayValue={flowRate < 0.01 ? flowRate.toFixed(4) : flowRate.toFixed(2)}
          />
          <Knob
            label="Taper Min"
            value={taperMin}
            min={0}
            max={1}
            step={0.05}
            onChange={onTaperMinChange}
            size={45}
            displayValue={`${(taperMin * 100).toFixed(0)}%`}
          />
          <Knob
            label="Taper Sens"
            value={taperSensitivity}
            min={0}
            max={1}
            step={0.05}
            onChange={onTaperSensitivityChange}
            size={45}
          />
          <Knob
            label="Smoothing"
            value={smoothing}
            min={0}
            max={0.99}
            step={0.01}
            onChange={onSmoothingChange}
            size={45}
            displayValue={`${(smoothing * 100).toFixed(0)}%`}
          />
        </div>
      </div>

      <div className="control-section">
        <div className="knobs-grid">
          <Knob
            label="Symmetry"
            value={symmetry}
            min={1}
            max={12}
            step={1}
            onChange={(val) => {
              const rounded = Math.round(val);
              onSymmetryChange(rounded);
            }}
            size={45}
            displayValue={symmetry === 1 ? 'None' : `${symmetry}×`}
            automationMin={symmetryAutomationMin}
            automationMax={symmetryAutomationMax}
            automationEnabled={symmetryAutomationEnabled}
            onAutomationRangeChange={onSymmetryAutomationRangeChange}
            onAutomationToggle={onSymmetryAutomationToggle}
          />
          <Knob
            label="Swirl"
            value={swirlSpeed}
            min={0}
            max={1}
            step={0.05}
            onChange={onSwirlSpeedChange}
            size={45}
            displayValue={swirlSpeed === 0 ? 'Off' : `${(swirlSpeed * 100).toFixed(0)}%`}
            automationMin={swirlAutomationMin}
            automationMax={swirlAutomationMax}
            automationEnabled={swirlAutomationEnabled}
            onAutomationRangeChange={onSwirlAutomationRangeChange}
            onAutomationToggle={onSwirlAutomationToggle}
          />
          <Knob
            label="Noise"
            value={gpuNoiseStrength}
            min={0}
            max={1}
            step={0.05}
            onChange={onGpuNoiseStrengthChange}
            size={45}
            displayValue={gpuNoiseStrength === 0 ? 'Off' : `${(gpuNoiseStrength * 100).toFixed(0)}%`}
          />
          <Knob
            label="Rotation"
            value={rotationSpeed}
            min={0}
            max={1}
            step={0.01}
            onChange={onRotationSpeedChange}
            size={45}
            displayValue={rotationSpeed === 0.5 ? 'Off' : rotationSpeed > 0.5 ? 'CW' : 'CCW'}
            automationMin={rotationAutomationMin}
            automationMax={rotationAutomationMax}
            automationEnabled={rotationAutomationEnabled}
            onAutomationRangeChange={onRotationAutomationRangeChange}
            onAutomationToggle={onRotationAutomationToggle}
          />
        </div>
      </div>

      <div className="control-section">
        <div className="knobs-grid">
          <Knob
            label="Auto Speed"
            value={automationSpeed}
            min={0}
            max={3}
            step={0.1}
            onChange={onAutomationSpeedChange}
            size={45}
            displayValue={automationSpeed === 0 ? 'Paused' : `${automationSpeed.toFixed(1)}×`}
          />
          <Knob
            label="Anim Speed"
            value={animationSpeed}
            min={0.1}
            max={20}
            step={0.1}
            onChange={onAnimationSpeedChange}
            size={45}
            displayValue={`${animationSpeed.toFixed(1)}×`}
          />
          <Knob
            label="Auto"
            value={curveIndex}
            min={0}
            max={CURVE_NAMES.length - 1}
            step={1}
            onChange={(index) => {
              const roundedIndex = Math.round(index);
              onAutomationCurveChange(CURVE_NAMES[roundedIndex]);
            }}
            size={45}
            displayValue={CURVE_DISPLAY_NAMES[automationCurve]}
          />
          <Knob
            label="Grid Size"
            value={gridSize}
            min={128}
            max={4096}
            step={128}
            onChange={onGridSizeChange}
            size={45}
          />
        </div>
      </div>

      <div className="button-group">
        <button onClick={onClearCanvas} className="control-button">
          Clear Canvas
        </button>
        <button onClick={onResetSettings} className="control-button">
          Reset Settings
        </button>
      </div>

      <div className="fps-display">
        {fps} FPS
      </div>
    </div>
  );
}
