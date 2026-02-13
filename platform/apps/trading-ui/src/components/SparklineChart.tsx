interface SparklinePoint {
  label: string;
  value: number;
}

interface SparklineChartProps {
  points: SparklinePoint[];
  height?: number;
}

export function SparklineChart({ points, height = 210 }: SparklineChartProps) {
  if (points.length < 2) {
    return <div className="empty-chart">No history yet</div>;
  }

  const width = 1000;
  const leftPad = 12;
  const rightPad = 12;
  const topPad = 20;
  const bottomPad = 28;

  const values = points.map((point) => point.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = Math.max(max - min, 1);

  const xForIndex = (index: number) =>
    leftPad + (index / (points.length - 1)) * (width - leftPad - rightPad);
  const yForValue = (value: number) =>
    topPad + (1 - (value - min) / spread) * (height - topPad - bottomPad);

  const coords = points.map((point, index) => ({
    x: xForIndex(index),
    y: yForValue(point.value)
  }));

  const linePath = coords
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`)
    .join(" ");
  const areaPath = `${linePath} L ${coords[coords.length - 1].x.toFixed(2)} ${(height - bottomPad).toFixed(
    2
  )} L ${coords[0].x.toFixed(2)} ${(height - bottomPad).toFixed(2)} Z`;

  const ticks = [min, min + spread / 2, max];

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="sparkline-chart" role="img" aria-label="Cash trend">
      <defs>
        <linearGradient id="cash-area-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
          <stop offset="0%" stopColor="var(--accent-strong)" stopOpacity="0.5" />
          <stop offset="100%" stopColor="var(--accent-strong)" stopOpacity="0.04" />
        </linearGradient>
      </defs>

      {ticks.map((tick) => {
        const y = yForValue(tick);
        return (
          <g key={tick}>
            <line x1={leftPad} y1={y} x2={width - rightPad} y2={y} className="sparkline-gridline" />
            <text x={leftPad} y={y - 6} className="sparkline-tick-label">
              ${tick.toFixed(0)}
            </text>
          </g>
        );
      })}

      <path d={areaPath} className="sparkline-area" />
      <path d={linePath} className="sparkline-line" />

      {coords.map((point, index) => (
        <circle key={index} cx={point.x} cy={point.y} r={3.5} className="sparkline-dot">
          <title>{`${points[index].label}: $${points[index].value.toFixed(2)}`}</title>
        </circle>
      ))}
    </svg>
  );
}
