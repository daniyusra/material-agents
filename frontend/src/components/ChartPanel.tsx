import Plot from "react-plotly.js";
import type { PlotlyFigure } from "../types";

const CHART_HEIGHT = 420;

interface ChartPanelProps {
  figure: PlotlyFigure;
}

function downloadJson(figure: PlotlyFigure) {
  const blob = new Blob([JSON.stringify(figure, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "chart.json";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

export default function ChartPanel({ figure }: ChartPanelProps) {
  const darkLayout = {
    ...figure.layout,
    autosize: true,
    height: CHART_HEIGHT,
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#7090a8", family: "IBM Plex Mono, monospace", size: 11 },
    margin: { l: 52, r: 16, t: 40, b: 52 },
    xaxis: {
      ...(figure.layout.xaxis as Record<string, unknown> ?? {}),
      gridcolor: "#192534",
      color: "#7090a8",
      zerolinecolor: "#223040",
    },
    yaxis: {
      ...(figure.layout.yaxis as Record<string, unknown> ?? {}),
      gridcolor: "#192534",
      color: "#7090a8",
      zerolinecolor: "#223040",
    },
  };

  return (
    <div className="mt-3">
      {/* Fixed height prevents Plotly resize-observer feedback loop on scroll */}
      <div className="w-full overflow-hidden rounded-md border border-border-dim bg-bg-base h-[420px]">
        <Plot
          data={figure.data}
          layout={darkLayout}
          style={{ width: "100%", height: "100%" }}
          useResizeHandler
          config={{
            responsive: true,
            displayModeBar: true,
            toImageButtonOptions: { format: "png", filename: "chart", scale: 2 },
          }}
        />
      </div>
      <button
        onClick={() => downloadJson(figure)}
        className="mt-2 py-[0.28rem] px-3 bg-transparent border border-border-muted rounded-sm font-mono text-[0.68rem] tracking-[0.06em] text-text-dim cursor-pointer transition-[color,border-color] duration-[120ms] ease hover:text-text-secondary hover:border-accent-border"
      >
        ↓ Export JSON
      </button>
    </div>
  );
}
