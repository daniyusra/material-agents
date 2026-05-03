import Plot from "react-plotly.js";
import type { PlotlyFigure } from "../types";

const CHART_HEIGHT = 420;

interface ChartPanelProps {
  figure: PlotlyFigure;
}

function downloadJson(figure: PlotlyFigure) {
  const blob = new Blob([JSON.stringify(figure, null, 2)], {
    type: "application/json",
  });
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
  return (
    <div style={styles.wrapper}>
      {/* Fixed-height container gives Plotly a stable box; prevents the
          resize-observer feedback loop that causes vertical overflow on scroll. */}
      <div style={styles.plotContainer}>
        <Plot
          data={figure.data}
          layout={{
            ...figure.layout,
            autosize: true,
            height: CHART_HEIGHT,
            margin: { l: 48, r: 16, t: 40, b: 48 },
          }}
          style={{ width: "100%", height: "100%" }}
          useResizeHandler
          config={{
            responsive: true,
            displayModeBar: true,
            toImageButtonOptions: { format: "png", filename: "chart", scale: 2 },
          }}
        />
      </div>
      <button onClick={() => downloadJson(figure)} style={styles.downloadBtn}>
        Download JSON
      </button>
    </div>
  );
}

const styles = {
  wrapper: {
    marginTop: "0.75rem",
  },
  plotContainer: {
    width: "100%",
    height: CHART_HEIGHT,
    overflow: "hidden",
  },
  downloadBtn: {
    marginTop: "0.5rem",
    padding: "0.35rem 0.85rem",
    borderRadius: 6,
    border: "1px solid #ccc",
    background: "white",
    fontSize: "0.8rem",
    cursor: "pointer",
    color: "#555",
  },
} as const;
