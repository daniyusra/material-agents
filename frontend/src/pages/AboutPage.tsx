const TOC = [
  ["overview", "Overview"],
  ["getting-started", "Getting Started"],
  ["formats", "Supported Formats"],
  ["privacy", "Data & Privacy"],
  ["models", "LLM Models"],
  ["shortcuts", "Keyboard Shortcuts"],
];

export default function AboutPage() {
  return (
    <>
      <style>{`
        .about-page {
          max-width: 1100px;
          margin: 0 auto;
          padding: var(--space-12) var(--space-6);
          display: grid;
          grid-template-columns: 1fr;
          gap: var(--space-12);
          align-items: start;
        }
        @media (min-width: 1024px) {
          .about-page { grid-template-columns: 200px 1fr; }
        }
        .about-toc { display: none; }
        @media (min-width: 1024px) {
          .about-toc {
            display: block;
            position: sticky;
            top: calc(var(--nav-h) + var(--space-8));
          }
        }
        .about-toc-label {
          font-family: var(--font-mono);
          font-size: 0.65rem;
          letter-spacing: 0.16em;
          text-transform: uppercase;
          color: var(--text-dim);
          margin-bottom: var(--space-4);
        }
        .about-toc-list {
          list-style: none;
          display: flex;
          flex-direction: column;
          gap: 2px;
        }
        .about-toc-link {
          display: block;
          padding: var(--space-1) var(--space-3);
          font-size: 0.8rem;
          color: var(--text-secondary);
          border-left: 1px solid var(--border-dim);
          transition: color var(--transition-fast), border-color var(--transition-fast);
          text-decoration: none;
        }
        .about-toc-link:hover { color: var(--accent); border-left-color: var(--accent); }

        .about-content { min-width: 0; }
        .about-page-title {
          font-family: var(--font-display);
          font-size: clamp(1.75rem, 4vw, 2.5rem);
          font-weight: 800;
          color: var(--text-primary);
          letter-spacing: -0.02em;
          margin-bottom: var(--space-4);
        }
        .about-lead {
          font-size: 1.05rem;
          color: var(--text-secondary);
          line-height: 1.75;
          max-width: 580px;
          font-weight: 300;
          margin-bottom: var(--space-12);
        }
        .about-section {
          padding-top: var(--space-8);
          border-top: 1px solid var(--border-dim);
          margin-bottom: var(--space-10);
        }
        .about-eyebrow {
          display: block;
          font-family: var(--font-mono);
          font-size: 0.65rem;
          letter-spacing: 0.14em;
          text-transform: uppercase;
          color: var(--accent);
          margin-bottom: var(--space-3);
        }
        .about-h2 {
          font-family: var(--font-display);
          font-size: 1.2rem;
          font-weight: 700;
          color: var(--text-primary);
          margin-bottom: var(--space-4);
          letter-spacing: -0.01em;
        }
        .about-p {
          color: var(--text-secondary);
          line-height: 1.8;
          margin-bottom: var(--space-4);
          font-weight: 300;
          font-size: 0.95rem;
        }
        .about-p:last-child { margin-bottom: 0; }

        /* Steps */
        .about-steps { display: flex; flex-direction: column; gap: var(--space-5); margin-top: var(--space-2); }
        .about-step { display: flex; gap: var(--space-4); align-items: flex-start; }
        .about-step-num {
          flex-shrink: 0;
          width: 1.75rem; height: 1.75rem;
          border-radius: 50%;
          border: 1px solid var(--accent-border);
          display: flex; align-items: center; justify-content: center;
          font-family: var(--font-mono);
          font-size: 0.7rem;
          color: var(--accent);
          background: var(--accent-glow);
          margin-top: 0.15rem;
        }
        .about-step-title {
          font-size: 0.9rem;
          font-weight: 500;
          color: var(--text-primary);
          margin-bottom: var(--space-1);
        }
        .about-step-desc {
          font-size: 0.85rem;
          color: var(--text-secondary);
          font-weight: 300;
          line-height: 1.65;
        }

        /* Table */
        .about-table {
          width: 100%;
          border-collapse: collapse;
          margin-top: var(--space-4);
          font-size: 0.85rem;
        }
        .about-table th {
          text-align: left;
          font-family: var(--font-mono);
          font-size: 0.63rem;
          letter-spacing: 0.14em;
          text-transform: uppercase;
          color: var(--text-dim);
          padding: var(--space-2) var(--space-3);
          border-bottom: 1px solid var(--border-dim);
          font-weight: 400;
        }
        .about-table td {
          padding: var(--space-3);
          color: var(--text-secondary);
          border-bottom: 1px solid var(--border-dim);
          font-weight: 300;
          vertical-align: top;
          line-height: 1.6;
        }
        .about-table td:first-child {
          font-family: var(--font-mono);
          font-size: 0.78rem;
          color: var(--accent-text);
          font-weight: 400;
          white-space: nowrap;
        }
        .about-table tr:last-child td { border-bottom: none; }

        /* Callout */
        .about-callout {
          background: var(--bg-surface);
          border: 1px solid var(--border-muted);
          border-left: 3px solid var(--accent);
          padding: var(--space-4) var(--space-5);
          border-radius: var(--radius-md);
          font-size: 0.875rem;
          color: var(--text-secondary);
          line-height: 1.7;
          font-weight: 300;
          margin-top: var(--space-4);
        }

        /* Keyboard shortcuts */
        .about-kbd-list { display: flex; flex-direction: column; gap: var(--space-3); margin-top: var(--space-4); }
        .about-kbd-row { display: flex; align-items: center; gap: var(--space-4); font-size: 0.875rem; }
        .about-kbd {
          font-family: var(--font-mono);
          font-size: 0.73rem;
          padding: 0.2rem 0.55rem;
          border: 1px solid var(--border-muted);
          border-radius: var(--radius-sm);
          background: var(--bg-surface);
          color: var(--text-primary);
          white-space: nowrap;
          min-width: 130px;
          text-align: center;
        }
        .about-kbd-desc { color: var(--text-secondary); font-weight: 300; }
      `}</style>

      <div className="about-page">
        <aside className="about-toc">
          <p className="about-toc-label">On this page</p>
          <ul className="about-toc-list">
            {TOC.map(([id, label]) => (
              <li key={id}><a href={`#${id}`} className="about-toc-link">{label}</a></li>
            ))}
          </ul>
        </aside>

        <div className="about-content">
          <h1 className="about-page-title">About Materia·Agents</h1>
          <p className="about-lead">
            An autonomous research assistant designed for materials scientists.
            Upload your experimental data, ask questions in plain language,
            and receive rigorous analytical responses.
          </p>

          <div id="overview" className="about-section">
            <span className="about-eyebrow">01 — Overview</span>
            <h2 className="about-h2">What this tool is</h2>
            <p className="about-p">
              Materia·Agents connects a large language model to your experimental data.
              You provide the data file; the assistant inspects it, runs pandas-based
              analyses, generates plots, and answers questions — all within a single
              browser session.
            </p>
            <p className="about-p">
              It is not a search engine, not a literature tool, and not a materials database
              interface. It is an analytical co-pilot for data already sitting in your lab's
              export folder.
            </p>
          </div>

          <div id="getting-started" className="about-section">
            <span className="about-eyebrow">02 — Getting Started</span>
            <h2 className="about-h2">Start in three steps</h2>
            <div className="about-steps">
              {[
                {
                  n: "1",
                  title: "Upload your dataset",
                  desc: "Drag a CSV, TSV, or Excel file onto the Research Terminal. The file is parsed server-side and held in memory for the duration of the session.",
                },
                {
                  n: "2",
                  title: "Configure your model",
                  desc: "Open Options and paste your Anthropic or OpenAI API key. Keys are stored in a browser cookie and sent directly to the provider — never to this application.",
                },
                {
                  n: "3",
                  title: "Ask your question",
                  desc: "Type a question in plain language. The assistant interprets the data, runs code if needed, and returns a structured response with optional charts.",
                },
              ].map(({ n, title, desc }) => (
                <div key={n} className="about-step">
                  <div className="about-step-num">{n}</div>
                  <div>
                    <div className="about-step-title">{title}</div>
                    <p className="about-step-desc">{desc}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div id="formats" className="about-section">
            <span className="about-eyebrow">03 — Supported Formats</span>
            <h2 className="about-h2">File formats</h2>
            <table className="about-table">
              <thead>
                <tr><th>Extension</th><th>Format</th><th>Notes</th></tr>
              </thead>
              <tbody>
                {[
                  [".csv", "Comma-Separated Values", "UTF-8; delimiter detected automatically"],
                  [".tsv", "Tab-Separated Values", "Common export from spectroscopy and diffraction software"],
                  [".xlsx", "Excel Workbook", "First sheet loaded; multi-sheet files not yet supported"],
                  [".xls", "Legacy Excel", "Pre-2007 format; converted on upload"],
                ].map(([ext, fmt, note]) => (
                  <tr key={ext}><td>{ext}</td><td>{fmt}</td><td>{note}</td></tr>
                ))}
              </tbody>
            </table>
          </div>

          <div id="privacy" className="about-section">
            <span className="about-eyebrow">04 — Data & Privacy</span>
            <h2 className="about-h2">How your data is handled</h2>
            <div className="about-callout">
              Uploaded files are held in server memory for the duration of the session only.
              They are not written to disk, not persisted to a database, and not shared with
              any third party. Restarting the backend process clears all uploaded files.
              Your LLM API key is stored in a browser cookie and transmitted directly to the
              model provider — it never passes through this application's logic.
            </div>
          </div>

          <div id="models" className="about-section">
            <span className="about-eyebrow">05 — LLM Models</span>
            <h2 className="about-h2">Supported models</h2>
            <p className="about-p">
              Two providers are supported. Bring your own API key via the Options panel
              in the Research Terminal.
            </p>
            <table className="about-table">
              <thead>
                <tr><th>Provider</th><th>Model</th><th>Characteristics</th></tr>
              </thead>
              <tbody>
                <tr>
                  <td>Anthropic</td>
                  <td>Claude</td>
                  <td>Strong scientific reasoning and structured output. Recommended for most materials analysis tasks.</td>
                </tr>
                <tr>
                  <td>OpenAI</td>
                  <td>GPT-4o</td>
                  <td>Strong general-purpose reasoning. Useful for cross-validation against Claude responses.</td>
                </tr>
              </tbody>
            </table>
          </div>

          <div id="shortcuts" className="about-section">
            <span className="about-eyebrow">06 — Keyboard Shortcuts</span>
            <h2 className="about-h2">Input shortcuts</h2>
            <div className="about-kbd-list">
              {[
                ["Enter", "Send message"],
                ["Shift + Enter", "Insert newline in message"],
              ].map(([key, desc]) => (
                <div key={key} className="about-kbd-row">
                  <span className="about-kbd">{key}</span>
                  <span className="about-kbd-desc">{desc}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
