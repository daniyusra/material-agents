import { useNavigate } from "react-router-dom";

const CAPABILITIES = [
  {
    icon: "⟁",
    title: "Structural Analysis",
    desc: "Interpret XRD, HRTEM, and diffraction datasets. Identify peak positions, d-spacings, and crystallite sizes from tabular exports.",
  },
  {
    icon: "⬡",
    title: "Composition Mapping",
    desc: "Analyze EDS, XPS, and elemental distribution data. Quantify atomic percentages and flag trace contaminants.",
  },
  {
    icon: "◈",
    title: "Phase Identification",
    desc: "Cross-reference datasets against known phases. Detect phase fractions and transformation onset temperatures.",
  },
  {
    icon: "∿",
    title: "Thermal Characterization",
    desc: "Process DSC and TGA outputs. Extract Tg, melting points, decomposition onset, and enthalpy values.",
  },
];

const SPECS = [
  { key: "File formats", val: "CSV · TSV · XLSX", note: "Any tabular export from your lab instruments" },
  { key: "LLM inference", val: "Claude + GPT-4o", note: "Bring your own API key — nothing leaves your session" },
  { key: "Data handling", val: "Session-only storage", note: "Files are never persisted beyond the active session" },
  { key: "Access control", val: "No account required", note: "Open the terminal and start working immediately" },
];

export default function LandingPage() {
  const navigate = useNavigate();

  return (
    <>
      <style>{`
        /* ---- Hero ---- */
        .lp-hero {
          min-height: calc(100vh - var(--nav-h));
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          padding: var(--space-12) var(--space-6);
          position: relative;
          text-align: center;
          overflow: hidden;
        }
        .lp-hero::before {
          content: '';
          position: absolute;
          inset: 0;
          background-image: radial-gradient(circle, var(--border-dim) 1px, transparent 1px);
          background-size: 30px 30px;
          pointer-events: none;
        }
        .lp-hero::after {
          content: '';
          position: absolute;
          inset: 0;
          background: radial-gradient(ellipse 80% 60% at 50% 50%, transparent 30%, var(--bg-void) 100%);
          pointer-events: none;
        }
        .lp-hero-content {
          position: relative;
          z-index: 1;
          max-width: 720px;
        }
        .lp-status {
          display: inline-flex;
          align-items: center;
          gap: var(--space-2);
          font-family: var(--font-mono);
          font-size: 0.7rem;
          color: var(--accent);
          letter-spacing: 0.14em;
          text-transform: uppercase;
          margin-bottom: var(--space-8);
          padding: 0.3rem var(--space-4);
          border: 1px solid var(--accent-border);
          border-radius: var(--radius-sm);
          background: var(--accent-glow);
        }
        .lp-status-dot {
          width: 6px;
          height: 6px;
          background: var(--accent);
          border-radius: 50%;
          animation: lp-pulse 2.4s ease-in-out infinite;
          flex-shrink: 0;
        }
        @keyframes lp-pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.25; }
        }
        .lp-h1 {
          font-family: var(--font-display);
          font-size: clamp(2rem, 5.5vw, 3.75rem);
          font-weight: 800;
          line-height: 1.08;
          color: var(--text-primary);
          letter-spacing: -0.025em;
          margin-bottom: var(--space-6);
        }
        .lp-h1 em {
          font-style: normal;
          color: var(--accent);
        }
        .lp-sub {
          font-size: clamp(0.95rem, 2vw, 1.1rem);
          color: var(--text-secondary);
          line-height: 1.75;
          max-width: 540px;
          margin: 0 auto var(--space-10);
          font-weight: 300;
        }
        .lp-cta-row {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: var(--space-4);
          flex-wrap: wrap;
        }
        .lp-btn-primary {
          display: inline-flex;
          align-items: center;
          gap: var(--space-2);
          padding: 0.8rem var(--space-6);
          background: var(--accent);
          color: var(--bg-void);
          font-family: var(--font-display);
          font-weight: 700;
          font-size: 0.9rem;
          letter-spacing: 0.05em;
          border: none;
          border-radius: var(--radius-md);
          cursor: pointer;
          transition: opacity var(--transition-fast), transform var(--transition-fast);
        }
        .lp-btn-primary:hover { opacity: 0.85; transform: translateY(-1px); }
        .lp-btn-arrow {
          transition: transform var(--transition-fast);
        }
        .lp-btn-primary:hover .lp-btn-arrow { transform: translateX(3px); }
        .lp-btn-ghost {
          display: inline-flex;
          align-items: center;
          padding: 0.8rem var(--space-6);
          background: transparent;
          color: var(--text-secondary);
          font-family: var(--font-display);
          font-weight: 500;
          font-size: 0.9rem;
          letter-spacing: 0.04em;
          border: 1px solid var(--border-muted);
          border-radius: var(--radius-md);
          cursor: pointer;
          transition: color var(--transition-fast), border-color var(--transition-fast);
          text-decoration: none;
        }
        .lp-btn-ghost:hover { color: var(--text-primary); border-color: var(--accent-border); }
        .lp-formats {
          margin-top: var(--space-6);
          font-family: var(--font-mono);
          font-size: 0.7rem;
          color: var(--text-dim);
          letter-spacing: 0.1em;
        }

        /* ---- Section shared ---- */
        .lp-divider {
          border: none;
          border-top: 1px solid var(--border-dim);
        }
        .lp-section {
          padding: var(--space-16) var(--space-6);
          max-width: 1200px;
          margin: 0 auto;
        }
        .lp-section-eyebrow {
          font-family: var(--font-mono);
          font-size: 0.68rem;
          letter-spacing: 0.18em;
          text-transform: uppercase;
          color: var(--accent);
          margin-bottom: var(--space-4);
        }
        .lp-section-title {
          font-family: var(--font-display);
          font-size: clamp(1.4rem, 3vw, 1.9rem);
          font-weight: 700;
          color: var(--text-primary);
          letter-spacing: -0.01em;
          margin-bottom: var(--space-3);
        }
        .lp-section-lead {
          font-size: 0.975rem;
          color: var(--text-secondary);
          max-width: 500px;
          font-weight: 300;
          line-height: 1.7;
        }

        /* ---- Capabilities ---- */
        .lp-caps-grid {
          display: grid;
          grid-template-columns: 1fr;
          gap: var(--space-4);
          margin-top: var(--space-8);
        }
        @media (min-width: 768px) {
          .lp-caps-grid { grid-template-columns: 1fr 1fr; }
        }
        @media (min-width: 1024px) {
          .lp-caps-grid { grid-template-columns: repeat(4, 1fr); }
        }
        .lp-cap-card {
          background: var(--bg-surface);
          border: 1px solid var(--border-dim);
          border-radius: var(--radius-lg);
          padding: var(--space-6);
          transition: border-color var(--transition-med), background var(--transition-med);
        }
        .lp-cap-card:hover {
          border-color: var(--accent-border);
          background: var(--bg-elevated);
        }
        .lp-cap-icon {
          font-family: var(--font-mono);
          font-size: 1.5rem;
          color: var(--accent);
          margin-bottom: var(--space-4);
          display: block;
        }
        .lp-cap-title {
          font-family: var(--font-display);
          font-size: 0.9rem;
          font-weight: 600;
          color: var(--text-primary);
          margin-bottom: var(--space-2);
          letter-spacing: 0.01em;
        }
        .lp-cap-desc {
          font-size: 0.825rem;
          color: var(--text-secondary);
          line-height: 1.6;
          font-weight: 300;
        }

        /* ---- Specs ---- */
        .lp-specs-wrap {
          background: var(--bg-base);
          border-top: 1px solid var(--border-dim);
          border-bottom: 1px solid var(--border-dim);
        }
        .lp-specs-grid {
          display: grid;
          grid-template-columns: 1fr;
          gap: var(--space-6);
          margin-top: var(--space-8);
        }
        @media (min-width: 768px) {
          .lp-specs-grid { grid-template-columns: 1fr 1fr; }
        }
        @media (min-width: 1024px) {
          .lp-specs-grid { grid-template-columns: repeat(4, 1fr); }
        }
        .lp-spec-item {
          border-top: 1px solid var(--border-dim);
          padding-top: var(--space-4);
        }
        .lp-spec-key {
          font-family: var(--font-mono);
          font-size: 0.65rem;
          letter-spacing: 0.14em;
          text-transform: uppercase;
          color: var(--text-dim);
          margin-bottom: var(--space-2);
        }
        .lp-spec-val {
          font-family: var(--font-display);
          font-size: 0.95rem;
          font-weight: 700;
          color: var(--text-primary);
          margin-bottom: var(--space-1);
        }
        .lp-spec-note {
          font-size: 0.8rem;
          color: var(--text-secondary);
          font-weight: 300;
          line-height: 1.5;
        }

        /* ---- Bottom CTA ---- */
        .lp-bottom-cta {
          text-align: center;
          padding: var(--space-24) var(--space-6);
        }
        .lp-bottom-title {
          font-family: var(--font-display);
          font-size: clamp(1.4rem, 3vw, 2.1rem);
          font-weight: 700;
          color: var(--text-primary);
          letter-spacing: -0.015em;
          margin-bottom: var(--space-3);
        }
        .lp-bottom-sub {
          color: var(--text-secondary);
          font-weight: 300;
          margin-bottom: var(--space-8);
          font-size: 0.975rem;
        }

        /* ---- Footer ---- */
        .lp-footer {
          border-top: 1px solid var(--border-dim);
          padding: var(--space-6);
          text-align: center;
          font-family: var(--font-mono);
          font-size: 0.65rem;
          color: var(--text-dim);
          letter-spacing: 0.08em;
          text-transform: uppercase;
        }
      `}</style>

      {/* Hero */}
      <section className="lp-hero">
        <div className="lp-hero-content">
          <div className="lp-status">
            <span className="lp-status-dot" />
            Analytical Engine — Online
          </div>
          <h1 className="lp-h1">
            Materials Intelligence<br />
            <em>Built for the Lab</em>
          </h1>
          <p className="lp-sub">
            An autonomous research assistant for structural analysis, composition
            mapping, and property characterization. Upload your experimental data.
            Ask questions in plain language.
          </p>
          <div className="lp-cta-row">
            <button className="lp-btn-primary" onClick={() => navigate("/research")}>
              Open Research Terminal
              <span className="lp-btn-arrow">→</span>
            </button>
            <a className="lp-btn-ghost" href="/about">
              Read the docs
            </a>
          </div>
          <p className="lp-formats">CSV · TSV · XLSX — your existing formats</p>
        </div>
      </section>

      <hr className="lp-divider" />

      {/* Capabilities */}
      <div className="lp-section">
        <p className="lp-section-eyebrow">Capabilities</p>
        <h2 className="lp-section-title">What the terminal does</h2>
        <p className="lp-section-lead">
          Designed around the analytical workflows of materials researchers,
          not around generic data science tooling.
        </p>
        <div className="lp-caps-grid">
          {CAPABILITIES.map(({ icon, title, desc }) => (
            <div key={title} className="lp-cap-card">
              <span className="lp-cap-icon">{icon}</span>
              <div className="lp-cap-title">{title}</div>
              <p className="lp-cap-desc">{desc}</p>
            </div>
          ))}
        </div>
      </div>

      <hr className="lp-divider" />

      {/* Specs */}
      <div className="lp-specs-wrap">
        <div className="lp-section">
          <p className="lp-section-eyebrow">System Specifications</p>
          <h2 className="lp-section-title">Built to research-grade standards</h2>
          <div className="lp-specs-grid">
            {SPECS.map(({ key, val, note }) => (
              <div key={key} className="lp-spec-item">
                <div className="lp-spec-key">{key}</div>
                <div className="lp-spec-val">{val}</div>
                <div className="lp-spec-note">{note}</div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Bottom CTA */}
      <div className="lp-bottom-cta">
        <h2 className="lp-bottom-title">Ready to analyze your data?</h2>
        <p className="lp-bottom-sub">
          No signup. No subscription. Just your data and a rigorous analytical partner.
        </p>
        <div className="lp-cta-row">
          <button className="lp-btn-primary" onClick={() => navigate("/research")}>
            Open Research Terminal →
          </button>
          <a className="lp-btn-ghost" href="/about">
            Read the docs
          </a>
        </div>
      </div>

      <footer className="lp-footer">
        Materia·Agents — autonomous research tooling for the materials science community
      </footer>
    </>
  );
}
