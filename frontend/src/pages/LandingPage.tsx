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
      {/* Hero */}
      <section className="lp-hero min-h-[calc(100vh-3.5rem)] flex flex-col items-center justify-center px-6 py-12 relative text-center overflow-hidden">
        <div className="relative z-[1] max-w-[720px]">
          <div className="inline-flex items-center gap-2 font-mono text-[0.7rem] text-accent tracking-[0.14em] uppercase mb-8 py-[0.3rem] px-4 border border-accent-border rounded-sm bg-accent-glow">
            <span className="w-[6px] h-[6px] bg-accent rounded-full animate-pulse-status shrink-0" />
            Analytical Engine — Online
          </div>
          <h1 className="font-display text-[clamp(2rem,5.5vw,3.75rem)] font-extrabold leading-[1.08] text-text-primary tracking-[-0.025em] mb-6">
            Materials Intelligence<br />
            <em className="not-italic text-accent">Built for the Lab</em>
          </h1>
          <p className="text-[clamp(0.95rem,2vw,1.1rem)] text-text-secondary leading-[1.75] max-w-[540px] mx-auto mb-10 font-light">
            An autonomous research assistant for structural analysis, composition
            mapping, and property characterization. Upload your experimental data.
            Ask questions in plain language.
          </p>
          <div className="flex items-center justify-center gap-4 flex-wrap">
            <button
              className="group inline-flex items-center gap-2 py-[0.8rem] px-6 bg-accent text-bg-void font-display font-bold text-[0.9rem] tracking-[0.05em] border-none rounded-md cursor-pointer transition-[opacity,transform] duration-[120ms] ease hover:opacity-85 hover:-translate-y-px"
              onClick={() => navigate("/research")}
            >
              Open Research Terminal
              <span className="transition-transform duration-[120ms] ease group-hover:translate-x-[3px]">→</span>
            </button>
            <a
              className="inline-flex items-center py-[0.8rem] px-6 bg-transparent text-text-secondary font-display font-medium text-[0.9rem] tracking-[0.04em] border border-border-muted rounded-md cursor-pointer transition-[color,border-color] duration-[120ms] ease no-underline hover:text-text-primary hover:border-accent-border"
              href="/about"
            >
              Read the docs
            </a>
          </div>
          <p className="mt-6 font-mono text-[0.7rem] text-text-dim tracking-[0.1em]">
            CSV · TSV · XLSX — your existing formats
          </p>
        </div>
      </section>

      <hr className="border-0 border-t border-border-dim" />

      {/* Capabilities */}
      <div className="py-16 px-6 max-w-[1200px] mx-auto">
        <p className="font-mono text-[0.68rem] tracking-[0.18em] uppercase text-accent mb-4">Capabilities</p>
        <h2 className="font-display text-[clamp(1.4rem,3vw,1.9rem)] font-bold text-text-primary tracking-[-0.01em] mb-3">
          What the terminal does
        </h2>
        <p className="text-[0.975rem] text-text-secondary max-w-[500px] font-light leading-[1.7]">
          Designed around the analytical workflows of materials researchers,
          not around generic data science tooling.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mt-8">
          {CAPABILITIES.map(({ icon, title, desc }) => (
            <div
              key={title}
              className="bg-bg-surface border border-border-dim rounded-lg p-6 transition-[border-color,background] duration-[220ms] ease hover:border-accent-border hover:bg-bg-elevated"
            >
              <span className="font-mono text-[1.5rem] text-accent mb-4 block">{icon}</span>
              <div className="font-display text-[0.9rem] font-semibold text-text-primary mb-2 tracking-[0.01em]">{title}</div>
              <p className="text-[0.825rem] text-text-secondary leading-[1.6] font-light">{desc}</p>
            </div>
          ))}
        </div>
      </div>

      <hr className="border-0 border-t border-border-dim" />

      {/* Specs */}
      <div className="bg-bg-base border-t border-b border-border-dim">
        <div className="py-16 px-6 max-w-[1200px] mx-auto">
          <p className="font-mono text-[0.68rem] tracking-[0.18em] uppercase text-accent mb-4">System Specifications</p>
          <h2 className="font-display text-[clamp(1.4rem,3vw,1.9rem)] font-bold text-text-primary tracking-[-0.01em] mb-3">
            Built to research-grade standards
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mt-8">
            {SPECS.map(({ key, val, note }) => (
              <div key={key} className="border-t border-border-dim pt-4">
                <div className="font-mono text-[0.65rem] tracking-[0.14em] uppercase text-text-dim mb-2">{key}</div>
                <div className="font-display text-[0.95rem] font-bold text-text-primary mb-1">{val}</div>
                <div className="text-[0.8rem] text-text-secondary font-light leading-[1.5]">{note}</div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Bottom CTA */}
      <div className="text-center py-24 px-6">
        <h2 className="font-display text-[clamp(1.4rem,3vw,2.1rem)] font-bold text-text-primary tracking-[-0.015em] mb-3">
          Ready to analyze your data?
        </h2>
        <p className="text-text-secondary font-light mb-8 text-[0.975rem]">
          No signup. No subscription. Just your data and a rigorous analytical partner.
        </p>
        <div className="flex items-center justify-center gap-4 flex-wrap">
          <button
            className="group inline-flex items-center gap-2 py-[0.8rem] px-6 bg-accent text-bg-void font-display font-bold text-[0.9rem] tracking-[0.05em] border-none rounded-md cursor-pointer transition-[opacity,transform] duration-[120ms] ease hover:opacity-85 hover:-translate-y-px"
            onClick={() => navigate("/research")}
          >
            Open Research Terminal →
          </button>
          <a
            className="inline-flex items-center py-[0.8rem] px-6 bg-transparent text-text-secondary font-display font-medium text-[0.9rem] tracking-[0.04em] border border-border-muted rounded-md cursor-pointer transition-[color,border-color] duration-[120ms] ease no-underline hover:text-text-primary hover:border-accent-border"
            href="/about"
          >
            Read the docs
          </a>
        </div>
      </div>

      <footer className="border-t border-border-dim py-6 px-6 text-center font-mono text-[0.65rem] text-text-dim tracking-[0.08em] uppercase">
        Materia·Agents — autonomous research tooling for the materials science community
      </footer>
    </>
  );
}
