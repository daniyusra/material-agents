const TOC = [
  ["overview", "Overview"],
  ["getting-started", "Getting Started"],
  ["formats", "Supported Formats"],
  ["privacy", "Data & Privacy"],
  ["models", "LLM Models"],
  ["shortcuts", "Keyboard Shortcuts"],
];

const tdBase = "py-3 px-3 text-text-secondary border-b border-border-dim font-light align-top leading-[1.6]";
const tdFirst = "py-3 px-3 border-b border-border-dim align-top leading-[1.6] font-mono text-[0.78rem] text-accent-text font-normal whitespace-nowrap";

export default function AboutPage() {
  return (
    <div className="max-w-[1100px] mx-auto py-12 px-6 grid grid-cols-1 lg:grid-cols-[200px_1fr] gap-12 items-start">
      <aside className="hidden lg:block lg:sticky lg:top-[calc(3.5rem+2rem)]">
        <p className="font-mono text-[0.65rem] tracking-[0.16em] uppercase text-text-dim mb-4">On this page</p>
        <ul className="list-none flex flex-col gap-[2px]">
          {TOC.map(([id, label]) => (
            <li key={id}>
              <a
                href={`#${id}`}
                className="block py-1 px-3 text-[0.8rem] text-text-secondary border-l border-l-border-dim transition-[color,border-color] duration-[120ms] ease no-underline hover:text-accent hover:border-l-accent"
              >
                {label}
              </a>
            </li>
          ))}
        </ul>
      </aside>

      <div className="min-w-0">
        <h1 className="font-display text-[clamp(1.75rem,4vw,2.5rem)] font-extrabold text-text-primary tracking-[-0.02em] mb-4">
          About Materia·Agents
        </h1>
        <p className="text-[1.05rem] text-text-secondary leading-[1.75] max-w-[580px] font-light mb-12">
          An autonomous research assistant designed for materials scientists.
          Upload your experimental data, ask questions in plain language,
          and receive rigorous analytical responses.
        </p>

        <div id="overview" className="pt-8 border-t border-border-dim mb-10">
          <span className="block font-mono text-[0.65rem] tracking-[0.14em] uppercase text-accent mb-3">01 — Overview</span>
          <h2 className="font-display text-[1.2rem] font-bold text-text-primary mb-4 tracking-[-0.01em]">What this tool is</h2>
          <p className="text-text-secondary leading-[1.8] mb-4 font-light text-[0.95rem]">
            Materia·Agents connects a large language model to your experimental data.
            You provide the data file; the assistant inspects it, runs pandas-based
            analyses, generates plots, and answers questions — all within a single
            browser session.
          </p>
          <p className="text-text-secondary leading-[1.8] font-light text-[0.95rem]">
            It is not a search engine, not a literature tool, and not a materials database
            interface. It is an analytical co-pilot for data already sitting in your lab's
            export folder.
          </p>
        </div>

        <div id="getting-started" className="pt-8 border-t border-border-dim mb-10">
          <span className="block font-mono text-[0.65rem] tracking-[0.14em] uppercase text-accent mb-3">02 — Getting Started</span>
          <h2 className="font-display text-[1.2rem] font-bold text-text-primary mb-4 tracking-[-0.01em]">Start in three steps</h2>
          <div className="flex flex-col gap-5 mt-2">
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
              <div key={n} className="flex gap-4 items-start">
                <div className="shrink-0 w-[1.75rem] h-[1.75rem] rounded-full border border-accent-border flex items-center justify-center font-mono text-[0.7rem] text-accent bg-accent-glow mt-[0.15rem]">
                  {n}
                </div>
                <div>
                  <div className="text-[0.9rem] font-medium text-text-primary mb-1">{title}</div>
                  <p className="text-[0.85rem] text-text-secondary font-light leading-[1.65]">{desc}</p>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div id="formats" className="pt-8 border-t border-border-dim mb-10">
          <span className="block font-mono text-[0.65rem] tracking-[0.14em] uppercase text-accent mb-3">03 — Supported Formats</span>
          <h2 className="font-display text-[1.2rem] font-bold text-text-primary mb-4 tracking-[-0.01em]">File formats</h2>
          <table className="w-full border-collapse mt-4 text-[0.85rem]">
            <thead>
              <tr>
                <th className="text-left font-mono text-[0.63rem] tracking-[0.14em] uppercase text-text-dim py-2 px-3 border-b border-border-dim font-normal">Extension</th>
                <th className="text-left font-mono text-[0.63rem] tracking-[0.14em] uppercase text-text-dim py-2 px-3 border-b border-border-dim font-normal">Format</th>
                <th className="text-left font-mono text-[0.63rem] tracking-[0.14em] uppercase text-text-dim py-2 px-3 border-b border-border-dim font-normal">Notes</th>
              </tr>
            </thead>
            <tbody>
              {[
                [".csv", "Comma-Separated Values", "UTF-8; delimiter detected automatically"],
                [".tsv", "Tab-Separated Values", "Common export from spectroscopy and diffraction software"],
                [".xlsx", "Excel Workbook", "First sheet loaded; multi-sheet files not yet supported"],
                [".xls", "Legacy Excel", "Pre-2007 format; converted on upload"],
              ].map(([ext, fmt, note], i, arr) => (
                <tr key={ext}>
                  <td className={`${tdFirst} ${i === arr.length - 1 ? "border-b-0" : ""}`}>{ext}</td>
                  <td className={`${tdBase} ${i === arr.length - 1 ? "border-b-0" : ""}`}>{fmt}</td>
                  <td className={`${tdBase} ${i === arr.length - 1 ? "border-b-0" : ""}`}>{note}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div id="privacy" className="pt-8 border-t border-border-dim mb-10">
          <span className="block font-mono text-[0.65rem] tracking-[0.14em] uppercase text-accent mb-3">04 — Data & Privacy</span>
          <h2 className="font-display text-[1.2rem] font-bold text-text-primary mb-4 tracking-[-0.01em]">How your data is handled</h2>
          <div className="bg-bg-surface border border-border-muted border-l-[3px] border-l-accent py-4 px-5 rounded-md text-[0.875rem] text-text-secondary leading-[1.7] font-light mt-4">
            Uploaded files are held in server memory for the duration of the session only.
            They are not written to disk, not persisted to a database, and not shared with
            any third party. Restarting the backend process clears all uploaded files.
            Your LLM API key is stored in a browser cookie and transmitted directly to the
            model provider — it never passes through this application's logic.
          </div>
        </div>

        <div id="models" className="pt-8 border-t border-border-dim mb-10">
          <span className="block font-mono text-[0.65rem] tracking-[0.14em] uppercase text-accent mb-3">05 — LLM Models</span>
          <h2 className="font-display text-[1.2rem] font-bold text-text-primary mb-4 tracking-[-0.01em]">Supported models</h2>
          <p className="text-text-secondary leading-[1.8] mb-4 font-light text-[0.95rem]">
            Two providers are supported. Bring your own API key via the Options panel
            in the Research Terminal.
          </p>
          <table className="w-full border-collapse mt-4 text-[0.85rem]">
            <thead>
              <tr>
                <th className="text-left font-mono text-[0.63rem] tracking-[0.14em] uppercase text-text-dim py-2 px-3 border-b border-border-dim font-normal">Provider</th>
                <th className="text-left font-mono text-[0.63rem] tracking-[0.14em] uppercase text-text-dim py-2 px-3 border-b border-border-dim font-normal">Model</th>
                <th className="text-left font-mono text-[0.63rem] tracking-[0.14em] uppercase text-text-dim py-2 px-3 border-b border-border-dim font-normal">Characteristics</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className={tdFirst}>Anthropic</td>
                <td className={tdBase}>Claude</td>
                <td className={tdBase}>Strong scientific reasoning and structured output. Recommended for most materials analysis tasks.</td>
              </tr>
              <tr>
                <td className={`${tdFirst} border-b-0`}>OpenAI</td>
                <td className={`${tdBase} border-b-0`}>GPT-4o</td>
                <td className={`${tdBase} border-b-0`}>Strong general-purpose reasoning. Useful for cross-validation against Claude responses.</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div id="shortcuts" className="pt-8 border-t border-border-dim mb-10">
          <span className="block font-mono text-[0.65rem] tracking-[0.14em] uppercase text-accent mb-3">06 — Keyboard Shortcuts</span>
          <h2 className="font-display text-[1.2rem] font-bold text-text-primary mb-4 tracking-[-0.01em]">Input shortcuts</h2>
          <div className="flex flex-col gap-3 mt-4">
            {[
              ["Enter", "Send message"],
              ["Shift + Enter", "Insert newline in message"],
            ].map(([key, desc]) => (
              <div key={key} className="flex items-center gap-4 text-[0.875rem]">
                <span className="font-mono text-[0.73rem] py-[0.2rem] px-[0.55rem] border border-border-muted rounded-sm bg-bg-surface text-text-primary whitespace-nowrap min-w-[130px] text-center">
                  {key}
                </span>
                <span className="text-text-secondary font-light">{desc}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
