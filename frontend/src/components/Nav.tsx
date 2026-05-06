import { useState } from "react";
import { NavLink } from "react-router-dom";

const NAV_LINKS = [
  { to: "/", label: "Home", end: true },
  { to: "/research", label: "Research Terminal", end: false },
  { to: "/about", label: "About", end: false },
];

export default function Nav() {
  const [open, setOpen] = useState(false);

  return (
    <>
      <style>{`
        .nav-root {
          position: fixed;
          top: 0; left: 0; right: 0;
          z-index: 100;
          height: var(--nav-h);
          background: var(--bg-base);
          border-bottom: 1px solid var(--border-dim);
          display: flex;
          align-items: center;
          padding: 0 var(--space-6);
        }
        .nav-inner {
          width: 100%;
          max-width: 1200px;
          margin: 0 auto;
          display: flex;
          align-items: center;
          justify-content: space-between;
        }
        .nav-brand {
          display: flex;
          align-items: center;
          gap: 0;
          font-family: var(--font-display);
          font-weight: 700;
          font-size: 0.95rem;
          letter-spacing: 0.05em;
          color: var(--text-primary);
          text-decoration: none;
          user-select: none;
        }
        .nav-brand-mark {
          color: var(--accent);
          margin-right: 0.45rem;
          font-size: 1.1rem;
        }
        .nav-brand-sep {
          color: var(--text-dim);
          font-family: var(--font-mono);
          font-weight: 300;
          font-size: 0.8rem;
          margin: 0 0.05em;
        }
        .nav-links {
          display: none;
          align-items: center;
          gap: var(--space-1);
          list-style: none;
        }
        @media (min-width: 768px) {
          .nav-links { display: flex; }
          .nav-hamburger { display: none !important; }
        }
        .nav-link {
          padding: 0.35rem var(--space-3);
          border-radius: var(--radius-md);
          font-size: 0.85rem;
          letter-spacing: 0.02em;
          color: var(--text-secondary);
          transition: color var(--transition-fast), background var(--transition-fast);
          text-decoration: none;
        }
        .nav-link:hover {
          color: var(--text-primary);
          background: var(--bg-surface);
        }
        .nav-link.active {
          color: var(--accent);
          background: var(--accent-glow);
        }
        .nav-hamburger {
          display: flex;
          flex-direction: column;
          gap: 5px;
          padding: var(--space-2);
          background: none;
          border: none;
          cursor: pointer;
        }
        .nav-hamburger span {
          display: block;
          width: 20px;
          height: 2px;
          background: var(--text-secondary);
          border-radius: 2px;
          transition: transform var(--transition-med), opacity var(--transition-med);
          transform-origin: center;
        }
        .nav-hamburger.open span:nth-child(1) { transform: translateY(7px) rotate(45deg); }
        .nav-hamburger.open span:nth-child(2) { opacity: 0; transform: scaleX(0); }
        .nav-hamburger.open span:nth-child(3) { transform: translateY(-7px) rotate(-45deg); }
        .nav-mobile-menu {
          position: fixed;
          top: var(--nav-h);
          left: 0; right: 0;
          background: var(--bg-base);
          border-bottom: 1px solid var(--border-dim);
          padding: var(--space-3) var(--space-6) var(--space-4);
          display: flex;
          flex-direction: column;
          gap: var(--space-1);
          transform: translateY(-110%);
          transition: transform var(--transition-med);
          z-index: 99;
        }
        .nav-mobile-menu.open { transform: translateY(0); }
        .nav-mobile-link {
          display: block;
          padding: var(--space-3) var(--space-4);
          border-radius: var(--radius-md);
          font-size: 0.95rem;
          color: var(--text-secondary);
          text-decoration: none;
          transition: color var(--transition-fast), background var(--transition-fast);
        }
        .nav-mobile-link:hover { color: var(--text-primary); background: var(--bg-surface); }
        .nav-mobile-link.active { color: var(--accent); background: var(--accent-glow); }
      `}</style>

      <nav className="nav-root">
        <div className="nav-inner">
          <NavLink to="/" className="nav-brand" onClick={() => setOpen(false)}>
            <span className="nav-brand-mark">◈</span>
            MATERIA
            <span className="nav-brand-sep">·</span>
            AGENTS
          </NavLink>

          <ul className="nav-links">
            {NAV_LINKS.map(({ to, label, end }) => (
              <li key={to}>
                <NavLink to={to} end={end} className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}>
                  {label}
                </NavLink>
              </li>
            ))}
          </ul>

          <button
            className={`nav-hamburger${open ? " open" : ""}`}
            onClick={() => setOpen((v) => !v)}
            aria-label={open ? "Close menu" : "Open menu"}
          >
            <span /><span /><span />
          </button>
        </div>
      </nav>

      <div className={`nav-mobile-menu${open ? " open" : ""}`}>
        {NAV_LINKS.map(({ to, label, end }) => (
          <NavLink
            key={to}
            to={to}
            end={end}
            className={({ isActive }) => `nav-mobile-link${isActive ? " active" : ""}`}
            onClick={() => setOpen(false)}
          >
            {label}
          </NavLink>
        ))}
      </div>
    </>
  );
}
