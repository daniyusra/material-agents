import { useState } from "react";
import { NavLink } from "react-router-dom";

const WHATSAPP_ENABLED = import.meta.env.VITE_WHATSAPP_ENABLED === "true";

const NAV_LINKS = [
  { to: "/", label: "Home", end: true },
  { to: "/research", label: "Research Terminal", end: false },
  ...(WHATSAPP_ENABLED ? [{ to: "/whatsapp", label: "WhatsApp Bot", end: false }] : []),
  { to: "/about", label: "About", end: false },
];

const linkClass = ({ isActive }: { isActive: boolean }) =>
  `px-3 py-[0.35rem] rounded-md text-[0.85rem] tracking-[0.02em] no-underline transition-colors duration-[120ms] ease ${
    isActive
      ? "text-accent bg-accent-glow"
      : "text-text-secondary hover:text-text-primary hover:bg-bg-surface"
  }`;

const mobileLinkClass = ({ isActive }: { isActive: boolean }) =>
  `block py-3 px-4 rounded-md text-[0.95rem] no-underline transition-colors duration-[120ms] ease ${
    isActive
      ? "text-accent bg-accent-glow"
      : "text-text-secondary hover:text-text-primary hover:bg-bg-surface"
  }`;

export default function Nav() {
  const [open, setOpen] = useState(false);

  return (
    <>
      <nav className="fixed top-0 left-0 right-0 z-[100] h-nav bg-bg-base border-b border-border-dim flex items-center px-6">
        <div className="w-full max-w-[1200px] mx-auto flex items-center justify-between">
          <NavLink
            to="/"
            className="flex items-center font-display font-bold text-[0.95rem] tracking-[0.05em] text-text-primary no-underline select-none"
            onClick={() => setOpen(false)}
          >
            <span className="text-accent mr-[0.45rem] text-[1.1rem]">◈</span>
            MATERIA
            <span className="text-text-dim font-mono font-light text-[0.8rem] mx-[0.05em]">·</span>
            AGENTS
          </NavLink>

          <ul className="hidden md:flex items-center gap-1 list-none">
            {NAV_LINKS.map(({ to, label, end }) => (
              <li key={to}>
                <NavLink to={to} end={end} className={linkClass}>
                  {label}
                </NavLink>
              </li>
            ))}
          </ul>

          <button
            className="md:hidden flex flex-col gap-[5px] p-2 bg-transparent border-none cursor-pointer"
            onClick={() => setOpen((v) => !v)}
            aria-label={open ? "Close menu" : "Open menu"}
          >
            <span className={`block w-5 h-[2px] bg-text-secondary rounded-[2px] transition-[transform,opacity] duration-[220ms] ease origin-center ${open ? "translate-y-[7px] rotate-45" : ""}`} />
            <span className={`block w-5 h-[2px] bg-text-secondary rounded-[2px] transition-[transform,opacity] duration-[220ms] ease origin-center ${open ? "opacity-0 scale-x-0" : ""}`} />
            <span className={`block w-5 h-[2px] bg-text-secondary rounded-[2px] transition-[transform,opacity] duration-[220ms] ease origin-center ${open ? "-translate-y-[7px] -rotate-45" : ""}`} />
          </button>
        </div>
      </nav>

      <div className={`fixed top-nav left-0 right-0 bg-bg-base border-b border-border-dim px-6 py-3 pb-4 flex flex-col gap-1 z-[99] transition-transform duration-[220ms] ease ${open ? "translate-y-0" : "-translate-y-[110%]"}`}>
        {NAV_LINKS.map(({ to, label, end }) => (
          <NavLink
            key={to}
            to={to}
            end={end}
            className={mobileLinkClass}
            onClick={() => setOpen(false)}
          >
            {label}
          </NavLink>
        ))}
      </div>
    </>
  );
}
