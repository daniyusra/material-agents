/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx,ts,tsx}"],
  theme: {
    extend: {
      colors: {
        "bg-void": "#080a0c",
        "bg-base": "#0d1219",
        "bg-surface": "#111b27",
        "bg-elevated": "#172233",
        "border-dim": "#192534",
        "border-muted": "#223040",
        "text-primary": "#c8d8e8",
        "text-secondary": "#7090a8",
        "text-dim": "#405060",
        accent: "#22c9c9",
        "accent-glow": "rgba(34, 201, 201, 0.12)",
        "accent-border": "rgba(34, 201, 201, 0.28)",
        "accent-text": "#9ee4e4",
        error: "#f87171",
        "error-bg": "rgba(248, 113, 113, 0.07)",
      },
      fontFamily: {
        display: ["Syne", "system-ui", "sans-serif"],
        body: ["DM Sans", "system-ui", "sans-serif"],
        mono: ["IBM Plex Mono", "Courier New", "monospace"],
      },
      borderRadius: {
        sm: "3px",
        md: "6px",
        lg: "10px",
      },
      spacing: {
        nav: "3.5rem",
      },
      animation: {
        "pulse-status": "pulse-status 2.4s ease-in-out infinite",
        "cursor-blink": "cursor-blink 1s step-end infinite",
      },
      keyframes: {
        "pulse-status": {
          "0%, 100%": { opacity: "1" },
          "50%": { opacity: "0.25" },
        },
        "cursor-blink": {
          "0%, 100%": { opacity: "1" },
          "50%": { opacity: "0" },
        },
      },
    },
  },
  plugins: [
    require("@tailwindcss/typography"),
  ],
};
