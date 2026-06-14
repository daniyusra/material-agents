import { FormEvent, useState } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { useAuth } from "../../contexts/AuthContext";

export default function AdminLoginPage() {
  const { login, isAdmin } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const from = (location.state as { from?: { pathname: string } })?.from?.pathname ?? "/admin";

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  // Already logged in — redirect immediately
  if (isAdmin) {
    navigate(from, { replace: true });
    return null;
  }

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      await login(username, password);
      navigate(from, { replace: true });
    } catch {
      setError("Invalid credentials.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex items-center justify-center min-h-[80vh] px-6">
      <div className="w-full max-w-[380px]">
        <div className="mb-8 text-center">
          <span className="text-accent text-3xl select-none">◈</span>
          <h1 className="font-display text-xl font-bold text-text-primary mt-3">
            Admin Login
          </h1>
          <p className="text-text-dim text-sm mt-1">Blog management console</p>
        </div>

        <form
          onSubmit={handleSubmit}
          className="bg-bg-surface border border-border-dim rounded-lg p-8 flex flex-col gap-5"
        >
          <div className="flex flex-col gap-1.5">
            <label className="text-text-secondary text-xs tracking-wide uppercase font-mono">
              Username
            </label>
            <input
              type="text"
              autoComplete="username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              required
              className="bg-bg-elevated border border-border-dim rounded-md px-3 py-2.5 text-text-primary text-sm outline-none focus:border-accent-border transition-colors"
            />
          </div>

          <div className="flex flex-col gap-1.5">
            <label className="text-text-secondary text-xs tracking-wide uppercase font-mono">
              Password
            </label>
            <input
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              className="bg-bg-elevated border border-border-dim rounded-md px-3 py-2.5 text-text-primary text-sm outline-none focus:border-accent-border transition-colors"
            />
          </div>

          {error && (
            <p className="text-error text-sm bg-error-bg rounded-md px-3 py-2">
              {error}
            </p>
          )}

          <button
            type="submit"
            disabled={loading}
            className="mt-1 w-full py-2.5 rounded-md bg-accent text-bg-void font-semibold text-sm tracking-wide hover:opacity-90 disabled:opacity-50 disabled:cursor-not-allowed transition-opacity"
          >
            {loading ? "Signing in…" : "Sign in"}
          </button>
        </form>
      </div>
    </div>
  );
}
