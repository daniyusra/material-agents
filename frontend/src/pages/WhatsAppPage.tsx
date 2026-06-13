import { useEffect, useState, useCallback } from "react";
import { waApi } from "../api";
import type { WaGoal, WaGroup, WaReminder, WaTodo } from "../types";

function ts(unix: number) {
  return new Date(unix * 1000).toLocaleString(undefined, {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
  });
}

function Section({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <section>
      <p className="font-mono text-[0.68rem] tracking-[0.18em] uppercase text-accent mb-3">{label}</p>
      {children}
    </section>
  );
}

function EmptyState({ text }: { text: string }) {
  return (
    <p className="font-mono text-[0.78rem] text-text-dim py-4 text-center border border-border-dim rounded-md">
      {text}
    </p>
  );
}

export default function WhatsAppPage() {
  const [groups, setGroups] = useState<WaGroup[]>([]);
  const [selectedJid, setSelectedJid] = useState<string>("");
  const [todos, setTodos] = useState<WaTodo[]>([]);
  const [goals, setGoals] = useState<WaGoal[]>([]);
  const [reminders, setReminders] = useState<WaReminder[]>([]);
  const [recap, setRecap] = useState<string>("");
  const [recapLoading, setRecapLoading] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // New todo / goal form state
  const [newTodo, setNewTodo] = useState("");
  const [newGoal, setNewGoal] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const loadGroups = useCallback(async () => {
    try {
      const data = await waApi.groups();
      setGroups(data);
      if (data.length > 0 && !selectedJid) setSelectedJid(data[0].group_jid);
    } catch (e) {
      setError(String(e));
    }
  }, [selectedJid]);

  const loadGroupData = useCallback(async (jid: string) => {
    if (!jid) return;
    setLoading(true);
    setError(null);
    try {
      const [t, g, r] = await Promise.all([
        waApi.todos(jid, false),
        waApi.goals(jid, false),
        waApi.reminders(jid, false),
      ]);
      setTodos(t);
      setGoals(g);
      setReminders(r);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadGroups(); }, []);
  useEffect(() => { loadGroupData(selectedJid); setRecap(""); }, [selectedJid]);

  const completeTodo = async (id: number) => {
    await waApi.completeTodo(id);
    setTodos((prev) => prev.filter((t) => t.id !== id));
  };

  const completeGoal = async (id: number) => {
    await waApi.completeGoal(id);
    setGoals((prev) => prev.filter((g) => g.id !== id));
  };

  const addTodo = async () => {
    if (!newTodo.trim() || !selectedJid) return;
    setSubmitting(true);
    try {
      const todo = await waApi.createTodo(selectedJid, newTodo.trim());
      setTodos((prev) => [todo, ...prev]);
      setNewTodo("");
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  const addGoal = async () => {
    if (!newGoal.trim() || !selectedJid) return;
    setSubmitting(true);
    try {
      const goal = await waApi.createGoal(selectedJid, newGoal.trim());
      setGoals((prev) => [goal, ...prev]);
      setNewGoal("");
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  const sendRecap = async () => {
    if (!selectedJid) return;
    setRecapLoading(true);
    setRecap("");
    try {
      const data = await waApi.recap(selectedJid);
      setRecap(data.recap);
    } catch (e) {
      setError(String(e));
    } finally {
      setRecapLoading(false);
    }
  };

  return (
    <div className="max-w-[1100px] mx-auto px-6 py-10">
      {/* Header */}
      <div className="mb-8">
        <div className="inline-flex items-center gap-2 font-mono text-[0.7rem] text-accent tracking-[0.14em] uppercase mb-4 py-[0.3rem] px-4 border border-accent-border rounded-sm bg-accent-glow">
          <span className="w-[6px] h-[6px] bg-accent rounded-full animate-pulse-status shrink-0" />
          WhatsApp Group Assistant
        </div>
        <h1 className="font-display text-[clamp(1.6rem,4vw,2.4rem)] font-extrabold text-text-primary tracking-[-0.02em]">
          Group Dashboard
        </h1>
        <p className="text-text-secondary text-[0.95rem] font-light mt-2">
          Todos, goals, and reminders extracted from your WhatsApp group.
        </p>
      </div>

      {/* Group selector */}
      <div className="mb-8 flex items-center gap-3 flex-wrap">
        {groups.length === 0 ? (
          <p className="font-mono text-[0.78rem] text-text-dim">
            No groups yet — send a message to the bot to register a group.
          </p>
        ) : (
          <>
            <span className="font-mono text-[0.72rem] text-text-dim tracking-[0.1em] uppercase">Group</span>
            <select
              className="bg-bg-surface border border-border-muted text-text-primary text-[0.85rem] rounded-md px-3 py-[0.4rem] font-mono outline-none focus:border-accent-border"
              value={selectedJid}
              onChange={(e) => setSelectedJid(e.target.value)}
            >
              {groups.map((g) => (
                <option key={g.group_jid} value={g.group_jid}>
                  {g.group_jid} · {g.message_count} msgs · last {ts(g.last_activity)}
                </option>
              ))}
            </select>
            <button
              className="font-mono text-[0.72rem] text-accent border border-accent-border rounded-md px-3 py-[0.4rem] bg-transparent hover:bg-accent-glow transition-colors duration-[120ms]"
              onClick={() => loadGroupData(selectedJid)}
            >
              Refresh
            </button>
          </>
        )}
      </div>

      {error && (
        <div className="mb-6 px-4 py-3 bg-error-bg border border-error rounded-md font-mono text-[0.8rem] text-error">
          {error}
        </div>
      )}

      {loading ? (
        <p className="font-mono text-[0.8rem] text-text-dim animate-pulse">Loading…</p>
      ) : selectedJid ? (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Todos */}
          <Section label="Open Action Items">
            <div className="space-y-2 mb-3">
              {todos.length === 0 ? (
                <EmptyState text="No open todos" />
              ) : (
                todos.map((t) => (
                  <div
                    key={t.id}
                    className="flex items-start gap-3 bg-bg-surface border border-border-dim rounded-md px-4 py-3 group"
                  >
                    <button
                      className="mt-[2px] w-4 h-4 shrink-0 rounded-sm border border-border-muted bg-transparent hover:border-accent transition-colors duration-[120ms] cursor-pointer"
                      onClick={() => completeTodo(t.id)}
                      title="Mark done"
                    />
                    <div className="flex-1 min-w-0">
                      <p className="text-text-primary text-[0.88rem] leading-snug">{t.text}</p>
                      <p className="font-mono text-[0.68rem] text-text-dim mt-1">
                        {t.owner ? `@${t.owner} · ` : ""}{ts(t.created_at)}
                      </p>
                    </div>
                  </div>
                ))
              )}
            </div>
            {/* Add todo */}
            <div className="flex gap-2">
              <input
                className="flex-1 bg-bg-surface border border-border-muted rounded-md px-3 py-[0.4rem] text-text-primary text-[0.85rem] placeholder:text-text-dim outline-none focus:border-accent-border"
                placeholder="New action item…"
                value={newTodo}
                onChange={(e) => setNewTodo(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && addTodo()}
              />
              <button
                className="px-4 py-[0.4rem] bg-accent text-bg-void font-display font-bold text-[0.82rem] rounded-md border-none cursor-pointer hover:opacity-85 transition-opacity duration-[120ms] disabled:opacity-40"
                onClick={addTodo}
                disabled={submitting || !newTodo.trim()}
              >
                Add
              </button>
            </div>
          </Section>

          {/* Goals */}
          <Section label="Goals">
            <div className="space-y-2 mb-3">
              {goals.length === 0 ? (
                <EmptyState text="No active goals" />
              ) : (
                goals.map((g) => (
                  <div
                    key={g.id}
                    className="flex items-start gap-3 bg-bg-surface border border-border-dim rounded-md px-4 py-3"
                  >
                    <button
                      className="mt-[2px] w-4 h-4 shrink-0 rounded-sm border border-border-muted bg-transparent hover:border-accent transition-colors duration-[120ms] cursor-pointer"
                      onClick={() => completeGoal(g.id)}
                      title="Mark done"
                    />
                    <div className="flex-1 min-w-0">
                      <p className="text-text-primary text-[0.88rem] font-medium leading-snug">{g.title}</p>
                      {g.description && (
                        <p className="text-text-secondary text-[0.8rem] mt-1 leading-snug">{g.description}</p>
                      )}
                      <p className="font-mono text-[0.68rem] text-text-dim mt-1">
                        {g.created_by ? `by ${g.created_by} · ` : ""}{ts(g.created_at)}
                        {g.target_date ? ` · due ${ts(g.target_date)}` : ""}
                      </p>
                    </div>
                  </div>
                ))
              )}
            </div>
            {/* Add goal */}
            <div className="flex gap-2">
              <input
                className="flex-1 bg-bg-surface border border-border-muted rounded-md px-3 py-[0.4rem] text-text-primary text-[0.85rem] placeholder:text-text-dim outline-none focus:border-accent-border"
                placeholder="New goal…"
                value={newGoal}
                onChange={(e) => setNewGoal(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && addGoal()}
              />
              <button
                className="px-4 py-[0.4rem] bg-accent text-bg-void font-display font-bold text-[0.82rem] rounded-md border-none cursor-pointer hover:opacity-85 transition-opacity duration-[120ms] disabled:opacity-40"
                onClick={addGoal}
                disabled={submitting || !newGoal.trim()}
              >
                Add
              </button>
            </div>
          </Section>

          {/* Reminders */}
          <Section label="Upcoming Reminders">
            {reminders.length === 0 ? (
              <EmptyState text="No pending reminders" />
            ) : (
              <div className="space-y-2">
                {reminders.map((r) => (
                  <div
                    key={r.id}
                    className="bg-bg-surface border border-border-dim rounded-md px-4 py-3"
                  >
                    <p className="text-text-primary text-[0.88rem]">{r.text}</p>
                    <p className="font-mono text-[0.68rem] text-text-dim mt-1">
                      Due {ts(r.due_at)}{r.created_by ? ` · set by ${r.created_by}` : ""}
                    </p>
                  </div>
                ))}
              </div>
            )}
          </Section>

          {/* Recap */}
          <Section label="Recap">
            <button
              className="inline-flex items-center gap-2 py-[0.7rem] px-5 bg-accent text-bg-void font-display font-bold text-[0.88rem] tracking-[0.04em] border-none rounded-md cursor-pointer transition-[opacity,transform] duration-[120ms] ease hover:opacity-85 hover:-translate-y-px disabled:opacity-40 disabled:translate-y-0 mb-4"
              onClick={sendRecap}
              disabled={recapLoading}
            >
              {recapLoading ? "Generating…" : "Send Recap Now"}
            </button>
            {recap && (
              <div className="bg-bg-surface border border-border-dim rounded-md px-5 py-4">
                <p className="font-mono text-[0.68rem] tracking-[0.12em] uppercase text-accent mb-3">AI Summary</p>
                <p className="text-text-secondary text-[0.88rem] leading-[1.7] whitespace-pre-wrap">{recap}</p>
              </div>
            )}
          </Section>
        </div>
      ) : null}

      {/* Commands reference */}
      <div className="mt-12 border-t border-border-dim pt-8">
        <p className="font-mono text-[0.68rem] tracking-[0.18em] uppercase text-text-dim mb-4">
          WhatsApp Commands
        </p>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {[
            { cmd: "!recap", desc: "Generate and send a group summary" },
            { cmd: "!todo <task>", desc: "Add an action item, optionally with @owner" },
            { cmd: "!remind <text> in 2h", desc: "Set a reminder (30m · 2h · 1d · 1w)" },
            { cmd: "!goal <title>", desc: "Track a group goal" },
          ].map(({ cmd, desc }) => (
            <div key={cmd} className="border border-border-dim rounded-md p-4 bg-bg-surface">
              <code className="font-mono text-[0.78rem] text-accent">{cmd}</code>
              <p className="text-text-secondary text-[0.8rem] mt-2 font-light leading-snug">{desc}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
