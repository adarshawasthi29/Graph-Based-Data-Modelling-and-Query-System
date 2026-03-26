import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import ForceGraph2D from "react-force-graph-2d";

// IMPORTANT: Ensure this matches your FastAPI backend URL
const API_BASE = "https://graph-based-data-modelling-and-query-2hw9.onrender.com";

type GraphNode = {
  id: string;
  title?: string;
  name?: string;
  color?: string;
  labels?: string[];
  kind?: "Table" | "Field" | string;
  bizId?: string;
  val?: number;
  props?: Record<string, unknown>;
};

type GraphLink = {
  source: string | GraphNode;
  target: string | GraphNode;
  label?: string;
  type?: string;
  relId?: string;
  color?: string;
};

type GraphData = { nodes: GraphNode[]; links: GraphLink[] };

type ChatMessage = {
  role: "user" | "assistant";
  text: string;
  blocked?: boolean;
  detail?: string;
};

const EMPTY: GraphData = { nodes: [], links: [] };

function linkKey(l: GraphLink): string {
  const src = typeof l.source === "object" ? (l.source as GraphNode).id : l.source;
  const tgt = typeof l.target === "object" ? (l.target as GraphNode).id : l.target;
  return `${src}|${tgt}|${l.relId ?? l.label ?? ""}`;
}

function mergeGraph(base: GraphData, more: GraphData): GraphData {
  const nodes = new Map<string, GraphNode>();
  base.nodes.forEach((n) => nodes.set(n.id, { ...n }));
  more.nodes.forEach((n) => {
    const prev = nodes.get(n.id);
    nodes.set(n.id, prev ? { ...prev, ...n } : { ...n });
  });
  const links = new Map<string, GraphLink>();
  base.links.forEach((l) => links.set(linkKey(l), { ...l }));
  more.links.forEach((l) => links.set(linkKey(l), { ...l }));
  return { nodes: [...nodes.values()], links: [...links.values()] };
}

export default function App() {
  const containerRef = useRef<HTMLDivElement>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  
  const [size, setSize] = useState({ w: 800, h: 600 });
  const [graphData, setGraphData] = useState<GraphData>(EMPTY);
  const [limit, setLimit] = useState(800);
  const [viewMode, setViewMode] = useState<"schema" | "data">("schema");
  const [loading, setLoading] = useState(false);
  const [stats, setStats] = useState<{ label: string; cnt: number }[]>([]);
  const [selected, setSelected] = useState<GraphNode | null>(null);
  const [messages, setMessages] = useState<ChatMessage[]>([
    {
      role: "assistant",
      text: "Ask questions about orders, deliveries, invoices, payments, and products. Off-topic questions are blocked.",
    },
  ]);
  const [input, setInput] = useState("");
  const [sending, setSending] = useState(false);

  useLayoutEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => {
      setSize({ w: el.clientWidth, h: el.clientHeight });
    });
    ro.observe(el);
    setSize({ w: el.clientWidth, h: el.clientHeight });
    return () => ro.disconnect();
  }, []);

  // Auto-scroll chat
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const loadSample = useCallback(async () => {
    setLoading(true);
    try {
      if (viewMode === "schema") {
        const res = await fetch(`${API_BASE}/api/graph/schema`);
        if (!res.ok) throw new Error(await res.text());
        const data = (await res.json()) as GraphData;
        setGraphData(data);
        setSelected(null);
        return;
      }

      const safeLimit = Math.max(
        50,
        Math.min(5000, Number.isFinite(limit) ? limit : 800)
      );
      const res = await fetch(`${API_BASE}/api/graph/sample?limit=${safeLimit}`);
      if (!res.ok) throw new Error(await res.text());
      const data = (await res.json()) as GraphData;
      setGraphData(data);
      setSelected(null);
    } catch (e) {
      console.error(e);
      setMessages((m) => [
        ...m,
        {
          role: "assistant",
          text: `Failed to load graph: ${e instanceof Error ? e.message : String(e)}`,
        },
      ]);
    } finally {
      setLoading(false);
    }
  }, [limit, viewMode]);

  useEffect(() => {
    void loadSample();
  }, [loadSample]);

  useEffect(() => {
    void (async () => {
      try {
        const res = await fetch(`${API_BASE}/api/graph/stats`);
        if (!res.ok) return;
        const rows = (await res.json()) as { label: string; cnt: number }[];
        setStats(rows);
      } catch {
        /* ignore */
      }
    })();
  }, []);

  const expandSelected = useCallback(async () => {
    if (viewMode !== "data") return;
    if (!selected?.id) return;
    setLoading(true);
    try {
      const url = `${API_BASE}/api/graph/expand/${encodeURIComponent(selected.id)}?limit=500`;
      const res = await fetch(url);
      if (!res.ok) throw new Error(await res.text());
      const more = (await res.json()) as GraphData;
      
      // Strict new object reference to force re-render
      setGraphData((prev) => {
        const merged = mergeGraph(prev, more);
        return { nodes: [...merged.nodes], links: [...merged.links] };
      });
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, [selected, viewMode]);

  const statSummary = useMemo(() => {
    if (!stats.length) return "";
    return stats
      .slice(0, 6)
      .map((s) => `${s.label}: ${s.cnt}`)
      .join(" · ");
  }, [stats]);

  const legendRows = useMemo(() => {
    const m = new Map<string, { label: string; color: string }>();
    for (const n of graphData.nodes) {
      // Allow legend for both Data and Schema nodes
      const label = n.labels?.[0] ?? n.title ?? n.id;
      const color = n.color ?? "#888";
      if (!m.has(label)) m.set(label, { label, color });
    }
    return [...m.values()].sort((a, b) => a.label.localeCompare(b.label));
  }, [graphData.nodes]);

  const sendChat = async (e: React.FormEvent) => {
    e.preventDefault();
    const text = input.trim();
    if (!text || sending) return;
    setInput("");
    setMessages((m) => [...m, { role: "user", text }]);
    setSending(true);
    try {
      const res = await fetch(`${API_BASE}/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: text }),
      });
      if (!res.ok) throw new Error(await res.text());
      const data = (await res.json()) as {
        answer: string;
        blocked?: boolean;
        intermediate_steps?: unknown;
      };
      let detail = "";
      if (data.intermediate_steps && Array.isArray(data.intermediate_steps)) {
        const parts: string[] = [];
        for (const step of data.intermediate_steps) {
          if (step && typeof step === "object" && "query" in step) {
            parts.push(`Cypher: ${String((step as { query: unknown }).query)}`);
          }
        }
        detail = parts.join("\n");
      }
      setMessages((m) => [
        ...m,
        {
          role: "assistant",
          text: data.answer,
          blocked: data.blocked,
          detail: detail || undefined,
        },
      ]);
    } catch (err) {
      setMessages((m) => [
        ...m,
        {
          role: "assistant",
          text: `Request failed: ${err instanceof Error ? err.message : String(err)}`,
        },
      ]);
    } finally {
      setSending(false);
    }
  };

  return (
    <div className="layout" style={{ display: 'flex', height: '100vh' }}>
      <div className="graph-panel" style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
        <div className="graph-toolbar" style={{ padding: '10px', background: '#f8f9fa' }}>
          <strong>Graph</strong>
          
          <button type="button" className={viewMode === "schema" ? "primary" : ""} onClick={() => { setViewMode("schema"); setSelected(null); }} disabled={loading}>
            Schema
          </button>
          
          <button type="button" className={viewMode === "data" ? "primary" : ""} onClick={() => { setViewMode("data"); setSelected(null); }} disabled={loading}>
            Data
          </button>
          
          {viewMode === "data" && (
            <label> Rel limit <input type="number" min={50} max={5000} value={limit} onChange={(e) => { const v = Number(e.target.value); if (!Number.isFinite(v)) return; setLimit(Math.max(50, Math.min(5000, v))); }} style={{ width: "5rem" }} /> </label>
          )}
          
          <button type="button" className="primary" onClick={() => void loadSample()} disabled={loading}>
            {loading ? "Loading…" : "Reload"}
          </button>
          
          <button type="button" onClick={() => void expandSelected()} disabled={viewMode !== "data" || !selected || loading}>
            Expand selection
          </button>
          
          <span className="graph-meta">
            {graphData.nodes.length} nodes · {graphData.links.length} links {statSummary ? ` · ${statSummary}` : ""}
          </span>
        </div>

        {/* Legend visible in both modes */}
        <div style={{ padding: "0.35rem 0.9rem 0.65rem", borderBottom: "1px solid #e2e6ea" }}>
          <strong>Legend</strong>
          <div className="legend" style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
            {legendRows.map((r) => (
              <div key={r.label} className="legend-item" style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
                <span className="legend-dot" style={{ background: r.color, width: '12px', height: '12px', borderRadius: '50%', display: 'inline-block' }} />
                <span>{r.label}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="graph-canvas-wrap" ref={containerRef} style={{ flex: 1, overflow: 'hidden' }}>
          {size.w > 0 && size.h > 0 && (
            <ForceGraph2D
              width={size.w}
              height={size.h}
              graphData={graphData}
              nodeVal="val"
              nodeColor={(n) => (n as GraphNode).color ?? "#888"}
              nodeLabel={(n) => (n as GraphNode).title ?? (n as GraphNode).id}
              linkDirectionalArrowLength={4}
              linkDirectionalArrowRelPos={1}
              linkColor={(l) => (l as GraphLink).color ?? "#a5abb6"}
              linkWidth={1}
              onNodeClick={(n) => setSelected(n as GraphNode)}
              cooldownTicks={120}
              onEngineStop={() => {}}
            />
          )}
        </div>

        {selected && (
          <div style={{ padding: "0.5rem 0.9rem", borderTop: "1px solid #e2e6ea", background: '#f8f9fa', maxHeight: '200px', overflowY: 'auto' }}>
            <strong>Selected:</strong> {selected.title ?? selected.id}
            <pre className="node-detail" style={{ fontSize: '12px', margin: '5px 0' }}>
              {JSON.stringify({ labels: selected.labels, bizId: selected.bizId, props: selected.props }, null, 2)}
            </pre>
          </div>
        )}
      </div>

      <div className="chat-panel" style={{ width: '400px', display: 'flex', flexDirection: 'column', borderLeft: '1px solid #ddd' }}>
        <div className="chat-header" style={{ padding: '15px', background: '#eee', fontWeight: 'bold' }}>Chat (NL → Cypher)</div>
        
        <div className="chat-messages" style={{ flex: 1, overflowY: 'auto', padding: '15px', display: 'flex', flexDirection: 'column', gap: '10px' }}>
          {messages.map((msg, i) => (
            <div key={i} style={{ alignSelf: msg.role === "user" ? 'flex-end' : 'flex-start', background: msg.role === 'user' ? '#007bff' : '#f1f1f1', color: msg.role === 'user' ? 'white' : 'black', padding: '10px', borderRadius: '8px', maxWidth: '85%' }} className={`msg ${msg.role === "user" ? "user" : "bot"}${msg.blocked ? " blocked" : ""}`}>
              {msg.text}
              {msg.detail && (
                <div className="msg-meta" style={{ marginTop: '8px', fontSize: '11px', background: 'rgba(0,0,0,0.05)', padding: '5px', borderRadius: '4px' }}>
                  <details>
                    <summary style={{ cursor: 'pointer' }}>Generated query</summary>
                    <pre style={{ margin: "0.35rem 0 0", whiteSpace: "pre-wrap", color: '#d63384' }}>{msg.detail}</pre>
                  </details>
                </div>
              )}
            </div>
          ))}
          {sending && (
            <div style={{ alignSelf: 'flex-start', color: '#888', fontStyle: 'italic', fontSize: '12px' }}>Thinking...</div>
          )}
          <div ref={messagesEndRef} /> 
        </div>

        <form className="chat-form" onSubmit={sendChat} style={{ padding: '15px', borderTop: '1px solid #ddd', display: 'flex', gap: '10px' }}>
          <input
            style={{ flex: 1, padding: '10px', borderRadius: '4px', border: '1px solid #ccc' }}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="e.g. How many invoices are there?"
            disabled={sending}
          />
          <button type="submit" disabled={sending} style={{ padding: '10px 15px', borderRadius: '4px', cursor: sending ? 'not-allowed' : 'pointer' }}>
            Send
          </button>
        </form>
      </div>
    </div>
  );
}
