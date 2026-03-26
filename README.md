SAP Order-to-Cash Graph Explorer & Conversational AI


📌 Project Overview


In enterprise systems, supply chain and financial data are often fragmented across isolated tables (Orders, Deliveries, Invoices, Payments). This project unifies that fragmented data into a cohesive Knowledge Graph and provides an Agentic LLM-powered conversational interface to query, trace, and analyze the data using natural language.

🔗 Quick Links
Live Demo: [Insert Demo Link Here]

GitHub Repository: [Insert Repo Link Here]

AI Coding Logs (Cursor/Claude): [Insert link to your logs here]

🏗️ Architecture & Tech Stack
The system is built on a modern, decoupled stack designed for high performance and complex LLM orchestration.

Frontend: React (Next.js/Vite) + react-force-graph-2d for dynamic, physics-based network visualization.

Backend: Python (FastAPI) for lightweight, fast API endpoints.

Database: Neo4j (AuraDB / Local) for native graph storage and traversal.

LLM Orchestration: LangChain integrated with custom Worker-Judge Agentic flow, powered by [Insert your LLM here, e.g., Groq Llama-3 / Google Gemini].

🧠 LLM Orchestration: The Worker-Judge Architecture
Instead of relying on a fragile, single-shot LLM prompt, this backend implements a robust, multi-agent evaluation loop to ensure Cypher syntax accuracy and answer quality.

1. The Guardrail (Pre-Execution Validation)

Before any query reaches the database, the user's input is intercepted by a strict domain-restriction guardrail.

Logic: An LLM call evaluates if the prompt is related to supply chain, SAP, or the dataset.

Outcome: If off-topic (e.g., "Write me a poem" or prompt-injection attempts), the system immediately halts and returns: "This system is designed to answer questions related to the provided dataset only."

1. The Orchestration Loop (Worker & Judge)

If the query passes the guardrail, it enters a MAX_ROUNDS=3 evaluation loop:

Worker (Cypher Generation): The LLM receives the graph schema, strict negative-query syntax rules, and the user's question. It generates raw Cypher.

Execution & Auto-Correction: The FastAPI backend executes the Cypher against Neo4j. If Neo4j throws a SyntaxError, the raw error is fed directly back to the Worker to fix its mistake in the next round.

Worker (Answer Generation): Once execution succeeds, the JSON results are passed back to the Worker to synthesize a natural language response.

The Judge (Semantic QA): A secondary LLM prompt acts as a strict QA Judge. It reviews the original question, the Cypher used, the DB results, and the synthesized answer.

If the answer hallucinates data or misrepresents an empty database result, the Judge returns a RETRY command with targeted feedback.

If accurate, the Judge returns PASS, and the data is sent to the client.

Code snippet
graph TD
    A[User Chat] --> B{Guardrail Check}
    B -->|Blocked| C[Return: Domain Restricted]
    B -->|Passed| D[Worker: Generate Cypher]
    D --> E{Execute Neo4j Query}
    E -->|Syntax Error| F[Feedback to Worker]
    F --> D
    E -->|Success| G[Worker: Generate NL Answer]
    G --> H{Judge: Semantic Check}
    H -->|Fail / Hallucination| I[Feedback to Worker]
    I --> D
    H -->|Pass| J[Return NL Answer & Cypher to UI]


🕸️ Graph Modeling & Database Tradeoffs
Why Neo4j instead of PostgreSQL/SQL?
Answering a query like "Trace the full flow of a billing document" in SQL requires highly complex, computationally expensive Recursive CTEs and multiple JOIN operations. In Neo4j, this is a native, highly performant traversal: MATCH path=(o:Order)-[*]->(p:Payment) RETURN path. Furthermore, LLMs excel at generating Cypher due to its highly semantic, ASCII-art syntax.

Key Architectural Decisions in Modeling:
Node Granularity: Every individual record (e.g., Order #101, Invoice #504) is instantiated as a unique Node, rather than just aggregating table names. This allows for precise, item-level traceability.

Property Flattening: Nested JSON objects (like timestamps) are flattened into single-level properties (e.g., time_hours) to comply with Neo4j's native property constraints.

Conditional Edge Creation: To handle incomplete flows, relationship edges are created conditionally (FOREACH ... CASE WHEN). This prevents "ghost nodes" and ensures data integrity.

🎨 Frontend & Graph Rendering
The UI features a split-pane design combining an interactive 2D canvas with the conversational AI.

Rendering Mechanics (react-force-graph-2d)
Safe State Mutation: The graph component intrinsically mutates string IDs into JavaScript object references. The frontend safely maps these IDs to prevent link duplication and graph crashing during state updates.

Data vs. Schema Views: * Schema Mode: Renders the high-level structural ontology of the database.

Data Mode: Pulls a dynamic sample (e.g., 800 nodes limit) to prevent browser memory exhaustion while still providing a rich visual representation of the interconnected supply chain.

Dynamic Legend & Expansion: The legend dynamically calculates colors based on the node labels present in the current view. Users can click any node to expand its direct relationships, triggering a targeted fetch to the backend.

Chat Transparency
To build user trust, the UI doesn't just return the natural language answer. It includes a collapsible "Generated Query" metadata block, revealing the exact Cypher query the Worker LLM generated. This proves the answer is grounded in actual database execution, not LLM hallucination.