This is a fantastic assignment. You are essentially building a lightweight version of an enterprise process mining and knowledge graph platform (similar to what companies like Palantir or Celonis do).

Since the evaluation heavily weighs architectural decisions, reasoning, and AI usage, the planning phase is indeed the most critical. You want a stack that is highly impressive but easy for AI coding assistants (like Cursor or Claude) to scaffold quickly.

Here is a comprehensive blueprint and implementation plan to tackle this assignment.

Phase 1: Architecture & Tech Stack Selection
You need a stack that natively supports graph traversal and is well-supported by LLM orchestration frameworks (like LangChain/LlamaIndex).

Database: Neo4j (AuraDB Free Tier)

Why: It is the industry standard for graph databases. It uses Cypher, a highly intuitive query language that LLMs are excellent at writing (Text-to-Cypher). The free cloud tier is perfect for this.

Backend: Python (FastAPI) + LangChain

Why: Python is the lingua franca of AI. FastAPI is fast and lightweight. LangChain has a built-in GraphCypherQAChain that does 80% of the heavy lifting for Text-to-Cypher translation.

LLM: Groq (Llama-3 70B) or Google Gemini

Why: Groq offers blazing-fast inference on its free tier, which is great for the multi-step reasoning required here (Guardrails -> Text-to-Cypher -> Natural Language generation).

Frontend: Next.js (React) + react-force-graph

Why: react-force-graph provides beautiful 2D/3D graph visualizations out-of-the-box. Next.js handles the chat UI easily.

Phase 2: Data Modeling (The Graph Schema)
The evaluators will look closely at how logically you model the fragmented tables into a cohesive graph. Do not just load tables as isolated nodes; build a story.

Nodes (Entities):

Customer (Properties: ID, Name)

Product (Properties: ID, Name, Category)

Order (Properties: OrderID, Date, Status)

Delivery (Properties: DeliveryID, Date, Plant)

Invoice (Properties: InvoiceID, Amount, Date)

Payment (Properties: PaymentID, Amount, Status)

Edges (Relationships):

(:Customer)-[:PLACED]->(:Order)

(:Order)-[:CONTAINS]->(:Product)

(:Order)-[:FULFILLED_BY]->(:Delivery)

(:Delivery)-[:GENERATED_BILL]->(:Invoice)

(:Invoice)-[:SETTLED_BY]->(:Payment)

Architectural Tradeoff Note (Put this in your README): You could make Address or Plant their own nodes, or keep them as properties on Customer and Delivery. Making them nodes is better for queries like "Show me all deliveries to Plant X".

Phase 3: The LLM "Brain" Pipeline (Backend)
This is where you solve the Conversational Query and Guardrails requirements. Do not use a single prompt. Use a pipeline:

Step 1: The Guardrail Router (Prompt 1)

Task: Check if the user's input is about the dataset (orders, invoices, supply chain).

Logic: If unrelated ("Write me a poem"), return the exact string: "This system is designed to answer questions related to the provided dataset only." If related, proceed to Step 2.

Step 2: Text-to-Cypher Translation (Prompt 2)

Task: Provide the LLM with your exact Neo4j database schema. Ask it to generate only the Cypher query.

Example Query: "Trace the flow of Invoice #123."

LLM Output: MATCH path=(o:Order)-[*]->(i:Invoice {id: "123"}) RETURN path

Step 3: Database Execution

Task: Run the generated Cypher query against Neo4j and retrieve the JSON results.

Step 4: Natural Language Answer Generation (Prompt 3)

Task: Pass the original user question AND the JSON results from Step 3 to the LLM. Ask it to summarize the data into a friendly, accurate response.

Phase 4: Frontend & Visualization Strategy
To hit the Optional Extensions (Bonus), I highly recommend aiming for "Highlighting nodes referenced in responses." It creates a massive "Wow" factor.

Layout: A split-screen design. Left side: The interactive graph. Right side: The chat interface.

Interactivity:

When the graph loads, fetch a default limit of nodes (e.g., MATCH (n) RETURN n LIMIT 100) to avoid crashing the browser.

Bonus Implementation: When the backend executes a query (Step 3 above), have it return the specific Node IDs involved. Send these IDs to the frontend alongside the chat message. Use a React state (highlightedNodes) to change the color and size of those specific nodes in react-force-graph.

Phase 5: Execution & AI Coding Session Strategy
Because they require your AI logs (Cursor/Claude Code), your process must show deliberate, iterative engineering. Do not just paste the whole prompt and say "build this."

Use this workflow to generate excellent logs:

Session 1: Data Ingestion Pipeline.

Prompt Cursor: "I have a dataset of CSVs (Orders, Deliveries, Invoices). Write a Python script using pandas to clean this data and output a structured JSON format ready for a graph database."

Session 2: Neo4j Population.

Prompt Cursor: "Using the official neo4j Python driver, write a script that takes the cleaned JSON data and creates nodes and relationships based on this schema: [paste schema]."

Session 3: FastAPI Backend & LangChain.

Prompt Cursor: "Create a FastAPI backend with an endpoint /chat. Implement a LangChain pipeline that takes a user query, applies a system prompt to act as a guardrail, uses GraphCypherQAChain to query Neo4j, and returns the answer."

Session 4: Frontend Integration.

Prompt Cursor: "Scaffold a Next.js application with Tailwind CSS. Create a split-pane layout with a chat window on the right and a react-force-graph on the left fetching data from my FastAPI backend."

What to put in your README (Crucial for Evaluation)
Database Choice: State clearly why you chose Neo4j (native traversal, Cypher's compatibility with LLMs) over a relational DB with recursive CTEs.

Prompting Strategy: Explain your multi-step pipeline (Guardrail -> Query Gen -> Answer Gen) to ensure the LLM doesn't hallucinate data.

Guardrails: Highlight that your first LLM call acts as a classifier to strictly enforce domain boundaries.

Would you like to dive deeper into how to structure the specific Text-to-Cypher LLM prompt to ensure it writes correct queries, or would you prefer to map out the exact Data Preprocessing script to clean the CSVs first?