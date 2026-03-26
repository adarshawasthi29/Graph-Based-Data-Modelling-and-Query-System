from contextlib import asynccontextmanager
import json
import re
from typing import Any, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from langchain_community.chains.graph_qa.cypher import GraphCypherQAChain
from langchain_community.graphs import Neo4jGraph
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel, Field

from app.config import get_settings
from app.guardrail import GUARDRAIL_BLOCKED_MESSAGE, is_query_allowed
from app.llm import build_chat_llm
from app.routers import graph as graph_router


# ── models ────────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    history: Optional[List[dict[str, str]]] = []


class ChatResponse(BaseModel):
    answer: str
    blocked: bool = False
    intermediate_steps: Optional[List[dict[str, Any]]] = None
    history: Optional[List[dict[str, str]]] = []


# ── lifespan ──────────────────────────────────────────────────────────────────

# ── lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    graph = Neo4jGraph(
        url=settings.neo4j_uri,
        username=settings.neo4j_username,
        password=settings.neo4j_password,
        database=settings.neo4j_database,
        refresh_schema=True,
    )
    chat_llm = build_chat_llm(settings)

    # 1. CREATE THE OVERRIDE PROMPT
    # This forces LangChain's built-in tool to stop making the syntax error
    cypher_template = """Task:Generate Cypher statement to query a graph database.
Instructions:
Use only the provided relationship types and properties in the schema.

CRITICAL NEO4J SYNTAX RULES:
1. NEVER introduce new variables inside a WHERE NOT pattern expression. 
   ❌ BAD: MATCH (s:StorageLocation) WHERE NOT (s)<-[:PICKED_FROM_STORAGE]-(di:DeliveryItem)
2. To find nodes that DO NOT have a relationship, you MUST use the EXISTS {{ MATCH ... }} subquery syntax.
   ✅ GOOD: MATCH (s:StorageLocation) WHERE NOT EXISTS {{ MATCH (s)<-[:PICKED_FROM_STORAGE]-(:DeliveryItem) }}
   ✅ GOOD: MATCH (c:Customer)-[:MADE_PAYMENT]->(p:Payment) WHERE NOT EXISTS {{ MATCH (p)<-[:CLEARED_BY]-(:JournalEntry) }}

Schema:
{schema}

Note: Do not include any explanations or apologies in your responses.
Return ONLY the raw Cypher query. No markdown formatting. No backticks.

The question is:
{question}"""

    custom_cypher_prompt = PromptTemplate(
        input_variables=["schema", "question"], 
        template=cypher_template
    )

    # 2. PASS THE PROMPT TO THE CHAIN
    cypher_chain = GraphCypherQAChain.from_llm(
        llm=chat_llm,
        graph=graph,
        cypher_prompt=custom_cypher_prompt, # <--- The magic fix is here
        verbose=True, # Set to True so you can see the generated query in your terminal
        return_intermediate_steps=True,
        top_k=settings.graph_cypher_top_k,
        allow_dangerous_requests=True,
        validate_cypher=True,
    )
    
    app.state.neo4j_graph = graph
    app.state.cypher_chain = cypher_chain
    app.state.chat_llm = chat_llm
    yield
    app.state.neo4j_graph = None
    app.state.cypher_chain = None
    app.state.chat_llm = None


# ── app ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="SAP Order-to-Cash Graph Chat API",
    description="Worker + Judge LLM orchestration over Neo4j",
    lifespan=lifespan,
)

_settings = get_settings()
_cors = [o.strip() for o in _settings.cors_origins.split(",") if o.strip()]
if not _cors:
    _cors = ["http://localhost:5173", "http://127.0.0.1:5173"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(graph_router.router)


@app.get("/health")
def health():
    return {"status": "ok"}


# ── prompts ───────────────────────────────────────────────────────────────────

_WORKER_CYPHER_PROMPT = """\
You are a Neo4j Cypher expert for a supply-chain / SAP order-to-cash dataset.
Write a valid Cypher query for the question below.

STRICT CYPHER RULES (CRITICAL):
1. NEVER introduce new variables inside a WHERE NOT pattern expression. This causes a syntax error.
   ❌ BAD: MATCH (s:StorageLocation) WHERE NOT (s)<-[:PICKED_FROM_STORAGE]-(di:DeliveryItem)
2. To find nodes that DO NOT have a relationship, you MUST use the EXISTS {{}} subquery syntax.
   ✅ GOOD: MATCH (s:StorageLocation) WHERE NOT EXISTS {{ MATCH (s)<-[:PICKED_FROM_STORAGE]-(:DeliveryItem) }}
   ✅ GOOD: MATCH (c:Customer)-[:MADE_PAYMENT]->(p:Payment) WHERE NOT EXISTS {{ MATCH (p)<-[:CLEARED_BY]-(:JournalEntry) }}
3. Return ONLY the raw Cypher query. No explanation. No markdown formatting. No backticks.

Schema:
{schema}

Question: {question}

{feedback_block}"""

_WORKER_ANSWER_PROMPT = """\
Question: {question}

Neo4j results:
{results}

Answer the question concisely using only the data above.
If results are empty, say no matching records were found."""


_JUDGE_PROMPT = """\
You are a strict QA judge for a Neo4j graph query system.
Evaluate whether the answer below should be shown to the user or sent back for revision.

Respond with ONLY valid JSON — no explanation, no markdown:
{{"action": "PASS", "feedback": ""}}
or
{{"action": "RETRY", "feedback": "one sentence: exactly what the worker must fix"}}

Return RETRY if:
- The answer does not address the question asked
- The answer misrepresents the results (claims data exists when results are empty, or vice versa)
- The Cypher queried the wrong nodes or relationships for the question

Return PASS if:
- The answer accurately and directly addresses the question
- Empty results are reported honestly as no matching records

Question: {question}

Cypher used:
{cypher}

Results:
{results}

Answer:
{answer}"""


_MAX_ROUNDS = 3


# ── utilities ─────────────────────────────────────────────────────────────────

def _strip_fences(text: str) -> str:
    """Remove markdown code fences from LLM output."""
    text = text.strip()
    text = re.sub(r"^```[a-zA-Z]*\n?", "", text)   # opening fence
    text = re.sub(r"\n?```$", "", text)              # closing fence
    return text.strip()


# ── worker LLM ────────────────────────────────────────────────────────────────

def _worker_cypher(
    question: str,
    schema: str,
    feedback: str,
    chat_llm: BaseChatModel,
) -> str:
    """
    Generate Cypher from question + schema.
    If feedback is provided (from a previous failed attempt), it is included
    so the worker knows exactly what to fix.
    """
    feedback_block = (
        f"IMPORTANT — your previous attempt failed with this error, fix it:\n{feedback}\n\n"
        if feedback else ""
    )
    prompt = _WORKER_CYPHER_PROMPT.format(
        schema=schema,
        question=question,
        feedback_block=feedback_block,
    )
    raw = chat_llm.invoke([HumanMessage(content=prompt)]).content
    return _strip_fences(raw)


def _worker_answer(
    question: str,
    results: Any,
    chat_llm: BaseChatModel,
) -> str:
    """Translate raw Neo4j results into a natural-language answer."""
    prompt = _WORKER_ANSWER_PROMPT.format(question=question, results=results)
    return chat_llm.invoke([HumanMessage(content=prompt)]).content.strip()


# ── judge LLM ─────────────────────────────────────────────────────────────────

def _judge(
    question: str,
    cypher: str,
    results: Any,
    answer: str,
    chat_llm: BaseChatModel,
) -> tuple[bool, str]:
    """
    Stateless semantic judge — called ONLY when execution succeeded.
    """
    prompt = _JUDGE_PROMPT.format(
        question=question,
        cypher=cypher,
        results=results,
        answer=answer,
    )
    raw = chat_llm.invoke([HumanMessage(content=prompt)]).content.strip()

    try:
        verdict = json.loads(_strip_fences(raw))
        passed = str(verdict.get("action", "")).upper() == "PASS"
        feedback = str(verdict.get("feedback", ""))
    except Exception:
        print(f"[judge] Failed to parse verdict JSON: {raw!r}")
        passed = True
        feedback = ""

    return passed, feedback


# ── orchestrator ──────────────────────────────────────────────────────────────

def _orchestrate(
    question: str,
    history: List[dict[str, str]],
    graph: Neo4jGraph,
    chat_llm: BaseChatModel,
) -> dict[str, Any]:
    """
    Worker / Judge orchestration loop.
    """
    schema = graph.schema
    feedback: str = ""
    cypher: str = ""
    results: Any = None
    answer: str = ""

    for round_ in range(_MAX_ROUNDS):

        # ── Step 1: Worker generates Cypher ──────────────────────────────────
        cypher = _worker_cypher(question, schema, feedback, chat_llm)

        # ── Step 2: Execute against Neo4j ────────────────────────────────────
        try:
            results = graph.query(cypher)
            feedback = "" # Clear execution errors on success
        except Exception as exc:
            feedback = f"Neo4j Execution Error: {exc!s}"
            continue    # go straight to next round, skip judge

        # ── Step 3: Worker generates natural-language answer ─────────────────
        answer = _worker_answer(question, results, chat_llm)

        # ── Step 4: Judge evaluates semantic quality ──────────────────────────
        passed, judge_feedback = _judge(question, cypher, results, answer, chat_llm)

        if passed:
            break
        else:
            feedback = f"Judge rejected answer. Reason: {judge_feedback}"
            
    else:
        # Triggers ONLY if the loop finishes without hitting 'break'
        raise RuntimeError(f"Worker failed to generate a valid answer after {_MAX_ROUNDS} attempts. Last feedback: {feedback}")

    # Update history with just the final Q+A
    new_history = history + [
        {"role": "human", "content": question},
        {"role": "ai",    "content": answer},
    ]

    return {
        "cypher": cypher,
        "results": results,
        "answer": answer,
        "history": new_history,
    }


# ── endpoint ──────────────────────────────────────────────────────────────────

@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest, request: Request):
    chat_llm: BaseChatModel | None = getattr(request.app.state, "chat_llm", None)
    if chat_llm is None:
        raise HTTPException(status_code=503, detail="Graph chain not initialized")

    if not is_query_allowed(chat_llm, req.message):
        return ChatResponse(answer=GUARDRAIL_BLOCKED_MESSAGE, blocked=True)

    neo4j_graph: Neo4jGraph | None = getattr(request.app.state, "neo4j_graph", None)
    if neo4j_graph is None:
        raise HTTPException(status_code=503, detail="Neo4j graph not initialized")

    try:
        result = _orchestrate(
            question=req.message,
            history=req.history or [],
            graph=neo4j_graph,
            chat_llm=chat_llm,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Query failed: {exc!s}",
        ) from exc

    return ChatResponse(
        answer=result["answer"],
        blocked=False,
        intermediate_steps=[{"query": result["cypher"], "context": result["results"]}],
        history=result["history"],
    )
