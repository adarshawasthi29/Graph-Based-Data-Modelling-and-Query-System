from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.graph_service import expand_node, graph_stats, sample_graph
from app.graph_service import schema_graph

router = APIRouter(prefix="/api/graph", tags=["graph"])


def get_neo4j_graph(request: Request) -> Any:
    g = getattr(request.app.state, "neo4j_graph", None)
    if g is None:
        raise HTTPException(status_code=503, detail="Neo4j graph not initialized")
    return g


@router.get("/sample")
def graph_sample(
    graph: Any = Depends(get_neo4j_graph),
    limit: int = Query(600, ge=50, le=8000, description="Max relationships to fetch"),
):
    """Subset of relationships for visualization (full DB at once is too heavy for the browser)."""
    try:
        return sample_graph(graph, limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Neo4j query failed: {exc!s}") from exc


@router.get("/expand/{node_id:path}")
def graph_expand(
    node_id: str,
    graph: Any = Depends(get_neo4j_graph),
    limit: int = Query(400, ge=10, le=3000),
):
    """Neighbors of a node by internal elementId (from graph node `id`)."""
    try:
        return expand_node(graph, node_id, limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Neo4j expand failed: {exc!s}") from exc


@router.get("/stats")
def stats(graph: Any = Depends(get_neo4j_graph)) -> List[dict[str, Any]]:
    try:
        return graph_stats(graph)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Neo4j stats failed: {exc!s}") from exc


@router.get("/schema")
def schema(
    graph: Any = Depends(get_neo4j_graph),
    max_fields_per_label: int = Query(
        40, ge=5, le=200, description="Max field (property keys) per label"
    ),
) -> Dict[str, Any]:
    """
    Schema graph for visualization:
    - Table nodes = Neo4j labels
    - Field nodes = property keys under each label
    - Edges = relationship types between labels + HAS_FIELD edges
    """
    try:
        return schema_graph(graph, max_fields_per_label=max_fields_per_label)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Neo4j schema query failed: {exc!s}") from exc
