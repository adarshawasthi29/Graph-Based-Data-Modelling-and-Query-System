"""Build force-graph compatible payloads from Neo4j (not Neo4j Browser embed — same data, your UI)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple


# Distinct colors for main business labels (fallback gray)
_LABEL_COLORS: Dict[str, str] = {
    "Customer": "#4e79a7",
    "SalesOrder": "#f28e2b",
    "SalesOrderItem": "#ffbe7d",
    "Delivery": "#59a14f",
    "DeliveryItem": "#8cd17d",
    "Invoice": "#e15759",
    "BillingDocumentItem": "#ff9d9a",
    "Payment": "#af7aa1",
    "JournalEntry": "#d37295",
    "Product": "#76b7b2",
    "Plant": "#9c755f",
    "StorageLocation": "#bab0ab",
    "Address": "#b6992d",
    "CompanyCode": "#499894",
    "SalesArea": "#79706e",
    "ScheduleLine": "#d7b5a6",
    "ProductDescription": "#86bcb6",
}


def _primary_label(labels: List[str]) -> str:
    return labels[0] if labels else "Node"


def _color(labels: List[str]) -> str:
    pl = _primary_label(labels)
    return _LABEL_COLORS.get(pl, "#888888")


def _node_view(
    element_id: str,
    labels: List[str],
    props: Dict[str, Any],
) -> Dict[str, Any]:
    biz_id = props.get("id", "")
    pl = _primary_label(labels)
    title = f"{pl}: {biz_id}" if biz_id else f"{pl}: {element_id[:12]}…"
    return {
        "id": element_id,
        "labels": labels,
        "bizId": biz_id,
        "title": title,
        "name": title,
        "color": _color(labels),
        "val": 4 + min(len(props), 20) * 0.15,
        "props": {k: _json_safe(v) for k, v in list(props.items())[:40]},
    }


def _json_safe(v: Any) -> Any:
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (list, dict)):
        try:
            import json

            return json.loads(json.dumps(v, default=str))
        except Exception:
            return str(v)
    return str(v)


def sample_graph(graph: Any, relationship_limit: int) -> Dict[str, Any]:
    """Run a bounded relationship scan; dedupe nodes; return {nodes, links}."""
    q = """
    MATCH (n)-[r]->(m)
    WITH n, r, m
    LIMIT $limit
    RETURN
      elementId(n) AS srcElem,
      labels(n) AS srcLabels,
      properties(n) AS srcProps,
      elementId(m) AS tgtElem,
      labels(m) AS tgtLabels,
      properties(m) AS tgtProps,
      elementId(r) AS relElem,
      type(r) AS relType,
      properties(r) AS relProps
    """
    rows = graph.query(q, {"limit": relationship_limit})
    return _rows_to_payload(rows)


def expand_node(graph: Any, node_element_id: str, relationship_limit: int) -> Dict[str, Any]:
    q = """
    MATCH (n)-[r]-(m)
    WHERE elementId(n) = $nid
    WITH n, r, m
    LIMIT $limit
    RETURN
      elementId(n) AS srcElem,
      labels(n) AS srcLabels,
      properties(n) AS srcProps,
      elementId(m) AS tgtElem,
      labels(m) AS tgtLabels,
      properties(m) AS tgtProps,
      elementId(r) AS relElem,
      type(r) AS relType,
      properties(r) AS relProps
    """
    rows = graph.query(q, {"nid": node_element_id, "limit": relationship_limit})
    return _rows_to_payload(rows)


def graph_stats(graph: Any) -> List[Dict[str, Any]]:
    q = """
    MATCH (n)
    UNWIND labels(n) AS label
    RETURN label, count(*) AS cnt
    ORDER BY cnt DESC
    """
    return graph.query(q)


def _backtick(label: str) -> str:
    # Safe because label is sourced from db.labels() or inferred labels; still escape to avoid syntax errors.
    return f"`{label.replace('`', '')}`"


def _hash_color(seed: str) -> str:
    # Deterministic fallback color.
    h = 0
    for ch in seed:
        h = (h * 31 + ord(ch)) % 360
    return f"hsl({h}, 55%, 55%)"


def _table_color(table_label: str) -> str:
    return _LABEL_COLORS.get(table_label, _hash_color(table_label))


def schema_graph(
    graph: Any,
    *,
    max_fields_per_label: int = 40,
) -> Dict[str, Any]:
    """
    Build a *schema* graph for visualization:
    - Table nodes: primary labels (e.g., SalesOrder, Invoice)
    - Field nodes: property keys under each label
    - Table edges: relationship types between labels
    - Table->Field edges: HAS_FIELD

    Important: this is derived from graph structure (labels/keys/rel types) and does not enumerate
    individual business entities/records.
    """

    # 1) Labels => Table nodes
    labels_rows = graph.query("CALL db.labels() YIELD label RETURN label ORDER BY label")
    table_labels = [r.get("label") for r in labels_rows if r.get("label")]

    table_nodes: Dict[str, Dict[str, Any]] = {}
    for tl in table_labels:
        tid = f"table|{tl}"
        table_nodes[tid] = {
            "id": tid,
            "kind": "Table",
            "labels": [tl],
            "bizId": "",
            "title": tl,
            "name": tl,
            "color": _table_color(tl),
            "val": 18,
            "props": {"label": tl},
        }

    # 2) Fields per label => Field nodes + HAS_FIELD edges
    field_nodes: Dict[str, Dict[str, Any]] = {}
    links: List[Dict[str, Any]] = []

    # Prefer join keys and IDs so dependency edges are always present.
    priority_keys: List[str] = [
        "id",
        # Common join keys in your ingestion
        "soldToParty",
        "customer",
        "customer_id",
        "material",
        "plant",
        "storageLocation",
        "deliveryDocument",
        "deliveryDocumentItem",
        "referenceSdDocument",
        "referenceSdDocumentItem",
        "accountingDocument",
        "clearingAccountingDocument",
        "companyCode",
        "billingDocument",
        "billingDocumentItem",
    ]

    # For each label, collect property keys. This does not fetch record entries.
    for tl in table_labels:
        q = f"""
        MATCH (n:{_backtick(tl)})
        UNWIND keys(n) AS k
        RETURN DISTINCT k AS propKey
        ORDER BY propKey
        """
        try:
            rows = graph.query(q)
        except Exception:
            # Fallback: no LIMIT on keys if $lim isn't supported.
            rows = graph.query(q.replace("LIMIT $lim", ""))
        prop_keys = [r.get("propKey") for r in rows if r.get("propKey")]
        # If there are too many keys, keep priorities first, then fill from remaining keys.
        if len(prop_keys) > max_fields_per_label:
            remaining = [k for k in prop_keys if k not in priority_keys]
            prioritized = [k for k in priority_keys if k in prop_keys]
            prop_keys = (prioritized + remaining)[:max_fields_per_label]

        table_id = f"table|{tl}"
        for pk in prop_keys:
            fid = f"field|{tl}|{pk}"
            if fid not in field_nodes:
                field_nodes[fid] = {
                    "id": fid,
                    "kind": "Field",
                    "labels": [tl, "Field"],
                    "bizId": "",
                    "title": f"{tl}.{pk}",
                    "name": f"{tl}.{pk}",
                    "color": "#cbd5e1",
                    "val": 6,
                    "props": {"table": tl, "property": pk},
                }
            links.append(
                {
                    "source": table_id,
                    "target": fid,
                    "label": "HAS_FIELD",
                    "type": "HAS_FIELD",
                    "color": "#94a3b8",
                    "props": {"property": pk},
                }
            )

    # 3) Relationship types between labels => Table->Table edges
    # Use labels(a)[0] and labels(b)[0] to keep schema readable (most nodes have single label).
    rel_q = """
    MATCH (a)-[r]->(b)
    RETURN labels(a)[0] AS fromLabel, type(r) AS relType, labels(b)[0] AS toLabel
    """
    rel_rows = graph.query(rel_q)

    rel_seen: Set[Tuple[str, str, str]] = set()
    for rr in rel_rows:
        from_l = rr.get("fromLabel")
        to_l = rr.get("toLabel")
        rel_type = rr.get("relType")
        if not from_l or not to_l or not rel_type:
            continue
        sid = f"table|{from_l}"
        tid = f"table|{to_l}"
        if sid not in table_nodes or tid not in table_nodes:
            continue
        key = (sid, tid, rel_type)
        if key in rel_seen:
            continue
        rel_seen.add(key)
        links.append(
            {
                "source": sid,
                "target": tid,
                "label": rel_type,
                "type": rel_type,
                "color": "#64748b",
                "props": {},
            }
        )

    # 4b) Deterministic field dependency edges for your domain flow.
    # These are derived from the ingestion join keys used to create relationships.
    def _field(t: str, prop: str) -> Optional[str]:
        fid = f"field|{t}|{prop}"
        return fid if fid in field_nodes else None

    dep_color = "#0ea5e9"
    dep_links: List[Dict[str, Any]] = []

    # DeliveryItem.referenceSdDocument -> SalesOrder.id
    # DeliveryItem.referenceSdDocumentItem -> SalesOrderItem.id
    if _field("DeliveryItem", "referenceSdDocument") and _field("SalesOrder", "id"):
        dep_links.append(
            {
                "source": _field("DeliveryItem", "referenceSdDocument"),
                "target": _field("SalesOrder", "id"),
                "label": "DEPENDS_ON",
                "type": "DEPENDS_ON",
                "color": dep_color,
                "props": {"joinKey": "referenceSdDocument"},
            }
        )
    if _field("DeliveryItem", "referenceSdDocumentItem") and _field(
        "SalesOrderItem", "id"
    ):
        dep_links.append(
            {
                "source": _field("DeliveryItem", "referenceSdDocumentItem"),
                "target": _field("SalesOrderItem", "id"),
                "label": "DEPENDS_ON",
                "type": "DEPENDS_ON",
                "color": dep_color,
                "props": {"joinKey": "referenceSdDocumentItem"},
            }
        )

    # BillingDocumentItem.referenceSdDocument/referenceSdDocumentItem -> DeliveryItem.deliveryDocument/deliveryDocumentItem
    if _field("BillingDocumentItem", "referenceSdDocument") and _field(
        "DeliveryItem", "deliveryDocument"
    ):
        dep_links.append(
            {
                "source": _field("BillingDocumentItem", "referenceSdDocument"),
                "target": _field("DeliveryItem", "deliveryDocument"),
                "label": "DEPENDS_ON",
                "type": "DEPENDS_ON",
                "color": dep_color,
                "props": {"joinKey": "referenceSdDocument"},
            }
        )
    if _field("BillingDocumentItem", "referenceSdDocumentItem") and _field(
        "DeliveryItem", "deliveryDocumentItem"
    ):
        dep_links.append(
            {
                "source": _field("BillingDocumentItem", "referenceSdDocumentItem"),
                "target": _field("DeliveryItem", "deliveryDocumentItem"),
                "label": "DEPENDS_ON",
                "type": "DEPENDS_ON",
                "color": dep_color,
                "props": {"joinKey": "referenceSdDocumentItem"},
            }
        )

    # BillingDocumentItem.material -> Product.id
    if _field("BillingDocumentItem", "material") and _field("Product", "id"):
        dep_links.append(
            {
                "source": _field("BillingDocumentItem", "material"),
                "target": _field("Product", "id"),
                "label": "ITEM_MATERIAL",
                "type": "ITEM_MATERIAL",
                "color": dep_color,
                "props": {},
            }
        )

    # SalesOrderItem.material -> Product.id
    if _field("SalesOrderItem", "material") and _field("Product", "id"):
        dep_links.append(
            {
                "source": _field("SalesOrderItem", "material"),
                "target": _field("Product", "id"),
                "label": "ITEM_MATERIAL",
                "type": "ITEM_MATERIAL",
                "color": dep_color,
                "props": {},
            }
        )

    # DeliveryItem.plant -> Plant.id
    if _field("DeliveryItem", "plant") and _field("Plant", "id"):
        dep_links.append(
            {
                "source": _field("DeliveryItem", "plant"),
                "target": _field("Plant", "id"),
                "label": "FOR_PLANT",
                "type": "FOR_PLANT",
                "color": dep_color,
                "props": {},
            }
        )

    # DeliveryItem.storageLocation -> StorageLocation.storageLocation (preferred) else StorageLocation.id
    storage_sl = _field("DeliveryItem", "storageLocation")
    sl_target = _field("StorageLocation", "storageLocation") or _field(
        "StorageLocation", "id"
    )
    if storage_sl and sl_target:
        dep_links.append(
            {
                "source": storage_sl,
                "target": sl_target,
                "label": "PICKED_FROM_STORAGE",
                "type": "PICKED_FROM_STORAGE",
                "color": dep_color,
                "props": {},
            }
        )

    links.extend(dep_links)

    # 4) Return merged payload
    nodes = list(table_nodes.values()) + list(field_nodes.values())
    return {"nodes": nodes, "links": links}


def _rows_to_payload(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    nodes: Dict[str, Dict[str, Any]] = {}
    links: List[Dict[str, Any]] = []
    seen_rel_elems: set = set()

    for row in rows:
        sid = row["srcElem"]
        tid = row["tgtElem"]
        if sid not in nodes:
            nodes[sid] = _node_view(sid, row["srcLabels"] or [], row["srcProps"] or {})
        if tid not in nodes:
            nodes[tid] = _node_view(tid, row["tgtLabels"] or [], row["tgtProps"] or {})

        rtype = row.get("relType") or "RELATED"
        rel_elem = row.get("relElem")
        if rel_elem and rel_elem in seen_rel_elems:
            continue
        if rel_elem:
            seen_rel_elems.add(rel_elem)
        rprops = row.get("relProps") or {}
        links.append(
            {
                "source": sid,
                "target": tid,
                "label": rtype,
                "type": rtype,
                "relId": rel_elem,
                "color": "#a5abb6",
                "props": {k: _json_safe(v) for k, v in list(rprops.items())[:10]},
            }
        )

    return {"nodes": list(nodes.values()), "links": links}
