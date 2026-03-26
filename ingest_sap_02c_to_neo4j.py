# ingest_sap_o2c_to_neo4j.py
import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from neo4j import GraphDatabase


# ------------------------------
# Utility helpers
# ------------------------------
def is_blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def to_str(v: Any) -> str:
    return "" if v is None else str(v)


def make_id(*parts: Any) -> str:
    return "|".join(to_str(p) for p in parts)


def flatten_dict(d: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    Flatten nested dict/list values to Neo4j-safe scalar/list-of-scalar fields.
    Nested objects become key_subkey.
    """
    out: Dict[str, Any] = {}
    for k, v in d.items():
        key = f"{prefix}{k}" if not prefix else f"{prefix}_{k}"

        if isinstance(v, dict):
            out.update(flatten_dict(v, key))
        elif isinstance(v, list):
            # If list has non-scalar items, serialize to JSON string
            if all(isinstance(x, (str, int, float, bool)) or x is None for x in v):
                out[key] = v
            else:
                out[key] = json.dumps(v, ensure_ascii=True)
        else:
            out[key] = v
    return out


def clean_props(d: Dict[str, Any], drop: Optional[set] = None) -> Dict[str, Any]:
    drop = drop or set()
    flat = flatten_dict(d)
    out: Dict[str, Any] = {}
    for k, v in flat.items():
        if k in drop:
            continue
        if v is None:
            continue
        out[k] = v
    return out


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


# ------------------------------
# Neo4j setup
# ------------------------------
CONSTRAINTS = [
    "CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (n:Customer) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT address_id IF NOT EXISTS FOR (n:Address) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT sales_order_id IF NOT EXISTS FOR (n:SalesOrder) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT sales_order_item_id IF NOT EXISTS FOR (n:SalesOrderItem) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT schedule_line_id IF NOT EXISTS FOR (n:ScheduleLine) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT delivery_id IF NOT EXISTS FOR (n:Delivery) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT delivery_item_id IF NOT EXISTS FOR (n:DeliveryItem) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT invoice_id IF NOT EXISTS FOR (n:Invoice) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT billing_item_id IF NOT EXISTS FOR (n:BillingDocumentItem) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT payment_id IF NOT EXISTS FOR (n:Payment) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT journal_id IF NOT EXISTS FOR (n:JournalEntry) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT product_id IF NOT EXISTS FOR (n:Product) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT plant_id IF NOT EXISTS FOR (n:Plant) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT storage_location_id IF NOT EXISTS FOR (n:StorageLocation) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT company_code_id IF NOT EXISTS FOR (n:CompanyCode) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT sales_area_id IF NOT EXISTS FOR (n:SalesArea) REQUIRE n.id IS UNIQUE",
]


def run_constraints(session):
    for c in CONSTRAINTS:
        session.run(c)


def clear_db(session):
    session.run("MATCH (n) DETACH DELETE n")


def run_batch(session, cypher: str, rows: List[Dict[str, Any]]):
    if rows:
        session.run(cypher, rows=rows)


# ------------------------------
# Ingestion class
# ------------------------------
class SAPO2CIngestor:
    def __init__(self, root: Path, batch_size: int = 500):
        self.root = root / "sap-o2c-data"
        self.batch_size = batch_size

    def part_files(self, entity_folder: str) -> List[Path]:
        p = self.root / entity_folder
        if not p.exists():
            return []
        return sorted(p.glob("part-*.jsonl"))

    def ingest_entity(self, session, entity_folder: str, handler, cypher: str):
        for f in self.part_files(entity_folder):
            batch: List[Dict[str, Any]] = []
            for rec in iter_jsonl(f):
                row = handler(rec)
                if row is None:
                    continue
                batch.append(row)
                if len(batch) >= self.batch_size:
                    run_batch(session, cypher, batch)
                    batch = []
            run_batch(session, cypher, batch)

    # ---------- Core master data ----------
    def ingest_customers(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (c:Customer {id: row.customer_id})
        SET c += row.props
        """
        def handler(r):
            cid = r.get("customer") or r.get("businessPartner")
            if is_blank(cid):
                return None
            return {
                "customer_id": to_str(cid),
                "props": clean_props(r)
            }
        self.ingest_entity(session, "business_partners", handler, cypher)

    def ingest_customer_addresses(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (c:Customer {id: row.customer_id})
        MERGE (a:Address {id: row.address_id})
        SET a += row.props
        MERGE (c)-[r:HAS_ADDRESS]->(a)
        SET r.validityStartDate = row.validity_start, r.validityEndDate = row.validity_end
        """
        def handler(r):
            cid = r.get("businessPartner")
            if is_blank(cid):
                return None
            addr_id = r.get("addressUuid") or r.get("addressId") or make_id(cid, "ADDR")
            return {
                "customer_id": to_str(cid),
                "address_id": to_str(addr_id),
                "validity_start": r.get("validityStartDate"),
                "validity_end": r.get("validityEndDate"),
                "props": clean_props(r)
            }
        self.ingest_entity(session, "business_partner_addresses", handler, cypher)

    def ingest_products(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (p:Product {id: row.product_id})
        SET p += row.props
        """
        def handler(r):
            pid = r.get("product")
            if is_blank(pid):
                return None
            return {"product_id": to_str(pid), "props": clean_props(r)}
        self.ingest_entity(session, "products", handler, cypher)

    def ingest_product_descriptions(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (p:Product {id: row.product_id})
        MERGE (d:ProductDescription {id: row.desc_id})
        SET d += row.props
        MERGE (p)-[:HAS_DESCRIPTION]->(d)
        """
        def handler(r):
            pid = r.get("product")
            if is_blank(pid):
                return None
            lang = r.get("language", "")
            desc_id = make_id(pid, lang)
            return {
                "product_id": to_str(pid),
                "desc_id": desc_id,
                "props": clean_props(r)
            }
        self.ingest_entity(session, "product_descriptions", handler, cypher)

    def ingest_plants(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (pl:Plant {id: row.plant_id})
        SET pl += row.props
        """
        def handler(r):
            pid = r.get("plant")
            if is_blank(pid):
                return None
            return {"plant_id": to_str(pid), "props": clean_props(r)}
        self.ingest_entity(session, "plants", handler, cypher)

    def ingest_product_plants(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (p:Product {id: row.product_id})
        MERGE (pl:Plant {id: row.plant_id})
        MERGE (p)-[r:AVAILABLE_AT]->(pl)
        SET r += row.rel_props
        """
        def handler(r):
            pid, pl = r.get("product"), r.get("plant")
            if is_blank(pid) or is_blank(pl):
                return None
            return {
                "product_id": to_str(pid),
                "plant_id": to_str(pl),
                "rel_props": clean_props(r, drop={"product", "plant"})
            }
        self.ingest_entity(session, "product_plants", handler, cypher)

    def ingest_product_storage_locations(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (p:Product {id: row.product_id})
        MERGE (pl:Plant {id: row.plant_id})
        MERGE (sl:StorageLocation {id: row.storage_id})
        SET sl.plant = row.plant_id, sl.storageLocation = row.storage_location
        MERGE (pl)-[:HAS_STORAGE_LOCATION]->(sl)
        MERGE (p)-[r:STORED_IN]->(sl)
        SET r += row.rel_props
        """
        def handler(r):
            pid, pl, sl = r.get("product"), r.get("plant"), r.get("storageLocation")
            if is_blank(pid) or is_blank(pl) or is_blank(sl):
                return None
            return {
                "product_id": to_str(pid),
                "plant_id": to_str(pl),
                "storage_id": make_id(pl, sl),
                "storage_location": to_str(sl),
                "rel_props": clean_props(r, drop={"product", "plant", "storageLocation"})
            }
        self.ingest_entity(session, "product_storage_locations", handler, cypher)

    # ---------- Sales and assignments ----------
    def ingest_customer_company_assignments(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (c:Customer {id: row.customer_id})
        MERGE (cc:CompanyCode {id: row.company_code})
        MERGE (c)-[r:ASSIGNED_TO_COMPANY]->(cc)
        SET r += row.rel_props
        """
        def handler(r):
            c, cc = r.get("customer"), r.get("companyCode")
            if is_blank(c) or is_blank(cc):
                return None
            return {
                "customer_id": to_str(c),
                "company_code": to_str(cc),
                "rel_props": clean_props(r, drop={"customer", "companyCode"})
            }
        self.ingest_entity(session, "customer_company_assignments", handler, cypher)

    def ingest_customer_sales_area_assignments(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (c:Customer {id: row.customer_id})
        MERGE (sa:SalesArea {id: row.sales_area_id})
        SET sa.salesOrganization = row.sales_org,
            sa.distributionChannel = row.dist_channel,
            sa.division = row.division
        MERGE (c)-[r:ASSIGNED_TO_SALES_AREA]->(sa)
        SET r += row.rel_props
        """
        def handler(r):
            c = r.get("customer")
            so = r.get("salesOrganization")
            dc = r.get("distributionChannel")
            dv = r.get("division")
            if is_blank(c) or is_blank(so) or is_blank(dc) or is_blank(dv):
                return None
            return {
                "customer_id": to_str(c),
                "sales_area_id": make_id(so, dc, dv),
                "sales_org": to_str(so),
                "dist_channel": to_str(dc),
                "division": to_str(dv),
                "rel_props": clean_props(
                    r, drop={"customer", "salesOrganization", "distributionChannel", "division"}
                )
            }
        self.ingest_entity(session, "customer_sales_area_assignments", handler, cypher)

    def ingest_sales_order_headers(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (so:SalesOrder {id: row.sales_order_id})
        SET so += row.props
        MERGE (c:Customer {id: row.customer_id})
        MERGE (c)-[:PLACED]->(so)
        """
        def handler(r):
            so = r.get("salesOrder")
            c = r.get("soldToParty")
            if is_blank(so):
                return None
            return {
                "sales_order_id": to_str(so),
                "customer_id": to_str(c),
                "props": clean_props(r)
            }
        self.ingest_entity(session, "sales_order_headers", handler, cypher)

    def ingest_sales_order_items(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (so:SalesOrder {id: row.sales_order_id})
        MERGE (soi:SalesOrderItem {id: row.soi_id})
        SET soi += row.props
        MERGE (so)-[:HAS_ITEM]->(soi)

        FOREACH (_ IN CASE WHEN row.product_id = '' THEN [] ELSE [1] END |
          MERGE (p:Product {id: row.product_id})
          MERGE (soi)-[:ITEM_MATERIAL]->(p)
        )

        FOREACH (_ IN CASE WHEN row.production_plant = '' THEN [] ELSE [1] END |
          MERGE (pl:Plant {id: row.production_plant})
          MERGE (soi)-[:PRODUCED_AT]->(pl)
        )

        FOREACH (_ IN CASE WHEN row.storage_location = '' THEN [] ELSE [1] END |
          MERGE (sl:StorageLocation {id: row.storage_id})
          SET sl.plant = row.production_plant, sl.storageLocation = row.storage_location
          MERGE (soi)-[:REQUESTED_FROM_STORAGE]->(sl)
        )
        """
        def handler(r):
            so = r.get("salesOrder")
            item = r.get("salesOrderItem")
            if is_blank(so) or is_blank(item):
                return None
            plant = to_str(r.get("productionPlant"))
            sl = to_str(r.get("storageLocation"))
            return {
                "sales_order_id": to_str(so),
                "soi_id": make_id(so, item),
                "product_id": to_str(r.get("material")),
                "production_plant": plant,
                "storage_location": sl,
                "storage_id": make_id(plant, sl),
                "props": clean_props(r)
            }
        self.ingest_entity(session, "sales_order_items", handler, cypher)

    def ingest_sales_order_schedule_lines(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (soi:SalesOrderItem {id: row.soi_id})
        MERGE (sl:ScheduleLine {id: row.schedule_id})
        SET sl += row.props
        MERGE (soi)-[:HAS_SCHEDULE_LINE]->(sl)
        """
        def handler(r):
            so = r.get("salesOrder")
            item = r.get("salesOrderItem")
            sch = r.get("scheduleLine")
            if is_blank(so) or is_blank(item) or is_blank(sch):
                return None
            return {
                "soi_id": make_id(so, item),
                "schedule_id": make_id(so, item, sch),
                "props": clean_props(r)
            }
        self.ingest_entity(session, "sales_order_schedule_lines", handler, cypher)

    # ---------- Delivery ----------
    def ingest_delivery_headers(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (d:Delivery {id: row.delivery_id})
        SET d += row.props
        """
        def handler(r):
            d = r.get("deliveryDocument")
            if is_blank(d):
                return None
            return {"delivery_id": to_str(d), "props": clean_props(r)}
        self.ingest_entity(session, "outbound_delivery_headers", handler, cypher)

    def ingest_delivery_items(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (d:Delivery {id: row.delivery_id})
        MERGE (di:DeliveryItem {id: row.di_id})
        SET di += row.props
        MERGE (d)-[:HAS_ITEM]->(di)

        FOREACH (_ IN CASE WHEN row.ref_so = '' OR row.ref_so_item = '' THEN [] ELSE [1] END |
          MERGE (soi:SalesOrderItem {id: row.ref_soi_id})
          MERGE (di)-[:DELIVERED_FROM]->(soi)
          MERGE (so:SalesOrder {id: row.ref_so})
          MERGE (so)-[:FULFILLED_BY]->(d)
        )

        FOREACH (_ IN CASE WHEN row.plant = '' THEN [] ELSE [1] END |
          MERGE (pl:Plant {id: row.plant})
          MERGE (di)-[:DELIVERED_FROM_PLANT]->(pl)
        )

        FOREACH (_ IN CASE WHEN row.storage_location = '' THEN [] ELSE [1] END |
          MERGE (sl:StorageLocation {id: row.storage_id})
          SET sl.plant = row.plant, sl.storageLocation = row.storage_location
          MERGE (di)-[:PICKED_FROM_STORAGE]->(sl)
        )
        """
        def handler(r):
            d = r.get("deliveryDocument")
            item = r.get("deliveryDocumentItem")
            if is_blank(d) or is_blank(item):
                return None
            ref_so = to_str(r.get("referenceSdDocument"))
            ref_so_item = to_str(r.get("referenceSdDocumentItem"))
            plant = to_str(r.get("plant"))
            sl = to_str(r.get("storageLocation"))
            return {
                "delivery_id": to_str(d),
                "di_id": make_id(d, item),
                "ref_so": ref_so,
                "ref_so_item": ref_so_item,
                "ref_soi_id": make_id(ref_so, ref_so_item),
                "plant": plant,
                "storage_location": sl,
                "storage_id": make_id(plant, sl),
                "props": clean_props(r)
            }
        self.ingest_entity(session, "outbound_delivery_items", handler, cypher)

    # ---------- Billing ----------
    def ingest_invoice_headers(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (inv:Invoice {id: row.invoice_id})
        SET inv += row.props

        FOREACH (_ IN CASE WHEN row.customer_id = '' THEN [] ELSE [1] END |
          MERGE (c:Customer {id: row.customer_id})
          MERGE (c)-[:INVOICED]->(inv)
        )
        """
        def handler(r):
            bid = r.get("billingDocument")
            if is_blank(bid):
                return None
            return {
                "invoice_id": to_str(bid),
                "customer_id": to_str(r.get("soldToParty")),
                "props": clean_props(r)
            }
        self.ingest_entity(session, "billing_document_headers", handler, cypher)

    def ingest_invoice_items(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (inv:Invoice {id: row.invoice_id})
        MERGE (bdi:BillingDocumentItem {id: row.bdi_id})
        SET bdi += row.props
        MERGE (inv)-[:HAS_ITEM]->(bdi)

        FOREACH (_ IN CASE WHEN row.product_id = '' THEN [] ELSE [1] END |
          MERGE (p:Product {id: row.product_id})
          MERGE (bdi)-[:ITEM_MATERIAL]->(p)
        )

        // referenceSdDocument in billing items points to DeliveryDocument
        FOREACH (_ IN CASE WHEN row.ref_delivery = '' OR row.ref_delivery_item = '' THEN [] ELSE [1] END |
          MERGE (di:DeliveryItem {id: row.ref_di_id})
          MERGE (bdi)-[:BILLED_FROM]->(di)
          MERGE (d:Delivery {id: row.ref_delivery})
          MERGE (d)-[:BILLED]->(inv)
        )
        """
        def handler(r):
            inv = r.get("billingDocument")
            item = r.get("billingDocumentItem")
            if is_blank(inv) or is_blank(item):
                return None
            ref_d = to_str(r.get("referenceSdDocument"))
            ref_di = to_str(r.get("referenceSdDocumentItem"))
            return {
                "invoice_id": to_str(inv),
                "bdi_id": make_id(inv, item),
                "product_id": to_str(r.get("material")),
                "ref_delivery": ref_d,
                "ref_delivery_item": ref_di,
                "ref_di_id": make_id(ref_d, ref_di),
                "props": clean_props(r)
            }
        self.ingest_entity(session, "billing_document_items", handler, cypher)

    def ingest_invoice_cancellations(self, session):
        # This folder has same fields as billing headers, but dedicated cancellation context.
        cypher = """
        UNWIND $rows AS row
        MERGE (inv:Invoice {id: row.invoice_id})
        SET inv += row.props
        SET inv.source_has_cancellation_record = true

        FOREACH (_ IN CASE WHEN row.cancelled_billing_document = '' THEN [] ELSE [1] END |
          MERGE (canc:Invoice {id: row.cancelled_billing_document})
          MERGE (inv)-[:CANCELS]->(canc)
        )
        """
        def handler(r):
            bid = r.get("billingDocument")
            if is_blank(bid):
                return None
            return {
                "invoice_id": to_str(bid),
                "cancelled_billing_document": to_str(r.get("cancelledBillingDocument")),
                "props": clean_props(r)
            }
        self.ingest_entity(session, "billing_document_cancellations", handler, cypher)

    # ---------- Finance ----------
    def ingest_journal_entries(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (je:JournalEntry {id: row.je_id})
        SET je += row.props

        FOREACH (_ IN CASE WHEN row.invoice_id = '' THEN [] ELSE [1] END |
          MERGE (inv:Invoice {id: row.invoice_id})
          MERGE (inv)-[:POSTED_AS]->(je)
        )

        FOREACH (_ IN CASE WHEN row.customer_id = '' THEN [] ELSE [1] END |
          MERGE (c:Customer {id: row.customer_id})
          MERGE (c)-[:HAS_JOURNAL_ENTRY]->(je)
        )
        """
        def handler(r):
            ad = r.get("accountingDocument")
            adi = r.get("accountingDocumentItem")
            if is_blank(ad):
                return None
            # referenceDocument here maps to billingDocument in sample
            return {
                "je_id": make_id(ad, adi if not is_blank(adi) else "1"),
                "invoice_id": to_str(r.get("referenceDocument")),
                "customer_id": to_str(r.get("customer")),
                "props": clean_props(r)
            }
        self.ingest_entity(session, "journal_entry_items_accounts_receivable", handler, cypher)

    def ingest_payments(self, session):
        cypher = """
        UNWIND $rows AS row
        MERGE (p:Payment {id: row.payment_id})
        SET p += row.props

        FOREACH (_ IN CASE WHEN row.customer_id = '' THEN [] ELSE [1] END |
          MERGE (c:Customer {id: row.customer_id})
          MERGE (c)-[:MADE_PAYMENT]->(p)
        )
        """
        def handler(r):
            ad = r.get("accountingDocument")
            adi = r.get("accountingDocumentItem")
            if is_blank(ad):
                return None
            return {
                "payment_id": make_id(ad, adi if not is_blank(adi) else "1"),
                "customer_id": to_str(r.get("customer")),
                "props": clean_props(r)
            }
        self.ingest_entity(session, "payments_accounts_receivable", handler, cypher)

    def link_invoice_payment_and_journal(self, session):
        # 1) Invoice.accountingDocument <-> Payment.accountingDocument
        session.run("""
        MATCH (inv:Invoice), (p:Payment)
        WHERE inv.accountingDocument IS NOT NULL
          AND p.accountingDocument IS NOT NULL
          AND inv.accountingDocument = p.accountingDocument
        MERGE (inv)-[:SETTLED_BY]->(p)
        """)

        # 2) Invoice.accountingDocument <-> Payment.clearingAccountingDocument
        session.run("""
        MATCH (inv:Invoice), (p:Payment)
        WHERE inv.accountingDocument IS NOT NULL
          AND p.clearingAccountingDocument IS NOT NULL
          AND inv.accountingDocument = p.clearingAccountingDocument
        MERGE (inv)-[:SETTLED_BY]->(p)
        """)

        # 3) JournalEntry.clearingAccountingDocument <-> Payment.clearingAccountingDocument
        session.run("""
        MATCH (je:JournalEntry), (p:Payment)
        WHERE je.clearingAccountingDocument IS NOT NULL
          AND p.clearingAccountingDocument IS NOT NULL
          AND je.clearingAccountingDocument = p.clearingAccountingDocument
        MERGE (je)-[:CLEARED_BY]->(p)
        """)

        # 4) JournalEntry.accountingDocument <-> Payment.accountingDocument
        session.run("""
        MATCH (je:JournalEntry), (p:Payment)
        WHERE je.accountingDocument IS NOT NULL
          AND p.accountingDocument IS NOT NULL
          AND je.accountingDocument = p.accountingDocument
        MERGE (je)-[:PAYMENT_ENTRY]->(p)
        """)

    def ingest_all(self, session):
        # Master data first
        self.ingest_customers(session)
        self.ingest_customer_addresses(session)
        self.ingest_products(session)
        self.ingest_product_descriptions(session)
        self.ingest_plants(session)
        self.ingest_product_plants(session)
        self.ingest_product_storage_locations(session)

        # Assignments + sales
        self.ingest_customer_company_assignments(session)
        self.ingest_customer_sales_area_assignments(session)
        self.ingest_sales_order_headers(session)
        self.ingest_sales_order_items(session)
        self.ingest_sales_order_schedule_lines(session)

        # Logistics + billing
        self.ingest_delivery_headers(session)
        self.ingest_delivery_items(session)
        self.ingest_invoice_headers(session)
        self.ingest_invoice_items(session)
        self.ingest_invoice_cancellations(session)

        # Finance
        self.ingest_journal_entries(session)
        self.ingest_payments(session)
        self.link_invoice_payment_and_journal(session)


# ------------------------------
# Main
# ------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", required=True, help="Path to sap-order-to-cash-dataset")
    parser.add_argument("--neo4j-uri", required=True, help="Neo4j URI, e.g. neo4j+s://xxxx.databases.neo4j.io")
    parser.add_argument("--neo4j-user", required=True)
    parser.add_argument("--neo4j-password", required=True)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--clear-first", action="store_true")
    parser.add_argument("--create-constraints", action="store_true")
    args = parser.parse_args()

    driver = GraphDatabase.driver(
        args.neo4j_uri,
        auth=(args.neo4j_user, args.neo4j_password)
    )

    try:
        ingestor = SAPO2CIngestor(Path(args.dataset_dir), batch_size=args.batch_size)
        with driver.session() as session:
            if args.create_constraints:
                run_constraints(session)
            if args.clear_first:
                clear_db(session)

            ingestor.ingest_all(session)

            # quick sanity counts (Neo4j-safe form)
            result = session.run("""
            MATCH (c:Customer)
            WITH count(c) AS customers
            MATCH (so:SalesOrder)
            WITH customers, count(so) AS sales_orders
            MATCH (d:Delivery)
            WITH customers, sales_orders, count(d) AS deliveries
            MATCH (i:Invoice)
            WITH customers, sales_orders, deliveries, count(i) AS invoices
            MATCH (p:Payment)
            WITH customers, sales_orders, deliveries, invoices, count(p) AS payments
            MATCH (pr:Product)
            RETURN
              customers,
              sales_orders,
              deliveries,
              invoices,
              payments,
              count(pr) AS products
            """).single()
            print("Ingestion complete:", dict(result))
    finally:
        driver.close()


if __name__ == "__main__":
    main()