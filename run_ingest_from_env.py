import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

def as_bool(v: str, default: bool = False) -> bool:
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}

# .env mapping for your keys
neo4j_uri = os.getenv("NEO4J_URI")
neo4j_user = os.getenv("NEO4J_USERNAME")  # your .env uses USERNAME
neo4j_password = os.getenv("NEO4J_PASSWORD")
neo4j_database = os.getenv("NEO4J_DATABASE")  # optional if script supports it

dataset_dir = os.getenv(
    "DATASET_DIR",
    r"c:\Users\adars\OneDrive\Desktop\GraphBasedDataModelingandQuerySystem\sap-order-to-cash-dataset"
)
batch_size = os.getenv("BATCH_SIZE", "500")
create_constraints = as_bool(os.getenv("CREATE_CONSTRAINTS", "true"), True)
clear_first = as_bool(os.getenv("CLEAR_FIRST", "false"), False)

required = {
    "NEO4J_URI": neo4j_uri,
    "NEO4J_USERNAME": neo4j_user,
    "NEO4J_PASSWORD": neo4j_password,
}
missing = [k for k, v in required.items() if not v]
if missing:
    print("Missing required .env keys:", ", ".join(missing))
    sys.exit(1)

script_candidates = [
    Path("ingest_sap_o2c_to_neo4j.py"),
    Path("ingest_sap_02c_to_neo4j.py"),
]
script_path = next((p for p in script_candidates if p.exists()), None)
if not script_path:
    print("Could not find ingest script file.")
    sys.exit(1)

cmd = [
    sys.executable, str(script_path),
    "--dataset-dir", dataset_dir,
    "--neo4j-uri", neo4j_uri,
    "--neo4j-user", neo4j_user,
    "--neo4j-password", neo4j_password,
    "--batch-size", str(batch_size),
]

# Add this only if your ingest script has --neo4j-database argument
# if neo4j_database:
#     cmd.extend(["--neo4j-database", neo4j_database])

if create_constraints:
    cmd.append("--create-constraints")
if clear_first:
    cmd.append("--clear-first")

print("Running ingestion...")
subprocess.run(cmd, check=True)
print("Done.")