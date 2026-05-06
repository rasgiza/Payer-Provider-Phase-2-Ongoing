"""
Deploy Graph Model from Ontology Metadata
===========================================
Reads ontology entity types, relationships, data bindings, and
contextualizations from the local ontology folder, then deploys a standalone
GraphModel item via the Fabric GraphModel REST API.

Uses the reusable client wrappers:
  - clients.graph_client.GraphModelClient           — all 9 REST endpoints
  - clients.graph_definition_builder.GraphDefinitionBuilder — 5-part builder

The graph model uses a 2-step create pattern:
  1. POST /graphModels (empty shell)
  2. POST /graphModels/{id}/updateDefinition (triggers data loading)

Modes:
  Default       — Create a new graph model (or skip if it already exists).
  --update      — Push a fresh definition to an existing graph model.
  --fix-existing — Fix an auto-provisioned graph model's entity keys,
                   contextualizations, and edge discovery by regenerating the
                   definition from ontology metadata and pushing via updateDefinition.

Usage:
    python scripts/deploy_graph_model.py --workspace "WS" --tenant-id "..."
    python scripts/deploy_graph_model.py --workspace "WS" --tenant-id "..." --update
    python scripts/deploy_graph_model.py --workspace "WS" --tenant-id "..." --fix-existing
"""

import sys
import json
import argparse
import requests
from pathlib import Path

# Add scripts/ to path for client imports
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from fabric_auth import get_fabric_token, get_auth_headers
from clients.graph_client import GraphModelClient
from clients.graph_definition_builder import GraphDefinitionBuilder

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ============================================================
# CONFIGURATION
# ============================================================
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
ONTOLOGY_NAME = "Healthcare_Demo_Ontology_HLS"
GRAPH_MODEL_NAME = "Healthcare_Demo_Graph"
LAKEHOUSE_NAME = "lh_gold_curated"
FOLDER_NAME = "Ontologies"


# ============================================================
# WORKSPACE HELPERS
# ============================================================

def get_workspace_id(token, name):
    url = f"{FABRIC_API_BASE}/workspaces"
    resp = requests.get(url, headers=get_auth_headers(token))
    resp.raise_for_status()
    for ws in resp.json().get("value", []):
        if ws["displayName"] == name:
            return ws["id"]
    available = [w["displayName"] for w in resp.json().get("value", [])]
    print(f"  [FAIL] Workspace '{name}' not found. Available: {available}")
    sys.exit(1)


def get_lakehouse_id(token, ws_id, name):
    url = f"{FABRIC_API_BASE}/workspaces/{ws_id}/lakehouses"
    resp = requests.get(url, headers=get_auth_headers(token))
    resp.raise_for_status()
    for lh in resp.json().get("value", []):
        if lh["displayName"] == name:
            return lh["id"]
    print(f"  [FAIL] Lakehouse '{name}' not found in workspace.")
    sys.exit(1)


def get_folder_id(token, ws_id, name):
    url = f"{FABRIC_API_BASE}/workspaces/{ws_id}/folders"
    resp = requests.get(url, headers=get_auth_headers(token))
    if resp.status_code != 200:
        return None
    for f in resp.json().get("value", []):
        if f["displayName"] == name:
            return f["id"]
    return None


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Deploy standalone GraphModel from ontology metadata")
    parser.add_argument("--workspace", required=True,
                        help="Target workspace name")
    parser.add_argument("--tenant-id", required=True,
                        help="Azure AD tenant ID")
    parser.add_argument("--account", default=None,
                        help="Login hint (email)")
    parser.add_argument("--graph-model", default=GRAPH_MODEL_NAME,
                        help=f"GraphModel display name (default: {GRAPH_MODEL_NAME})")
    parser.add_argument("--ontology", default=ONTOLOGY_NAME,
                        help=f"Source ontology folder name (default: {ONTOLOGY_NAME})")
    parser.add_argument("--update", action="store_true",
                        help="Update existing GraphModel definition")
    parser.add_argument("--fix-existing", action="store_true",
                        help="Fix an auto-provisioned graph model by pushing "
                             "the correct definition from ontology metadata")
    parser.add_argument("--no-refresh", action="store_true",
                        help="Skip waiting for data load after deploy")
    args = parser.parse_args()

    ontology_dir = SCRIPT_DIR.parent / "ontology" / args.ontology

    print("=" * 70)
    print("  DEPLOY GRAPHMODEL FROM ONTOLOGY METADATA")
    print("=" * 70)
    print()
    print(f"  Source ontology:  {args.ontology}")
    print(f"  Target workspace: {args.workspace}")
    print(f"  GraphModel name:  {args.graph_model}")
    if args.fix_existing:
        print(f"  Mode:             --fix-existing (repair auto-provisioned graph)")
    elif args.update:
        print(f"  Mode:             --update (overwrite existing definition)")
    else:
        print(f"  Mode:             create (new graph model)")
    print()

    if not ontology_dir.exists():
        print(f"  [FAIL] Ontology directory not found: {ontology_dir}")
        sys.exit(1)

    # ── Step 1: Authenticate & resolve targets ────────────────
    print("  Step 1: Authenticate & resolve workspace")
    token = get_fabric_token(args.tenant_id, args.account)
    ws_id = get_workspace_id(token, args.workspace)
    print(f"    Workspace: {args.workspace} ({ws_id})")

    lh_id = get_lakehouse_id(token, ws_id, LAKEHOUSE_NAME)
    print(f"    Lakehouse: {LAKEHOUSE_NAME} ({lh_id})")

    folder_id = get_folder_id(token, ws_id, FOLDER_NAME)
    if folder_id:
        print(f"    Folder:    {FOLDER_NAME} ({folder_id})")

    # Initialize clients
    client = GraphModelClient(token)
    builder = GraphDefinitionBuilder(ontology_dir, ws_id, lh_id)

    # ── Step 2: Load ontology metadata ────────────────────────
    print("\n  Step 2: Parse ontology metadata")
    builder.load_ontology()

    entities = builder.entities
    relationships = builder.relationships

    # ── Step 3: Generate definition parts ─────────────────────
    print("\n  Step 3: Generating GraphModel definition parts")
    description = (
        f"Graph model generated from {args.ontology}. "
        f"Contains {len(entities)} node types and "
        f"{len(relationships)} edge types mapped to "
        f"lh_gold_curated delta tables."
    )
    parts = builder.build_all_parts(
        display_name=args.graph_model,
        description=description,
    )
    print(f"    Generated {len(parts)} definition parts")

    # ── Step 4: Deploy ────────────────────────────────────────
    result = ""
    gm_id = None

    if args.fix_existing:
        print(f"\n  Step 4: Fix auto-provisioned graph model")
        existing = client.find_by_name(ws_id, args.graph_model)
        if not existing:
            print(f"    [FAIL] Graph model '{args.graph_model}' not found.")
            print(f"    Create the ontology in the Fabric UI first, which")
            print(f"    auto-provisions a graph model. Then re-run with --fix-existing.")
            sys.exit(1)

        gm_id = existing["id"]
        print(f"    Found: {args.graph_model} ({gm_id})")

        # Show current definition for comparison
        print(f"\n    --- Current definition (before fix) ---")
        current_def = client.get_definition(ws_id, gm_id)
        if current_def:
            for path, content in current_def.items():
                if path == "graphType.json":
                    nodes = content.get("nodeTypes", [])
                    edges = content.get("edgeTypes", [])
                    print(f"      {path}: {len(nodes)} nodeTypes, {len(edges)} edgeTypes")
                elif path == "graphDefinition.json":
                    nt = content.get("nodeTables", [])
                    et = content.get("edgeTables", [])
                    print(f"      {path}: {len(nt)} nodeTables, {len(et)} edgeTables")
                elif path == "dataSources.json":
                    ds = content.get("dataSources", [])
                    print(f"      {path}: {len(ds)} data sources")
                else:
                    print(f"      {path}: {len(json.dumps(content)):,} chars")

        # Push the corrected definition
        print(f"\n    --- Pushing corrected definition ---")
        ok = client.update_definition(ws_id, gm_id, parts)
        if ok:
            result = "[OK] Definition fixed via updateDefinition"
        else:
            result = "[FAIL] updateDefinition failed"

    else:
        existing = client.find_by_name(ws_id, args.graph_model)

        if existing and args.update:
            gm_id = existing["id"]
            print(f"\n  Step 4: Updating existing GraphModel ({gm_id})")
            ok = client.update_definition(ws_id, gm_id, parts)
            result = "[OK] Definition updated" if ok else "[FAIL] Update failed"

        elif existing and not args.update:
            gm_id = existing["id"]
            print(f"\n  Step 4: GraphModel '{args.graph_model}' already exists ({gm_id})")
            print(f"    Use --update to overwrite the definition.")
            print(f"    Use --fix-existing to repair an auto-provisioned graph.")
            result = "[SKIP] Already exists"

        else:
            print(f"\n  Step 4: Creating GraphModel '{args.graph_model}'")
            gm_id = client.create(ws_id, args.graph_model, description,
                                  definition_parts=parts, folder_id=folder_id)
            if gm_id:
                result = f"[OK] Created ({gm_id})"
            else:
                result = "[FAIL] Create failed"
                gm_id = None

    # ── Step 5: Wait for data load ────────────────────────────
    if gm_id and not args.no_refresh and "FAIL" not in result and "SKIP" not in result:
        print(f"\n  Step 5: Waiting for data load")
        loaded = client.wait_for_data_load(ws_id, gm_id, timeout=600)
        if loaded:
            result += " + data loaded"
        else:
            print(f"    [WARN] Data load incomplete — check job history in Fabric UI")

    # ── Step 6: Verify with GQL query ─────────────────────────
    if gm_id and "FAIL" not in result and "SKIP" not in result:
        print(f"\n  Step 6: Verify graph via GQL queries")

        schema = client.get_queryable_graph_type(ws_id, gm_id)
        if schema:
            node_types = schema.get("nodeTypes", [])
            edge_types = schema.get("edgeTypes", [])
            print(f"    Queryable schema: {len(node_types)} node types, "
                  f"{len(edge_types)} edge types")

            key_issues = []
            for nt in node_types:
                label = nt.get("labels", ["?"])[0]
                pk_props = [p.get("name") for p in nt.get("properties", [])
                            if p.get("isPrimaryKey")]
                if not pk_props:
                    key_issues.append(f"    [WARN] {label}: no primary key detected")
            if key_issues:
                for issue in key_issues:
                    print(issue)
            else:
                print(f"    Entity keys: all {len(node_types)} node types have PKs")
        else:
            print(f"    [WARN] Could not retrieve queryable schema (graph may still be loading)")

        node_count = client.execute_query(
            ws_id, gm_id, "MATCH (n) RETURN count(n) AS total")
        if node_count:
            rows = node_count.get("results", [{}])
            if rows:
                print(f"    Node count: {rows}")

        edge_count = client.execute_query(
            ws_id, gm_id,
            "MATCH ()-[r]->() RETURN type(r) AS edge_type, count(r) AS cnt")
        if edge_count:
            rows = edge_count.get("results", [{}])
            if rows:
                total_edges = sum(r.get("cnt", 0) for r in rows
                                  if isinstance(r.get("cnt"), (int, float)))
                print(f"    Edge count: {total_edges} across {len(rows)} types")

    elif gm_id and "SKIP" in result:
        print(f"\n  Step 6: Checking current status")
        item = client.get(ws_id, gm_id)
        if item:
            props = item.get("properties", {})
            readiness = props.get("queryReadiness", "Unknown")
            loading = props.get("lastDataLoadingStatus") or {}
            status = loading.get("status", "Unknown")
            print(f"    queryReadiness: {readiness}")
            print(f"    loadingStatus:  {status}")

    # ── Summary ───────────────────────────────────────────────
    print()
    print("=" * 70)
    print("  DEPLOYMENT SUMMARY")
    print("=" * 70)
    print(f"  {args.graph_model:<45} {result}")
    print()
    print("  Graph model definition:")
    print(f"    Nodes:  {len(entities)} types  "
          f"(Patient, Encounter, Claim, Provider, ...)")
    print(f"    Edges:  {len(relationships)} types  "
          f"(covers, involves, livesIn, treatedBy, ...)")

    all_tables = (
        set(e["source_table"] for e in entities.values() if e["source_table"])
        | set(r["ctx_table"] for r in relationships.values() if r["ctx_table"])
    )
    print(f"    Tables: {len(all_tables)} delta table sources")
    print()

    if args.fix_existing:
        print("  " + "-" * 66)
        print("  --fix-existing mode: The auto-provisioned graph has been repaired.")
        print("  Entity keys, edge contextualizations, and data sources are now")
        print("  aligned with the ontology definition.")
    else:
        print("  " + "-" * 66)
        print("  NOTE: This is a STANDALONE GraphModel, independent of the Ontology.")
        print("  The Fabric Preview API does not support linking them.")
        print("  To fix an auto-provisioned graph, re-run with --fix-existing.")

    print()
    print("  Next steps:")
    print("    - Open Fabric workspace -> Graph view to explore visually")
    print("    - Run GQL queries via executeQuery API or Fabric UI")
    print()

    if "FAIL" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
