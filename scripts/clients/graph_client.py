"""
Fabric Graph Model REST API Client
====================================
Reusable wrapper for the Microsoft Fabric GraphModel REST API (Preview).

Covers all 9 endpoints:
  GET    /workspaces/{id}/graphModels                              — list
  GET    /workspaces/{id}/graphModels/{id}                          — get (status)
  POST   /workspaces/{id}/graphModels                              — create
  PATCH  /workspaces/{id}/graphModels/{id}                          — update metadata
  DELETE /workspaces/{id}/graphModels/{id}                          — delete
  POST   /workspaces/{id}/graphModels/{id}/getDefinition            — get definition
  POST   /workspaces/{id}/graphModels/{id}/updateDefinition         — update definition
  GET    /workspaces/{id}/graphModels/{id}/queryableGraphType?preview — schema (beta)
  POST   /workspaces/{id}/graphModels/{id}/executeQuery?preview=true — GQL query (beta)

Usage (standalone):
    python -m clients.graph_client
"""
from __future__ import annotations

import sys
import json
import time
import base64
import requests

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


class GraphModelClient:
    """Thin wrapper around the Fabric GraphModel REST API."""

    def __init__(self, token: str, api_base: str = FABRIC_API_BASE):
        self.token = token
        self.api_base = api_base
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    # ── List / Get / Find ─────────────────────────────────────

    def list(self, workspace_id: str) -> list[dict]:
        """List all graph models in a workspace."""
        url = f"{self.api_base}/workspaces/{workspace_id}/graphModels"
        resp = requests.get(url, headers=self._headers)
        if resp.status_code == 200:
            return resp.json().get("value", [])
        # Fallback: generic items endpoint
        url = f"{self.api_base}/workspaces/{workspace_id}/items?type=GraphModel"
        resp = requests.get(url, headers=self._headers)
        if resp.status_code == 200:
            return resp.json().get("value", [])
        return []

    def get(self, workspace_id: str, graph_model_id: str) -> dict | None:
        """
        Get a graph model by ID.

        Returns the full item dict including properties:
          properties.queryReadiness        — "Full" | "None" | "Partial"
          properties.lastDataLoadingStatus — {status: "Completed"|"Failed"|...}
        """
        url = f"{self.api_base}/workspaces/{workspace_id}/graphModels/{graph_model_id}"
        resp = requests.get(url, headers=self._headers)
        if resp.status_code == 200:
            return resp.json()
        return None

    def find_by_name(self, workspace_id: str, name: str) -> dict | None:
        """Find a graph model by display name. Returns the item dict or None."""
        for item in self.list(workspace_id):
            if item.get("displayName") == name:
                return item
        return None

    # ── Create ────────────────────────────────────────────────

    def create(self, workspace_id: str, display_name: str, description: str,
               definition_parts: list[dict] = None,
               folder_id: str = None) -> str | None:
        """
        Create a graph model, optionally with a full definition.

        Uses a two-step pattern when definition_parts are provided:
          1. POST  /graphModels              — create empty shell
          2. POST  /graphModels/{id}/updateDefinition — push definition

        This is required because the Fabric API does NOT trigger a data
        load when definition parts are included inline in the create body.
        Only updateDefinition triggers the data-loading pipeline.

        Args:
            workspace_id: Target workspace GUID.
            display_name: Graph model display name.
            description: Human-readable description.
            definition_parts: Optional list of 4 definition parts
                (graphType.json, graphDefinition.json, dataSources.json,
                 stylingConfiguration.json) as {path, payload, payloadType} dicts.
            folder_id: Optional workspace folder GUID.

        Returns:
            Graph model item ID on success, None on failure.
        """
        url = f"{self.api_base}/workspaces/{workspace_id}/graphModels"

        # Step 1: Create empty shell (no definition inline — that doesn't
        #         trigger data loading).
        body: dict = {
            "displayName": display_name,
            "description": description,
        }
        if folder_id:
            body["folderId"] = folder_id

        resp = requests.post(url, headers=self._headers, json=body)
        item_id = None

        if resp.status_code == 201:
            item_id = resp.json().get("id")
            print(f"  [OK] Created graph model shell: {display_name} (ID: {item_id})")

        elif resp.status_code == 202:
            ok = self._wait_lro(resp, display_name)
            if ok:
                item = self.find_by_name(workspace_id, display_name)
                if item:
                    item_id = item["id"]
                    print(f"  [OK] Created graph model shell: {display_name} (ID: {item_id})")
            if not item_id:
                print(f"  [FAIL] Async create failed: {display_name}")
                return None
        else:
            print(f"  [FAIL] Create graph model: HTTP {resp.status_code}")
            print(f"    {resp.text[:500]}")
            return None

        # Step 2: Push definition via updateDefinition — this triggers
        #         the data-loading pipeline.
        if definition_parts and item_id:
            print(f"  Pushing definition via updateDefinition (triggers data load)...")
            ok = self.update_definition(workspace_id, item_id, definition_parts)
            if not ok:
                print(f"  [WARN] Graph created but definition push failed.")
                print(f"    Re-run with --update to retry.")

        return item_id

    # ── Update Metadata ───────────────────────────────────────

    def update(self, workspace_id: str, graph_model_id: str,
               display_name: str = None, description: str = None) -> bool:
        """Update graph model metadata (name/description)."""
        url = f"{self.api_base}/workspaces/{workspace_id}/graphModels/{graph_model_id}"
        body = {}
        if display_name is not None:
            body["displayName"] = display_name
        if description is not None:
            body["description"] = description
        resp = requests.patch(url, headers=self._headers, json=body)
        if resp.status_code == 200:
            print(f"  [OK] Updated metadata: {graph_model_id}")
            return True
        print(f"  [FAIL] Update metadata: HTTP {resp.status_code} — {resp.text[:300]}")
        return False

    # ── Delete ────────────────────────────────────────────────

    def delete(self, workspace_id: str, graph_model_id: str) -> bool:
        """Delete a graph model."""
        url = f"{self.api_base}/workspaces/{workspace_id}/graphModels/{graph_model_id}"
        resp = requests.delete(url, headers=self._headers)
        if resp.status_code in (200, 204):
            print(f"  [OK] Deleted graph model: {graph_model_id}")
            return True
        print(f"  [FAIL] Delete: HTTP {resp.status_code} — {resp.text[:300]}")
        return False

    # ── Get Definition ────────────────────────────────────────

    def get_definition(self, workspace_id: str,
                       graph_model_id: str) -> dict[str, dict] | None:
        """
        Retrieve the 4-part graph model definition.

        Returns a dict mapping part path → decoded JSON content:
            {
                "graphType.json": {...},
                "graphDefinition.json": {...},
                "dataSources.json": {...},
                "stylingConfiguration.json": {...},
            }
        Returns None on failure.
        """
        url = (f"{self.api_base}/workspaces/{workspace_id}"
               f"/graphModels/{graph_model_id}/getDefinition")
        resp = requests.post(url, headers=self._headers)

        if resp.status_code == 202:
            ok = self._wait_lro(resp, "getDefinition")
            if not ok:
                return None
            # After LRO, re-fetch via the result URL
            result_url = resp.headers.get("Location")
            if result_url:
                resp = requests.get(
                    result_url.replace("/operations/", "/operations/") + "/result",
                    headers=self._headers)

        if resp.status_code != 200:
            print(f"  [FAIL] getDefinition: HTTP {resp.status_code}")
            print(f"    {resp.text[:500]}")
            return None

        parts = resp.json().get("definition", {}).get("parts", [])
        return self._decode_definition_parts(parts)

    # ── Update Definition ─────────────────────────────────────

    def update_definition(self, workspace_id: str, graph_model_id: str,
                          definition_parts: list[dict]) -> bool:
        """
        Push an updated 4-part definition to an existing graph model.

        Args:
            definition_parts: List of 4 parts with path/payload/payloadType.

        Returns True on success.
        """
        url = (f"{self.api_base}/workspaces/{workspace_id}"
               f"/graphModels/{graph_model_id}"
               f"/updateDefinition?updateMetadata=True")
        body = {
            "definition": {
                "format": "json",
                "parts": definition_parts,
            },
        }
        resp = requests.post(url, headers=self._headers, json=body)

        if resp.status_code == 200:
            print(f"  [OK] Definition updated")
            return True
        if resp.status_code == 202:
            ok = self._wait_lro(resp, "updateDefinition")
            if ok:
                print(f"  [OK] Definition updated")
            return ok

        print(f"  [FAIL] updateDefinition: HTTP {resp.status_code}")
        print(f"    {resp.text[:500]}")
        return False

    # ── Preview / Beta APIs ───────────────────────────────────

    def get_queryable_graph_type(self, workspace_id: str,
                                 graph_model_id: str) -> dict | None:
        """
        Get the queryable graph type schema (Preview API).

        Returns the schema dict with nodeTypes and edgeTypes, or None.
        """
        url = (f"{self.api_base}/workspaces/{workspace_id}"
               f"/graphModels/{graph_model_id}/queryableGraphType?preview")
        resp = requests.get(url, headers=self._headers)
        if resp.status_code == 200:
            return resp.json()
        print(f"  [WARN] queryableGraphType: HTTP {resp.status_code}")
        return None

    def execute_query(self, workspace_id: str, graph_model_id: str,
                      gql_query: str) -> dict | None:
        """
        Execute a GQL query against the graph model (Preview API).

        Example queries:
            MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt
            MATCH ()-[r]->() RETURN type(r), count(r) AS cnt

        Returns the response dict with status and results, or None.
        """
        url = (f"{self.api_base}/workspaces/{workspace_id}"
               f"/graphModels/{graph_model_id}/executeQuery?preview=true")
        body = {"query": gql_query}
        resp = requests.post(url, headers=self._headers, json=body)
        if resp.status_code == 200:
            return resp.json()
        print(f"  [WARN] executeQuery: HTTP {resp.status_code}")
        if resp.text:
            print(f"    {resp.text[:500]}")
        return None

    # ── Wait for Data Load ────────────────────────────────────

    def wait_for_data_load(self, workspace_id: str, graph_model_id: str,
                           timeout: int = 600, poll_interval: int = 30) -> bool:
        """
        Poll until graph model finishes loading data.

        After creating a graph model with a definition, Fabric auto-triggers
        a data load. This method polls GET /graphModels/{id} until
        queryReadiness == "Full" and loadingStatus == "Completed".
        """
        start = time.time()
        while time.time() - start < timeout:
            item = self.get(workspace_id, graph_model_id)
            if not item:
                time.sleep(poll_interval)
                continue
            props = item.get("properties", {})
            readiness = props.get("queryReadiness", "")
            loading = props.get("lastDataLoadingStatus") or {}
            status = loading.get("status", "")
            print(f"    queryReadiness={readiness}  loadingStatus={status}")
            if readiness == "Full" and status == "Completed":
                return True
            if status == "Failed":
                print(f"    [WARN] Data load failed")
                return False
            time.sleep(poll_interval)
        print(f"    [WARN] Timed out waiting for data load ({timeout}s)")
        return False

    # ── Internal helpers ──────────────────────────────────────

    def _wait_lro(self, response, label: str, timeout: int = 180) -> bool:
        """Poll a long-running operation until completion."""
        location = response.headers.get("Location")
        if not location:
            time.sleep(5)
            return True
        start = time.time()
        while time.time() - start < timeout:
            retry = int(response.headers.get("Retry-After", 5))
            time.sleep(retry)
            r = requests.get(location, headers=self._headers)
            if r.status_code == 200:
                status = r.json().get("status", "")
                if status == "Succeeded":
                    return True
                if status in ("Failed", "Cancelled"):
                    err = r.json().get("error", {}).get("message", "")
                    print(f"    [FAIL] {label}: {status} — {err}")
                    return False
            elif r.status_code == 404:
                time.sleep(3)
                return True
        print(f"    [FAIL] {label}: timed out after {timeout}s")
        return False

    @staticmethod
    def _decode_definition_parts(parts: list[dict]) -> dict[str, dict]:
        """Decode base64 definition parts into a path→content dict."""
        result = {}
        for part in parts:
            path = part.get("path", "")
            payload = part.get("payload", "")
            try:
                raw = base64.b64decode(payload).decode("utf-8")
                result[path] = json.loads(raw)
            except Exception:
                result[path] = {"_raw": payload[:200]}
        return result


# ============================================================
# Standalone smoke test
# ============================================================
if __name__ == "__main__":
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from fabric_auth import get_fabric_token  # noqa: F401
    from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME

    GRAPH_MODEL_NAME = os.environ.get("GRAPH_MODEL_NAME", "Healthcare_Ontology_Graph")

    print("=" * 60)
    print("  Graph Model Client — Smoke Test")
    print("=" * 60)

    token = get_fabric_token(TENANT_ID, ADMIN_ACCOUNT)
    client = GraphModelClient(token)

    # Resolve workspace
    ws_resp = requests.get(f"{FABRIC_API_BASE}/workspaces", headers=client._headers)
    ws_id = None
    for ws in ws_resp.json().get("value", []):
        if ws["displayName"] == WORKSPACE_NAME:
            ws_id = ws["id"]
            break

    if not ws_id:
        print(f"  Workspace '{WORKSPACE_NAME}' not found")
        sys.exit(1)

    print(f"\n  Workspace: {WORKSPACE_NAME} ({ws_id})")
    print()

    # List graph models
    models = client.list(ws_id)
    print(f"  Found {len(models)} graph model(s):")
    for m in models:
        print(f"    {m['displayName']:<45} {m['id']}")

    # Find our graph model
    gm = client.find_by_name(ws_id, GRAPH_MODEL_NAME)
    if not gm:
        print(f"\n  Graph model '{GRAPH_MODEL_NAME}' not found — skipping deep inspection")
        sys.exit(0)

    gm_id = gm["id"]
    print(f"\n  Inspecting: {GRAPH_MODEL_NAME} ({gm_id})")

    # Get definition (decode 4 parts)
    print("\n  --- Definition Parts ---")
    defn = client.get_definition(ws_id, gm_id)
    if defn:
        for path, content in defn.items():
            if path == "graphType.json":
                nodes = content.get("nodeTypes", [])
                edges = content.get("edgeTypes", [])
                print(f"    {path}: {len(nodes)} nodeTypes, {len(edges)} edgeTypes")
            elif path == "graphDefinition.json":
                nt = content.get("nodeTables", [])
                et = content.get("edgeTables", [])
                print(f"    {path}: {len(nt)} nodeTables, {len(et)} edgeTables")
            elif path == "dataSources.json":
                ds = content.get("dataSources", [])
                print(f"    {path}: {len(ds)} data sources")
            else:
                print(f"    {path}: {len(json.dumps(content)):,} chars")

    # Queryable graph type (preview)
    print("\n  --- Queryable Graph Type (preview) ---")
    schema = client.get_queryable_graph_type(ws_id, gm_id)
    if schema:
        for n in schema.get("nodeTypes", []):
            labels = n.get("labels", ["?"])
            props = [p.get("name") for p in n.get("properties", [])]
            print(f"    Node: {labels[0]} ({len(props)} props)")
        for e in schema.get("edgeTypes", []):
            labels = e.get("labels", ["?"])
            src = e.get("sourceNodeType", {}).get("labels", ["?"])[0]
            dst = e.get("destinationNodeType", {}).get("labels", ["?"])[0]
            print(f"    Edge: {labels[0]}: {src} → {dst}")

    # Execute a count query
    print("\n  --- GQL Count Query ---")
    result = client.execute_query(ws_id, gm_id, "MATCH (n) RETURN count(n) AS total")
    if result:
        print(f"    {json.dumps(result, indent=2)[:500]}")
