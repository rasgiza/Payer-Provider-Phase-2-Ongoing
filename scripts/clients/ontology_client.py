"""
Fabric Ontology REST API Client
================================
Reusable wrapper for the Microsoft Fabric Ontology REST API (Preview).

Endpoints covered:
  GET    /workspaces/{id}/ontologies                   — list
  GET    /workspaces/{id}/ontologies/{id}               — get
  POST   /workspaces/{id}/ontologies                   — create
  POST   /workspaces/{id}/ontologies/{id}/updateDefinition — update definition
  DELETE /workspaces/{id}/ontologies/{id}               — delete

Usage (standalone):
    python -m clients.ontology_client
"""
from __future__ import annotations

import sys
import time
import requests

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


class OntologyClient:
    """Thin wrapper around the Fabric Ontology REST API."""

    def __init__(self, token: str, api_base: str = FABRIC_API_BASE):
        self.token = token
        self.api_base = api_base
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    # ── List / Get / Find ─────────────────────────────────────

    def list(self, workspace_id: str) -> list[dict]:
        """List all ontologies in a workspace."""
        url = f"{self.api_base}/workspaces/{workspace_id}/ontologies"
        resp = requests.get(url, headers=self._headers)
        if resp.status_code == 200:
            return resp.json().get("value", [])
        # Fallback: generic items endpoint
        url = f"{self.api_base}/workspaces/{workspace_id}/items?type=Ontology"
        resp = requests.get(url, headers=self._headers)
        if resp.status_code == 200:
            return resp.json().get("value", [])
        return []

    def get(self, workspace_id: str, ontology_id: str) -> dict | None:
        """Get a single ontology by ID."""
        url = f"{self.api_base}/workspaces/{workspace_id}/ontologies/{ontology_id}"
        resp = requests.get(url, headers=self._headers)
        if resp.status_code == 200:
            return resp.json()
        return None

    def find_by_name(self, workspace_id: str, name: str) -> dict | None:
        """Find an ontology by display name. Returns the item dict or None."""
        for item in self.list(workspace_id):
            if item.get("displayName") == name:
                return item
        return None

    # ── Create ────────────────────────────────────────────────

    def create(self, workspace_id: str, display_name: str, description: str,
               parts: list[dict], folder_id: str = None) -> str | None:
        """
        Create an ontology with a full definition.

        Args:
            workspace_id: Target workspace GUID.
            display_name: Ontology display name.
            description: Human-readable description.
            parts: List of definition parts (path/payload/payloadType dicts).
            folder_id: Optional workspace folder GUID.

        Returns:
            Ontology item ID on success, None on failure.
        """
        url = f"{self.api_base}/workspaces/{workspace_id}/ontologies"
        body: dict = {
            "displayName": display_name,
            "description": description,
            "definition": {"parts": parts},
        }
        if folder_id:
            body["folderId"] = folder_id

        resp = requests.post(url, headers=self._headers, json=body)

        if resp.status_code in (200, 201):
            item_id = resp.json().get("id")
            print(f"  [OK] Created ontology: {display_name} (ID: {item_id})")
            return item_id

        if resp.status_code == 202:
            ok = self._wait_lro(resp, display_name)
            if ok:
                item = self.find_by_name(workspace_id, display_name)
                if item:
                    print(f"  [OK] Created ontology: {display_name} (ID: {item['id']})")
                    return item["id"]
            print(f"  [FAIL] Async create failed: {display_name}")
            return None

        if resp.status_code == 409 or "AlreadyInUse" in resp.text:
            print(f"  [WARN] Already exists: {display_name}")
            item = self.find_by_name(workspace_id, display_name)
            return item["id"] if item else None

        print(f"  [FAIL] Create ontology: HTTP {resp.status_code}")
        print(f"    {resp.text[:500]}")
        return None

    # ── Update Definition ─────────────────────────────────────

    def update_definition(self, workspace_id: str, ontology_id: str,
                          parts: list[dict]) -> bool:
        """
        Push updated definition parts to an existing ontology.

        Returns True on success.
        """
        url = (f"{self.api_base}/workspaces/{workspace_id}"
               f"/ontologies/{ontology_id}/updateDefinition")
        body = {"definition": {"parts": parts}}
        resp = requests.post(url, headers=self._headers, json=body)

        if resp.status_code in (200, 201):
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

    # ── Delete ────────────────────────────────────────────────

    def delete(self, workspace_id: str, ontology_id: str) -> bool:
        """Delete an ontology. Returns True on success."""
        url = f"{self.api_base}/workspaces/{workspace_id}/ontologies/{ontology_id}"
        resp = requests.delete(url, headers=self._headers)
        if resp.status_code in (200, 204):
            print(f"  [OK] Deleted ontology: {ontology_id}")
            return True
        print(f"  [FAIL] Delete: HTTP {resp.status_code} — {resp.text[:300]}")
        return False

    # ── Internal helpers ──────────────────────────────────────

    def _wait_lro(self, response, label: str, timeout: int = 180) -> bool:
        """Poll a long-running operation until completion."""
        location = response.headers.get("Location")
        if not location:
            time.sleep(10)
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


# ============================================================
# Standalone smoke test
# ============================================================
if __name__ == "__main__":
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from fabric_auth import get_fabric_token, get_auth_headers  # noqa: F401
    from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME

    print("=" * 60)
    print("  Ontology Client — Smoke Test")
    print("=" * 60)

    token = get_fabric_token(TENANT_ID, ADMIN_ACCOUNT)
    client = OntologyClient(token)

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

    ontologies = client.list(ws_id)
    print(f"  Found {len(ontologies)} ontology(ies):")
    for o in ontologies:
        print(f"    {o['displayName']:<45} {o['id']}")
