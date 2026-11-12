"""SQLite-backed dispatcher and FastAPI webhook receiver.

The database is the source of truth for our intent to dial.  A request id is committed before
the carrier request, which makes every recovery path a harmless replay of that same request.
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import hmac
import json
import sqlite3
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request

from .schedule import in_window, next_campaign_open, priority, retry_due

FINAL = {"completed", "exhausted", "dnd_blocked"}
TERMINAL = {"interested", "not_interested", "wrong_number", "dnd"}


def iso(value: datetime) -> str:
    return value.isoformat()


def dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


class Store:
    def __init__(self, path: Path, campaigns: dict[str, dict], tenants: dict[str, dict], cfg: dict):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.campaigns, self.tenants, self.cfg = campaigns, tenants, cfg
        with self.lock, self.db:
            self.db.executescript("""
              CREATE TABLE IF NOT EXISTS leads (
                campaign_id TEXT NOT NULL, lead_id TEXT NOT NULL, tenant_id TEXT NOT NULL,
                phone TEXT NOT NULL, state TEXT NOT NULL, due_kind TEXT NOT NULL,
                due_at TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                last_status TEXT, last_disposition TEXT,
                PRIMARY KEY (campaign_id, lead_id));
              CREATE TABLE IF NOT EXISTS calls (
                request_id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, lead_id TEXT NOT NULL,
                phone TEXT NOT NULL, state TEXT NOT NULL, accepted INTEGER NOT NULL DEFAULT 0,
                call_sid TEXT, retry_at TEXT, ended_at TEXT, processed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL);
              CREATE INDEX IF NOT EXISTS calls_pending ON calls(state, retry_at);
            """)

    def import_fixtures(self, fixture_dir: Path) -> None:
        """INSERT OR IGNORE makes startup and restart safe without mutating live progress."""
        with self.lock, self.db:
            for campaign in self.campaigns.values():
                with (fixture_dir / campaign["leads_file"]).open(newline="") as file:
                    for row in csv.DictReader(file):
                        self.db.execute("""INSERT OR IGNORE INTO leads
                          (campaign_id, lead_id, tenant_id, phone, state, due_kind, due_at)
                          VALUES (?, ?, ?, ?, 'waiting', 'fresh', '2000-01-01T00:00:00+05:30')""",
                          (campaign["campaign_id"], row["lead_id"], campaign["tenant_id"], row["phone"]))

    def pending_calls(self) -> list[dict]:
        with self.lock:
            return [dict(row) for row in self.db.execute("""SELECT c.*, l.tenant_id, l.state AS lead_state
              FROM calls c JOIN leads l USING(campaign_id, lead_id)
              WHERE c.state IN ('prepared', 'accepted') ORDER BY c.created_at""")]

    def prepare_one(self, now: datetime) -> dict | None:
        """Atomically reserve a candidate and persist its idempotency key before I/O."""
        with self.lock, self.db:
            rows = [dict(row) for row in self.db.execute("""SELECT * FROM leads
              WHERE state='waiting' AND due_at <= ? ORDER BY due_at, campaign_id, lead_id""", (iso(now),))]
            # A prepared request may be an ambiguous 504 that the carrier accepted. Reserve its
            # tenant slot and phone until reconciliation proves otherwise; this is conservative
            # but prevents a restart from over-dialling while an outcome is unknown.
            active_rows = self.db.execute("""SELECT l.tenant_id, c.phone FROM calls c
              JOIN leads l USING(campaign_id, lead_id) WHERE c.state IN ('prepared','accepted')""").fetchall()
            active_by_tenant: dict[str, int] = {}
            active_phones = set()
            for row in active_rows:
                active_by_tenant[row["tenant_id"]] = active_by_tenant.get(row["tenant_id"], 0) + 1
                active_phones.add(row["phone"])
            rows.sort(key=lambda row: (priority(row["due_kind"]), row["due_at"], row["campaign_id"], row["lead_id"]))
            for lead in rows:
                campaign = self.campaigns[lead["campaign_id"]]
                if not in_window(now, campaign, self.cfg["regulatory_window"]):
                    next_at = next_campaign_open(now, campaign, self.cfg["regulatory_window"])
                    self.db.execute("UPDATE leads SET due_at=? WHERE campaign_id=? AND lead_id=?",
                                    (iso(next_at), lead["campaign_id"], lead["lead_id"]))
                    continue
                if lead["phone"] in active_phones:
                    continue
                if active_by_tenant.get(lead["tenant_id"], 0) >= self.tenants[lead["tenant_id"]]["max_concurrent_calls"]:
                    continue
                request_id = str(uuid.uuid4())
                self.db.execute("""INSERT INTO calls
                    (request_id,campaign_id,lead_id,phone,state,created_at) VALUES (?, ?, ?, ?, 'prepared', ?)""",
                    (request_id, lead["campaign_id"], lead["lead_id"], lead["phone"], iso(now)))
                return {**lead, "request_id": request_id, "state": "prepared", "retry_at": None}
        return None

    def cancelled(self, call: dict) -> bool:
        with self.lock, self.db:
            row = self.db.execute("SELECT state FROM leads WHERE campaign_id=? AND lead_id=?",
                                  (call["campaign_id"], call["lead_id"])).fetchone()
            if row and row["state"] in FINAL:
                self.db.execute("UPDATE calls SET state='cancelled' WHERE request_id=? AND state='prepared'",
                                (call["request_id"],))
                return True
        return False

    def defer(self, request_id: str, now: datetime, seconds: int = 30) -> None:
        with self.lock, self.db:
            self.db.execute("UPDATE calls SET retry_at=? WHERE request_id=? AND state='prepared'",
                            (iso(now + timedelta(seconds=seconds)), request_id))

    def postpone_prepared(self, call: dict, now: datetime) -> None:
        """Cancel an unsent intent when a window closed between selection and HTTP I/O."""
        campaign = self.campaigns[call["campaign_id"]]
        next_at = next_campaign_open(now, campaign, self.cfg["regulatory_window"])
        with self.lock, self.db:
            self.db.execute("UPDATE calls SET state='cancelled' WHERE request_id=? AND state='prepared'",
                            (call["request_id"],))
            self.db.execute("""UPDATE leads SET due_at=? WHERE campaign_id=? AND lead_id=?
              AND state='waiting'""", (iso(next_at), call["campaign_id"], call["lead_id"]))

    def accepted(self, call: dict, provider: dict) -> None:
        """Count exactly once.  A webhook is also sufficient proof of acceptance."""
        with self.lock, self.db:
            row = self.db.execute("SELECT accepted FROM calls WHERE request_id=?", (call["request_id"],)).fetchone()
            if not row:
                return
            if not row["accepted"]:
                self.db.execute("UPDATE leads SET attempts=attempts+1 WHERE campaign_id=? AND lead_id=?",
                                (call["campaign_id"], call["lead_id"]))
            self.db.execute("""UPDATE calls SET accepted=1, state='accepted', call_sid=?, retry_at=NULL
              WHERE request_id=? AND state != 'ended'""", (provider.get("call_sid"), call["request_id"]))

    def ingest_result(self, result: dict[str, Any]) -> None:
        """Idempotently transition one ended call, including retry/callback/DND policy."""
        required = {"request_id", "campaign_id", "lead_id", "status", "ended_at"}
        if not required <= result.keys():
            return
        cid, lid, rid = result["campaign_id"], result["lead_id"], result["request_id"]
        if cid not in self.campaigns:
            return
        ended = dt(result["ended_at"])
        if ended is None:
            return
        with self.lock, self.db:
            call = self.db.execute("SELECT * FROM calls WHERE request_id=?", (rid,)).fetchone()
            if not call or call["processed"]:
                return
            if not call["accepted"]:
                self.db.execute("UPDATE leads SET attempts=attempts+1 WHERE campaign_id=? AND lead_id=?", (cid, lid))
            self.db.execute("""UPDATE calls SET accepted=1,state='ended',processed=1,call_sid=?,ended_at=?
              WHERE request_id=?""", (result.get("call_sid"), iso(ended), rid))
            lead = dict(self.db.execute("SELECT * FROM leads WHERE campaign_id=? AND lead_id=?", (cid, lid)).fetchone())
            disposition = result.get("disposition")
            self.db.execute("UPDATE leads SET last_status=?, last_disposition=? WHERE campaign_id=? AND lead_id=?",
                            (result.get("status"), disposition, cid, lid))
            if disposition in TERMINAL:
                self.db.execute("UPDATE leads SET state='completed' WHERE campaign_id=? AND lead_id=?", (cid, lid))
                if disposition == "dnd":
                    self.db.execute("""UPDATE leads SET state='dnd_blocked'
                      WHERE phone=? AND NOT (campaign_id=? AND lead_id=?)
                      AND state NOT IN ('completed','exhausted')""", (lead["phone"], cid, lid))
                return
            attempts = lead["attempts"] + (0 if call["accepted"] else 1)
            campaign = self.campaigns[cid]
            if attempts >= campaign["max_attempts"]:
                self.db.execute("UPDATE leads SET state='exhausted' WHERE campaign_id=? AND lead_id=?", (cid, lid))
                return
            if disposition == "callback_requested" and result.get("callback_at"):
                due, kind = dt(result["callback_at"]), "callback"
            else:
                due, kind = retry_due(ended, result["status"], self.cfg["retry_policy"]), "retry"
            self.db.execute("UPDATE leads SET state='waiting',due_kind=?,due_at=? WHERE campaign_id=? AND lead_id=?",
                            (kind, iso(due), cid, lid))

    def complete(self, output: Path) -> bool:
        with self.lock:
            left = self.db.execute("SELECT COUNT(*) FROM leads WHERE state NOT IN ('completed','exhausted','dnd_blocked')").fetchone()[0]
            inflight = self.db.execute("SELECT COUNT(*) FROM calls WHERE state IN ('prepared','accepted')").fetchone()[0]
            if left or inflight:
                return False
            rows = self.db.execute("""SELECT campaign_id,lead_id,state,attempts,last_status,last_disposition
              FROM leads ORDER BY campaign_id,lead_id""").fetchall()
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["campaign_id", "lead_id", "final_state", "attempts", "last_status", "last_disposition"])
            writer.writerows([[r["campaign_id"], r["lead_id"], r["state"], r["attempts"],
                              r["last_status"] or "", r["last_disposition"] or ""] for r in rows])
        return True

    def status(self) -> dict[str, int]:
        with self.lock:
            return dict(self.db.execute("SELECT state, COUNT(*) FROM leads GROUP BY state").fetchall())


class Dispatcher:
    def __init__(self, store: Store, provider_url: str, callback_url: str, output: Path):
        self.store, self.provider_url, self.callback_url, self.output = store, provider_url.rstrip("/"), callback_url, output

    async def sim_now(self, client: httpx.AsyncClient) -> datetime:
        response = await client.get(f"{self.provider_url}/v1/clock")
        response.raise_for_status()
        return dt(response.json()["sim_now"])  # type: ignore[return-value]

    async def submit(self, client: httpx.AsyncClient, call: dict, now: datetime) -> None:
        if self.store.cancelled(call):
            return
        # Selection is durable but not permission to dial: take a fresh provider-clock reading
        # immediately before I/O so a very fast simulated clock cannot cross a window boundary.
        try:
            now = await self.sim_now(client)
        except httpx.HTTPError:
            self.store.defer(call["request_id"], now)
            return
        if not in_window(now, self.store.campaigns[call["campaign_id"]], self.store.cfg["regulatory_window"]):
            self.store.postpone_prepared(call, now)
            return
        payload = {key: call[key] for key in ("request_id", "campaign_id", "lead_id", "phone")}
        payload["callback_url"] = self.callback_url
        try:
            response = await client.post(f"{self.provider_url}/v1/calls", json=payload)
        except httpx.HTTPError:
            self.store.defer(call["request_id"], now)
            return
        if response.status_code in (200, 201):
            body = response.json()
            self.store.accepted(call, body)
            if body.get("state") == "ended":
                self.store.ingest_result(body["result"])
        elif response.status_code in (503, 504):
            # 504 is deliberately ambiguous: do not create a new intent; reconcile this key later.
            self.store.defer(call["request_id"], now, 15 if response.status_code == 504 else 30)
        else:
            # This should never happen if our local state machine is correct. Preserve the intent
            # for inspection rather than silently discarding a lead.
            self.store.defer(call["request_id"], now, 60)

    async def reconcile(self, client: httpx.AsyncClient, now: datetime) -> None:
        for call in self.store.pending_calls():
            retry_at = dt(call.get("retry_at"))
            if retry_at and retry_at > now:
                continue
            try:
                response = await client.get(f"{self.provider_url}/v1/calls", params={"request_id": call["request_id"]})
            except httpx.HTTPError:
                continue
            if response.status_code == 404:
                if call["state"] == "prepared":
                    await self.submit(client, call, now)
                continue
            if response.status_code != 200:
                continue
            body = response.json()
            self.store.accepted(call, body)
            if body.get("state") == "ended":
                self.store.ingest_result(body["result"])

    async def run(self) -> None:
        async with httpx.AsyncClient(timeout=3.0) as client:
            while not self.store.complete(self.output):
                try:
                    now = await self.sim_now(client)
                    await self.reconcile(client, now)
                    call = self.store.prepare_one(now)
                    if call:
                        await self.submit(client, call, now)
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.12)


def load_config(fixtures: Path) -> tuple[dict, dict, dict]:
    cfg = json.loads(fixtures.read_text())
    return cfg, {c["campaign_id"]: c for c in cfg["campaigns"]}, {t["tenant_id"]: t for t in cfg["tenants"]}


def make_app(fixtures: Path, database: Path, provider_url: str, callback_url: str, output: Path) -> FastAPI:
    cfg, campaigns, tenants = load_config(fixtures)
    store = Store(database, campaigns, tenants, cfg)
    store.import_fixtures(fixtures.parent)
    dispatcher = Dispatcher(store, provider_url, callback_url, output)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = asyncio.create_task(dispatcher.run())
        yield
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    app = FastAPI(title="Durable Campaign Dispatcher", lifespan=lifespan)

    @app.post("/webhooks/telephony")
    async def telephony_webhook(request: Request):
        body = await request.body()
        signature = request.headers.get("X-AU-Signature", "")
        expected = hmac.new(cfg["webhook_secret"].encode(), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise HTTPException(401, "invalid webhook signature")
        try:
            event = json.loads(body)
        except json.JSONDecodeError as exc:
            raise HTTPException(400, "invalid json") from exc
        if event.get("event") == "call.completed":
            store.ingest_result(event)
        return {"ok": True}

    @app.get("/status")
    async def status():
        return {"leads": store.status(), "final_state_written": output.exists()}

    return app
