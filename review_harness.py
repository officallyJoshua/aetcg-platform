#!/usr/bin/env python3
"""
review_harness.py - a safe way to probe AETCG without Docker, Redis, or an LLM key.

Place this file in the ROOT of your fork of aetcg-platform, then:

    pip install fakeredis fastapi httpx pydantic pydantic-settings redis rq openai

What it does
------------
It runs the app's REAL code (pipeline/, services/, main.py, worker.py) against an
in-memory fake Redis and a scripted stand-in for the language model. You decide what
the "model" says, then watch what the app does with it. Nothing leaves your machine.

Commands
--------
  scrub  "text"                          run the PII scrubber on text
  schema --email someone@example.com     test the ingestion schema
  orchestrate --text "..." --triage X --critic Y
                                         run the workflow once with scripted model replies
  flow  --ticket-id T-1 --text "..." --triage X --critic Y
                                         run the full API flow: POST /triage -> worker ->
                                         GET /state -> PUT /approve, and print each step

Scripted model replies (--triage / --critic)
--------------------------------------------
  '{"risk_score": 9, "category": "x", "verdict": "v"}'   any text is returned verbatim
  FAIL                                                    the model call raises a timeout
  @reply.txt                                              read the reply from a file

Rules of engagement: local only, no real personal data, never commit secrets.
"""
import argparse, json, os, sys

os.environ.setdefault("LLM_ACCESS_TOKEN", "harness-dummy-token")   # satisfies the OpenAI client
sys.path.insert(0, os.getcwd())

try:
    import fakeredis, redis
except ImportError:
    sys.exit("Missing packages. Run: pip install fakeredis fastapi httpx pydantic pydantic-settings redis rq openai")

import logging
for _noisy in ("httpx", "rq.worker", "rq.queue", "rq.job", "rq"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)      # keep the output readable; the app's own logs stay visible

SERVER = fakeredis.FakeServer()
redis.Redis = lambda *a, **k: fakeredis.FakeRedis(server=SERVER)    # every Redis client shares one fake server


def _reply(spec):
    if spec is None:
        return "FAIL"
    return open(spec[1:]).read() if spec.startswith("@") else spec


class _FakeLLM:
    """Looks enough like the OpenAI client for the app's agents."""
    def __init__(self, triage, critic):
        self.triage, self.critic = _reply(triage), _reply(critic)
        self.chat = type("Chat", (), {"completions": self})()

    def create(self, **kw):
        system = kw["messages"][0]["content"]
        text = self.critic if "risk auditor" in system else self.triage
        if text == "FAIL":
            raise TimeoutError("scripted model failure")
        msg = type("M", (), {"content": text})()
        return type("R", (), {"choices": [type("C", (), {"message": msg})()]})()


def _install_llm(args):
    import services.agents as agents
    llm = _FakeLLM(args.triage, args.critic)
    agents.OpenAI = lambda **kw: llm


def cmd_scrub(args):
    from pipeline.validation import DataSanitizationPipeline
    print(DataSanitizationPipeline().scrub_pii(args.text))


def cmd_schema(args):
    from pipeline.validation import TicketIngestionSchema
    try:
        ok = TicketIngestionSchema(ticket_id="T-0", reporter_email=args.email, raw_text="test")
        print("ACCEPTED:", ok.reporter_email)
    except Exception as e:
        print("REJECTED:", e.errors()[0]["msg"] if hasattr(e, "errors") else e)


def cmd_orchestrate(args):
    _install_llm(args)
    from pipeline.orchestrator import StateMachineOrchestrator
    o = StateMachineOrchestrator()
    try:
        result = o.execute_workflow({"ticket_id": args.ticket_id, "reporter_email": "a@enterprise.com", "raw_text": args.text})
    except Exception as e:
        print(f"WORKFLOW CRASHED: {type(e).__name__}: {e}")
    else:
        print(json.dumps(result, indent=2))
    keys = sorted(k.decode() for k in o.redis_client.keys("*"))
    print("\nRedis keys now:", keys)
    for k in keys:
        print(f"  TTL {k}: {o.redis_client.ttl(k)}  (-1 means never expires)")


def cmd_flow(args):
    _install_llm(args)
    from fastapi.testclient import TestClient
    import main, worker
    from rq import SimpleWorker
    client = TestClient(main.app)

    def show(label, r):
        print(f"\n{label}\n  request : {r.request.method} {r.request.url}\n  response: HTTP {r.status_code}  {r.text[:300]}")
        return r

    body = {"ticket_id": args.ticket_id, "reporter_email": "a@enterprise.com", "raw_text": args.text}
    r = show(f"1) POST /api/v1/triage  ticket_id={args.ticket_id}", client.post("/api/v1/triage", json=body))
    job_id = r.json().get("job_id")
    SimpleWorker([worker.task_queue], connection=worker.redis_connection).work(burst=True, logging_level="WARNING")
    print("\n   (worker ran the queued job)")
    show(f"2) GET /api/v1/state/{job_id}      <- the job_id returned in step 1", client.get(f"/api/v1/state/{job_id}"))
    show(f"3) GET /api/v1/state/{args.ticket_id}   <- the ticket_id you submitted", client.get(f"/api/v1/state/{args.ticket_id}"))
    token = main.settings.SUPERVISOR_TOKEN if args.use_default_token else "wrong-token"
    show(f"4) PUT /api/v1/approve/{args.ticket_id}?action=APPROVE", client.put(f"/api/v1/approve/{args.ticket_id}", params={"action": "APPROVE", "supervisor_token": token}))


def main_cli():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("scrub"); s.add_argument("text"); s.set_defaults(fn=cmd_scrub)
    s = sub.add_parser("schema"); s.add_argument("--email", required=True); s.set_defaults(fn=cmd_schema)
    for name, fn in (("orchestrate", cmd_orchestrate), ("flow", cmd_flow)):
        s = sub.add_parser(name)
        s.add_argument("--ticket-id", default="T-1")
        s.add_argument("--text", default="Several failed logins followed by a successful one from a new location.")
        s.add_argument("--triage", default='{"risk_score": 3, "category": "routine", "verdict": "Looks routine."}')
        s.add_argument("--critic", default='{"audit_passed": true, "feedback": "Fine."}')
        if name == "flow":
            s.add_argument("--use-default-token", action="store_true", help="use the SUPERVISOR_TOKEN from config/settings.py")
        s.set_defaults(fn=fn)
    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main_cli()