import time
import logging
import json
import re
import threading
from contextlib import asynccontextmanager
from typing import Dict
from fastapi import FastAPI, HTTPException, BackgroundTasks, status, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from config.settings import settings
from pipeline.validation import TicketIngestionSchema
from worker import queue_triage_job
import redis
from app.metrics import (
    REGISTRY,
    increment_counter,
    observe_histogram,
    set_gauge,
    HTTP_REQUESTS_TOTAL,
    HTTP_REQUEST_DURATION_SECONDS,
    HTTP_REQUEST_SIZE_BYTES,
    HTTP_RESPONSE_SIZE_BYTES,
    TICKET_INGESTION_TOTAL,
    TICKET_PROCESSING_TIME_SECONDS,
    QUEUE_DEPTH_JOBS,
    QUEUE_PROCESSING_JOBS,
    QUEUE_FAILED_JOBS_TOTAL,
    WORKER_STATUS,
    CACHE_HIT_RATIO,
    CACHE_HITS_TOTAL,
    CACHE_MISSES_TOTAL,
    LLM_API_CALLS_TOTAL,
    LLM_API_DURATION_SECONDS,
    LLM_API_TOKEN_USAGE,
    TICKET_RISK_SCORE_DISTRIBUTION,
    TICKET_CATEGORY_TOTAL,
    HUMAN_APPROVAL_WAIT_TIME_SECONDS,
    TICKET_APPROVAL_TOTAL,
    TICKET_REJECTION_TOTAL,
    COMPLIANCE_PASS_TOTAL,
    COMPLIANCE_FAIL_TOTAL,
    REDIS_CONNECTED,
    REDIS_LATENCY_SECONDS,
    EXTERNAL_API_AVAILABLE,
    EXTERNAL_API_LATENCY_SECONDS,
    MetricsMiddleware
)
set_gauge(EXTERNAL_API_AVAILABLE, 1)

# Milestone 4: Structured JSON Telemetry Configurations
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("aetcg_telemetry")


def check_worker_heartbeat():
    """Periodically check worker heartbeat and update WORKER_STATUS gauge."""
    while True:
        try:
            # Check if worker has sent a heartbeat recently
            heartbeat_timestamp = redis_client.get("worker:health_signal")
            if heartbeat_timestamp is not None:
                last_heartbeat = int(heartbeat_timestamp)
                # Consider worker healthy if heartbeat is within last 15 seconds
                # (allowing for 5-second heartbeat interval plus some buffer)
                if time.time() - last_heartbeat < 15:
                    set_gauge(WORKER_STATUS, 1, {"worker_id": "worker_1"})
                else:
                    set_gauge(WORKER_STATUS, 0, {"worker_id": "worker_1"})
            else:
                # No heartbeat found - worker may not be running
                set_gauge(WORKER_STATUS, 0, {"worker_id": "worker_1"})
            time.sleep(3)  # Check every 3 seconds
        except Exception as e:
            logger.error(f"Error checking worker heartbeat: {e}")
            set_gauge(WORKER_STATUS, 0, {"worker_id": "worker_1"})
            time.sleep(3)  # Wait before retrying


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background thread to monitor worker heartbeat."""
    heartbeat_thread = threading.Thread(target=check_worker_heartbeat, daemon=True)
    heartbeat_thread.start()
    logger.info("[*] Started worker heartbeat monitor")
    try:
        yield
    finally:
        logger.info("[*] Worker heartbeat monitor shutting down")


app = FastAPI(
    title="Autonomous Enterprise Triage & Compliance Guard (AETCG)",
    version="1.0.0-rc1",
    lifespan=lifespan,
)

# Add CORS middleware with environment-based configuration
origins = ["*"] if settings.APP_ENV == "development" else ["https://yourdomain.com"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT"],
    allow_headers=["*"],
)

# Add metrics middleware if enabled
if settings.METRICS_ENABLED:
    app.add_middleware(MetricsMiddleware)

# Milestone 6: Redis Connection Pool for In-Memory Caching
redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)

# Update Redis connection status on initialization
try:
    redis_client.ping()
    set_gauge(REDIS_CONNECTED, 1)
except Exception:
    set_gauge(REDIS_CONNECTED, 0)

# Simple in-memory request tracking for rate limiting (use Redis in production)
_request_counts: Dict[str, list] = {}

def check_rate_limit(request: Request) -> bool:
    """Basic rate limiting by IP address"""
    if not settings.RATE_LIMIT_REQUESTS_PER_MINUTE:
        return True

    client_ip = request.client.host if request.client else "unknown"
    current_time = time.time()

    # Clean old requests (older than 1 minute)
    if client_ip in _request_counts:
        _request_counts[client_ip] = [
            req_time for req_time in _request_counts[client_ip]
            if current_time - req_time < 60
        ]
    else:
        _request_counts[client_ip] = []

    # Check if rate limit exceeded
    if len(_request_counts[client_ip]) >= settings.RATE_LIMIT_REQUESTS_PER_MINUTE:
        return False

    # Add current request
    _request_counts[client_ip].append(current_time)
    return True

# Metrics endpoint
@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    if not settings.METRICS_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Metrics endpoint is disabled"
        )
    from app.metrics import get_metrics
    return Response(content=get_metrics(), media_type="text/plain")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    if not settings.METRICS_ENABLED:
        # Return basic health check even if metrics are disabled
        return JSONResponse(content={"status": "healthy", "timestamp": time.time()})

    from app.metrics import REDIS_CONNECTED, EXTERNAL_API_AVAILABLE
    from psutil import Process
    import psutil

    redis_value = REDIS_CONNECTED._value.get()
    api_value = EXTERNAL_API_AVAILABLE._value.get()

    health_status = {
        "status": "healthy",
        "timestamp": time.time(),
        "checks": {
            "redis": {
                "status": "healthy" if redis_value == 1 else "unhealthy",
                "value": redis_value
            },
            "external_api": {
                "status": "healthy" if api_value == 1 else "unhealthy",
                "value": api_value
            }
        }
    }

    # Add process metrics if psutil is available
    try:
        process = Process()
        health_status["checks"]["process"] = {
            "status": "healthy",
            "memory_mb": process.memory_info().rss / 1024 / 1024,
            "cpu_percent": process.cpu_percent()
        }
    except Exception:
        health_status["checks"]["process"] = {
            "status": "unknown",
            "error": "psutil not available"
        }

    # Determine overall status
    redis_healthy = REDIS_CONNECTED._value.get() == 1
    api_healthy = EXTERNAL_API_AVAILABLE._value.get() == 1

    if not (redis_healthy and api_healthy):
        health_status["status"] = "unhealthy"
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=health_status
        )

    return JSONResponse(content=health_status)

# Dashboard endpoint
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Serve the monitoring dashboard"""
    if not settings.METRICS_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dashboard endpoint is disabled"
        )

    try:
        with open("app/static/dashboard/index.html", "r") as f:
            content = f.read()
        return HTMLResponse(content=content)
    except FileNotFoundError:
        return HTMLResponse(
            content="<h1>Dashboard not found</h1><p>Please ensure the dashboard files are properly installed.</p>",
            status_code=404
        )

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Middleware to enforce rate limiting"""
    # Skip rate limiting for health checks, metrics, or dashboard endpoints if needed
    if request.url.path in ["/health", "/metrics", "/dashboard"]:
        return await call_next(request)

    if not check_rate_limit(request):
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"detail": "Rate limit exceeded"}
        )

    response = await call_next(request)
    return response

@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """Add security headers to responses"""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    # In production, add HSTS: response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response

@app.post("/api/v1/triage", status_code=status.HTTP_202_ACCEPTED)
async def ingest_ticket(request: Request, payload: TicketIngestionSchema):
    start_time = time.time()
    payload_hash = hash(payload.raw_text)

    # Extract request information for metrics and logging
    client_ip = request.client.host if request.client else "unknown"
    method = request.method
    endpoint = request.url.path
    http_version = request.scope.get("http_version", "1.1")

    # Milestone 6: Optimization / Query Caching Layer
    cached_result = redis_client.get(f"cache:{payload_hash}")
    if cached_result:
        # Record cache hit metrics
        increment_counter(CACHE_HITS_TOTAL, {"cache_type": "redis"})
        observe_histogram(REDIS_LATENCY_SECONDS, time.time() - start_time, {"operation": "get"})
        logger.info(json.dumps({
            "event": "cache_hit",
            "latency_ms": (time.time() - start_time) * 1000,
            "client_ip": client_ip
        }))
        return JSONResponse(content=json.loads(cached_result), status_code=status.HTTP_200_OK)
    else:
        # Record cache miss
        increment_counter(CACHE_MISSES_TOTAL, {"cache_type": "redis"})

    # Additional input sanitization beyond PII scrubbing
    # Remove potential script tags or dangerous HTML (basic XSS protection)
    if isinstance(payload.raw_text, str):
        # Remove script tags and their contents
        clean_text = re.sub(r'<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>', '', payload.raw_text, flags=re.IGNORECASE)
        # Remove on* attributes (onclick, onload, etc.)
        clean_text = re.sub(r'on\w+\s*=\s*["\'][^"\']*["\']', '', clean_text, flags=re.IGNORECASE)
        # Update payload with sanitized text if changes were made
        if clean_text != payload.raw_text:
            payload.raw_text = clean_text
            # Re-hash the sanitized payload
            payload_hash = hash(payload.raw_text)
            # Check cache again with sanitized payload
            cached_result = redis_client.get(f"cache:{payload_hash}")
            if cached_result:
                # Record cache hit after sanitization
                increment_counter(CACHE_HITS_TOTAL, {"cache_type": "redis"})
                observe_histogram(REDIS_LATENCY_SECONDS, time.time() - start_time, {"operation": "get"})
                logger.info(json.dumps({
                    "event": "cache_hit_after_sanitization",
                    "latency_ms": (time.time() - start_time) * 1000,
                    "client_ip": client_ip
                }))
                return json.loads(cached_result)
        else:
            # Record cache miss after sanitization check
            increment_counter(CACHE_MISSES_TOTAL, {"cache_type": "redis"})

    try:
        # Milestone 4: Enqueue task asynchronously to decouple runtime processing
        job = queue_triage_job(payload.model_dump())

        # Record ingestion metrics
        increment_counter(TICKET_INGESTION_TOTAL, {"status": "success"})
        observe_histogram(HTTP_REQUEST_DURATION_SECONDS, time.time() - start_time,
                         {"method": method, "endpoint": endpoint})

        logger.info(json.dumps({
            "event": "ticket_ingested_and_queued",
            "job_id": job.id,
            "latency_ms": (time.time() - start_time) * 1000,
            "client_ip": client_ip
        }))
        return {"status": "queued", "job_id": job.id, "message": "Ticket verification initiated."}

    except Exception as e:
        # Record ingestion failure metrics
        increment_counter(TICKET_INGESTION_TOTAL, {"status": "failure"})
        increment_counter(HTTP_REQUESTS_TOTAL,
                         {"method": method, "endpoint": endpoint, "status_code": "500", "http_version": http_version})
        logger.error(json.dumps({
            "event": "ingestion_failure",
            "error": str(e),
            "client_ip": client_ip
        }))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal messaging backbone unreached."
        )

# State Check Endpoint
@app.get("/api/v1/state/{job_id}")
async def get_state(job_id: str):
    # Input validation for job_id
    if not job_id or not isinstance(job_id, str) or len(job_id) > 100:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid job ID format")

    start_time = time.time()
    state_key = f"state:{job_id}"
    ticket_state = redis_client.get(state_key)
    observe_histogram(REDIS_LATENCY_SECONDS, time.time() - start_time, {"operation": "get"})

    if not ticket_state:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="State vector context unrecovered.")

    return json.loads(ticket_state)


# Milestone 5: Human-In-The-Loop Interactive Webhook Routing
@app.put("/api/v1/approve/{job_id}", status_code=status.HTTP_200_OK)
async def administrative_approval_gate(request: Request, job_id: str, action: str, supervisor_token: str):
    # Input validation
    if not job_id or not isinstance(job_id, str) or len(job_id) > 100:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid job ID format")

    if action not in ["APPROVE", "REJECT"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid action. Must be APPROVE or REJECT")

    start_time = time.time()

    # Use token from settings instead of hardcoded value
    if supervisor_token != settings.SUPERVISOR_TOKEN:
        logger.warning(json.dumps({
            "event": "unauthorized_gate_access_attempt",
            "job_id": job_id,
            "client_ip": request.client.host if request.client else "unknown"
        }))
        increment_counter(HTTP_REQUESTS_TOTAL,
                         {"method": "PUT", "endpoint": "/api/v1/approve/{job_id}", "status_code": "403", "http_version": "1.1"})
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Unauthorized gate access.")

    # Simulate loading state container from database/Redis cache and advancing graph
    # Simulate loading state container from database/Redis cache and advancing graph
    state_key = f"state:{job_id}"
    cached_state = redis_client.get(state_key)
    if not cached_state:
        # Fallback to secondary lookup if client provided ticket_id
        cached_state = redis_client.get(f"state:{job_id}")
    observe_histogram(REDIS_LATENCY_SECONDS, time.time() - start_time, {"operation": "get"})

    if not cached_state:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="State vector context unrecovered.")

    state = json.loads(cached_state)
    state["human_approval"] = action
    state["status"] = "COMPLETED" if action == "APPROVE" else "REJECTED"

    redis_client.set(state_key, json.dumps(state))

    # Record approval/rejection metrics
    if action == "APPROVE":
        increment_counter(TICKET_APPROVAL_TOTAL)
    else:
        increment_counter(TICKET_REJECTION_TOTAL)

    # Record human approval wait time (simplified - in practice you'd track when the job was queued for approval)
    observe_histogram(HUMAN_APPROVAL_WAIT_TIME_SECONDS, time.time() - start_time)

    logger.info(json.dumps({
        "event": "human_gate_resolved",
        "job_id": job_id,
        "decision": action
    }))
    return {"status": "state_resumed", "final_disposition": state["status"]}
