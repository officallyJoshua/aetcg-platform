import json
import logging
import time
import threading
from redis import Redis
from rq import Queue, Worker
from config.settings import settings
from app.metrics import (
    increment_counter,
    observe_histogram,
    TICKET_PROCESSING_TIME_SECONDS,
    TICKET_PROCESSING_TOTAL,
    QUEUE_PROCESSING_JOBS,
    QUEUE_FAILED_JOBS_TOTAL,
    TICKET_RISK_SCORE_DISTRIBUTION,
    COMPLIANCE_PASS_TOTAL,
    COMPLIANCE_FAIL_TOTAL,
    set_gauge,
    QUEUE_DEPTH_JOBS,
    TICKET_INGESTION_TOTAL,
    WORKER_STATUS,
)

logger = logging.getLogger(__name__)

# Use the same Redis connection pattern as in main.py for consistency
redis_connection = Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)
task_queue = Queue("aetcg_jobs", connection=redis_connection)


def update_worker_health_signal():
    """Periodically update worker health signal in Redis to indicate liveness and job processing capability."""
    while True:
        try:
            # Set health signal timestamp (expires in 20 seconds)
            # Updated when worker successfully processes a job or periodically when idle
            redis_connection.set("worker:health_signal", 20, int(time.time()))
            time.sleep(5)  # Update every 5 seconds
        except Exception as e:
            logger.error(f"Failed to update worker health signal: {e}")
            time.sleep(5)  # Wait before retrying

def process_pipeline_task(payload: dict):
    from pipeline.orchestrator import StateMachineOrchestrator
    from rq import get_current_job
    start_time = time.time()

    current_job = get_current_job()
    if current_job:
        payload["job_id"] = current_job.id

    try:
        orchestrator = StateMachineOrchestrator()
        result = orchestrator.execute_workflow(payload)

        # Record successful processing metrics
        processing_time = time.time() - start_time
        observe_histogram(TICKET_PROCESSING_TIME_SECONDS, processing_time, {"status": "success"})
        increment_counter(TICKET_PROCESSING_TOTAL, {"status": "success"})

        # Extract and record business metrics from result
        if isinstance(result, dict):
            # Risk score
            if "risk_score" in result:
                observe_histogram(TICKET_RISK_SCORE_DISTRIBUTION, result["risk_score"])

            # Category (if available)
            # Note: category might be in the triage verdict or elsewhere in a real implementation

            # Compliance check results
            if "critic_feedback" in result:
                # Simplified compliance check - in reality this would be more structured
                if "pass" in result["critic_feedback"].lower() or "compliant" in result["critic_feedback"].lower():
                    increment_counter(COMPLIANCE_PASS_TOTAL)
                else:
                    increment_counter(COMPLIANCE_FAIL_TOTAL)

        # Cache the final state result for API lookup
        redis_connection.set(f"cache:{hash(payload['raw_text'])}", json.dumps(result), ex=600)

        logger.info(json.dumps({
            "event": "task_processed_successfully",
            "processing_time_ms": processing_time * 1000,
            "payload_hash": hash(payload['raw_text'])
        }))
        return result

    except Exception as e:
        # Record failure metrics
        processing_time = time.time() - start_time
        observe_histogram(TICKET_PROCESSING_TIME_SECONDS, processing_time, {"status": "failure"})
        increment_counter(QUEUE_FAILED_JOBS_TOTAL, {"queue_name": "aetcg_jobs", "failure_reason": "processing_error"})
        increment_counter(TICKET_PROCESSING_TOTAL, {"status": "failure"})

        logger.error(json.dumps({
            "event": "task_processing_failed",
            "error": str(e),
            "processing_time_ms": processing_time * 1000,
            "payload_hash": hash(payload['raw_text'])
        }))
        raise

    finally:
        # A completed job does not make the worker unhealthy.
        set_gauge(WORKER_STATUS, 1, {"worker_id": "worker_1"})
        set_gauge(QUEUE_PROCESSING_JOBS, 0, {"queue_name": "aetcg_jobs"})

def queue_triage_job(payload: dict):
    # Record job enqueue metrics
    increment_counter(TICKET_INGESTION_TOTAL, {"status": "queued"})
    job = task_queue.enqueue(process_pipeline_task, payload)

    # Update queue depth gauge (approximate - in practice you'd get this from Redis)
    # For simplicity, we'll increment/decrement based on enqueue/dequeue
    # A more accurate implementation would query Redis for the actual queue length
    set_gauge(QUEUE_DEPTH_JOBS, task_queue.count, {"queue_name": "aetcg_jobs"})

    return job

def main():
    """Main worker entrypoint for running as a background process."""
    # Internal execution target wrapper matching automated Docker environment activations
    logging.basicConfig(level=logging.INFO)
    print(f"[*] Booting AETCG Background Worker Network Layer connecting to Redis on {settings.REDIS_HOST}...")

    # Initialize worker metrics
    set_gauge(WORKER_STATUS, 1, {"worker_id": "worker_1"})
    set_gauge(QUEUE_DEPTH_JOBS, 0, {"queue_name": "aetcg_jobs"})  # Will be updated as jobs come in

    # Start health signal thread to signal worker liveness
    health_signal_thread = threading.Thread(target=update_worker_health_signal, daemon=True)
    health_signal_thread.start()

    worker = Worker([task_queue], connection=redis_connection)
    worker.work()

if __name__ == "__main__":
    main()