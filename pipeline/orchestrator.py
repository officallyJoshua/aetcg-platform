import json
import time
import redis
from pipeline.validation import DataSanitizationPipeline
from services.agents import TriageAgent, ComplianceCriticAgent
from config.settings import settings
from app.metrics import (
    increment_counter,
    observe_histogram,
    TICKET_RISK_SCORE_DISTRIBUTION,
    TICKET_CATEGORY_TOTAL,
    COMPLIANCE_PASS_TOTAL,
    COMPLIANCE_FAIL_TOTAL,
    LLM_API_CALLS_TOTAL,
    LLM_API_DURATION_SECONDS,
    TICKET_PROCESSING_TIME_SECONDS
)

class StateMachineOrchestrator:
    def __init__(self):
        self.scrubber = DataSanitizationPipeline()
        self.triage_agent = TriageAgent()
        self.critic_agent = ComplianceCriticAgent()
        # Use settings for Redis configuration consistently
        self.redis_client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=0
        )

    def execute_workflow(self, raw_job_data: dict) -> dict:
        start_time = time.time()

        # Step 1: Ingestion & Sanitization
        sanitization_start = time.time()
        clean_text = self.scrubber.scrub_pii(raw_job_data["raw_text"])
        sanitization_time = time.time() - sanitization_start

        # State Initialization
        state = {
            "ticket_id": raw_job_data["ticket_id"],
            "sanitized_text": clean_text,
            "risk_score": 0,
            "triage_verdict": "",
            "critic_feedback": "",
            "human_approval": "PENDING",
            "status": "PROCESSING"
        }

        # Step 2: Probabilistic Multi-Agent Analysis Execution Loop
        triage_start = time.time()
        triage_output = self.triage_agent.analyze(state["sanitized_text"])
        triage_time = time.time() - triage_start
        state["risk_score"] = triage_output["risk_score"]
        state["triage_verdict"] = triage_output["verdict"]

        # Record triage metrics
        increment_counter(LLM_API_CALLS_TOTAL, {"agent_name": "triage", "status": "success"})
        observe_histogram(LLM_API_DURATION_SECONDS, triage_time, {"agent_name": "triage"})
        observe_histogram(TICKET_RISK_SCORE_DISTRIBUTION, state["risk_score"])

        # Extract category from verdict if possible (simplified)
        # In a real implementation, the category would be a separate field
        if "category" in triage_output:
            increment_counter(TICKET_CATEGORY_TOTAL, {"category": triage_output["category"]})

        # Step 3: Governance Feedback Iteration Check
        critic_start = time.time()
        critic_output = self.critic_agent.critique(state["triage_verdict"], state["risk_score"])
        critic_time = time.time() - critic_start
        state["critic_feedback"] = critic_output["feedback"]

        # Record critic metrics
        increment_counter(LLM_API_CALLS_TOTAL, {"agent_name": "critic", "status": "success"})
        observe_histogram(LLM_API_DURATION_SECONDS, critic_time, {"agent_name": "critic"})

        # Record compliance metrics based on critic output
        # Simplified - in reality this would be more structured
        if "pass" in critic_output["feedback"].lower() or "compliant" in critic_output["feedback"].lower():
            increment_counter(COMPLIANCE_PASS_TOTAL)
        else:
            increment_counter(COMPLIANCE_FAIL_TOTAL)

        # Step 4: Conditional Routing Gate Based on Risk Thresholds & Critic Governance
        audit_passed = critic_output.get("audit_passed", True)

        if state["risk_score"] >= 7 or not audit_passed:
            state["status"] = "PAUSED_FOR_REVIEW"
            if not audit_passed:
                state["escalation_reason"] = f"CRITIC_DISAGREEMENT: {critic_output.get('feedback', '')}"

            # Store under ticket_id
            self.redis_client.set(f"state:{raw_job_data['ticket_id']}", json.dumps(state))
            # Store under job_id mapping if available
            if "job_id" in raw_job_data:
                self.redis_client.set(f"state:{raw_job_data['job_id']}", json.dumps(state))
            return state

        state["status"] = "COMPLETED"

        # Record total workflow time
        workflow_time = time.time() - start_time
        observe_histogram(TICKET_PROCESSING_TIME_SECONDS, workflow_time, {"status": "success"})

        return state

        # Record total workflow time
        workflow_time = time.time() - start_time
        observe_histogram(TICKET_PROCESSING_TIME_SECONDS, workflow_time, {"status": "success"})

        return state