from enum import StrEnum
class Role(StrEnum):
    ADMIN='admin'
    PLANNER='planner'
    VIEWER='viewer'
class SurveyState(StrEnum):
    RECORDED='recorded'
    EVALUATING='evaluating'
    ASSESSED='assessed'
    SCHEDULED='scheduled'
    RETESTED='retested'
    CLOSED='closed'
    BLOCKED='blocked'
    STALE='stale'
class MissionState(StrEnum):
    DRAFT='draft'
    DISPATCHED='dispatched'
    RETESTED='retested'
    ACCEPTED='accepted'
    SUPERSEDED='superseded'
class JobState(StrEnum):
    QUEUED='queued'
    RUNNING='running'
    EVIDENCE_PENDING='evidence_pending'
    DONE='done'
    FAILED='failed'
