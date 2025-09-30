import os, time
import strawberry
from strawberry.types import Info
from redis import Redis
from pydantic import BaseModel
from validation import sha256_hex, validate_voter_card, validate_fingerprint

STREAM = os.getenv("STREAM", "votes")
GROUP = os.getenv("GROUP", "workers")
PRECHECK_PREFIX = os.getenv("PRECHECK_PREFIX", "voted")

class VoteModel(BaseModel):
    election_id: int
    candidate_id: int
    voter_id: str
    voter_card_hash: str
    fingerprint_hash: str

@strawberry.type
class VoteResult:
    status: str
    id: str

def enqueue(redis: Redis, v: VoteModel) -> str:
    soft_key = f"{PRECHECK_PREFIX}:{v.election_id}"
    # precheck to throttle client duplicates
    redis.sadd(soft_key, v.voter_id)
    redis.expire(soft_key, 86400)
    return redis.xadd(
        STREAM,
        {
            "election_id": str(v.election_id),
            "candidate_id": str(v.candidate_id),
            "voter_id": v.voter_id,
            "voter_card_hash": v.voter_card_hash,
            "fingerprint_hash": v.fingerprint_hash,
            "ts": str(int(time.time())),
        },
        maxlen=200000,
        approximate=True,
    )

@strawberry.type
class Mutation:
    @strawberry.mutation
    def vote(self, info: Info, election_id: int, candidate_id: int,
             voter_id: str, voter_card: str, fingerprint_b64: str) -> VoteResult:
        r: Redis = info.context["redis"]
        if not validate_voter_card(voter_id, voter_card):
            return VoteResult(status="invalid_card", id="")
        if not validate_fingerprint(fingerprint_b64):
            return VoteResult(status="invalid_fingerprint", id="")
        v = VoteModel(
            election_id=election_id,
            candidate_id=candidate_id,
            voter_id=voter_id,
            voter_card_hash=sha256_hex(voter_card),
            fingerprint_hash=sha256_hex(fingerprint_b64),
        )
        msg_id = enqueue(r, v)
        return VoteResult(status="queued", id=msg_id)

schema = strawberry.Schema(mutation=Mutation)
