import os, hashlib, time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from redis import Redis
from dotenv import load_dotenv

load_dotenv()

r = Redis.from_url(os.getenv("REDIS_URL"), decode_responses=True)
STREAM = os.getenv("QUEUE_STREAM", "votes")
GROUP = os.getenv("CONSUMER_GROUP", "workers")
SET_PREFIX = os.getenv("DOUBLE_VOTE_SET_PREFIX", "voted")
API_PORT = int(os.getenv("API_PORT", "8080"))

# ensure stream group exists
try:
    r.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
except Exception:
    pass

app = FastAPI(title="Voting API")

class VoteIn(BaseModel):
    election_id: int = Field(..., ge=1)
    candidate_id: int = Field(..., ge=1)
    voter_id: str = Field(..., min_length=1)
    voter_card: str = Field(..., min_length=6)

def hash_card(card: str) -> str:
    return hashlib.sha256(card.encode()).hexdigest()

def validate_voter_card(voter_id: str, voter_card: str) -> bool:
    # replace with real validation
    return len(voter_card) >= 6

@app.get("/healthz")
def health():
    return {"ok": True}

@app.post("/vote")
def enqueue_vote(v: VoteIn):
    if not validate_voter_card(v.voter_id, v.voter_card):
        raise HTTPException(status_code=400, detail="invalid voter card")

    soft_key = f"{SET_PREFIX}:{v.election_id}"
    if r.sismember(soft_key, v.voter_id):
        # soft duplicate hint. final check happens in DB
        pass

    msg = {
        "election_id": str(v.election_id),
        "candidate_id": str(v.candidate_id),
        "voter_id": v.voter_id,
        "voter_card_hash": hash_card(v.voter_card),
        "ts": str(int(time.time())),
    }
    msg_id = r.xadd(STREAM, msg, maxlen=200000, approximate=True)
    r.sadd(soft_key, v.voter_id)
    r.expire(soft_key, 86400)
    return {"status": "queued", "id": msg_id}
