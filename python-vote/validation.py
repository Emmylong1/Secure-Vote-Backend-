import hashlib
from typing import Optional

def sha256_hex(v: str) -> str:
    return hashlib.sha256(v.encode("utf-8")).hexdigest()

def validate_voter_card(voter_id: str, voter_card: str) -> bool:
    if not voter_id or not voter_card or len(voter_card) < 6:
        return False
    return True

def validate_fingerprint(fp_template_base64: Optional[str]) -> bool:
    if fp_template_base64 is None:
        return False
    # replace with real matcher. here we just length check
    return len(fp_template_base64) >= 40
