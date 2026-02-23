def evaluate_candidate(*, healthy: bool):
    if not healthy:
        return None
    return {"ok": True}
