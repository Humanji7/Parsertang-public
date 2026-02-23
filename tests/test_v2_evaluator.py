from parsertang.v2.evaluator import evaluate_candidate


def test_evaluator_returns_none_when_unhealthy():
    assert evaluate_candidate(healthy=False) is None
