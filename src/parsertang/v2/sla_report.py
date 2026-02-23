def format_sla_report(*, healthy_ratio: float, fresh_ratio_min: float) -> str:
    return f"V2 SLA: {healthy_ratio*100:.0f}% healthy, min fresh {fresh_ratio_min:.2f}"
