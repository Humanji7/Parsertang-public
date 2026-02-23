# Publication Checklist (Public GitHub)

Use this before making the repository public.

## 1) Secret / Sensitive Data Review

- Confirm no real API keys, secrets, passphrases, bot tokens, chat IDs
- Confirm no private hostnames/IPs in docs/scripts/examples
- Confirm no private logs/runtime artifacts are tracked
- Confirm `.env` is not included (only `.env.example`)

## 2) Private Ops Material Review

- Remove or redact:
  - deployment runbooks with private infrastructure details
  - incident logs containing environment-specific identifiers
  - internal planning/checkpoint docs not meant for public readers

## 3) Portfolio Positioning

- README clearly explains:
  - problem
  - architecture
  - engineering challenges solved
  - how to run/tests
- Add one architecture note and one incident/debugging write-up

## 4) Code Readiness

- Run tests (at least a representative subset)
- Ensure `README` commands are valid
- Remove obvious dead files / local artifacts

## 5) GitHub Presentation

- Add repo description + topics (e.g. `python`, `asyncio`, `websocket`, `trading`, `observability`)
- Pin the repo on profile
- Add a short project summary in the top of the README

## 6) Job-Search Ready Package

- Prepare:
  - 1 short architecture summary (EN)
  - 1 incident postmortem / debugging case (EN)
  - 1-2 example fixes with tests
  - short "AI-assisted but engineer-owned" explanation

