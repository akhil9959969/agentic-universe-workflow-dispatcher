# AI Usage Disclosure

AI tools are allowed. We want to know **how** you used them, because using them well is a
skill. Honest answers are never held against you. Discovering undisclosed use is.

## Tools used
ChatGPT / Codex

## What I used them for
I used it to scaffold the FastAPI/SQLite implementation, draft focused scheduling tests, and
review crash-recovery paths around the carrier idempotency key.

## What I wrote or decided myself
I selected the database-backed state machine, reviewed the state transitions and failure cases,
and chose the strict-window and final-callback policies documented in `SUBMISSION.md`.

## Where the AI was wrong or unhelpful
An early draft treated every completed call as terminal. I rejected that because
`callback_requested` is a completed carrier call but must be scheduled again; the assignment's
retry/callback rules made the mistake clear.

## How I verified AI-generated code
I read the implementation, ran `pytest -q`, and ran the dispatcher against the supplied mock,
checking the provider's report and lead-state endpoints.
