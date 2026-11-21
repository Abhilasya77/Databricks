SHOPPING SESSIONS — OUR PART (EVENTS + SESSIONS) — v1
==================================================

New files in this bundle:
  pipeline/events_sessions.py
  scripts/check_events_sessions.py

WHAT IS IMPLEMENTED
- Reads every event CSV part using the exact restricted CSV dialect.
- Drops malformed lines, preserves literal CR and whitespace, parses quoted fields.
- Keeps the earliest-ingested copy per event_id BEFORE validating the copy.
- Strictly validates event timestamp formats F1/F2/F3, event type, ID,
  product, purchase price and the allowed event time window.
- Rejects late events (> 24 hours after event ts).
- Maps raw event user IDs through the teammate's identity map.
- Creates sessions from each person's events (new session only for gap > 30m).
- Attributes sessions to their UTC start day, even if events cross midnight.
- Builds user_daily rows and purchase-category tie breakers.

HOW TO RUN OUR TESTS (no extra dependencies needed)
  cd ~/Desktop/Buildathon/Databricks
  python3 scripts/check_events_sessions.py

EXPECTED RESULT
  36/36 user_daily fixture comparisons passed

HOW TO USE FROM THE INTEGRATED PIPELINE
  from events_sessions import compute_events_sessions
  sessions, user_rows = compute_events_sessions(data_dir, identity_map)

The teammate's identity_map is dict[str,str]. Its keys are IDs from the links
files and its values are canonical person IDs. Any ID absent from the map is
an unlinked person and defaults to itself.

Each Session object exposes:
  session_id, person_id, session_start_ms, day,
  events, revenue_cents, has_purchase, category_revenue

Each user_rows element has EXACT user_daily columns:
  user_id, day, sessions, events, revenue_cents, top_category

The main integration will write Parquet using DuckDB (DATE for day) and invoke
the teammate's campaign attribution to produce campaign_daily.

IMPORTANT LIMITATIONS
- This is a correctness-focused implementation for our half, not a complete
  runnable contest pipeline. Do not submit it alone to the leaderboard.
- The 36/36 result checks user_daily only. It does not prove campaign_daily,
  dev-answer 1M rows, Docker or hidden scoring correctness.
- This version holds deduped events in memory and sorts in Python. It has NOT
  been benchmarked on 20M or 60M events. Further optimization is required before
  targeting top-three leaderboard times.
- The fixture-only identity resolver in scripts/check_events_sessions.py is
  just a test stub until our teammate's production module is integrated.
