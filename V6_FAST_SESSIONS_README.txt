Shopping Sessions V6 event-side fast-session path / profiling patch

Apply ONLY on an isolated branch starting from perf/campaign-intervals-v5.
This patch changes ONLY: native_events.cpp, pipeline/native_events_bridge.py,
pipeline/first_submission.py, scripts/docker_run.sh. It does NOT touch your teammate's campaign SQL.

What it does:
- C++ emits the same sessions.jsonl and users.jsonl as V5.
- Python decodes user_daily but avoids decoding millions of session records.
- DuckDB loads native sessions.jsonl directly. No redundant JSON serialization.
- SHOPPING_PROFILE=1 enables stderr-only phase timings.
- Temporary files remain in SCRATCH_DIR until DuckDB is finished.

After unzip into repository root:
  git diff --stat
  python3 -m py_compile pipeline/first_submission.py pipeline/native_events_bridge.py
  make fixtures PLATFORM=linux/arm64 CPUS=4 MEM=6g
  make run-dev PLATFORM=linux/arm64 CPUS=4 MEM=6g && make check-dev
  SHOPPING_PROFILE=1 PLATFORM=linux/arm64 CPUS=4 MEM=6g scripts/docker_run.sh "$HOME/shopping-bench-3m" "$HOME/shopping-bench-3m-v6-out"
  python3 scripts/compare.py "$HOME/shopping-bench-3m-v6-out" "$HOME/shopping-bench-3m-v5-out"

Warnings:
- This patch has been statically checked and g++ compiled in preparation, but must
  pass YOUR local full Docker fixtures and 1M/3M comparisons before submission.
- It is not a promise of any particular time savings on hidden data.
- If slower or incorrect, return to the safe V5 branch.
