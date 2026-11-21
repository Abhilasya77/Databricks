SHOPPING SESSIONS — NATIVE EVENTS V3 (EXPERIMENTAL)
====================================================

What this does:
- Adds native_events.cpp, a compiled C++ path for exact event parsing,
  earliest-ingest deduplication, validation, session construction, and user_daily.
- Keeps the existing Python campaign attribution code unchanged.
- Adds pipeline/native_events_bridge.py and a small dispatch change in
  pipeline/first_submission.py.
- Updates Dockerfile to compile native_events using GCC in a builder stage.
  The runtime is still python:3.12-slim and uses linux/amd64 on the scorer.
- Does NOT edit main, your teammate's branch, or the rest of the project.

Validation already performed:
- 36/36 native sessions and user_daily comparisons vs Python baseline PASS.
- 36/36 complete user_daily and campaign_daily comparisons PASS.
- Synthetic 200,000 events + 40,000 clicks: measured 1.4–1.5x improvement
  in full compute_both on the development environment. Synthetic != scoring.
- No official 1M dataset nor Docker runtime here; run these on YOUR Mac.

IMPORTANT: The previous hidden-data submission timed out. This version is
experimental, not a guarantee of passing the hidden 20M or final 60M test.
The current Python campaign code still needs performance work.

SETUP on your Mac:
  cd ~/Databricks
  git switch integration/first-submission
  git status --short
  git switch -c perf/native-events-v3
  unzip -o ~/Downloads/ShoppingSessions_NativeEvents_v3.zip -d .

TEST native fixture integration without Docker:
  c++ -std=c++17 -O3 -DNDEBUG native_events.cpp -o native_events
  NATIVE_EVENTS_BINARY="$PWD/native_events" python3 scripts/check_both_baseline.py

TEST Docker against all official fixtures and 1M development data:
  make fixtures PLATFORM=linux/arm64 CPUS=4 MEM=6g
  make run-dev PLATFORM=linux/arm64 CPUS=4 MEM=6g
  make check-dev

On Apple Silicon, the AMD64 image must also build for official scoring:
  make build PLATFORM=linux/amd64

Do not submit if any test fails. Compare its Docker dev runtime with the
previous baseline (~7.54 seconds). The timing may vary; repeat measurements.

Before committing, ignore the locally compiled binary:
  printf '\n/native_events\n' >> .gitignore
  git add Dockerfile native_events.cpp pipeline/first_submission.py \
      pipeline/native_events_bridge.py scripts/test_native_fixtures.py \
      scripts/bench_native_synthetic.py .gitignore
  git commit -m "Add compiled event-processing backend v3"
  git push -u origin perf/native-events-v3

Full Docker tests are the source of truth. Avoid merging into main until
correctness and scaling are established. Existing c01d47c baseline is kept.
