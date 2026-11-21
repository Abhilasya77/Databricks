SHOPPING SESSIONS - V9 PARALLEL NATIVE EVENT INGESTION
=====================================================
BASE: integration/native-campaign-v8, commit 44de73b1906166540c118dd7038d62fa099068a8
TARGET: perf/native-parallel-v9 (start clean from V8)
FILES: native_events.cpp and this README

CHANGES
- Splits each event CSV into ~8 MiB byte ranges; threads can process different
  ranges of the SAME file, so the optimization also works with one big file.
- Uses a shared 128-way sharded event_id hash table; duplicates from different
  byte ranges/files meet in the same shard and retain the earliest ingest_ts.
- Each shard uses a mutex; no global lock or large duplicate intermediate maps.
- Preserves the custom CSV dialect: only LF ends lines; CR remains data;
  malformed rows are dropped; quotes wrap only entire fields.
- Deduplication STILL happens before timestamp/price/event validation.
- Keeps the existing V8 user_daily/session outputs and all campaign code intact.
- Uses NCPU for worker count (default <= 8 threads; capped at 16), with
  SHOPPING_NATIVE_THREADS optional override for benchmarking.

INDEPENDENT VALIDATION IN LINUX (NATIVE ENGINE ONLY)
- Compiled V8 and V9 C++17 with -O3 -DNDEBUG -pthread on x86-64.
- Compared V8 vs V9 native JSONL byte-for-byte on 36 official fixtures: PASS.
- Compared V8 vs V9 native JSONL on synthetic 3M events split across two files:
  PASS. V8 5.38s/5.90s; V9 3.83s/3.87s (individual wall-clock runs).
- Compared V8 vs V9 native JSONL on synthetic 3M events as ONE large file:
  PASS. V8 6.07s; V9 3.93s (single runs).
- Native memory in the 3M two-file test was ~590MB V8 vs ~607MB V9.
These are synthetic NATIVE ONLY measurements. Full Docker pipeline speed and
hidden Buildathon correctness/scoring remain unverified until local tests and
an official submission are completed.

ON MAC
1. cd ~/Databricks
2. git branch --show-current   # should be perf/native-parallel-v9
3. unzip -o ~/Downloads/ShoppingSessions_V9_PARALLEL_EVENTS_PATCH.zip -d .
4. Add -pthread to the g++ compiler invocation in Dockerfile, e.g.:
   RUN g++ -std=c++17 -O3 -DNDEBUG -pthread -s /src/native_events.cpp -o /native_events
   Do not replace the entire Dockerfile or change gcc:12-bookworm.
5. make fixtures PLATFORM=linux/arm64 CPUS=4 MEM=6g  # require 36/36
6. make run-dev PLATFORM=linux/arm64 CPUS=4 MEM=6g && make check-dev # require MATCH
7. time env SHOPPING_PROFILE=1 PLATFORM=linux/arm64 CPUS=4 MEM=6g \
     scripts/docker_run.sh "$HOME/shopping-bench-3m" "$HOME/shopping-bench-3m-v9-out"
8. python3 scripts/compare.py "$HOME/shopping-bench-3m-v9-out" \
     "$HOME/shopping-bench-3m-v8-out"   # require MATCH
9. If V9 genuinely improves: commit native_events.cpp, Dockerfile, this README,
   push perf/native-parallel-v9, verify AMD64 build before an official submission.

BEFORE SUBMISSION: If correctness fails, DO NOT submit or merge. Preserve V8 as
our scored fallback at 98.90 seconds (receipt BB-FFA7AA1E).
