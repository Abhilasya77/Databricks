SHOPPING SESSIONS - V7 COMPACT NATIVE EVENTS
===========================================
BASE: perf/native-fast-sessions-v6, commit ef5f89c05697079eb9b978c5eb78d359ca4dc395
FILES: native_events.cpp (drop-in replacement) and this README.
No DuckDB campaign changes, no changes to the existing session-file bridge.

WHAT CHANGED
1. Replaced event-field allocations with std::string_view (strict custom CSV parsing retained).
2. Replaced per-event Raw.ingest string with offsets into the retained raw line.
3. Reduced duplicate lookup to one hash-table insertion/search (try_emplace).
4. Interned canonical person IDs and purchase categories into numeric IDs.
5. Stored compact numeric event records, with canonical person bytewise sort ranks.
6. Kept session and user_daily JSONL output format exactly unchanged.

INDEPENDENT CHECKS ALREADY PERFORMED
- Compiled both V6 and V7 with c++ -std=c++17 -O3 -DNDEBUG on Linux x86-64.
- Compared both native output tables on all 36 official fixture input datasets: zero mismatches.
- Compared V6/V7 native JSONL files byte-for-byte on a synthetic 3M-event dataset: identical.
- Synthetic 3M native engine: V6 7.77s / 912168 KiB peak RSS; V7 5.54s / 599004 KiB peak RSS.
  These are native-only synthetic benchmarks, NOT an official/hidden score or a
  representative guaranteed speedup for the full Docker pipeline.

ON YOUR MAC (DO NOT CHANGE SCORED V5 OR THE V6 BRANCH)
1. cd ~/Databricks
2. git switch perf/native-fast-sessions-v6
3. git switch -c perf/native-compact-events-v7
4. unzip -o ~/Downloads/ShoppingSessions_V7_COMPACT_EVENTS_PATCH.zip -d .
5. git diff --stat
6. make fixtures PLATFORM=linux/arm64 CPUS=4 MEM=6g
7. make run-dev PLATFORM=linux/arm64 CPUS=4 MEM=6g && make check-dev
8. time env SHOPPING_PROFILE=1 PLATFORM=linux/arm64 CPUS=4 MEM=6g \
     scripts/docker_run.sh "$HOME/shopping-bench-3m" "$HOME/shopping-bench-3m-v7-out"
9. python3 scripts/compare.py "$HOME/shopping-bench-3m-v7-out" "$HOME/shopping-bench-3m-v6-out"

IF the full pipeline matches and improves, commit:
  git add native_events.cpp V7_COMPACT_EVENTS_README.txt
  git commit -m "Compact native event records and optimize deduplication v7"
  git push -u origin perf/native-compact-events-v7

For submission to official hidden practice, ALSO run:
  make build PLATFORM=linux/amd64
  docker image inspect shopping-sessions:latest --format '{{.Os}}/{{.Architecture}}'
  git rev-parse HEAD

Any mismatch: DO NOT commit/submit; share failing output and restore V6.
Keep untracked V4_INTEGRATION_README.txt out of Git.
