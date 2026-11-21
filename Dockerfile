# Fast event processing built natively for the requested Docker platform.
# Separate build stage keeps the runtime image small and does not require a
# compiler when the scorer runs the read-only container.
FROM gcc:12-bookworm AS native_builder
COPY native_events.cpp /src/native_events.cpp
RUN g++ -std=c++17 -O3 -DNDEBUG -s /src/native_events.cpp -o /native_events

FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY --from=native_builder /native_events /app/native_events
COPY pipeline/ /app/pipeline/
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SHOPPING_NATIVE_EVENTS=1
WORKDIR /app
CMD ["/app/run.sh"]
