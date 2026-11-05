# Linux runner for tests and workers (crash tests rely on POSIX signals).
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[dev]"
COPY . .

CMD ["pytest", "-q"]
