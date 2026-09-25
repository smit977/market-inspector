FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --create-home app \
    && chown app:app /app

COPY --chown=app:app pyproject.toml README.md ./
COPY --chown=app:app src/ ./src/
COPY --chown=app:app scripts/ ./scripts/

RUN python -m pip install --editable '.[dev]'

USER app

# Development placeholder; replace with the application entrypoint later.
CMD ["sleep", "infinity"]
