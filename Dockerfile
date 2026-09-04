# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.95
ARG PYTHON_VERSION=3.12

FROM rust:${RUST_VERSION}-bookworm AS rust-builder

RUN apt-get update \
    && apt-get install --yes --no-install-recommends build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build/tq
COPY tq/ ./

RUN mkdir -p /tmp/tq-test-home \
    && HOME=/tmp/tq-test-home cargo test --locked --release \
    && cargo build --locked --release


FROM python:${PYTHON_VERSION}-slim-bookworm AS python-base

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LABEL_CHECK_CONTAINER=true \
    EASYOCR_FORCE_CPU=true \
    EASYOCR_MODEL_DIR=/opt/easyocr-models \
    HOME=/home/labelcheck \
    TQ_HOME_DIR=/home/labelcheck/.tq

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        libglib2.0-0 \
        libgl1 \
        libgomp1 \
        libopenslide0 \
        libsm6 \
        libxext6 \
        krb5-user \
        unixodbc \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor --yes --output /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
        -o /etc/apt/sources.list.d/microsoft-prod.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install --yes --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --disable-pip-version-check --upgrade pip

COPY requirements.txt ./requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --disable-pip-version-check \
        --require-hashes \
        --requirement requirements.txt

RUN mkdir -p "$EASYOCR_MODEL_DIR" \
    && python -c "import easyocr; easyocr.Reader(['en'], gpu=False, model_storage_directory='$EASYOCR_MODEL_DIR')"

# Source edits should invalidate only these inexpensive layers, not Python
# dependencies or the downloaded OCR models above.
COPY src/ ./src/
COPY tests/ ./tests/
COPY --chmod=0755 container/entrypoint.sh ./container/entrypoint.sh
COPY nightly_label_check.py requirements-test.txt requirements-windows-worker.txt ./

RUN sed -i 's/\r$//' /app/container/entrypoint.sh \
    && /bin/sh -n /app/container/entrypoint.sh


FROM python-base AS test

COPY compose.yaml ./compose.yaml
COPY container/caddy/ ./container/caddy/

RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --disable-pip-version-check \
        --require-hashes \
        --requirement requirements-test.txt \
    && python -m pip check \
    && python -W error::ResourceWarning -m unittest discover \
        --start-directory tests --verbose


FROM python-base AS runtime

COPY --from=rust-builder /build/tq/target/release/tq /app/bin/tq

RUN groupadd --gid 10001 labelcheck \
    && useradd --uid 10001 --gid labelcheck --create-home labelcheck \
    && mkdir -p /data/state/instance /home/labelcheck/.ssh /home/labelcheck/.tq \
    && sha256sum /app/bin/tq > /app/bin/tq.sha256 \
    && chown -R labelcheck:labelcheck /app /data/state /home/labelcheck \
    && sh -c '/app/bin/tq >/dev/null 2>&1; test "$?" -eq 1'

ENV TQ_EXECUTABLE=/app/bin/tq \
    INSTANCE_DIR=/data/state/instance \
    SDL_FILE_PATH=/data/state/Slide_Digitization_Log.xlsx \
    BACKUP_DIR=/data/state/csv_backups \
    SCANNER_INVENTORIES=/data/scanner-inventories \
    LABEL_CHECK_BATCHES=/data/label-check-batches \
    IMAGE_STAGING_ROOT=/data/image-staging \
    COPATH_CLONE=/data/copath-clone \
    TQ_TRANSFER_LOG_DIR=/data/label-check-batches/transfer_logs \
    GT450_IMAGES_CONTAINER_ROOT=/data/gt450-images \
    LABEL_CHECK_BATCHES_CONTAINER_ROOT=/data/label-check-batches \
    COPATH_QUERY_MODE=windows_queue \
    COPATH_QUERY_QUEUE=/data/state/copath-query \
    COPATH_QUERY_TIMEOUT_SECONDS=300 \
    PORT=5000

WORKDIR /app/src
USER labelcheck
EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5000/login', timeout=5)" || exit 1

ENTRYPOINT ["/app/container/entrypoint.sh"]
CMD ["web"]
