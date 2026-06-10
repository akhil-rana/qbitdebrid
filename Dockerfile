# ==============================================================================
# Stage 1: Build stage
# ==============================================================================
FROM python:3.12-slim AS builder

WORKDIR /build

# Prevent Python from writing pyc files to disk and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copy requirements file first to utilize Docker build layer caching
COPY requirements.txt .

# Install dependencies into a separate local directory
RUN pip install --no-cache-dir --user -r requirements.txt

# ==============================================================================
# Stage 2: Final ultra-lean runtime stage
# ==============================================================================
FROM python:3.12-slim AS runner

WORKDIR /app

# Ensure Python doesn't write compilation artifacts and logs are flushed instantly
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Add the non-root local bin path to the executable search path
ENV PATH=/home/qbitdebrid/.local/bin:$PATH

# Add the /app folder to PYTHONPATH so that the local package imports successfully
ENV PYTHONPATH=/app

# Create a secure, non-privileged system group and user
RUN groupadd -g 10001 qbitdebrid && \
    useradd -u 10001 -g qbitdebrid -m -s /bin/bash qbitdebrid

# Copy only the compiled Python packages from the builder stage
COPY --from=builder --chown=qbitdebrid:qbitdebrid /root/.local /home/qbitdebrid/.local

# Copy the application source code into the package namespace
COPY --chown=qbitdebrid:qbitdebrid src/ /app/qbitdebrid

# Switch to the non-root security context
USER qbitdebrid

# Expose the default HTTP seed proxy port
EXPOSE 8593

# Native Python portable healthcheck requiring no external binaries (like curl)
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; import sys; [sys.exit(0) if urllib.request.urlopen('http://127.0.0.1:8593/health', timeout=3).getcode() == 200 else sys.exit(1)]" || exit 1

# Launch the application as a module
CMD ["python", "-m", "qbitdebrid.main"]
