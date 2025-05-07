FROM python:3.11-slim

# set unbuffered stdout/stderr
ENV PYTHONUNBUFFERED=1

# set Dagster home
ENV DAGSTER_HOME=/opt/dagster/dagster_home

WORKDIR /app

# Install system packages:
# - Base dependencies for python build/postgres (libpq-dev, gcc)
# - Dependencies for Firefox and geckodriver download/install (wget, ca-certificates, tar)
# - The Firefox browser itself (firefox-esr)
# Then download and install the correct geckodriver for the container's architecture
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libpq-dev \
       gcc \
       wget \
       ca-certificates \
       tar \
       firefox-esr \
    # Determine architecture and download corresponding geckodriver
    # Make sure to check for the latest geckodriver version on GitHub releases!
    && GECKODRIVER_VERSION="0.34.0" \
    && ARCH=$(uname -m) \
    && if [ "$ARCH" = "aarch64" ]; then \
         GECKODRIVER_ARCH="linux-aarch64"; \
       elif [ "$ARCH" = "x86_64" ]; then \
         GECKODRIVER_ARCH="linux64"; \
       else \
         # Add other architectures if needed, or fail
         echo "Unsupported architecture: $ARCH" && exit 1; \
       fi \
    && echo "Downloading geckodriver v${GECKODRIVER_VERSION} for ${GECKODRIVER_ARCH}" \
    && wget --quiet "https://github.com/mozilla/geckodriver/releases/download/v${GECKODRIVER_VERSION}/geckodriver-v${GECKODRIVER_VERSION}-${GECKODRIVER_ARCH}.tar.gz" -O /tmp/geckodriver.tar.gz \
    # Extract geckodriver to /usr/local/bin (which is usually in PATH)
    && tar -C /usr/local/bin -xzf /tmp/geckodriver.tar.gz \
    # Make it executable
    && chmod +x /usr/local/bin/geckodriver \
    # Clean up downloaded files and unnecessary packages
    && rm /tmp/geckodriver.tar.gz \
    && apt-get purge -y --auto-remove wget \
    && rm -rf /var/lib/apt/lists/* \
    # Create Dagster home directory
    && mkdir -p $DAGSTER_HOME

# install Python deps (ensure selenium is in requirements.txt)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy project and config files
COPY . .

# ensure dagster.yaml lives in DAGSTER_HOME
COPY dagster.yaml $DAGSTER_HOME/

# bring in workspace.yaml for multiple code locations
COPY workspace.yaml /app/

EXPOSE 3000

# start Dagit / gRPC server and load your workspace.yaml
CMD ["dagster", "dev", "-h", "0.0.0.0", "-w", "/app/workspace.yaml"]