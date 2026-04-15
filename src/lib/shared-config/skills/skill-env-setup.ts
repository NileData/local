import type { SkillDefinition } from "../system-skills.js";

export const SKILL_ENV_SETUP: SkillDefinition = {
  name: "env-setup",
  description: "Nile local environment setup -- Podman, Python venv, and troubleshooting common issues.",
  modes: ["local"],
  content: `# Environment Setup

Guide for setting up and troubleshooting the Nile local environment: Podman containers, Python virtual environments, and common issues.

## Podman Setup

Nile local uses Podman to run PySpark in isolated containers.

### Verify Podman Installation

\`\`\`bash
podman --version
podman machine list
podman machine start  # if not running
\`\`\`

### Common Podman Issues

**Machine not running:**
\`\`\`bash
podman machine init   # first time only
podman machine start
\`\`\`

**Permission denied:**
\`\`\`bash
podman machine stop
podman machine rm
podman machine init --cpus 4 --memory 8192
podman machine start
\`\`\`

**Disk space:**
\`\`\`bash
podman system prune -a   # remove unused images/containers
podman system df          # check disk usage
\`\`\`

## Python Virtual Environment

Nile uses a Python venv for local PySpark execution.

### Create/Recreate Venv

\`\`\`bash
# Default venv location
python3 -m venv ~/.nile/venv
source ~/.nile/venv/bin/activate  # Linux/macOS
# .nile\\venv\\Scripts\\activate   # Windows

pip install pyspark pandas pyarrow
\`\`\`

### Install Extra Libraries

\`\`\`bash
source ~/.nile/venv/bin/activate
pip install geopandas fiona          # for GIS data
pip install openpyxl                  # for Excel export
pip install pyyaml                    # for YAML config files
pip install ofxparse                  # for financial statements
pip install delta-spark               # for Delta Lake tables
\`\`\`

### Verify PySpark

\`\`\`bash
source ~/.nile/venv/bin/activate
python -c "import pyspark; print(pyspark.__version__)"
\`\`\`

## Troubleshooting

### "Java not found"

PySpark requires Java 11 or 17:
\`\`\`bash
java -version
# Install if missing:
# macOS: brew install openjdk@17
# Ubuntu: sudo apt install openjdk-17-jdk
# Windows: download from adoptium.net
\`\`\`

Set JAVA_HOME:
\`\`\`bash
export JAVA_HOME=/usr/lib/jvm/java-17  # adjust path
\`\`\`

### "Port already in use"

\`\`\`bash
# Find process using the port
lsof -i :4040   # Spark UI port
# Kill if needed
kill -9 <PID>
\`\`\`

### "Out of memory"

Increase Spark driver memory in your transform:
\`\`\`python
# This is configured by Nile automatically, but for reference:
# spark.conf.set("spark.driver.memory", "4g")
\`\`\`

### Container Networking

If containers cannot reach the internet (for web imports):
\`\`\`bash
podman machine ssh -- cat /etc/resolv.conf   # check DNS
podman run --rm alpine ping -c 1 google.com  # test connectivity
\`\`\`

## File Locations

| Path | Purpose |
|------|---------|
| \`~/.nile/\` | Nile home directory |
| \`~/.nile/venv/\` | Python virtual environment |
| \`~/.nile/operations/\` | Operation logs and results |
| \`~/.nile/operations/results/\` | Exported files |
| \`~/.nile/warehouse/\` | Local Iceberg warehouse |
`,
};
