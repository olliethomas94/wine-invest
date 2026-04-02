import subprocess
import sys

steps = [
    ["python3", "scripts/init_db_safe.py"],
    ["python3", "scripts/download_bbx.py"],
    ["python3", "scripts/import_snapshot.py"],
    ["python3", "scripts/build_market_stats.py"],
]

for step in steps:
    print("\nRunning:", " ".join(step))
    result = subprocess.run(step)
    if result.returncode != 0:
        sys.exit(result.returncode)

print("\nPipeline complete")
