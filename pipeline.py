import subprocess
import sys

steps = ["ingest.py", "transform.py", "schema.py", "model.py"]

for step in steps:
    print(f"\nRunning {step}...")
    result = subprocess.run([sys.executable, step], check=False)
    if result.returncode != 0:
        print(f"Failed at {step}")
        sys.exit(result.returncode)

print("\nPipeline complete.")