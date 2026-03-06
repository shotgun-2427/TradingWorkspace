from __future__ import annotations

import subprocess
from pathlib import Path


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    subprocess.run(["streamlit", "run", str(root / "src" / "dashboard" / "app.py")], check=True)
