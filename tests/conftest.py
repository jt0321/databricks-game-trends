import sys
from pathlib import Path

# Make `src/` importable without installing the package, so the tests are
# runnable straight from a fresh clone via `pytest`.
ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
