from pathlib import Path
import sys


API_DIR = Path(__file__).resolve().parent / "doris-api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from dc_runtime.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
