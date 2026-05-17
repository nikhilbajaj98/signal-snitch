import sys

if sys.version_info < (3, 10):
    raise RuntimeError(
        "Signal Snitch services require Python 3.10+. "
        "Your `python` is older (often Python 2.7). Run: python3 -m aggregator"
    )

import uvicorn

from aggregator.app import app

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
