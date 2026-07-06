from __future__ import annotations

import sys
import uvicorn

from aggregator.app import *
from aggregator.app import app, create_app, IncidentReport, TelemetryEvent

if __name__ == "__main__":
    if sys.version_info < (3, 10):
        raise RuntimeError("Signal Snitch services require Python 3.10+.")
    uvicorn.run(app, host="0.0.0.0", port=8000)
