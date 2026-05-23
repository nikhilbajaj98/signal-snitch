"""HTTP aggregator / telemetry API for Signal Snitch."""

import sys

if sys.version_info < (3, 10):
    raise RuntimeError(
        "Signal Snitch services require Python 3.10+. "
        "Your `python` is older (often Python 2.7). Run: python3 -m aggregator"
    )

def __getattr__(name: str):
    if name == "app":
        from aggregator.app import app

        return app
    raise AttributeError(f"module 'aggregator' has no attribute '{name}'")


__all__ = ["app"]
