import sys

if sys.version_info < (3, 10):
    raise RuntimeError(
        "Signal Snitch services require Python 3.10+. "
        "Run: python3 -m amqp_producer"
    )

from amqp_producer.runner import main

if __name__ == "__main__":
    main()
