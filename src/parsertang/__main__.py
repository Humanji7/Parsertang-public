import asyncio
import sys

from .diagnostics import run as diag_run
from .main import main

# NOTE: --bot mode was REMOVED (2025-12-25)
# The full scanner (main()) includes embedded Telegram bot via SimpleBot.start()
# Standalone bot mode was deprecated due to:
# 1. Missing setup_logging() causing logging silence
# 2. No arbitrage scanning functionality
# 3. Telegram session conflicts with main() mode


if __name__ == "__main__":
    if "--diag" in sys.argv:
        sys.exit(diag_run())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
