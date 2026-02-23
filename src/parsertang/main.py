"""Parsertang entry point.

Minimal entry point that delegates to Orchestrator for all application logic.
All state, lifecycle management, and business logic lives in core modules.
"""

from __future__ import annotations

import asyncio
import logging

from parsertang.core.orchestrator import Orchestrator

logger = logging.getLogger(__name__)


async def main() -> None:
    """Main entry point — creates and runs orchestrator.

    Orchestrator handles:
    - Exchange initialization
    - Symbol selection
    - Background task management
    - WebSocket/REST subscription
    - Graceful shutdown
    """
    orchestrator = Orchestrator()
    await orchestrator.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)
        raise
