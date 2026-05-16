from __future__ import annotations

import asyncio
import signal
import sys

from core import (
    Alerter,
    CrossChainDetector,
    CrossChainMonitor,
    CrossChainOpportunity,
    TokenMapper,
)
from utils import close_redis, get_logger, get_redis, setup_logging

setup_logging()
log = get_logger("main")


class CrossChainScanner:
    def __init__(self) -> None:
        self._mapper   = TokenMapper()
        self._alerter  = Alerter()
        self._monitor  = CrossChainMonitor()
        self._detector = CrossChainDetector(
            on_opportunity=self._on_opportunity,
            cycle_trigger=self._monitor.cycle_done_event,
        )
        self._tasks: list[asyncio.Task] = []
        self._running = False

    async def start(self) -> None:
        log.info("scanner.starting")
        await get_redis()
        await self._alerter.setup()

        log.info("scanner.refreshing_registry")
        stats = await self._mapper.refresh()
        log.info("scanner.registry_ready", **stats)

        asyncio.create_task(self._periodic_refresh_loop(), name="registry_refresh")

        self._running = True
        self._tasks = [
            asyncio.create_task(self._monitor.start(),  name="monitor"),
            asyncio.create_task(self._detector.start(), name="detector"),
        ]
        log.info("scanner.ready")
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        log.info("scanner.stopping")
        for t in self._tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await asyncio.gather(
            self._monitor.stop(),
            self._monitor.close(),
            self._detector.stop(),
            self._detector.close(),
            self._alerter.close(),
            return_exceptions=True,
        )
        await close_redis()
        log.info("scanner.stopped")

    async def _on_opportunity(self, opp: CrossChainOpportunity) -> None:
        try:
            await self._alerter.send(opp)
        except Exception as e:
            log.error("scanner.alert_error", err=str(e))

    async def _periodic_refresh_loop(self) -> None:
        """Re-refresh the registry every 24h so cg2:group:* never decays
        from the 7-day TTL."""
        while self._running:
            await asyncio.sleep(24 * 3600)
            try:
                log.info("scanner.periodic_refresh.start")
                stats = await self._mapper.refresh(force=True)
                log.info("scanner.periodic_refresh.done", **stats)
            except Exception as e:
                log.warning("scanner.periodic_refresh.err", err=str(e)[:120])


async def main() -> None:
    scanner = CrossChainScanner()
    loop = asyncio.get_running_loop()

    def _handle_signal(sig: signal.Signals) -> None:
        log.info("scanner.signal_received", signal=sig.name)
        asyncio.create_task(scanner.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig)
        except NotImplementedError:
            signal.signal(sig, lambda s, f: asyncio.create_task(scanner.stop()))

    try:
        await scanner.start()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error("scanner.fatal", err=str(e))
        sys.exit(1)
    finally:
        await scanner.stop()


if __name__ == "__main__":
    asyncio.run(main())
