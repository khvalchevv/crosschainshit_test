from __future__ import annotations

import itertools
import random
from pathlib import Path

from config import ROOT, get_settings
from utils.logger import get_logger

log = get_logger(__name__)


def _normalize(line: str) -> str:
    """Accept any of:
        http://user:pass@host:port      (already valid — passthrough)
        host:port:user:pass             (webshare — most common)
        user:pass@host:port             (add scheme only)
        host:port                       (add scheme only)
    and return a aiohttp-valid `scheme://[user:pass@]host:port`.
    """
    if "://" in line:
        return line
    if "@" in line:
        return f"http://{line}"
    parts = line.split(":")
    if len(parts) == 4:  # webshare host:port:user:pass
        host, port, user, pwd = parts
        return f"http://{user}:{pwd}@{host}:{port}"
    return f"http://{line}"  # host:port (no auth)


class ProxyManager:
    def __init__(self, proxies: list[str]) -> None:
        self._proxies = proxies
        self._cycle = itertools.cycle(proxies) if proxies else None
        self._bad: set[str] = set()
        log.info("proxy_manager.init", total=len(proxies))

    @classmethod
    def from_file(cls, path: str | Path | None = None) -> "ProxyManager":
        raw = path or get_settings().proxies_file
        fp = Path(raw)
        if not fp.is_absolute():
            fp = (ROOT / fp).resolve()
        if not fp.exists():
            log.warning("proxy_manager.no_file", path=str(fp))
            return cls([])
        proxies = []
        for line in fp.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            proxies.append(_normalize(line))
        return cls(proxies)

    def next(self) -> str | None:
        if not self._proxies or self._cycle is None:
            return None
        for _ in range(len(self._proxies)):
            proxy = next(self._cycle)
            if proxy not in self._bad:
                return proxy
        return None

    def random(self) -> str | None:
        available = [p for p in self._proxies if p not in self._bad]
        return random.choice(available) if available else None

    def mark_bad(self, proxy: str) -> None:
        self._bad.add(proxy)

    def mark_good(self, proxy: str) -> None:
        self._bad.discard(proxy)

    @property
    def total(self) -> int:
        return len(self._proxies)


_pm: ProxyManager | None = None


def get_proxy_manager() -> ProxyManager:
    global _pm
    if _pm is None:
        _pm = ProxyManager.from_file()
    return _pm
