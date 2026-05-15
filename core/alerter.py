from __future__ import annotations

import asyncio

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    CallbackQuery,
)

from config import get_settings
from core.bridge_sources import get_bridges_for
from core.detector import CrossChainOpportunity
from core.monitor import get_price
from utils import get_logger, get_redis

log = get_logger(__name__)

import datetime as _dt
import re

_SUBSCRIBERS_KEY = "cc2_subscribers"


def _now_kyiv() -> str:
    """Current time in UTC+3 (Kyiv)."""
    return (_dt.datetime.utcnow() + _dt.timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S UTC+3")
_BLACKLIST_KEY   = "cc2_blacklist"     # set of lowercase token identifiers (symbol or cg_id)


def _strip_html(s: str) -> str:
    """Remove HTML tags — fallback when Telegram rejects HTML formatting."""
    return re.sub(r"<[^>]+>", "", s)

_EXPLORER = {
    "ethereum":  "https://etherscan.io/address/{}",
    "bsc":       "https://bscscan.com/address/{}",
    "polygon":   "https://polygonscan.com/address/{}",
    "arbitrum":  "https://arbiscan.io/address/{}",
    "base":      "https://basescan.org/address/{}",
    "optimism":  "https://optimistic.etherscan.io/address/{}",
    "avalanche": "https://snowtrace.io/address/{}",
    "fantom":    "https://ftmscan.com/address/{}",
    "zksync":    "https://explorer.zksync.io/address/{}",
    "linea":     "https://lineascan.build/address/{}",
    "blast":     "https://blastscan.io/address/{}",
    "scroll":    "https://scrollscan.com/address/{}",
    "mantle":    "https://explorer.mantle.xyz/address/{}",
    "berachain": "https://berascan.com/address/{}",
    "solana":    "https://solscan.io/token/{}",
    "sui":       "https://suivision.xyz/coin/{}",
    "aptos":     "https://explorer.aptoslabs.com/coin/{}",
    "tron":      "https://tronscan.org/#/token20/{}",
    "near":      "https://nearblocks.io/token/{}",
}


def _dexscreener_url(chain: str, addr: str) -> str:
    return f"https://dexscreener.com/{chain}/{addr}"


def _gmgn_url(chain: str, addr: str) -> str:
    """GMGN supports sol/eth/base/bsc/tron — for others fall back to DS."""
    slug = {
        "solana": "sol", "ethereum": "eth", "base": "base",
        "bsc": "bsc", "tron": "tron",
    }.get(chain)
    return f"https://gmgn.ai/{slug}/token/{addr}" if slug else _dexscreener_url(chain, addr)


def _dextools_url(chain: str, addr: str) -> str:
    slug = {
        "ethereum": "ether", "bsc": "bnb", "polygon": "polygon",
        "arbitrum": "arbitrum", "base": "base", "optimism": "optimism",
        "avalanche": "avalanche", "fantom": "fantom",
    }.get(chain, chain)
    return f"https://www.dextools.io/app/en/{slug}/pair-explorer/{addr}"


# ── Format helpers (module-level so /check can reuse) ─────────────────────────

def _fmt_money(v: float | None) -> str:
    if v is None:        return "—"
    if v >= 1_000_000:   return f"${v/1_000_000:.2f}M"
    if v >= 1_000:       return f"${v/1_000:.1f}k"
    return f"${v:,.0f}"


def _fmt_price(p: float) -> str:
    """Compact price keeping significant digits."""
    if p is None or p <= 0:  return "—"
    if p >= 1000:            return f"{p:,.2f}"
    if p >= 1:               return f"{p:.4g}"
    if p >= 0.0001:          return f"{p:.4g}"
    return f"{p:.6g}"


def _liq_emoji(v: float | None) -> str:
    if v is None:        return "❓"
    if v < 5_000:        return "🔴"
    if v < 50_000:       return "🟠"
    if v < 200_000:      return "🟡"
    if v < 1_000_000:    return "🟢"
    return "💎"


def _spread_emoji(s: float) -> str:
    """Return emoji based on spread magnitude."""
    if s >= 200:  return "⚠️"   # suspicious — likely ghost pool
    if s >= 30:   return "💥"   # explosive
    if s >= 15:   return "🔥"   # hot
    return "👍"                  # ok / small


def _trim_addr(addr: str) -> str:
    """For tight inline display; full addr stays in <code> for copy."""
    if not addr:           return "—"
    if len(addr) <= 16:    return addr
    return f"{addr[:6]}...{addr[-4:]}"


def _chain_cell(url: str, chain: str, width: int = 12) -> str:
    """Render '<a>CHAIN</a>{padding}' — link only on the letters, not trailing spaces."""
    name = chain.upper()[:width]
    pad  = " " * max(0, width - len(name))
    return f"<a href='{url}'>{name}</a>{pad}"


def _build_alert_text(
    opp: "CrossChainOpportunity",
    bridges: list[str],
    other_chains: list[tuple[str, str, float | None, float | None]] | None = None,
) -> str:
    """Alert layout: header → route → bridges → unified per-chain block w/ contracts."""
    emoji = _spread_emoji(opp.gross_spread_pct)
    cheap_url = _dexscreener_url(opp.cheap_chain, opp.cheap_addr)
    exp_url   = _dexscreener_url(opp.expensive_chain, opp.expensive_addr)

    parts = []

    # Header
    parts.append(
        f"{emoji}  <b>{opp.symbol}</b>      "
        f"▸  <b><u>+{opp.gross_spread_pct:.2f}%</u></b>  ◂\n"
        f"<i>{_now_kyiv()}</i>"
    )

    # Route
    parts.append(
        f"<b>Route:</b> "
        f"<a href='{cheap_url}'>{opp.cheap_chain.upper()}</a>"
        f" → "
        f"<a href='{exp_url}'>{opp.expensive_chain.upper()}</a>"
    )

    # Bridges
    if bridges:
        pretty = " · ".join(b.upper() for b in bridges)
        parts.append(f"<b>Bridges:</b> <i>{pretty}</i>")

    # Thin-liq warning
    if (opp.cheap_liq_usd is not None and opp.cheap_liq_usd < 5000) or \
       (opp.expensive_liq_usd is not None and opp.expensive_liq_usd < 5000):
        parts.append("⚠️ <b>Thin liquidity — risky execution</b>")

    # Unified per-chain section: every chain in the group gets
    #   <emoji role> <chain link>  <price>  <liq>  <vol>
    #   <contract>
    # Order: BUY, SELL, then others (by price asc)
    def _row(chain: str, addr: str, price: float | None, liq: float | None,
             vol: float | None, role: str) -> str:
        url = _dexscreener_url(chain, addr)
        le  = _liq_emoji(liq)
        p_s = f"${_fmt_price(price)}" if price else "—"
        l_s = _fmt_money(liq) if liq else "—"
        v_s = _fmt_money(vol) if vol is not None else "—"
        head = (
            f"{role} <a href='{url}'>{chain.upper()}</a>  "
            f"<b>{p_s}</b>  {le}{l_s}  ·  {v_s} vol"
        )
        return f"{head}\n<code>{addr}</code>"

    chain_rows = [
        _row(opp.cheap_chain,     opp.cheap_addr,     opp.cheap_price,
             opp.cheap_liq_usd,   opp.cheap_vol_h24_usd,    "🟢 BUY  "),
        _row(opp.expensive_chain, opp.expensive_addr, opp.expensive_price,
             opp.expensive_liq_usd, opp.expensive_vol_h24_usd, "🔴 SELL "),
    ]
    if other_chains:
        for chain, addr, price, liq in other_chains:
            chain_rows.append(_row(chain, addr, price, liq, None, "        "))

    parts.append("\n\n".join(chain_rows))

    parts.append(f"<i>cg_id: {opp.cg_id}</i>")

    return "\n\n".join(parts)


def _main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Status",     callback_data="menu:status"),
            InlineKeyboardButton(text="🚫 Blacklist",  callback_data="menu:blacklist"),
        ],
        [
            InlineKeyboardButton(text="🔍 Check token", callback_data="menu:check_hint"),
            InlineKeyboardButton(text="❌ Unsubscribe", callback_data="menu:stop"),
        ],
        [
            InlineKeyboardButton(text="ℹ️ Help", callback_data="menu:help"),
        ],
    ])


class Alerter:
    def __init__(self) -> None:
        settings = get_settings()
        self._bot_token        = settings.telegram_bot_token
        self._fallback_chat_id = settings.telegram_chat_id
        self._dry_run          = settings.dry_run

        self._bot: Bot | None = None
        self._dp: Dispatcher | None = None
        self._poll_task: asyncio.Task | None = None

    async def setup(self) -> None:
        if not self._bot_token or self._bot_token.startswith("your_"):
            log.warning("alerter.no_token")
            return
        self._bot = Bot(token=self._bot_token)
        self._dp  = Dispatcher()
        self._register_handlers()

        if self._fallback_chat_id:
            r = await get_redis()
            await r.sadd(_SUBSCRIBERS_KEY, self._fallback_chat_id)

        await self._bot.set_my_commands([
            {"command": "start",        "description": "Subscribe + open menu"},
            {"command": "status",       "description": "Scanner stats"},
            {"command": "check",        "description": "Check a token: /check <id|addr>"},
            {"command": "blacklist",    "description": "Manage blacklist"},
            {"command": "stop",         "description": "Unsubscribe"},
            {"command": "help",         "description": "Show commands"},
        ])

        self._poll_task = asyncio.create_task(
            self._dp.start_polling(self._bot, handle_signals=False),
            name="telegram_polling",
        )
        me = await self._bot.get_me()
        log.info("alerter.ready", bot=me.username)

    async def close(self) -> None:
        if self._dp:
            try:
                await self._dp.stop_polling()
            except Exception:
                pass
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._bot:
            await self._bot.session.close()

    # ── Handlers ──────────────────────────────────────────────────────────

    def _register_handlers(self) -> None:
        assert self._dp is not None
        dp = self._dp

        @dp.message(CommandStart())
        async def on_start(msg: Message) -> None:
            r = await get_redis()
            await r.sadd(_SUBSCRIBERS_KEY, str(msg.chat.id))
            subs = await r.scard(_SUBSCRIBERS_KEY)
            await msg.answer(
                "✅ <b>Welcome to Cross-Chain Arb Scanner</b>\n\n"
                "You're now subscribed to real-time alerts when the same token "
                "has a price gap across chains (Ethereum, BSC, Polygon, Arbitrum, "
                "Base, Optimism, Solana, SUI, Aptos, and 15+ more).\n\n"
                f"👥 Active subscribers: <b>{subs}</b>\n\n"
                "Use the menu below or type /help.",
                parse_mode=ParseMode.HTML,
                reply_markup=_main_menu(),
            )

        @dp.message(Command("stop"))
        async def on_stop(msg: Message) -> None:
            r = await get_redis()
            await r.srem(_SUBSCRIBERS_KEY, str(msg.chat.id))
            await msg.answer("❌ Unsubscribed. /start to re-enable.")

        @dp.message(Command("help"))
        async def on_help(msg: Message) -> None:
            await msg.answer(_help_text(), parse_mode=ParseMode.HTML,
                             reply_markup=_main_menu())

        @dp.message(Command("status"))
        async def on_status(msg: Message) -> None:
            await msg.answer(await self._status_text(),
                             parse_mode=ParseMode.HTML, reply_markup=_main_menu())

        @dp.message(Command("check"))
        async def on_check(msg: Message) -> None:
            await self._handle_check(msg)

        @dp.message(Command("blacklist"))
        async def on_blacklist(msg: Message) -> None:
            await self._handle_blacklist(msg)

        @dp.callback_query(F.data.startswith("check:"))
        async def on_check_btn(q: CallbackQuery) -> None:
            cg_id = q.data.split(":", 1)[1]
            # Reuse the /check logic by faking a message
            r = await get_redis()
            group = await r.hgetall(f"cg2:group:{cg_id}")
            if not group:
                await q.answer("Not found", show_alert=True)
                return
            prices: dict[str, tuple[str, float]] = {}
            for chain, addr in group.items():
                p = await get_price(chain, addr)
                if p and p > 0:
                    prices[chain] = (addr, p)
            lines = [f"🔍 <b>{cg_id.upper()}</b> — all chains"]
            if not prices:
                lines.append("⚠️ No fresh prices")
            else:
                sp = sorted(prices.items(), key=lambda kv: kv[1][1])
                cheap = sp[0][1][1]
                for chain, (addr, price) in sp:
                    spread = (price - cheap) / cheap * 100
                    dex_url = _dexscreener_url(chain, addr)
                    lines.append(
                        f"<b>{chain.upper()}</b>: ${price:.6g} ({spread:+.2f}%) "
                        f"<a href='{dex_url}'>→</a>"
                    )
            await q.message.answer("\n".join(lines), parse_mode=ParseMode.HTML,
                                   disable_web_page_preview=True)
            await q.answer()

        @dp.callback_query(F.data.startswith("bl:"))
        async def on_bl_btn(q: CallbackQuery) -> None:
            cg_id = q.data.split(":", 1)[1].lower()
            r = await get_redis()
            await r.sadd(_BLACKLIST_KEY, cg_id)
            await q.answer(f"🚫 Blacklisted: {cg_id}", show_alert=True)

        @dp.callback_query(F.data.startswith("menu:"))
        async def on_menu(q: CallbackQuery) -> None:
            action = q.data.split(":", 1)[1]
            if action == "status":
                await q.message.answer(await self._status_text(),
                                       parse_mode=ParseMode.HTML)
            elif action == "help":
                await q.message.answer(_help_text(), parse_mode=ParseMode.HTML)
            elif action == "blacklist":
                await q.message.answer(await self._blacklist_view(),
                                       parse_mode=ParseMode.HTML)
            elif action == "check_hint":
                await q.message.answer(
                    "🔍 Send <code>/check &lt;cg_id&gt;</code> or "
                    "<code>/check &lt;contract&gt;</code>\n\n"
                    "Examples:\n"
                    "<code>/check chainlink</code>\n"
                    "<code>/check 0x514910771af9ca656af840dff83e8264ecf986ca</code>",
                    parse_mode=ParseMode.HTML,
                )
            elif action == "stop":
                r = await get_redis()
                await r.srem(_SUBSCRIBERS_KEY, str(q.from_user.id))
                await q.message.answer("❌ Unsubscribed. /start to re-enable.")
            await q.answer()

    # ── /status ───────────────────────────────────────────────────────────

    async def _status_text(self) -> str:
        r = await get_redis()
        subs = await r.scard(_SUBSCRIBERS_KEY)

        groups = 0
        async for _ in r.scan_iter(match="cg2:group:*", count=1000):
            groups += 1

        prices = 0
        async for _ in r.scan_iter(match="cc2:price:*", count=1000):
            prices += 1

        alerts_active = 0
        async for _ in r.scan_iter(match="cc2_alerted:*", count=1000):
            alerts_active += 1

        bl = await r.scard(_BLACKLIST_KEY)

        return (
            "📊 <b>Scanner Status</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 Subscribers: <code>{subs}</code>\n"
            f"🔗 Token groups: <code>{groups:,}</code>\n"
            f"💲 Fresh prices: <code>{prices:,}</code>\n"
            f"🚫 Blacklisted: <code>{bl}</code>\n"
            f"⏸ Alerts in cooldown: <code>{alerts_active}</code>\n"
        )

    # ── /check ────────────────────────────────────────────────────────────

    async def _handle_check(self, msg: Message) -> None:
        parts = (msg.text or "").strip().split()
        if len(parts) < 2:
            await msg.answer(
                "Usage: <code>/check &lt;cg_id&gt;</code> or "
                "<code>/check &lt;contract_addr&gt;</code>\n\n"
                "Example: <code>/check chainlink</code>",
                parse_mode=ParseMode.HTML,
            )
            return

        query = parts[1].strip().lower()
        r = await get_redis()

        # Try as contract address first
        cg_id: str | None = None
        if query.startswith("0x") and len(query) == 42:
            # scan all chains
            async for key in r.scan_iter(match=f"cg2:contract:*:{query}", count=100):
                cg_id = await r.get(key)
                if cg_id:
                    break
        else:
            # treat as cg_id directly
            if await r.exists(f"cg2:group:{query}"):
                cg_id = query

        if not cg_id:
            await msg.answer(
                f"❌ Not found: <code>{parts[1]}</code>\n\n"
                f"Try a CoinGecko ID like <code>chainlink</code> or a contract.",
                parse_mode=ParseMode.HTML,
            )
            return

        group = await r.hgetall(f"cg2:group:{cg_id}")
        if not group:
            await msg.answer(f"❌ Token group empty: <code>{cg_id}</code>",
                             parse_mode=ParseMode.HTML)
            return

        # Fetch prices + on-chain liq for each chain
        prices: dict[str, tuple[str, float, float | None]] = {}  # chain -> (addr, price, liq)
        for chain, addr in group.items():
            price = await get_price(chain, addr)
            liq_raw = await r.get(f"cc2:liq_usd:{chain}:{addr.lower()}")
            try:
                liq = float(liq_raw) if liq_raw else None
            except (TypeError, ValueError):
                liq = None
            if price and price > 0:
                prices[chain] = (addr, price, liq)

        bridges = sorted(await r.smembers(f"cg2:bridges:{cg_id}"))
        bridge_str = " · ".join(b.upper() for b in bridges) if bridges else "—"

        lines: list[str] = []
        lines.append(f"🔍 <b>{cg_id.upper()}</b>  ·  <i>{len(prices)}/{len(group)} chains priced</i>")
        lines.append(f"<b>Bridges:</b> <i>{bridge_str}</i>")
        lines.append("")

        if not prices:
            lines.append("⚠️ No fresh prices yet — monitor hasn't cycled.")
        else:
            sorted_prices = sorted(prices.items(), key=lambda kv: kv[1][1])
            cheapest_price = sorted_prices[0][1][1]

            # Header row (monospaced via <code>)
            lines.append(f"<code>CHAIN         PRICE         SPREAD     LIQ    </code>")
            for chain, (addr, price, liq) in sorted_prices:
                spread = (price - cheapest_price) / cheapest_price * 100
                # spread tier dot
                if spread < 0.5:    flag = "🟢"
                elif spread < 5:    flag = "🟡"
                elif spread < 30:   flag = "🔥"
                elif spread < 200:  flag = "🚀"
                else:               flag = "⚠️"
                liq_e = _liq_emoji(liq)
                spread_str = f"+{spread:.2f}%" if spread > 0 else "  base"
                lines.append(
                    f"{flag}<code>{chain.upper()[:11]:<11s} </code>"
                    f"<code>${_fmt_price(price):>11s}</code>  "
                    f"<code>{spread_str:>8s}</code>  "
                    f"{liq_e}<code>{_fmt_money(liq):>7s}</code>  "
                    f"<a href='{_dexscreener_url(chain, addr)}'>→</a>"
                )

        # Chains WITHOUT price
        no_price = [c for c in group if c not in prices]
        if no_price:
            lines.append(f"\n<i>No price on:</i> {', '.join(no_price)}")

        # Contracts on all chains (full addresses, copy-friendly)
        lines.append(f"\n<b>Contracts:</b>")
        for chain, addr in sorted(group.items()):
            lines.append(f"<b>{chain.upper()}</b>  <code>{addr}</code>")

        # Blacklist status
        is_bl = await r.sismember(_BLACKLIST_KEY, cg_id)
        if is_bl:
            lines.append(f"\n🚫 <b>Blacklisted</b> — alerts suppressed")

        await msg.answer(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    # ── /blacklist ────────────────────────────────────────────────────────

    async def _handle_blacklist(self, msg: Message) -> None:
        parts = (msg.text or "").strip().split()
        r = await get_redis()

        if len(parts) == 1:
            await msg.answer(await self._blacklist_view(),
                             parse_mode=ParseMode.HTML)
            return

        action = parts[1].lower()

        if action == "list":
            await msg.answer(await self._blacklist_view(),
                             parse_mode=ParseMode.HTML)
            return

        if action in ("add", "block", "+"):
            if len(parts) < 3:
                await msg.answer("Usage: <code>/blacklist add &lt;cg_id&gt;</code>",
                                 parse_mode=ParseMode.HTML)
                return
            cg_id = parts[2].lower()
            await r.sadd(_BLACKLIST_KEY, cg_id)
            await msg.answer(f"🚫 Blacklisted: <code>{cg_id}</code>",
                             parse_mode=ParseMode.HTML)
            return

        if action in ("remove", "rm", "del", "-"):
            if len(parts) < 3:
                await msg.answer("Usage: <code>/blacklist remove &lt;cg_id&gt;</code>",
                                 parse_mode=ParseMode.HTML)
                return
            cg_id = parts[2].lower()
            removed = await r.srem(_BLACKLIST_KEY, cg_id)
            if removed:
                await msg.answer(f"✅ Unblocked: <code>{cg_id}</code>",
                                 parse_mode=ParseMode.HTML)
            else:
                await msg.answer(f"Not on blacklist: <code>{cg_id}</code>",
                                 parse_mode=ParseMode.HTML)
            return

        if action == "clear":
            await r.delete(_BLACKLIST_KEY)
            await msg.answer("🧹 Blacklist cleared.")
            return

        # Single-arg treat as "add this token"
        cg_id = action
        await r.sadd(_BLACKLIST_KEY, cg_id)
        await msg.answer(f"🚫 Blacklisted: <code>{cg_id}</code>",
                         parse_mode=ParseMode.HTML)

    async def _blacklist_view(self) -> str:
        r = await get_redis()
        items = await r.smembers(_BLACKLIST_KEY)
        if not items:
            return (
                "🚫 <b>Blacklist</b> — empty\n\n"
                "Add a token: <code>/blacklist add &lt;cg_id&gt;</code>\n"
                "Remove:       <code>/blacklist remove &lt;cg_id&gt;</code>\n"
                "Clear all:    <code>/blacklist clear</code>"
            )
        lines = [f"🚫 <b>Blacklist</b> ({len(items)} tokens)\n━━━━━━━━━━━━━"]
        for it in sorted(items):
            lines.append(f"  <code>{it}</code>")
        lines.append(
            "\n<i>Remove:</i> <code>/blacklist remove &lt;cg_id&gt;</code>\n"
            "<i>Clear all:</i> <code>/blacklist clear</code>"
        )
        return "\n".join(lines)

    async def is_blacklisted(self, cg_id: str) -> bool:
        r = await get_redis()
        return bool(await r.sismember(_BLACKLIST_KEY, cg_id.lower()))

    # ── Broadcasting ──────────────────────────────────────────────────────

    async def send(self, opp: CrossChainOpportunity) -> None:
        # Respect blacklist
        if await self.is_blacklisted(opp.cg_id):
            log.info("alerter.skipped_blacklisted", cg_id=opp.cg_id)
            return

        bridges = await get_bridges_for(opp.cg_id)
        text = await self._format(opp, bridges)
        kb = self._alert_keyboard(opp)
        log.info("alerter.broadcasting",
                 cg_id=opp.cg_id, net_pct=opp.net_profit_pct)

        if self._dry_run:
            log.info("alerter.dry_run", text=text)
            return
        if not self._bot:
            return

        r = await get_redis()
        chat_ids = await r.smembers(_SUBSCRIBERS_KEY)
        if not chat_ids:
            return

        sem = asyncio.Semaphore(25)
        stats = {"sent": 0, "failed": 0}

        async def _send_one(chat_id: str) -> None:
            async with sem:
                # Try 1: HTML + keyboard
                try:
                    await self._bot.send_message(
                        chat_id=chat_id, text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                        reply_markup=kb,
                    )
                    stats["sent"] += 1
                    return
                except Exception as e1:
                    err = str(e1)
                    if "blocked" in err.lower() or "chat not found" in err.lower():
                        await r.srem(_SUBSCRIBERS_KEY, chat_id)
                        log.info("alerter.pruned_subscriber", chat_id=chat_id)
                        stats["failed"] += 1
                        return
                    log.warning("alerter.send_failed_html",
                                chat_id=chat_id, err=err[:200])

                # Try 2: plain text fallback (no HTML / no keyboard)
                try:
                    plain = _strip_html(text)
                    await self._bot.send_message(
                        chat_id=chat_id, text=plain,
                        disable_web_page_preview=True,
                    )
                    stats["sent"] += 1
                except Exception as e2:
                    log.error("alerter.send_failed_plain",
                              chat_id=chat_id, err=str(e2)[:200])
                    stats["failed"] += 1

        await asyncio.gather(*[_send_one(cid) for cid in chat_ids])
        log.info("alerter.broadcast_complete",
                 cg_id=opp.cg_id, sent=stats["sent"], failed=stats["failed"])

    def _alert_keyboard(self, opp: CrossChainOpportunity) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"🛒 {opp.cheap_chain.upper()} pool",
                    url=_dexscreener_url(opp.cheap_chain, opp.cheap_addr),
                ),
                InlineKeyboardButton(
                    text=f"💸 {opp.expensive_chain.upper()} pool",
                    url=_dexscreener_url(opp.expensive_chain, opp.expensive_addr),
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📊 All chains",
                    callback_data=f"check:{opp.cg_id}",
                ),
                InlineKeyboardButton(
                    text="🚫 Blacklist this",
                    callback_data=f"bl:{opp.cg_id}",
                ),
            ],
        ])

    async def _format(self, opp: CrossChainOpportunity, bridges: list[str] | None = None) -> str:
        bridges = bridges or []
        other_chains = await self._collect_other_chains(opp)
        return _build_alert_text(opp, bridges, other_chains)

    async def _collect_other_chains(
        self, opp: CrossChainOpportunity,
    ) -> list[tuple[str, str, float | None, float | None]]:
        """All chains in the group EXCEPT cheap/expensive — for context display."""
        r = await get_redis()
        group = await r.hgetall(f"cg2:group:{opp.cg_id}")
        if not group:
            return []
        # Filter out the two chains already shown in DEX table
        other = [(c, a) for c, a in group.items()
                 if c not in (opp.cheap_chain, opp.expensive_chain)]
        if not other:
            return []
        # Pipelined fetch: price + liq for each
        async with r.pipeline(transaction=False) as pipe:
            for c, a in other:
                pipe.get(f"cc2:price:{c}:{a.lower()}")
                pipe.get(f"cc2:liq_usd:{c}:{a.lower()}")
            vals = await pipe.execute()
        rows: list[tuple[str, str, float | None, float | None]] = []
        for i, (c, a) in enumerate(other):
            try:    price = float(vals[i*2]) if vals[i*2] else None
            except: price = None
            try:    liq   = float(vals[i*2+1]) if vals[i*2+1] else None
            except: liq   = None
            rows.append((c, a, price, liq))
        # Sort: priced chains first (by price asc to show spread), then unpriced
        rows.sort(key=lambda x: (x[2] is None, x[2] or 0))
        return rows


def _help_text() -> str:
    return (
        "ℹ️ <b>Cross-Chain Arb Bot</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Real-time scanner comparing token prices across 20+ chains.\n\n"
        "<b>Commands:</b>\n"
        "/start — subscribe to alerts\n"
        "/status — scanner stats\n"
        "/check &lt;id|addr&gt; — show all prices for a token\n"
        "/blacklist — view blocklist\n"
        "/blacklist add &lt;id&gt; — mute alerts for token\n"
        "/blacklist remove &lt;id&gt; — unmute\n"
        "/blacklist clear — wipe\n"
        "/stop — unsubscribe\n\n"
        "<b>Alert buttons:</b>\n"
        "Every alert has quick links to DexScreener pools on both chains, "
        "an <i>All chains</i> button to see every price, and "
        "<i>Blacklist this</i> to mute further alerts for that token."
    )
