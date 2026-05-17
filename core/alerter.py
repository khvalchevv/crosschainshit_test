from __future__ import annotations

import asyncio
import datetime as _dt
import re

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import get_settings
from core.detector import CrossChainOpportunity
from utils import get_logger, get_redis, norm_addr

log = get_logger(__name__)

_SUBSCRIBERS_KEY = "cc2_subscribers"
_BLACKLIST_KEY   = "cc2_blacklist"      # set of lowercase token ids
_LEGBLACKLIST_KEY = "cc2_blacklist_leg"  # set of "{cg_id}@{chain}" legs

_RESEARCHER_BOT = "researcheer_bot"     # opens with /start <ticker>


def _researcher_url(ticker: str, fallback: str = "") -> str:
    import re as _re
    payload = _re.sub(r"[^A-Za-z0-9_-]", "", (ticker or fallback))[:64]
    return f"https://t.me/{_RESEARCHER_BOT}?start={payload}"


def _now_kyiv() -> str:
    return (_dt.datetime.utcnow() + _dt.timedelta(hours=3)).strftime(
        "%Y-%m-%d %H:%M:%S UTC+3")


def _now_kyiv_time() -> str:
    return (_dt.datetime.utcnow() + _dt.timedelta(hours=3)).strftime(
        "%H:%M:%S UTC+3")


def _strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s)


async def _bridges_for(cg_id: str) -> list[str]:
    r = await get_redis()
    members = await r.smembers(f"cg2:bridges:{cg_id}") or set()
    return sorted(
        m.decode() if isinstance(m, bytes) else m for m in members)


def _dexscreener_url(chain: str, addr: str) -> str:
    return f"https://dexscreener.com/{chain}/{addr}"


# Our chain name → OKX Web3 token-page slug. Only chains OKX Web3 actually
# serves are listed; anything else falls back to DexScreener so a button is
# never a dead link.
_OKX_CHAIN = {
    "ethereum": "ethereum", "bsc": "bsc",        "polygon": "polygon",
    "arbitrum": "arbitrum-one", "optimism": "optimism", "base": "base",
    "avalanche": "avalanche", "fantom": "fantom", "linea": "linea",
    "scroll":   "scroll",   "blast":   "blast",   "mantle": "mantle",
    "zksync":   "zksync",   "opbnb":   "opbnb",   "manta":  "manta",
    "sonic":    "sonic",    "okex":    "x-layer", "core":   "core",
    "solana":   "solana",   "tron":    "tron",    "ton":    "ton",
    "sui":      "sui",      "aptos":   "aptos",
}


def _okx_url(chain: str, addr: str) -> str:
    slug = _OKX_CHAIN.get(chain)
    if slug:
        return f"https://web3.okx.com/ru/token/{slug}/{addr}"
    return _dexscreener_url(chain, addr)


# ── Format helpers ────────────────────────────────────────────────────────

def _fmt_money(v: float | None) -> str:
    if v is None:        return "—"
    if v >= 1_000_000:   return f"${v/1_000_000:.2f}M"
    if v >= 1_000:       return f"${v/1_000:.1f}k"
    return f"${v:,.0f}"


def _fmt_price(p: float | None) -> str:
    if p is None or p <= 0:  return "—"
    if p >= 1000:            return f"{p:,.2f}"
    if p >= 0.0001:          return f"{p:.4g}"
    return f"{p:.6g}"


def _liq_emoji(v: float | None) -> str:
    if v is None:        return "❓"   # no liquidity data
    if v < 5_000:        return "🔴"   # < $5k
    if v < 40_000:       return "🟡"   # $5k–$40k
    return "🟢"                         # ≥ $40k


def _spread_emoji(s: float) -> str:
    if s > 20:   return "🔥"   # >20%
    if s >= 10:  return "💥"   # 10–20%
    return "👍"                 # <10%


# Only these chains are abbreviated (UPPERCASE). Every other chain is shown
# with a normal Capitalised name (e.g. base → Base, polygon → Polygon).
_CHAIN_LABEL = {
    "ethereum": "ETH",
    "arbitrum": "ARB",
    "solana":   "SOL",
    "optimism": "OP",
    "bsc":      "BSC",
}


def _chain_label(chain: str) -> str:
    return _CHAIN_LABEL.get(chain) or chain.capitalize()


def _fmt_age(sec: float) -> str:
    sec = int(sec)
    if sec < 60:
        return f"{sec}s"
    m, s = divmod(sec, 60)
    if m < 60:
        return f"{m}m {s}s" if s else f"{m}m"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"


def _pct(p: float) -> str:
    return f"{p:.2f}".rstrip("0").rstrip(".")


def _liq_str(liq: float | None) -> str:
    return _fmt_money(liq) if liq else "-"


def _build_alert_text(
    opp: "CrossChainOpportunity",
    bridges: list[str],
    other_chains: list[tuple[str, str, float | None, float | None]] | None = None,
    muted_chains: list[str] | None = None,
) -> str:
    emoji = _spread_emoji(opp.gross_spread_pct)
    ticker = (opp.ticker or "").upper() or opp.cg_id.upper()
    cheap_url = _dexscreener_url(opp.cheap_chain, opp.cheap_addr)
    exp_url   = _dexscreener_url(opp.expensive_chain, opp.expensive_addr)

    parts = []

    # Header: #TICKER / % / 🔥
    parts.append(
        f"<b>#{ticker}</b> / <b>{_pct(opp.gross_spread_pct)}%</b> / {emoji}"
    )

    # Route → (Bridge, only if the token came from the Wormhole/L0 JSON)
    # → spread age
    _BR = {"wormhole": "Wormhole", "layerzero": "L0"}
    route = (
        f"<b>Route:</b> "
        f"<a href='{cheap_url}'>{_chain_label(opp.cheap_chain)}</a> → "
        f"<a href='{exp_url}'>{_chain_label(opp.expensive_chain)}</a>"
    )
    if bridges:
        pretty = " · ".join(_BR.get(b, b.upper()) for b in bridges)
        route += f"\n<b>Bridge:</b> <i>{pretty}</i>"
    route += f"\n🕙 <b>Spread alive:</b> {_fmt_age(opp.spread_age_sec)}"
    parts.append(route)

    # Buy / Sell
    parts.append(
        f"<b>Buy:</b> <a href='{cheap_url}'>{_chain_label(opp.cheap_chain)}</a>  "
        f"<b>${_fmt_price(opp.cheap_price)}</b>  "
        f"(Liq: {_liq_str(opp.cheap_liq_usd)})  {_liq_emoji(opp.cheap_liq_usd)}\n"
        f"<code>{opp.cheap_addr}</code>\n"
        f"<b>Sell:</b> <a href='{exp_url}'>{_chain_label(opp.expensive_chain)}</a>  "
        f"<b>${_fmt_price(opp.expensive_price)}</b>  "
        f"(Liq: {_liq_str(opp.expensive_liq_usd)})  "
        f"{_liq_emoji(opp.expensive_liq_usd)}\n"
        f"<code>{opp.expensive_addr}</code>"
    )

    # Other chains
    if other_chains:
        rows = ["<b>other chains:</b>"]
        for chain, addr, price, liq in other_chains:
            url = _dexscreener_url(chain, addr)
            p_s = f"${_fmt_price(price)}" if price else "-"
            rows.append(
                f"<a href='{url}'>{_chain_label(chain)}</a>:  {p_s}  "
                f"(Liq: {_liq_str(liq)})\n<code>{addr}</code>"
            )
        parts.append("\n".join(rows))

    # Project name at the very end; muted-chain note + time below it
    tail = f"<b>Project:</b> {opp.cg_id}"
    if muted_chains:
        labels = ", ".join(_chain_label(c) for c in muted_chains)
        tail += f"\n🚫 <b>Muted:</b> {labels}"
    tail += f"\n<i>{_now_kyiv_time()}</i>"
    parts.append(tail)
    return "\n\n".join(parts)


def _main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Status",    callback_data="menu:status"),
            InlineKeyboardButton(text="🚫 Blacklist", callback_data="menu:blacklist"),
        ],
        [
            InlineKeyboardButton(text="❌ Unsubscribe", callback_data="menu:stop"),
            InlineKeyboardButton(text="ℹ️ Help",         callback_data="menu:help"),
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
            {"command": "start",     "description": "Subscribe + open menu"},
            {"command": "status",    "description": "Scanner stats"},
            {"command": "check",     "description": "Check a token: /check <ticker|id|contract>"},
            {"command": "blacklist", "description": "Manage blacklist"},
            {"command": "stop",      "description": "Unsubscribe"},
            {"command": "help",      "description": "Show commands"},
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
                "✅ <b>Cross-Chain Spread Scanner</b>\n\n"
                "You're subscribed to real-time alerts when the same token "
                "has a price gap across chains.\n\n"
                f"👥 Active subscribers: <b>{subs}</b>\n\n"
                "Use the menu below or /help.",
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
                             parse_mode=ParseMode.HTML,
                             reply_markup=_main_menu())

        @dp.message(Command("check"))
        async def on_check(msg: Message) -> None:
            parts = (msg.text or "").strip().split(maxsplit=1)
            if len(parts) < 2:
                await msg.answer(
                    "Usage: <code>/check &lt;ticker | id | contract&gt;</code>\n\n"
                    "Examples:\n"
                    "<code>/check ADS</code>\n"
                    "<code>/check adshares</code>\n"
                    "<code>/check 0xcfcecfe2bd2fed07a9145222e8a7ad9cf1ccd22a</code>",
                    parse_mode=ParseMode.HTML)
                return
            await msg.answer(await self._check_text(parts[1].strip()),
                             parse_mode=ParseMode.HTML,
                             disable_web_page_preview=True)

        @dp.message(Command("blacklist"))
        async def on_blacklist(msg: Message) -> None:
            await self._handle_blacklist(msg)

        @dp.callback_query(F.data.startswith("chk:"))
        async def on_chk_btn(q: CallbackQuery) -> None:
            gid = q.data.split(":", 1)[1]
            await q.message.answer(await self._check_text(gid),
                                   parse_mode=ParseMode.HTML,
                                   disable_web_page_preview=True)
            await q.answer()

        @dp.callback_query(F.data.startswith("mn:"))
        async def on_mute_menu(q: CallbackQuery) -> None:
            cg_id = q.data.split(":", 1)[1]
            r = await get_redis()
            group = await r.hgetall(f"cg2:group:{cg_id}")
            if not group:
                await q.answer("Group not found", show_alert=True)
                return
            await q.message.answer(
                f"<b>Mute a network for</b> <code>{cg_id}</code>\n"
                f"<i>✅ = active (tap to mute) · 🚫 = muted (tap to "
                f"un-mute).</i>",
                parse_mode=ParseMode.HTML,
                reply_markup=await self._mute_kb(cg_id, group))
            await q.answer()

        @dp.callback_query(F.data == "mnx:")
        async def on_mute_close(q: CallbackQuery) -> None:
            try:
                await q.message.delete()
            except Exception:
                pass
            await q.answer()

        @dp.callback_query(F.data.startswith("bln:"))
        async def on_bln_btn(q: CallbackQuery) -> None:
            leg = q.data.split(":", 1)[1].lower()  # "{cg_id}@{chain}"
            cg_id = leg.split("@", 1)[0]
            r = await get_redis()
            if await r.sismember(_LEGBLACKLIST_KEY, leg):
                await r.srem(_LEGBLACKLIST_KEY, leg)
                await q.answer(f"✅ Un-muted: {leg}")
            else:
                await r.sadd(_LEGBLACKLIST_KEY, leg)
                await q.answer(f"🚫 Muted: {leg}")
            # Refresh the picker so the tapped chain flips colour in place.
            group = await r.hgetall(f"cg2:group:{cg_id}")
            if group:
                try:
                    await q.message.edit_reply_markup(
                        reply_markup=await self._mute_kb(cg_id, group))
                except Exception:
                    pass

        @dp.callback_query(F.data.startswith("bl:"))
        async def on_bl_btn(q: CallbackQuery) -> None:
            cg_id = q.data.split(":", 1)[1].lower()
            r = await get_redis()
            added = await r.sadd(_BLACKLIST_KEY, cg_id)
            if added:
                await q.answer(f"🚫 Blacklisted: {cg_id}", show_alert=True)
            else:
                await q.answer(f"ℹ️ Already blacklisted: {cg_id}",
                               show_alert=True)

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
        async for _ in r.scan_iter(match="cc2_cooldown:*", count=1000):
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

    # ── /blacklist ────────────────────────────────────────────────────────

    async def _handle_blacklist(self, msg: Message) -> None:
        parts = (msg.text or "").strip().split()
        r = await get_redis()

        if len(parts) == 1 or parts[1].lower() == "list":
            await msg.answer(await self._blacklist_view(),
                             parse_mode=ParseMode.HTML)
            return

        action = parts[1].lower()

        # An arg containing "@" targets a single leg (token@chain); plain
        # arg targets the whole token.
        if action in ("add", "block", "+"):
            if len(parts) < 3:
                await msg.answer(
                    "Usage: <code>/blacklist add &lt;id&gt;</code> or "
                    "<code>/blacklist add &lt;id&gt;@&lt;chain&gt;</code>",
                    parse_mode=ParseMode.HTML)
                return
            tgt = parts[2].lower()
            key = _LEGBLACKLIST_KEY if "@" in tgt else _BLACKLIST_KEY
            added = await r.sadd(key, tgt)
            kind = "Muted leg" if "@" in tgt else "Blacklisted"
            await msg.answer(
                (f"🚫 {kind}: <code>{tgt}</code>" if added
                 else f"ℹ️ Already there: <code>{tgt}</code>"),
                parse_mode=ParseMode.HTML)
            return

        if action in ("remove", "rm", "del", "-"):
            if len(parts) < 3:
                await msg.answer(
                    "Usage: <code>/blacklist remove &lt;id|id@chain&gt;</code>",
                    parse_mode=ParseMode.HTML)
                return
            tgt = parts[2].lower()
            key = _LEGBLACKLIST_KEY if "@" in tgt else _BLACKLIST_KEY
            removed = await r.srem(key, tgt)
            await msg.answer(
                (f"✅ Removed: <code>{tgt}</code>" if removed
                 else f"Not on list: <code>{tgt}</code>"),
                parse_mode=ParseMode.HTML)
            return

        if action == "clear":
            await r.delete(_BLACKLIST_KEY, _LEGBLACKLIST_KEY)
            await msg.answer("🧹 Blacklist + leg mutes cleared.")
            return

        tgt = action
        key = _LEGBLACKLIST_KEY if "@" in tgt else _BLACKLIST_KEY
        added = await r.sadd(key, tgt)
        kind = "Muted leg" if "@" in tgt else "Blacklisted"
        await msg.answer(
            (f"🚫 {kind}: <code>{tgt}</code>" if added
             else f"ℹ️ Already there: <code>{tgt}</code>"),
            parse_mode=ParseMode.HTML)

    async def _blacklist_view(self) -> str:
        r = await get_redis()
        items = sorted(await r.smembers(_BLACKLIST_KEY) or [])
        legs = sorted(await r.smembers(_LEGBLACKLIST_KEY) or [])
        if not items and not legs:
            return (
                "🚫 <b>Blacklist</b> — empty\n\n"
                "Token: <code>/blacklist add &lt;id&gt;</code>\n"
                "Network: <code>/blacklist add &lt;id&gt;@&lt;chain&gt;</code>\n"
                "Remove: <code>/blacklist remove &lt;id|id@chain&gt;</code>\n"
                "Clear: <code>/blacklist clear</code>"
            )
        lines = [f"🚫 <b>Blacklist</b>"]
        if items:
            lines.append(f"\n<b>Tokens ({len(items)}):</b>")
            lines += [f"  <code>{i}</code>" for i in items]
        if legs:
            lines.append(f"\n<b>Muted networks ({len(legs)}):</b>")
            lines += [f"  <code>{l}</code>" for l in legs]
        lines.append(
            "\n<i>Remove:</i> <code>/blacklist remove &lt;id|id@chain&gt;</code>")
        return "\n".join(lines)

    async def is_blacklisted(self, cg_id: str) -> bool:
        r = await get_redis()
        return bool(await r.sismember(_BLACKLIST_KEY, cg_id.lower()))

    # ── /check ────────────────────────────────────────────────────────────

    async def _resolve_query(self, q: str) -> list[str]:
        """Resolve a /check arg to group id(s). Accepts: exact group id,
        wh-<symbol>, a contract address (any chain), or a ticker."""
        r = await get_redis()
        q = q.strip()
        ql = q.lower()

        for cand in (q, ql, f"wh-{ql}"):
            if await r.exists(f"cg2:group:{cand}"):
                return [cand]

        # contract address (EVM 0x..., or non-EVM with :: / long base58)
        if q.startswith("0x") or "::" in q or len(q) >= 30:
            na = norm_addr(q)
            gids: list[str] = []
            async for k in r.scan_iter(match=f"cg2:contract:*:{na}", count=500):
                v = await r.get(k)
                if v:
                    gids.append(v.decode() if isinstance(v, bytes) else v)
            if gids:
                return list(dict.fromkeys(gids))

        # ticker → symbol index
        members = await r.smembers(f"cg2:sym:{ql}")
        out = []
        for m in (members or []):
            m = m.decode() if isinstance(m, bytes) else m
            if await r.exists(f"cg2:group:{m}"):
                out.append(m)
        return out

    async def _check_text(self, query: str) -> str:
        r = await get_redis()
        gids = await self._resolve_query(query)
        if not gids:
            return (f"❌ Not found: <code>{query}</code>\n\n"
                    "Try a ticker (<code>ADS</code>), id "
                    "(<code>adshares</code>) or a contract.")

        # If a ticker matched several projects, pick the one with the most
        # chains (most likely the real multichain token).
        note = ""
        if len(gids) > 1:
            sizes = []
            for g in gids:
                sizes.append((await r.hlen(f"cg2:group:{g}"), g))
            sizes.sort(reverse=True)
            gid = sizes[0][1]
            note = (f"\n<i>{len(gids)} matches for that ticker — showing "
                    f"<code>{gid}</code>. Others: "
                    + ", ".join(f"<code>{g}</code>" for _, g in sizes[1:6])
                    + "</i>")
        else:
            gid = gids[0]

        group = await r.hgetall(f"cg2:group:{gid}")
        if not group:
            return f"❌ Group empty: <code>{gid}</code>"

        rows: list[tuple[str, str, float | None, float | None]] = []
        async with r.pipeline(transaction=False) as pipe:
            for chain, addr in group.items():
                pipe.get(f"cc2:price:{chain}:{norm_addr(addr)}")
                pipe.get(f"cc2:liq_usd:{chain}:{norm_addr(addr)}")
            vals = await pipe.execute()
        for i, (chain, addr) in enumerate(group.items()):
            try:    price = float(vals[i*2]) if vals[i*2] else None
            except: price = None
            try:    liq = float(vals[i*2+1]) if vals[i*2+1] else None
            except: liq = None
            rows.append((chain, addr, price, liq))

        priced = sorted([x for x in rows if x[2]], key=lambda x: x[2])
        bridges = await _bridges_for(gid)
        br = " · ".join(b.upper() for b in bridges) if bridges else "—"

        lines = [f"🔍 <b>{gid}</b>  ·  <i>{len(priced)}/{len(group)} priced</i>",
                 f"<b>Bridges:</b> <i>{br}</i>", ""]
        if not priced:
            lines.append("⚠️ No fresh prices (monitor hasn't covered it / "
                         "no live pool).")
        else:
            base = priced[0][2]
            for chain, addr, price, liq in priced:
                spr = (price - base) / base * 100
                spr_s = f"+{spr:.2f}%" if spr > 0 else "  base"
                lines.append(
                    f"{_liq_emoji(liq)} <a href='{_dexscreener_url(chain, addr)}'>"
                    f"{chain.upper()}</a>  <b>${_fmt_price(price)}</b>  "
                    f"{spr_s}  {_fmt_money(liq)}")
            if len(priced) >= 2:
                spread = (priced[-1][2] - base) / base * 100
                lines.append(f"\n<b>Max spread: {spread:.2f}%</b>")
        priced_chains = {x[0] for x in priced}
        no_p = [c for c, _a, _p, _l in rows if c not in priced_chains]
        if no_p:
            lines.append(f"\n<i>No price:</i> {', '.join(sorted(no_p))}")
        lines.append("\n<b>Contracts:</b>")
        for chain, addr in sorted(group.items()):
            lines.append(f"<b>{chain.upper()}</b> <code>{addr}</code>")
        if await self.is_blacklisted(gid):
            lines.append("\n🚫 <b>Blacklisted</b>")
        return "\n".join(lines) + note

    # ── Broadcasting ──────────────────────────────────────────────────────

    async def send(self, opp: CrossChainOpportunity) -> None:
        if await self.is_blacklisted(opp.cg_id):
            log.info("alerter.skipped_blacklisted", cg_id=opp.cg_id)
            return

        bridges = await _bridges_for(opp.cg_id)
        other_chains = await self._collect_other_chains(opp)
        r = await get_redis()
        legs = await r.smembers(_LEGBLACKLIST_KEY) or set()
        pref = f"{opp.cg_id.lower()}@"
        muted_chains = sorted(m[len(pref):] for m in legs
                              if m.startswith(pref))
        text = _build_alert_text(opp, bridges, other_chains, muted_chains)
        kb = self._alert_keyboard(opp)
        log.info("alerter.broadcasting",
                 cg_id=opp.cg_id, spread_pct=opp.gross_spread_pct)

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
                try:
                    await self._bot.send_message(
                        chat_id=chat_id, text=_strip_html(text),
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

    async def _mute_kb(self, cg_id: str,
                       group: dict[str, str]) -> InlineKeyboardMarkup:
        r = await get_redis()
        muted = await r.smembers(_LEGBLACKLIST_KEY) or set()
        row, kb = [], []
        for ch in sorted(group):
            on = f"{cg_id.lower()}@{ch}" in muted
            row.append(InlineKeyboardButton(
                text=f"{'🚫' if on else '✅'} {_chain_label(ch)}",
                callback_data=f"bln:{cg_id}@{ch}"))
            if len(row) == 3:
                kb.append(row); row = []
        if row:
            kb.append(row)
        kb.append([InlineKeyboardButton(text="⬅ Back",
                                        callback_data="mnx:")])
        return InlineKeyboardMarkup(inline_keyboard=kb)

    def _alert_keyboard(self, opp: CrossChainOpportunity) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"{_chain_label(opp.cheap_chain)} OKX",
                    url=_okx_url(opp.cheap_chain, opp.cheap_addr),
                ),
                InlineKeyboardButton(
                    text=f"{_chain_label(opp.expensive_chain)} OKX",
                    url=_okx_url(opp.expensive_chain, opp.expensive_addr),
                ),
            ],
            [
                InlineKeyboardButton(
                    text="BL Net",
                    callback_data=f"mn:{opp.cg_id}",
                ),
                InlineKeyboardButton(
                    text="BL Token",
                    callback_data=f"bl:{opp.cg_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="Info",
                    url=_researcher_url(opp.ticker, opp.cg_id),
                ),
            ],
        ])

    async def _collect_other_chains(
        self, opp: CrossChainOpportunity,
    ) -> list[tuple[str, str, float | None, float | None]]:
        r = await get_redis()
        group = await r.hgetall(f"cg2:group:{opp.cg_id}")
        if not group:
            return []
        other = [(c, a) for c, a in group.items()
                 if c not in (opp.cheap_chain, opp.expensive_chain)]
        if not other:
            return []
        async with r.pipeline(transaction=False) as pipe:
            for c, a in other:
                pipe.get(f"cc2:price:{c}:{norm_addr(a)}")
                pipe.get(f"cc2:liq_usd:{c}:{norm_addr(a)}")
            vals = await pipe.execute()
        rows: list[tuple[str, str, float | None, float | None]] = []
        for i, (c, a) in enumerate(other):
            try:    price = float(vals[i*2]) if vals[i*2] else None
            except: price = None
            try:    liq = float(vals[i*2+1]) if vals[i*2+1] else None
            except: liq = None
            rows.append((c, a, price, liq))
        rows.sort(key=lambda x: (x[2] is None, x[2] or 0))
        return rows


def _help_text() -> str:
    return (
        "ℹ️ <b>Cross-Chain Spread Bot</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Scans the same token's price across 20+ chains and alerts on gaps.\n\n"
        "<b>Commands:</b>\n"
        "/start — subscribe to alerts\n"
        "/status — scanner stats\n"
        "/check &lt;ticker|id|contract&gt; — all chains for a token\n"
        "/blacklist — view blocklist\n"
        "/blacklist add &lt;id&gt; — mute a token\n"
        "/blacklist remove &lt;id&gt; — unmute\n"
        "/blacklist clear — wipe\n"
        "/stop — unsubscribe"
    )
