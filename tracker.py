"""
tracker.py — Multi-Chain Fair-Launch Intelligence & Copy-Trading Engine

Monitors Solana, BNB Chain, and a configurable third EVM chain ("Robinhood
Chain" slot — see NOTE below) for new fair-launch token deployments and
tracked-wallet buys, runs everything through a three-stage filtering engine
(dev-trust, cross-chain narrative clustering, smart-money conviction), and
broadcasts every actionable signal to a live dashboard over WebSocket.

NOTE ON ENDPOINT/CONTRACT VERIFICATION (as researched, September 2026)
-----------------------------------------------------------------------------
Pump.fun's public firehose (wss://pumpportal.fun/api/data) is a real,
documented, unauthenticated endpoint, hardcoded below, and confirmed live
against production traffic during development of this file.

"Robinhood Chain" is real (Ethereum L2 on Arbitrum Dedicated Blockchains,
mainnet since 2026-07-01, chain ID 4663, explorer at
robinhoodchain.blockscout.com) — an earlier version of this file incorrectly
assumed it didn't exist.

Per-platform confidence, based on cross-referenced docs (Bitquery, official
project docs, GitHub) at the time this was written:

  - Four.meme (BNB Chain): HIGH confidence. Factory = TokenManager2 V2 at
    0x5c952063c7fc8610FFDB798152D69F0B9550762b, confirmed by 3 independent
    sources. Full `TokenCreate` event ABI sourced from the four-meme-ai
    GitHub docs and hardcoded into a dedicated decoder.
  - Pons / Ponsfamily (Robinhood Chain): HIGH confidence. Factory =
    0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e, event
    `TokenLaunched(address,address,address,address,uint256,uint256)`. The
    topic0 hash quoted by Bitquery's docs was independently recomputed with
    keccak256 here and matched exactly, which is strong evidence the
    signature is transcribed correctly.
  - StonkFun (Solana): MEDIUM-HIGH confidence. It is not a proprietary
    firehose but runs through Raydium's LaunchLab program
    (LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj), filtered to two known
    platform-config accounts. Implemented as on-chain log subscription, not
    a WS URL — there is no STONKFUN_WS_URL anymore.
  - Flap.sh (BNB Chain): MEDIUM confidence on the factory address (official
    docs.flap.sh "Portal" contract, 0xe2cE6ab80874Fa9Fa2aAE65D277Dd6B8e65c9De0),
    LOW confidence on the exact `TokenCreated` event parameter layout — no
    full ABI was found, so it falls back to the generic best-effort decoder.
    Verify against BscScan before trusting decoded name/symbol/dev fields.
  - Long.xyz (Robinhood Chain): NOT resolved. Its entry contract is called
    "LongLauncher" but no verified address was found. Stays idle until you
    supply LONGXYZ_FACTORY_ADDRESS yourself (check robinhoodchain.blockscout.com
    against a known long.xyz launch tx, or their docs/support).
  - Ember (Solana): HIGH confidence. Not a WebSocket API — Server-Sent Events
    at EMBER_FEED_URL (https://embercurve.fun/api/solana/feed), found by
    pulling their production JS bundle and grepping it for endpoint strings.
    `launch` is the confirmed event kind for new tokens, per a doc string
    embedded directly in their own shipped code.

Every listener still logs a warning and idles (instead of crash-looping)
when it's missing what it needs, so a blank/unresolved entry never breaks
the rest of the stack.
-----------------------------------------------------------------------------
"""

import asyncio
import base64
import html
import json
import logging
import os
import random
import re
import struct
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from typing import Any, Optional

import aiohttp
import websockets
from Crypto.Hash import keccak as _keccak
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

# ============================================================================
# SECTION 0 — LOGGING & CONFIGURATION
# ============================================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("tracker")

DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8000"))
DATA_DIR = os.getenv("DATA_DIR", "/app/data")

# --- Solana ------------------------------------------------------------
PUMPPORTAL_WS_URL = os.getenv("PUMPPORTAL_WS_URL", "wss://pumpportal.fun/api/data")
# StonkFun runs on Raydium's LaunchLab program, not a proprietary WS API —
# see STONKFUN_LAUNCHLAB_PROGRAM_ID / STONKFUN_PLATFORM_CONFIGS below. It
# only needs SOLANA_WS_RPC_URL, same as wallet monitoring.
# Ember doesn't use WebSocket at all — found by pulling their production JS
# bundle (index-*.js from embercurve.fun/developers) and grepping it for
# endpoint strings. It's Server-Sent Events, confirmed by this exact doc
# string embedded in their own bundle: "Server-sent events: buy, sell,
# holders, lotto, jackpot, burn, dip, bounty, payout, launch, graduate."
# HIGH confidence — this came directly from their shipped code, not a guess.
EMBER_FEED_URL = os.getenv("EMBER_FEED_URL", "https://embercurve.fun/api/solana/feed")
SOLANA_WS_RPC_URL = os.getenv("SOLANA_WS_RPC_URL", "")  # e.g. a Helius/QuickNode Solana WS endpoint
# Tried only after the primary (Helius) endpoint fails/errors — e.g. the
# "max usage reached" (-32429) quota exhaustion observed this session. A free
# public endpoint is NOT a real substitute for a dedicated RPC plan (Solana's
# own docs say as much — low rate limits, and expensive calls like
# getProgramAccounts are commonly disabled outright on free tiers), so this
# only ever softens an outage for cheap calls (getAccountInfo), not a fix for
# a Helius plan that stays exhausted long-term.
SOLANA_FALLBACK_RPC_URL = os.getenv("SOLANA_FALLBACK_RPC_URL", "https://api.mainnet-beta.solana.com")

# --- BNB Chain -----------------------------------------------------------
BNB_WS_RPC_URL = os.getenv("BNB_WS_RPC_URL", "")  # e.g. a QuickNode/Ankr/Chainstack BSC WS endpoint
# TokenManager2 V2 — verified against 3 independent sources (see module docstring).
FOUR_MEME_FACTORY_ADDRESS = os.getenv("FOUR_MEME_FACTORY_ADDRESS", "0x5c952063c7fc8610FFDB798152D69F0B9550762b")
FOUR_MEME_EVENT_SIGNATURE = os.getenv(
    "FOUR_MEME_EVENT_SIGNATURE",
    "TokenCreate(address,address,uint256,string,string,uint256,uint256,uint256)",
)
# Flap.sh "Portal" contract per official docs.flap.sh — address confidence is
# medium, exact event param layout is NOT verified (see module docstring).
FLAP_FACTORY_ADDRESS = os.getenv("FLAP_FACTORY_ADDRESS", "0xe2cE6ab80874Fa9Fa2aAE65D277Dd6B8e65c9De0")
FLAP_EVENT_SIGNATURE = os.getenv(
    "FLAP_EVENT_SIGNATURE", "TokenCreated(address,address,string,string,uint256)"
)

# --- Robinhood Chain (real network, chain ID 4663 — see module docstring) --
ROBINHOOD_WS_RPC_URL = os.getenv("ROBINHOOD_WS_RPC_URL", "")
# Pons factory — cryptographically verified (recomputed keccak256 of the
# event signature matches the topic0 hash quoted in Bitquery's docs).
PONSFAMILY_FACTORY_ADDRESS = os.getenv("PONSFAMILY_FACTORY_ADDRESS", "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e")
PONSFAMILY_EVENT_SIGNATURE = os.getenv(
    "PONSFAMILY_EVENT_SIGNATURE", "TokenLaunched(address,address,address,address,uint256,uint256)"
)
LONGXYZ_FACTORY_ADDRESS = os.getenv("LONGXYZ_FACTORY_ADDRESS", "")  # UNRESOLVED — see module docstring
LONGXYZ_EVENT_SIGNATURE = os.getenv(
    "LONGXYZ_EVENT_SIGNATURE", "TokenCreated(address,address,string,string,uint256)"
)

# --- Market data -----------------------------------------------------------
DEXSCREENER_API_BASE = os.getenv("DEXSCREENER_API_BASE", "https://api.dexscreener.com")

# --- Telegram opportunity pings ---------------------------------------------
# Bot token from @BotFather; chat ID is whichever chat/user/channel should
# receive pings — Telegram gives no way to discover it from the token alone,
# it has to come from a getUpdates call after the target chat has sent the
# bot at least one message. Both blank = feature silently disabled (every
# send site already no-ops in that case), not a startup requirement.
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API_BASE = "https://api.telegram.org"
TELEGRAM_OPPORTUNITY_SCORE_THRESHOLD = float(os.getenv("TELEGRAM_OPPORTUNITY_SCORE_THRESHOLD", "60"))
TELEGRAM_MIN_SEND_INTERVAL_SECONDS = 1.5  # keeps sends under Telegram's per-chat rate limit even if several tokens cross threshold at once
# Early Momentum gets its own, independent ping — same mechanics, different
# score field and threshold. Gated to the same $50k-$200k band the
# dashboard's Early Momentum panel uses (EARLY_MOMENTUM_MIN_MCAP/MAX_MCAP in
# static/dashboard.html) so a noisy ratio spike on a $500 mcap token can't
# fire Telegram — keep these two pairs of numbers in sync if either changes.
TELEGRAM_EARLY_MOMENTUM_SCORE_THRESHOLD = float(os.getenv("TELEGRAM_EARLY_MOMENTUM_SCORE_THRESHOLD", "55"))
EARLY_MOMENTUM_PING_MIN_MCAP_USD = float(os.getenv("EARLY_MOMENTUM_PING_MIN_MCAP_USD", "50000"))
EARLY_MOMENTUM_PING_MAX_MCAP_USD = float(os.getenv("EARLY_MOMENTUM_PING_MAX_MCAP_USD", "200000"))
# Paper-trading size for the Mock Portfolio panel — purely a display multiplier
# (pnl_pct * this / 100), no real funds involved. Answers "how much would I be
# up" without needing per-position custom stake sizing.
MOCK_BUY_SIZE_USD = float(os.getenv("MOCK_BUY_SIZE_USD", "100"))

# --- Filtering engine tunables ----------------------------------------------
SPAM_WINDOW_SECONDS = float(os.getenv("SPAM_WINDOW_SECONDS", str(24 * 3600)))
SPAM_THRESHOLD = int(os.getenv("SPAM_THRESHOLD", "3"))
NARRATIVE_DECAY_SECONDS = float(os.getenv("NARRATIVE_DECAY_SECONDS", "180"))
NARRATIVE_ACCEL_RATIO = float(os.getenv("NARRATIVE_ACCEL_RATIO", "2.5"))
CONVICTION_WINDOW_SECONDS = float(os.getenv("CONVICTION_WINDOW_SECONDS", "180"))
CONVICTION_MULTIPLIER = float(os.getenv("CONVICTION_MULTIPLIER", "3.0"))
HIVE_MIND_WINDOW_SECONDS = float(os.getenv("HIVE_MIND_WINDOW_SECONDS", "180"))
HIVE_MIND_MIN_WALLETS = int(os.getenv("HIVE_MIND_MIN_WALLETS", "2"))
# A brand-new token's buy/sell ratio is almost always lopsided toward buys —
# nobody who bought minutes ago has had time to sell yet, not because the
# token has proven durable demand. A mock-portfolio backtest (n=81) found
# "strong/leaning buy pressure" in 71% of big losers vs only 29% of winners,
# consistent with that being an artifact of newness rather than real signal.
# Requiring the token to have survived this long first means any one-sided
# ratio reflects buyers who chose to hold through an actual window where
# selling was possible, not just "too early for sells to exist yet."
BUY_PRESSURE_MIN_AGE_SECONDS = float(os.getenv("BUY_PRESSURE_MIN_AGE_SECONDS", "600"))

# --- Post-trade feedback loop tunables --------------------------------------
TARGET_MARKET_CAP_USD = float(os.getenv("TARGET_MARKET_CAP_USD", "100000"))
# Market cap alone is gameable (it's just price × supply — one thin trade can
# inflate it with no real buyers behind it). Require real trading activity too
# before calling something GRADUATED, not just a bare mcap crossing.
MIN_GRADUATION_VOLUME_USD = float(os.getenv("MIN_GRADUATION_VOLUME_USD", "20000"))
MIN_GRADUATION_LIQUIDITY_USD = float(os.getenv("MIN_GRADUATION_LIQUIDITY_USD", "5000"))
# A high dollar-volume figure can come from a handful of large/wash trades —
# require a minimum number of distinct buy+sell transactions too, or a token
# with a big mcap and almost no real participants still won't graduate.
MIN_GRADUATION_TXNS = int(os.getenv("MIN_GRADUATION_TXNS", "50"))
# marketCap/fdv is price × supply — a handful of trades in thin liquidity can
# push price (and therefore mcap) far out of proportion to real turnover.
# Require volume to be at least this fraction of mcap, or the number is
# probably not backed by real trading. Caught a real case: a token hit $6.7M
# mcap on its first poll (~50s after creation) via 211 buys / 1 sell — a
# sniping-bot pump, not organic growth — which passed the dollar-volume and
# txn-count gates alone but has a volume/mcap ratio near 1.5%.
MIN_GRADUATION_VOLUME_TO_MCAP_RATIO = float(os.getenv("MIN_GRADUATION_VOLUME_TO_MCAP_RATIO", "0.15"))
# Refuse to graduate anything younger than this — an instant, single-poll
# graduation is almost always bots racing the creation firehose, not sustained
# organic demand. Forces at least a couple of poll cycles of confirmation.
MIN_GRADUATION_AGE_SECONDS = float(os.getenv("MIN_GRADUATION_AGE_SECONDS", "120"))
# An extremely lopsided buy:sell ratio (e.g. 211 buys / 1 sell) means nobody
# has taken profit yet — early-stage bot accumulation, not a proven market.
MAX_GRADUATION_BUY_SELL_SKEW = float(os.getenv("MAX_GRADUATION_BUY_SELL_SKEW", "10.0"))

# Platforms with a verified, authoritative native graduation signal (see
# mark_token_graduated and the listeners that call it) — the DexScreener-
# polling heuristic below is skipped for these so the platform's own ground
# truth always wins instead of our approximation possibly firing first/instead.
PLATFORMS_WITH_NATIVE_GRADUATION = {"pump.fun", "ember", "pons"}

# Hard floor for even being considered in the Top Opportunities panel — no
# combination of dev-trust/narrative/smart-money signals should outrank basic
# "does this even have real market activity yet."
MIN_OPPORTUNITY_MARKET_CAP_USD = float(os.getenv("MIN_OPPORTUNITY_MARKET_CAP_USD", "28000"))
MIN_OPPORTUNITY_VOLUME_USD = float(os.getenv("MIN_OPPORTUNITY_VOLUME_USD", "500"))

# Multicall3 — a generic batching contract deployed at this identical address
# on nearly every EVM chain (canonical, well-known infra, not a person). If a
# token launch gets routed through a multicall/batch tx, the factory's
# "deployer" event field legitimately records the caller as this contract
# instead of the real human behind it — confirmed by comparing a live Pons
# launch's event "deployer" field against its actual transaction sender (they
# matched for a normal launch; this address showed up only for batched ones).
# Treated as neutral infrastructure, never attributed reputation or counted
# as a distinct "dev" for narrative clustering.
KNOWN_INFRASTRUCTURE_ADDRESSES = {"0xca11bde05977b3631167028862be2a173976ca11"}
RUG_WINDOW_SECONDS = float(os.getenv("RUG_WINDOW_SECONDS", "600"))
RUG_DRAWDOWN_PCT = float(os.getenv("RUG_DRAWDOWN_PCT", "0.9"))  # 90% down from peak = rug
POST_TRADE_POLL_INTERVAL = float(os.getenv("POST_TRADE_POLL_INTERVAL", "20"))
STATE_SNAPSHOT_INTERVAL = float(os.getenv("STATE_SNAPSHOT_INTERVAL", "60"))

# ============================================================================
# SECTION 1 — HARDCODED REGISTRIES & IN-MEMORY STATE DATABASES
# ============================================================================
# The four registries below are the schemas requested. SMART_WALLETS and
# DEV_REPUTATION_DATABASE are seeded with EXAMPLE data so the system is
# runnable out of the box — replace these with the wallets/devs you actually
# want to track. NARRATIVE_CACHE and HIVE_MIND_CACHE start empty; they are
# populated live.

SMART_WALLETS: dict[str, dict[str, Any]] = {
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": {
        "alias": "ExampleWallet-SOL-Alpha",
        "chain": "solana",
        "avg_trade_size_usd": 2500.0,
        "win_rate": 0.71,
    },
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1": {
        "alias": "ExampleWallet-SOL-Sniper",
        "chain": "solana",
        "avg_trade_size_usd": 800.0,
        "win_rate": 0.63,
    },
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": {
        "alias": "ExampleWallet-BNB-Degen",
        "chain": "bnb",
        "avg_trade_size_usd": 4200.0,
        "win_rate": 0.58,
    },
    "0x28c6c06298d514db089934071355e5743bf21d60": {
        "alias": "ExampleWallet-BNB-Scalper",
        "chain": "bnb",
        "avg_trade_size_usd": 1100.0,
        "win_rate": 0.66,
    },
    "0x6b175474e89094c44da98b954eedeac495271d0f": {
        "alias": "ExampleWallet-Robinhood-Chain-Whale",
        "chain": "robinhood",
        "avg_trade_size_usd": 3000.0,
        "win_rate": 0.60,
    },
}

DEV_REPUTATION_DATABASE: dict[str, dict[str, Any]] = {
    "7VfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b3xnvcGw": {
        "alias": "ExampleDev-ProvenBuilder",
        "chain": "solana",
        "successful_launches": 3,
        "failed_spams": 0,
        "is_blacklisted": False,
        "last_launch_time": 0.0,
    },
    "0x000000000000000000000000000000deadbeef": {
        "alias": "ExampleDev-KnownRugger",
        "chain": "bnb",
        "successful_launches": 0,
        "failed_spams": 3,
        "is_blacklisted": True,
        "last_launch_time": 0.0,
    },
}

NARRATIVE_CACHE: dict[str, list[tuple[float, str, str, str, str]]] = defaultdict(list)

HIVE_MIND_CACHE: dict[str, set[str]] = defaultdict(set)

# --- Supporting internal caches (not part of the four required schemas, but
# needed to correctly implement the windowed/velocity logic requested) ------
DEV_SPAM_LOG: dict[str, list[float]] = defaultdict(list)
# What specifically got a dev blacklisted — surfaced on SKIPPED alerts so
# "blacklisted" isn't a black box; capped per-dev so it can't grow unbounded.
DEV_RUG_HISTORY: dict[str, list[dict[str, Any]]] = defaultdict(list)
DEV_RUG_HISTORY_MAX_PER_DEV = 5

# --- Bundled-wallet detection (see detect_evm_bundle / detect_solana_bundle) -
# A "bundle" is a set of wallets that only *look* like independent holders —
# in reality they were all funded (Solana: same System Program transfer
# source) or all distributed to in one shot (EVM: same transaction hash) by
# one operator. BUNDLE_OPERATOR_HISTORY links that operator identity to every
# token/dev_wallet it has touched, which is the whole point: a dev wallet is
# trivial for a bad actor to throw away and recreate, but the operator
# fingerprint behind the bundle persists across that — so a serial bundler
# rotating dev wallets to dodge DEV_REPUTATION_DATABASE's per-wallet blacklist
# still gets caught here once the same operator reappears on a second token.
BUNDLE_OPERATOR_HISTORY: dict[str, list[dict[str, Any]]] = defaultdict(list)
BUNDLE_OPERATOR_BLACKLIST: set[str] = set()
TOKEN_BUNDLE_INFO: dict[str, dict[str, Any]] = {}

# Which tokens have already fired a Telegram opportunity ping — persisted
# (see persist_state/_load_state_sync) specifically so a redeploy doesn't
# re-ping every still-above-threshold token still WATCHING at restart time.
# Bounded like TOKEN_FEED so long-running uptime can't grow this unbounded.
TELEGRAM_PINGED_TOKENS: dict[str, float] = {}
TELEGRAM_PINGED_MAX = 2000
# Separate, lower-bar dedup set from TELEGRAM_PINGED_TOKENS: the Top
# Opportunities panel shows ANY WATCHING token with opportunity_score > 0,
# not just ones crossing TELEGRAM_OPPORTUNITY_SCORE_THRESHOLD, so a token can
# flash into that panel and back out (graduating, or dipping back under the
# hard $ floor for one poll) without ever reaching the Telegram bar. This
# records the first time EVERY such token appears there, independent of
# Telegram, so it isn't just lost.
OPPORTUNITY_RECORDED_TOKENS: dict[str, float] = {}
OPPORTUNITY_RECORDED_MAX = 2000
TELEGRAM_EARLY_PINGED_TOKENS: dict[str, float] = {}
TELEGRAM_EARLY_PINGED_MAX = 2000
# One simulated position per token, opened the instant it's first flagged as
# an opportunity (same trigger as RECENT_OPPORTUNITIES/OPPORTUNITY_RECORDED_TOKENS
# above) — "if I'd bought $MOCK_BUY_SIZE_USD the moment this showed up, would I
# be up or down right now." Never closed automatically: mcap keeps refreshing
# for GRADUATED/RUGGED tokens too (see the terminal-token refresh fix), so a
# position naturally reflects the full outcome, good or bad.
MOCK_PORTFOLIO: dict[str, dict[str, Any]] = {}
MOCK_PORTFOLIO_MAX = 500
NARRATIVE_STATUS: dict[str, dict[str, Any]] = {}
HIVE_MIND_TIMESTAMPS: dict[str, list[tuple[str, float]]] = defaultdict(list)
TOKEN_CREATION_TIME: dict[str, float] = {}
TOKEN_WATCHLIST: dict[str, dict[str, Any]] = {}

# Bounded, dashboard-facing token feed (survives a browser refresh via the
# /ws/dashboard snapshot). Insertion order is creation order; updates mutate
# in place without moving position. Capped so a 24/7 deployment doesn't leak
# memory indefinitely.
TOKEN_FEED: dict[str, dict[str, Any]] = {}
TOKEN_FEED_MAX = 1500
SNAPSHOT_TOKEN_LIMIT = 400  # cap on how many tokens a reconnect snapshot sends at once
SPARKLINE_MAX_POINTS = 24


def token_feed_upsert(token_address: str, **fields: Any) -> dict[str, Any]:
    entry = TOKEN_FEED.get(token_address)
    if entry is None:
        entry = {"token_address": token_address}
        TOKEN_FEED[token_address] = entry
        if len(TOKEN_FEED) > TOKEN_FEED_MAX:
            oldest_key = next(iter(TOKEN_FEED))
            if oldest_key != token_address:
                del TOKEN_FEED[oldest_key]
    entry.update(fields)
    return entry

CONNECTED_CLIENTS: set[WebSocket] = set()
ALERT_HISTORY: deque = deque(maxlen=300)
BACKGROUND_TASKS: list[asyncio.Task] = []

# Per-UTC-day counters for the "Today's Summary" panel — resets naturally at
# UTC midnight since the key is the date string. Kept separate from
# ALERT_HISTORY (which is capped at 300 and would roll over well within a day
# on an active feed) so daily totals stay accurate regardless of alert volume.
DAILY_STATS: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
DAILY_STATS_MAX_DAYS = 14

# Per-UTC-hour new-token counts (key "YYYY-MM-DDTHH"), for the "Launch
# Activity by Hour" panel — answers "what time of day does the market see
# the most new contracts deployed," which a per-day counter can't show.
# Pruned in lockstep with DAILY_STATS (same retention window) so it can't
# grow unbounded on a long-running instance.
HOURLY_LAUNCH_STATS: dict[str, int] = defaultdict(int)

# Dedicated, persisted lists for the "Recent Graduations"/"Recent Rugs" panels.
# The shared ALERT_HISTORY buffer (300 cap, every alert type combined) gets
# dominated by high-frequency SKIPPED alerts on an active feed, crowding out
# the much rarer GRADUATED/RUGGED ones within minutes — these are separate so
# that noise can never evict them.
RECENT_GRADUATIONS: deque = deque(maxlen=100)
RECENT_RUGS: deque = deque(maxlen=100)
# A token crossing the opportunity threshold is a genuinely rare, meaningful
# event — same reasoning as above, but for Top Opportunities specifically.
# The live panel only ever shows CURRENTLY-qualifying WATCHING tokens, so a
# token that crossed the bar and then graduated (leaving WATCHING) or whose
# mcap/volume dipped back under the hard floor for one poll cycle would just
# vanish with no record it was ever flagged — this is the permanent log of
# "every token that was ever flagged," independent of what happens to it after.
RECENT_OPPORTUNITIES: deque = deque(maxlen=100)


# ============================================================================
# SECTION 2 — UTILITIES
# ============================================================================

def normalize_ticker(raw: Optional[str]) -> str:
    """Uppercase, strip whitespace/emoji/punctuation — used for narrative clustering."""
    if not raw:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", raw).upper()


def narrative_cluster_key(normalized_ticker: str) -> str:
    """Collapses deliberate ticker-squat variants ($PEPE2, $PEPEX, $PEPEV2)
    onto their base name so a copycat narrative isn't missed just because the
    squatter changed one character on top of an already-trending ticker.
    Exact-match-only clustering would treat $PEPE and $PEPE2 as unrelated.
    Only collapses when the stripped base is still >=3 chars, to avoid
    degenerate collisions on already-short tickers (and a ticker whose real
    identity happens to end in digits, e.g. $BASE64, is an accepted false
    positive of this heuristic — it just joins a cluster, doesn't get
    excluded or blocked)."""
    if not normalized_ticker:
        return normalized_ticker
    squatted = re.sub(r"(V\d+|X)$", "", normalized_ticker)
    squatted = re.sub(r"\d+$", "", squatted)
    return squatted if len(squatted) >= 3 else normalized_ticker


def keccak256_topic(event_signature: str) -> str:
    """Compute the 0x-prefixed Keccak-256 topic0 hash for an EVM event signature."""
    k = _keccak.new(digest_bits=256)
    k.update(event_signature.encode("utf-8"))
    return "0x" + k.hexdigest()


def address_to_topic(address: str) -> str:
    """Left-pad a 20-byte EVM address into a 32-byte indexed-topic hex string."""
    return "0x" + address.lower().replace("0x", "").rjust(64, "0")


TRANSFER_EVENT_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
FOUR_MEME_TOPIC0 = keccak256_topic(FOUR_MEME_EVENT_SIGNATURE)
FLAP_TOPIC0 = keccak256_topic(FLAP_EVENT_SIGNATURE)
PONSFAMILY_TOPIC0 = keccak256_topic(PONSFAMILY_EVENT_SIGNATURE)
# LaunchSwept(address indexed token, uint256, uint256) — discovered by live-
# subscribing to ALL logs from the Pons factory (no topic filter) and computing
# keccak256 of candidate signatures until one matched exactly (it did, exactly,
# for this one). Used here as Pons's bonding-curve-finalization signal.
PONS_LAUNCH_SWEPT_TOPIC0 = keccak256_topic("LaunchSwept(address,uint256,uint256)")
LONGXYZ_TOPIC0 = keccak256_topic(LONGXYZ_EVENT_SIGNATURE)

# StonkFun (Solana) runs through Raydium's LaunchLab program rather than its
# own WS API. Program ID + the two known platform-config accounts per
# Bitquery's StonkFun API docs (see module docstring for confidence notes).
STONKFUN_LAUNCHLAB_PROGRAM_ID = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"
STONKFUN_PLATFORM_CONFIGS = [
    "6BwHHDg3u1854jC8PDLXvR4spTcLNaoBxLJNGC4nTESt",  # reward launches
    "4E876qZTE9FJMrBzgVtBrSrzz2TLivB5Y5QXPjB4gZL7",  # standard launches
]


async def run_forever(coro_factory, name: str) -> None:
    """Wrap an async loop with exponential-backoff reconnection so it never fatally dies."""
    backoff = 1.0
    while True:
        started = time.time()
        try:
            logger.info(f"[{name}] starting...")
            await coro_factory()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - top-level resiliency boundary by design
            logger.warning(f"[{name}] error: {exc!r}")
        elapsed = time.time() - started
        backoff = 1.0 if elapsed > 60 else min(backoff * 2, 60.0)
        sleep_for = backoff + random.uniform(0, 1)
        logger.info(f"[{name}] reconnecting in {sleep_for:.1f}s")
        await asyncio.sleep(sleep_for)


# ============================================================================
# SECTION 3 — GENERIC JSON-RPC-OVER-WEBSOCKET CLIENT (EVM + Solana)
# ============================================================================

class JsonRpcWsClient:
    """
    Minimal JSON-RPC 2.0 client over a persistent WebSocket connection.
    Correlates request/response pairs by id while separately queueing
    subscription push notifications (eth_subscription / logsNotification /
    etc.) so a caller can `call()` for request/response RPCs and
    `next_notification()` for streamed events on the same socket.
    """

    def __init__(self, url: str):
        self.url = url
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._next_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._notifications: asyncio.Queue = asyncio.Queue()
        self._reader_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        self.ws = await websockets.connect(
            self.url, ping_interval=20, ping_timeout=20, max_size=2 ** 23
        )
        self._reader_task = asyncio.create_task(self._reader())

    async def _reader(self) -> None:
        assert self.ws is not None
        async for raw in self.ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(msg, dict):
                continue
            if "id" in msg and msg["id"] in self._pending:
                fut = self._pending.pop(msg["id"])
                if not fut.done():
                    fut.set_result(msg)
            elif "method" in msg:
                await self._notifications.put(msg)

    async def call(self, method: str, params: list, timeout: float = 20.0) -> dict:
        self._next_id += 1
        req_id = self._next_id
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut
        assert self.ws is not None
        await self.ws.send(json.dumps({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}))
        return await asyncio.wait_for(fut, timeout)

    async def subscribe(self, method: str, params: list) -> Any:
        resp = await self.call(method, params)
        return resp.get("result")

    async def next_notification(self) -> dict:
        msg = await self._notifications.get()
        params = msg.get("params", {})
        result = params.get("result")
        return result if isinstance(result, dict) else {}

    async def close(self) -> None:
        if self._reader_task:
            self._reader_task.cancel()
        if self.ws:
            await self.ws.close()


# ============================================================================
# SECTION 4 — EVM LOG DECODING HELPERS
# ============================================================================

def decode_evm_abi_string(hex_data: str) -> str:
    """Decode a single ABI-encoded dynamic `string` return value (e.g. from name()/symbol())."""
    if not hex_data or hex_data == "0x":
        return ""
    data = bytes.fromhex(hex_data[2:])
    if len(data) < 64:
        return ""
    length = int.from_bytes(data[32:64], "big")
    raw = data[64:64 + length]
    return raw.decode("utf-8", errors="ignore").strip("\x00").strip()


def decode_token_created_log(data_hex: str, topics: list[str]) -> dict[str, str]:
    """
    Generic decoder for a `TokenCreated(address creator, address token, string name,
    string symbol, uint256 param)`-shaped event — a common pattern across many
    BEP-20/ERC-20 fair-launch factory contracts. Handles 0, 1, or 2 indexed
    address parameters. If a specific factory's verified ABI differs, adjust
    this function to match its source on the block explorer.
    """
    data_hex = data_hex or "0x"
    data = bytes.fromhex(data_hex[2:]) if data_hex.startswith("0x") else bytes.fromhex(data_hex)
    if len(data) < 128:
        raise ValueError("log data too short for TokenCreated decode")

    indexed_addresses = [("0x" + t[-40:]) for t in topics[1:] if isinstance(t, str) and len(t) == 66]

    def word(i: int) -> bytes:
        start, end = i * 32, (i + 1) * 32
        return data[start:end]

    def addr_from_word(i: int) -> str:
        return "0x" + word(i)[-20:].hex()

    def dyn_string(word_index: int) -> str:
        offset = int.from_bytes(word(word_index), "big")
        length = int.from_bytes(data[offset:offset + 32], "big")
        raw = data[offset + 32: offset + 32 + length]
        return raw.decode("utf-8", errors="ignore").strip("\x00").strip()

    if len(indexed_addresses) >= 2:
        creator, token = indexed_addresses[0], indexed_addresses[1]
        name, symbol = dyn_string(0), dyn_string(1)
    elif len(indexed_addresses) == 1:
        creator = indexed_addresses[0]
        token = addr_from_word(0)
        name, symbol = dyn_string(1), dyn_string(2)
    else:
        creator = addr_from_word(0)
        token = addr_from_word(1)
        name, symbol = dyn_string(2), dyn_string(3)

    return {"creator": creator, "token": token, "name": name, "symbol": symbol}


async def evm_get_decimals(client: JsonRpcWsClient, token_address: str) -> int:
    try:
        resp = await client.call("eth_call", [{"to": token_address, "data": "0x313ce567"}, "latest"])
        result = resp.get("result", "0x")
        if not result or result == "0x":
            return 18
        return int(result, 16)
    except Exception:
        return 18


async def evm_get_token_metadata(client: JsonRpcWsClient, token_address: str) -> tuple[str, str]:
    """Standard ERC-20 name()/symbol() calls — used when an event doesn't carry
    the token's metadata directly (e.g. Pons only emits addresses in topics)."""
    try:
        name_resp = await client.call("eth_call", [{"to": token_address, "data": "0x06fdde03"}, "latest"])
        symbol_resp = await client.call("eth_call", [{"to": token_address, "data": "0x95d89b41"}, "latest"])
        name = decode_evm_abi_string(name_resp.get("result", "0x"))
        symbol = decode_evm_abi_string(symbol_resp.get("result", "0x"))
        return name, symbol
    except Exception as exc:
        logger.debug(f"evm_get_token_metadata({token_address}) failed: {exc!r}")
        return "", ""


def decode_four_meme_token_create(data_hex: str) -> dict[str, Any]:
    """Dedicated decoder for four.meme's verified TokenCreate event:
    TokenCreate(address creator, address token, uint256 requestId, string name,
    string symbol, uint256 totalSupply, uint256 launchTime, uint256 launchFee)
    — no indexed parameters, all data. See module docstring for the source."""
    data_hex = data_hex or "0x"
    data = bytes.fromhex(data_hex[2:]) if data_hex.startswith("0x") else bytes.fromhex(data_hex)
    if len(data) < 256:
        raise ValueError("four.meme TokenCreate log data too short")

    def word(i: int) -> bytes:
        return data[i * 32:(i + 1) * 32]

    def addr_from_word(i: int) -> str:
        return "0x" + word(i)[-20:].hex()

    def uint_from_word(i: int) -> int:
        return int.from_bytes(word(i), "big")

    def dyn_string(word_index: int) -> str:
        offset = uint_from_word(word_index)
        length = int.from_bytes(data[offset:offset + 32], "big")
        raw = data[offset + 32: offset + 32 + length]
        return raw.decode("utf-8", errors="ignore").strip("\x00").strip()

    return {
        "creator": addr_from_word(0),
        "token": addr_from_word(1),
        "request_id": uint_from_word(2),
        "name": dyn_string(3),
        "symbol": dyn_string(4),
        "total_supply": uint_from_word(5),
        "launch_time": uint_from_word(6),
        "launch_fee": uint_from_word(7),
    }


# ============================================================================
# SECTION 5 — MARKET DATA (DexScreener — public, unauthenticated, real)
# ============================================================================

async def fetch_dexscreener_info(token_address: str) -> dict[str, Any]:
    """Single DexScreener lookup returning market cap, price, token name/symbol,
    24h volume, 24h buy/sell tx counts, and liquidity. Volume/liquidity matter a
    lot here: a token's marketCap/fdv is just (price × total supply) — it can be
    pushed up by one thin trade with no real buyers behind it. Volume and
    liquidity are the actual-activity signals that tell a real graduation apart
    from a bloated, low-volume one. NOTE: DexScreener does not expose holder
    counts, and neither BscScan (needs a paid/keyed API this project doesn't
    have) nor Blockscout (its API sits behind a Cloudflare bot-challenge that
    blocks non-browser requests, confirmed by testing it directly) are usable
    for that from a server-side process — holder count is not implemented."""
    url = f"{DEXSCREENER_API_BASE}/latest/dex/tokens/{token_address}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return {}
                payload = await resp.json()
                pairs = payload.get("pairs") or []
                if not pairs:
                    return {}
                pairs.sort(key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0), reverse=True)
                best = pairs[0]
                base = best.get("baseToken") or {}
                volume = best.get("volume") or {}
                txns_24h = (best.get("txns") or {}).get("h24") or {}
                # DexScreener only populates `info` once a project has
                # submitted a token profile — usually null for a brand-new
                # fair-launch token, populated for anything more established.
                # Best-effort, not guaranteed coverage; free since this
                # endpoint is already being polled for every other field here.
                info = best.get("info") or {}
                return {
                    "market_cap": float(best.get("marketCap") or best.get("fdv") or 0.0),
                    "price_usd": float(best.get("priceUsd") or 0.0),
                    "symbol": (base.get("symbol") or "").strip(),
                    "name": (base.get("name") or "").strip(),
                    "volume_24h": float(volume.get("h24") or 0.0),
                    "liquidity_usd": float((best.get("liquidity") or {}).get("usd") or 0.0),
                    "buys_24h": int(txns_24h.get("buys") or 0),
                    "sells_24h": int(txns_24h.get("sells") or 0),
                    "txns_24h": int(txns_24h.get("buys") or 0) + int(txns_24h.get("sells") or 0),
                    "image_url": info.get("imageUrl"),
                }
    except Exception as exc:
        logger.debug(f"fetch_dexscreener_info({token_address}) failed: {exc!r}")
        return {}


async def fetch_token_price_usd(token_address: str) -> float:
    info = await fetch_dexscreener_info(token_address)
    return info.get("price_usd", 0.0)


async def fetch_token_market_cap_usd(token_address: str) -> float:
    info = await fetch_dexscreener_info(token_address)
    return info.get("market_cap", 0.0)


CHAIN_EXPLORER_URLS = {
    "solana": "https://solscan.io/token/{addr}",
    "bnb": "https://bscscan.com/token/{addr}",
    "robinhood": "https://robinhoodchain.blockscout.com/token/{addr}",
}
# DexScreener chain slugs — "solana"/"bsc" are certain; "robinhood" is a
# reasonable guess for a brand-new chain and may 404 if not indexed yet, but
# that's a harmless dead link, not a functional risk.
DEXSCREENER_CHAIN_SLUGS = {"solana": "solana", "bnb": "bsc", "robinhood": "robinhood"}


def build_token_links(chain: str, token_address: str) -> dict[str, str]:
    links = {}
    explorer_tpl = CHAIN_EXPLORER_URLS.get(chain)
    if explorer_tpl:
        links["explorer"] = explorer_tpl.format(addr=token_address)
    slug = DEXSCREENER_CHAIN_SLUGS.get(chain)
    if slug:
        links["dexscreener"] = f"https://dexscreener.com/{slug}/{token_address}"
    return links


# --- Pump.fun bonding curve account decoding (free alternative to PumpPortal's
# paid subscribeTokenTrade feed) -------------------------------------------
# Layout verified empirically: fetched a real bonding curve account, and its
# first 8 bytes matched pump.fun's documented BondingCurve discriminator
# [23,183,248,55,96,216,172,96] exactly; decoded token_total_supply came out
# to precisely 1,000,000,000 tokens (pump.fun's known standard supply); and
# virtual reserve deltas between two time-separated fetches moved in the
# economically-correct direction for real buy activity in between. Not a guess.
PUMPFUN_BONDING_CURVE_DISCRIMINATOR = bytes([23, 183, 248, 55, 96, 216, 172, 96])
# Commonly-cited target for the classic pump.fun curve to complete (~85 SOL of
# REAL reserves raised). Used only to render an approximate progress % — the
# `complete` boolean decoded from the account itself is the actual ground
# truth and doesn't depend on this number being exactly right.
PUMPFUN_BONDING_CURVE_SOL_TARGET = float(os.getenv("PUMPFUN_BONDING_CURVE_SOL_TARGET", "85"))
# A token still on pump.fun's bonding curve (status WATCHING, not yet
# migrated) is mechanically capped near PUMPFUN_BONDING_CURVE_SOL_TARGET
# worth of SOL raised — nowhere close to hundreds of thousands of dollars.
# Confirmed empirically this session: several "new token create" events
# PumpPortal reported turned out to have mints belonging to already-
# established, unrelated tokens with real six/seven-figure mcap — most
# visibly a run of real xStocks tokenized-equity tickers (AAPLx, TSLAx,
# MCDx, COINx, ...) showing up as if freshly launched on pump.fun, bloating
# the opportunity list with tokens that were never actually new. Root cause
# on PumpPortal's/parsing side not fully confirmed, but the anomaly itself
# is unambiguous and mechanically impossible for a real bonding-curve token,
# so it's used as a data-integrity tripwire rather than left unexplained.
PUMPFUN_IMPLAUSIBLE_WATCHING_MCAP_USD = float(os.getenv("PUMPFUN_IMPLAUSIBLE_WATCHING_MCAP_USD", "200000"))


def _solana_https_rpc_url() -> str:
    """getAccountInfo returned 'Method not found' over Helius's WS endpoint
    (confirmed by testing directly) — Helius restricts WS to a subscription-
    oriented method subset. The same base URL works fine over plain HTTPS."""
    return SOLANA_WS_RPC_URL.replace("wss://", "https://").replace("ws://", "http://")


async def _solana_rpc_post(session: aiohttp.ClientSession, method: str, params: list, timeout: float = 10.0) -> Optional[dict]:
    """Shared POST-with-fallback for every plain-HTTPS Solana RPC call in this
    file. Tries the primary (Helius) endpoint first; on ANY failure there —
    non-200, an "error" envelope (this is how a 429 quota-exhaustion surfaces:
    HTTP 200 with {"error": {"code": -32429, "message": "max usage reached"}}),
    or a network exception — retries once against SOLANA_FALLBACK_RPC_URL
    before giving up. Returns the "result" field directly (not the raw
    envelope), or None if both attempts failed."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    urls = [u for u in (_solana_https_rpc_url(), SOLANA_FALLBACK_RPC_URL) if u]
    for i, url in enumerate(urls):
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()
                if "error" in data:
                    logger.debug(f"[solana-rpc] {method} via {'primary' if i == 0 else 'fallback'} error: {data['error']}")
                    continue
                return data.get("result")
        except Exception as exc:
            logger.debug(f"[solana-rpc] {method} via {'primary' if i == 0 else 'fallback'} failed: {exc!r}")
            continue
    return None


async def fetch_pumpfun_bonding_curve_state(bonding_curve_key: str) -> dict[str, Any]:
    if not SOLANA_WS_RPC_URL or not bonding_curve_key:
        return {}
    try:
        async with aiohttp.ClientSession() as session:
            result = await _solana_rpc_post(session, "getAccountInfo", [bonding_curve_key, {"encoding": "base64"}])
            value = (result or {}).get("value")
            if not value:
                return {}
            raw = base64.b64decode(value["data"][0])
            if len(raw) < 49 or raw[0:8] != PUMPFUN_BONDING_CURVE_DISCRIMINATOR:
                return {}
            virtual_token_reserves, virtual_sol_reserves, real_token_reserves, real_sol_reserves, token_total_supply = \
                struct.unpack("<5Q", raw[8:48])
            complete = bool(raw[48])
            sol_raised = real_sol_reserves / 1e9
            progress_pct = min(100.0, (sol_raised / PUMPFUN_BONDING_CURVE_SOL_TARGET) * 100.0) if PUMPFUN_BONDING_CURVE_SOL_TARGET else 0.0
            return {
                "bonding_sol_raised": sol_raised,
                "bonding_progress_pct": progress_pct,
                "bonding_complete": complete,
                "bonding_virtual_sol": virtual_sol_reserves / 1e9,
                "bonding_virtual_tokens": virtual_token_reserves / 1e6,
            }
    except Exception as exc:
        logger.debug(f"fetch_pumpfun_bonding_curve_state({bonding_curve_key}) failed: {exc!r}")
        return {}


PUMPFUN_COIN_API_BASE = "https://frontend-api-v3.pump.fun"


async def fetch_pumpfun_image(mint_address: str) -> Optional[str]:
    """pump.fun's own coin API — undocumented (found by testing directly,
    not from official docs), but confirmed real: its bonding_curve field
    matches the address our own on-chain bonding-curve decoder already
    derives independently. Used for exactly one thing — image_uri — never
    for scoring-relevant fields (mcap, bonding progress, its own
    security_verdict), since an unofficial surface that could change without
    notice shouldn't become load-bearing when DexScreener/on-chain sources
    already cover that ground."""
    url = f"{PUMPFUN_COIN_API_BASE}/coins/{mint_address}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=10), headers={"User-Agent": "Mozilla/5.0"}
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                return data.get("image_uri") or None
    except Exception as exc:
        logger.debug(f"fetch_pumpfun_image({mint_address}) failed: {exc!r}")
        return None


PUMPFUN_IMAGE_MAX_ATTEMPTS = 5  # ~5 poll cycles — pump.fun's own backend can lag a few seconds behind a mint actually existing on-chain


async def _maybe_fetch_pumpfun_image(token_address: str, info: dict[str, Any]) -> None:
    if info.get("pumpfun_image_checked"):
        return
    if info.get("platform") != "pump.fun" or (TOKEN_FEED.get(token_address) or {}).get("image_url"):
        info["pumpfun_image_checked"] = True
        return
    image_url = await fetch_pumpfun_image(token_address)
    if image_url:
        info["pumpfun_image_checked"] = True
        token_feed_upsert(token_address, image_url=image_url)
        await broadcast_token_card(TOKEN_FEED[token_address])
        return
    # Confirmed empirically: a mint that already exists on-chain (and is
    # independently fetchable moments later) can still 404/miss on pump.fun's
    # own API on the very first poll — their backend indexing lags slightly
    # behind chain state. Retry a bounded number of times before giving up,
    # rather than permanently missing it on one early miss.
    attempts = info.get("pumpfun_image_attempts", 0) + 1
    info["pumpfun_image_attempts"] = attempts
    if attempts >= PUMPFUN_IMAGE_MAX_ATTEMPTS:
        info["pumpfun_image_checked"] = True


# pump.fun/StonkFun/Ember mints aren't all one program: newer pump.fun launches
# mint under Token-2022 (metadata-extension tokens), older ones and most other
# platforms still use the classic SPL Token program. Token-2022 accounts carry
# variable-length extension data, so they can't be found with a fixed dataSize
# filter the way classic accounts can — confirmed empirically: a classic-only
# dataSize=165 filter silently matched zero accounts against a real,
# actively-traded Token-2022 mint.
SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SPL_TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
SPL_TOKEN_ACCOUNT_SIZE = 165
SOLANA_LAST_HOLDER_BALANCES: dict[str, dict[str, float]] = {}


async def fetch_solana_holder_stats(mint_address: str) -> dict[str, Any]:
    """Holder count and concentration aren't something DexScreener exposes at
    all — but every token account is public state, so getProgramAccounts
    filtered to this mint returns literally every holder directly, no
    transaction-history replay needed. This is the one RPC call in the whole
    pipeline expensive enough to warrant its own slow, separately-throttled
    poll cadence (see HOLDER_STATS_POLL_INTERVAL_SECONDS) — a full scan of
    every token account for a mint, not a cheap indexed lookup."""
    if not SOLANA_WS_RPC_URL:
        return {}
    mint_authority_active = False
    freeze_authority_active = False
    try:
        async with aiohttp.ClientSession() as session:
            # jsonParsed (not base64) so the SAME call that identifies which
            # token program owns this mint also hands back mintAuthority/
            # freezeAuthority pre-decoded (null once renounced) — no separate
            # RPC round-trip or manual byte-offset parsing needed for the
            # mintable/freezable check below.
            mint_result = await _solana_rpc_post(session, "getAccountInfo", [mint_address, {"encoding": "jsonParsed"}])
            if mint_result is None:
                return {}
            mint_value = mint_result.get("value") or {}
            owner_program = mint_value.get("owner")
            parsed_info = ((mint_value.get("data") or {}).get("parsed") or {}).get("info") or {}
            mint_authority_active = parsed_info.get("mintAuthority") is not None
            freeze_authority_active = parsed_info.get("freezeAuthority") is not None
            if owner_program not in (SPL_TOKEN_PROGRAM_ID, SPL_TOKEN_2022_PROGRAM_ID):
                return {}

            filters: list[dict[str, Any]] = [{"memcmp": {"offset": 0, "bytes": mint_address}}]
            if owner_program == SPL_TOKEN_PROGRAM_ID:
                # Only the classic program has a fixed, non-extensible account
                # size — safe to narrow the scan with it. Token-2022 accounts
                # vary in length once extensions are attached, so no dataSize
                # filter is applied for that program.
                filters.insert(0, {"dataSize": SPL_TOKEN_ACCOUNT_SIZE})

            # getProgramAccounts is a full-table-scan-style call that most
            # free public RPCs explicitly disable — the fallback in
            # _solana_rpc_post will still be attempted, but only ever expect
            # it to help with the cheap getAccountInfo call above.
            accounts = await _solana_rpc_post(
                session, "getProgramAccounts",
                [owner_program, {"encoding": "jsonParsed", "filters": filters}],
                timeout=20.0,
            )
            if accounts is None:
                return {}
    except Exception as exc:
        logger.debug(f"fetch_solana_holder_stats({mint_address}) failed: {exc!r}")
        return {}

    balances: dict[str, float] = {}
    for acc in accounts:
        try:
            info = acc["account"]["data"]["parsed"]["info"]
            amount = float(info["tokenAmount"]["uiAmount"] or 0)
            if amount <= 0:
                continue
            owner = info["owner"]
            balances[owner] = balances.get(owner, 0.0) + amount
        except (KeyError, TypeError):
            continue

    # Stashed for detect_solana_bundle — the per-wallet balances behind these
    # aggregates are exactly what bundle detection needs to pick which wallets
    # are worth checking, without re-running this whole scan a second time.
    SOLANA_LAST_HOLDER_BALANCES[mint_address] = balances

    if not balances:
        return {
            "holder_count": 0, "top_holder_pct": 0.0, "top10_holder_pct": 0.0,
            "mint_authority_active": mint_authority_active,
            "freeze_authority_active": freeze_authority_active,
        }

    total = sum(balances.values())
    sorted_items = sorted(balances.items(), key=lambda kv: kv[1], reverse=True)
    sorted_amounts = [v for _, v in sorted_items]
    top_holder_pct = (sorted_amounts[0] / total * 100.0) if total > 0 else 0.0
    top10_holder_pct = (sum(sorted_amounts[:10]) / total * 100.0) if total > 0 else 0.0
    return {
        "holder_count": len(balances),
        "top_holder_pct": top_holder_pct,
        "top10_holder_pct": top10_holder_pct,
        "top_holder_address": sorted_items[0][0],
        "top_holder_balance": sorted_items[0][1],
        "mint_authority_active": mint_authority_active,
        "freeze_authority_active": freeze_authority_active,
    }


SOLANA_BUNDLE_MIN_WALLETS = 2  # holders sharing one funder before it counts as a bundle
SOLANA_BUNDLE_MAX_HOLDERS_TO_CHECK = 30  # bounds RPC cost — bundle wallets are near-always among the largest holders anyway


async def _resolve_solana_wallet_funder(wallet: str) -> Optional[str]:
    """Best-effort: walk to this wallet's OLDEST transaction and look for a
    System Program transfer landing in it, returning who sent it. Confirmed
    directly against Helius that getSignaturesForAddress/getTransaction have
    no archive restriction (unlike this project's EVM RPC plan), so this is
    reliable for genuinely fresh throwaway wallets — which is exactly what a
    real sniper/bundle wallet is: a handful of lifetime transactions, so
    `limit: 1000` reliably captures its entire history in one page.
    Degrades to None (not a false "no funder") when the earliest transaction
    isn't a simple funding transfer — e.g. a wallet that already existed
    before ever touching this token, which happens for genuine early buyers
    and shouldn't be forced into a false bundle match."""
    if not SOLANA_WS_RPC_URL:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            sigs = await _solana_rpc_post(session, "getSignaturesForAddress", [wallet, {"limit": 1000}], timeout=15.0)
            if not sigs:
                return None
            oldest_sig = sigs[-1]["signature"]

            tx = await _solana_rpc_post(
                session, "getTransaction",
                [oldest_sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
                timeout=15.0,
            )
            if tx:
                message = (tx.get("transaction") or {}).get("message") or {}
                instructions = list(message.get("instructions") or [])
                for inner in (tx.get("meta") or {}).get("innerInstructions") or []:
                    instructions.extend(inner.get("instructions") or [])
                for instr in instructions:
                    parsed = instr.get("parsed")
                    if not isinstance(parsed, dict) or parsed.get("type") != "transfer":
                        continue
                    info = parsed.get("info") or {}
                    if info.get("destination") == wallet and info.get("source"):
                        return info["source"]
    except Exception as exc:
        logger.debug(f"_resolve_solana_wallet_funder({wallet}) failed: {exc!r}")
    return None


async def _detect_solana_bundle(mint_address: str) -> Optional[tuple[str, list[str]]]:
    """Checks the token's largest holders (bounded — see
    SOLANA_BUNDLE_MAX_HOLDERS_TO_CHECK) for a shared funding wallet. Returns
    (funder, wallets) for the largest such group found, or None."""
    balances = SOLANA_LAST_HOLDER_BALANCES.get(mint_address) or {}
    if not balances:
        return None
    top_holders = sorted(balances.items(), key=lambda kv: kv[1], reverse=True)[:SOLANA_BUNDLE_MAX_HOLDERS_TO_CHECK]
    funder_groups: dict[str, list[str]] = defaultdict(list)
    for wallet, _ in top_holders:
        funder = await _resolve_solana_wallet_funder(wallet)
        if funder:
            funder_groups[funder].append(wallet)
    best: Optional[tuple[str, list[str]]] = None
    for funder, wallets in funder_groups.items():
        if len(wallets) >= SOLANA_BUNDLE_MIN_WALLETS and (best is None or len(wallets) > len(best[1])):
            best = (funder, wallets)
    return best


def add_token_signal(token_address: str, tag: str) -> None:
    entry = TOKEN_FEED.get(token_address)
    if entry is None:
        return
    signals = entry.setdefault("signals", [])
    if tag not in signals:
        signals.append(tag)
    # Timestamped separately from the signals list itself — the list stays a
    # permanent record for post-outcome "why it graduated/rugged" storytelling,
    # but CONVICTION_BUY/HIVE_MIND scoring (see compute_opportunity_score)
    # needs to know how recently it actually fired, re-touched every time the
    # underlying condition re-triggers, so a one-off early buying cluster
    # doesn't keep boosting the score hours after it went quiet.
    entry.setdefault("signal_ts", {})[tag] = time.time()


def remove_token_signal(token_address: str, tag: str) -> None:
    """Only used for THIN_VOLUME so far: it marks a specific past graduation
    ATTEMPT as unconvincing, but the hard floor at the top of
    compute_opportunity_score already re-validates the CURRENT volume/mcap
    ratio on every score — so once a token's activity genuinely catches up
    and it graduates for real, the flag is stale, resolved information, not
    an ongoing concern. Left un-removed, it would keep applying its -25
    scoring penalty forever after a real graduation, incorrectly implying
    fresh activity is still fake. CONVICTION_BUY/HIVE_MIND don't need this —
    they already decay via their own signal_ts + window check instead."""
    entry = TOKEN_FEED.get(token_address)
    if entry is None:
        return
    signals = entry.get("signals")
    if signals and tag in signals:
        signals.remove(tag)


def _drawdown_from_peak_signal(entry: dict[str, Any]) -> tuple[int, Optional[str]]:
    """peak_market_cap is already tracked on every token from its first poll
    onward (see the RUG_DRAWDOWN_PCT=90% hard trigger in post_trade_feedback_worker)
    — reused here for a softer, earlier version of the same pattern. A token
    that's clearly declined from its own high but hasn't fallen far enough to
    trip the hard rug threshold is still WATCHING and still eligible to be
    scored, and without this it would be scored purely off its current
    snapshot as if it were still climbing, with no memory that it already
    peaked and is on the way down. Deliberately well below the 90% hard rug
    cutoff so this only ever fires on tokens still very much alive."""
    market_cap = entry.get("market_cap", 0.0)
    peak_market_cap = entry.get("peak_market_cap", 0.0)
    if peak_market_cap <= 0 or market_cap <= 0 or market_cap >= peak_market_cap:
        return 0, None
    drawdown_pct = (peak_market_cap - market_cap) / peak_market_cap
    if drawdown_pct >= 0.6:
        return -20, f"Down {drawdown_pct * 100:.0f}% from its own peak (${peak_market_cap:,.0f}) — well past its high (-20)"
    if drawdown_pct >= 0.35:
        return -10, f"Down {drawdown_pct * 100:.0f}% from its own peak (${peak_market_cap:,.0f}) (-10)"
    return 0, None


def _ticker_starts_lowercase(ticker: Optional[str]) -> bool:
    """All-lowercase-first-letter names ("fomobrain") read as low-effort/
    spam-tier naming compared to Title Case ("Fomobrain") or full caps
    ("FOMOBRAIN") — a real, if informal, signal the user asked to filter on.
    Only judges the FIRST letter (covers both Title Case and full caps in one
    check); tickers with no alphabetic character at all (pure symbols/emoji)
    or still-unresolved "UNKNOWN" aren't judged either way."""
    if not ticker or ticker == "UNKNOWN":
        return False
    for ch in ticker:
        if ch.isalpha():
            return ch.islower()
    return False


def compute_opportunity_score(entry: dict[str, Any]) -> tuple[int, list[str]]:
    """A heuristic 0-100 composite of everything this pipeline already knows about
    a token, so a viewer isn't left manually cross-referencing raw numbers to
    guess whether something looks promising. This is NOT a predictive model or
    financial advice — it's a transparent weighted sum of the same signals shown
    elsewhere in the UI, and every point is attributed so it's never a black box."""
    market_cap = entry.get("market_cap", 0.0)
    volume_24h = entry.get("volume_24h", 0.0)
    volume_to_mcap_ratio = (volume_24h / market_cap) if market_cap > 0 else 0.0

    # Hard floor: dust-level mcap/volume disqualifies a token outright,
    # regardless of what other signals fired. No dev-trust or narrative flag
    # should be able to outrank "this barely has any real activity."
    if market_cap < MIN_OPPORTUNITY_MARKET_CAP_USD or volume_24h < MIN_OPPORTUNITY_VOLUME_USD:
        return 0, [
            f"Below minimum floor (mcap {market_cap:.0f} / vol {volume_24h:.0f}) "
            f"— too little real activity to be considered"
        ]

    ticker = entry.get("ticker")
    if not ticker or ticker == "UNKNOWN":
        # Some platforms (StonkFun, some Ember events) don't supply a symbol
        # at creation — DexScreener backfills it once indexed (see
        # _process_watchlist_token). Surfacing an "opportunity" the viewer
        # can't even identify by name isn't useful, and unresolved tickers
        # correlate with exactly the kind of bogus/mislabeled event
        # PUMPFUN_IMPLAUSIBLE_WATCHING_MCAP_USD was added to catch (that
        # $187M ghost graduation was also ticker "UNKNOWN").
        return 0, ["Ticker not yet resolved — not considered until a real name is known"]
    if _ticker_starts_lowercase(ticker):
        return 0, [f"Ticker \"{ticker}\" starts lowercase — low-effort naming, not considered"]

    # mcap is just price × supply — a $50k mcap with $1k volume (2% ratio) is
    # exactly the "obvious rug, price is fake" pattern: the number looks big
    # but almost nobody is actually trading it. Same ratio requirement as
    # graduation, applied here too.
    if volume_to_mcap_ratio < MIN_GRADUATION_VOLUME_TO_MCAP_RATIO:
        return 0, [
            f"Volume/mcap ratio only {volume_to_mcap_ratio * 100:.1f}% "
            f"(${volume_24h:.0f} vol on ${market_cap:.0f} mcap) — mcap not backed by real trading"
        ]

    score = 0
    reasons: list[str] = []

    dev_wallet = entry.get("dev_wallet", "")
    is_infra_wallet = dev_wallet.lower() in KNOWN_INFRASTRUCTURE_ADDRESSES

    if is_infra_wallet:
        reasons.append("Dev field is a shared router/multicall contract, not a trackable individual — reputation ignored")
    else:
        dev_rep = DEV_REPUTATION_DATABASE.get(dev_wallet)
        if dev_rep and dev_rep.get("is_blacklisted"):
            # Shouldn't normally reach here (blacklisted devs get SKIPPED at
            # launch), but this dev could have been blacklisted by a LATER
            # rug of a different token after this one already launched.
            score -= 40
            reasons.append("Dev has since been blacklisted (-40)")
        elif dev_rep and dev_rep.get("failed_spams", 0) > 0:
            penalty = min(30, dev_rep["failed_spams"] * 15)
            score -= penalty
            reasons.append(f"Dev has {dev_rep['failed_spams']} prior rug(s) on record (-{penalty})")
        elif entry.get("dev_decision") == "ELITE":
            # Halved from the original +30: "ELITE" only ever meant "crossed the
            # graduation bar once before," which turned out to say little about
            # whether *this* launch takes off — narrative and volume are the
            # stronger signals for that. It's still a real, mildly-informative
            # prior (and post-graduation rugs now retroactively strip it, see
            # the GRADUATED branch in post_trade_feedback_worker), just no
            # longer weighted as if it were.
            score += 15
            grad_count = dev_rep.get("successful_launches", 0) if dev_rep else 0
            reasons.append(f"Elite dev — {grad_count} prior graduation(s) on record, 0 rugs (+15)")

    signals = entry.get("signals", [])
    signal_ts = entry.get("signal_ts", {})
    now_ts = time.time()

    def _signal_fresh(tag: str, window: float) -> bool:
        ts = signal_ts.get(tag)
        return ts is not None and (now_ts - ts) <= window

    # CONVICTION_BUY/HIVE_MIND decay using the same windows their own alert
    # logic uses (stage_c_smart_money) — a signal that fired once and never
    # recurred shouldn't keep boosting the score indefinitely after it's gone
    # stale. THIN_VOLUME doesn't decay: it's evaluating a specific past
    # graduation attempt's quality, not ongoing activity.
    if "CONVICTION_BUY" in signals and _signal_fresh("CONVICTION_BUY", CONVICTION_WINDOW_SECONDS):
        score += 15
        reasons.append("Conviction buy from a tracked wallet (+15)")
    if "HIVE_MIND" in signals and _signal_fresh("HIVE_MIND", HIVE_MIND_WINDOW_SECONDS):
        score += 15
        reasons.append("Multiple tracked wallets bought in (+15)")
    if "THIN_VOLUME" in signals:
        score -= 25
        reasons.append("Hit mcap target on fake/thin volume (-25)")

    # Narrative velocity scored as a spectrum, not just a flat bonus for the one
    # "ACCELERATING" state — a cluster that's already qualified (2+ independent
    # devs on the same ticker) is informative even before it's visibly speeding
    # up, and a cluster that's cooling off is a real negative, not a zero.
    ticker_key = narrative_cluster_key(normalize_ticker(entry.get("ticker", "")))
    narrative_entry = NARRATIVE_STATUS.get(ticker_key, {}) if ticker_key else {}
    narrative_status = narrative_entry.get("status")
    if narrative_entry.get("same_operator_suspected"):
        # What looked like independent devs jumping on a trend is actually one
        # bundle operator wearing multiple masks (see
        # _narrative_same_operator_suspected) — this isn't a real organic
        # narrative, so it gets penalized regardless of how fast it looks like
        # it's accelerating, not just denied the bonus.
        score -= 30
        reasons.append(f"Cross-chain narrative looks orchestrated by one bundle operator, not organic (-30)")
    else:
        # Reweighted after a mock-portfolio backtest (n=81 flagged opportunities):
        # ACCELERATING/EMERGING_CLUSTER were actually OVER-represented among big
        # losers (22%/17%) vs winners (4%/4%) — a copycat-cluster hype spike is
        # structurally a synchronized swarm reaction that peaks and dies fast, so
        # "accelerating right now" tends to mean "buying at the top of that wave,"
        # not "durable momentum." DECAYING was the opposite: 59% of winners vs
        # 24% of losers — once the copycat swarm has thinned out, whatever's
        # still standing is more likely surviving on its own merit, not narrative
        # froth, so the old flat -10 penalty was punishing exactly the wrong thing.
        narrative_points = {
            "ACCELERATING": 5,
            "STABLE": 10,
            "EMERGING_CLUSTER": 0,
            "DECAYING": 0,
        }.get(narrative_status, 0)
        if narrative_points:
            score += narrative_points
            sign = "+" if narrative_points > 0 else ""
            reasons.append(f"Cross-chain narrative is {narrative_status} ({sign}{narrative_points})")

    # Volume/liquidity/txns scored as a spectrum above the hard floor, not a
    # flat bonus for merely crossing the graduation thresholds — a token with
    # $19k volume shouldn't score identically to one with $150.
    volume_ratio = min(1.0, volume_24h / MIN_GRADUATION_VOLUME_USD) if MIN_GRADUATION_VOLUME_USD else 0
    volume_points = round(volume_ratio * 20)
    if volume_points:
        score += volume_points
        reasons.append(f"Volume activity (+{volume_points})")

    liquidity_usd = entry.get("liquidity_usd", 0.0)
    liquidity_ratio = min(1.0, liquidity_usd / MIN_GRADUATION_LIQUIDITY_USD) if MIN_GRADUATION_LIQUIDITY_USD else 0
    liquidity_points = round(liquidity_ratio * 10)
    if liquidity_points:
        score += liquidity_points
        reasons.append(f"Liquidity depth (+{liquidity_points})")

    txns_24h = entry.get("txns_24h", 0)
    if txns_24h < MIN_GRADUATION_TXNS:
        score -= 10
        reasons.append(f"Only {txns_24h} txns in 24h — thin real participation (-10)")

    # Broad buy pressure — distinct from CONVICTION_BUY/HIVE_MIND above:
    # those only fire for wallets already in SMART_WALLETS (a small, slowly-
    # growing set), so they miss ordinary buyers entirely. This reads
    # buys-vs-sells directly off DexScreener's own txn counts, independent of
    # any tracked-wallet list — "is a crowd piling into this coin right now,"
    # not just "did one wallet we already know about."
    buys_24h_for_pressure = entry.get("buys_24h") or 0
    sells_24h_for_pressure = entry.get("sells_24h") or 0
    total_txns_for_pressure = buys_24h_for_pressure + sells_24h_for_pressure
    token_age_seconds = time.time() - (entry.get("created_at") or 0)
    # Sample-size floor raised to match MIN_GRADUATION_TXNS (was 10) — a
    # RUGGED-opportunity backtest found tokens with 10-49 total txns getting
    # BOTH the "thin real participation" penalty above AND this bonus at the
    # same time (e.g. 26 buys/4 sells = "thin" by the 50-txn bar, but still
    # "strong buy pressure" by the old 10-txn bar), which is a contradiction:
    # the bonus shouldn't fire in the same range already flagged as too thin
    # a sample to trust.
    if total_txns_for_pressure >= MIN_GRADUATION_TXNS and token_age_seconds >= BUY_PRESSURE_MIN_AGE_SECONDS:
        buy_ratio = buys_24h_for_pressure / total_txns_for_pressure
        if buy_ratio >= 0.75:
            score += 8
            reasons.append(f"Strong buy pressure: {buys_24h_for_pressure} buys vs {sells_24h_for_pressure} sells (+8)")
        elif buy_ratio >= 0.6:
            score += 4
            reasons.append(f"Buy-leaning: {buys_24h_for_pressure} buys vs {sells_24h_for_pressure} sells (+4)")

    # Scaled by magnitude, not just direction — a token up 300% over its last
    # few polls is a much stronger signal than one up 5%, and a flat bonus for
    # "up" couldn't tell those apart. Caps at +-20 so one huge swing can't
    # single-handedly dominate the score; hits that cap around a 100% move.
    sparkline = entry.get("sparkline", [])
    if len(sparkline) >= 2:
        first_mcap, last_mcap = sparkline[0][1], sparkline[-1][1]
        if first_mcap > 0:
            pct_change = (last_mcap - first_mcap) / first_mcap
            momentum_points = max(-20, min(20, round(pct_change * 20)))
            if momentum_points:
                score += momentum_points
                sign = "+" if momentum_points > 0 else ""
                direction = "up" if momentum_points > 0 else "down"
                reasons.append(f"Market cap {direction} {abs(pct_change) * 100:.0f}% over recent polls ({sign}{momentum_points})")

    drawdown_points, drawdown_reason = _drawdown_from_peak_signal(entry)
    if drawdown_points:
        score += drawdown_points
        reasons.append(drawdown_reason)

    # Holder concentration (see fetch_solana_holder_stats / _evm_holder_stats_from_ledger)
    # — a healthy-looking mcap/volume can still be one wallet away from a rug
    # if almost the entire supply sits in a single holder. If a bundle has
    # been confirmed (see _maybe_check_bundle), its combined share of supply
    # is the real concentration number — several wallets that are actually one
    # operator control more than any single raw top_holder_pct would show.
    top_holder_pct = entry.get("top_holder_pct")
    bundle_supply_pct = entry.get("bundle_supply_pct")
    effective_concentration_pct = max(top_holder_pct or 0.0, bundle_supply_pct or 0.0)
    have_concentration_data = top_holder_pct is not None or bundle_supply_pct is not None
    if have_concentration_data:
        if effective_concentration_pct >= 50:
            score -= 25
            reasons.append(f"Top holder(s) control {effective_concentration_pct:.0f}% of supply — one-entity rug risk (-25)")
        elif effective_concentration_pct >= 25:
            score -= 10
            reasons.append(f"Top holder(s) control {effective_concentration_pct:.0f}% of supply (-10)")

        holder_count = entry.get("holder_count") or 0
        if holder_count >= 50 and effective_concentration_pct < 25:
            score += 5
            reasons.append(f"Healthy holder spread ({holder_count} holders, +5)")

    # Same top holder's raw balance actually shrinking between polls (see
    # _maybe_refresh_holder_stats) — an earlier warning than the hard
    # rug-drawdown trigger, and distinct from concentration above: this is
    # about a real reduction in their holdings, not just dilution from new
    # buyers arriving.
    if entry.get("top_holder_selling"):
        score -= 15
        reasons.append("Top holder's balance is shrinking — may be selling down (-15)")

    # EVM-only (see _evm_holder_stats_from_ledger) — a live transfer count out
    # of the same ledger that gives holder_count. A tiny holder set doing a
    # lot of transfers among themselves is the wash-trading/bot pattern, not
    # organic participation, even if mcap/volume look fine on the surface.
    holder_transfer_count = entry.get("holder_transfer_count")
    holder_count_for_wash_check = entry.get("holder_count")
    if holder_transfer_count is not None and holder_count_for_wash_check is not None:
        if holder_count_for_wash_check < 5 and holder_transfer_count >= 20:
            score -= 15
            reasons.append(
                f"{holder_transfer_count} transfers among only {holder_count_for_wash_check} holders — looks like wash trading (-15)"
            )

    # Bundled wallets (see _maybe_check_bundle) — several holders funded/paid
    # out by one operator in a single coordinated action. On its own this is
    # NOT a red flag: bundling insider/team wallets at launch to seed initial
    # liquidity and show conviction is extremely common practice across fair
    # launches, not a scam signature. It's only informative once there's an
    # actual track record behind it:
    #   - a fresh operator, seen once: neutral, no penalty — just a fact,
    #     surfaced so a viewer knows the "N holders" figure includes wallets
    #     that are really one entity (concentration risk is scored separately
    #     below via bundle_supply_pct feeding effective_concentration_pct).
    #   - the SAME operator behind multiple launches, no confirmed rug on
    #     record yet: ambiguous — could be a legitimate serial launcher
    #     reusing a pattern, could be a scammer warming up. Small caution
    #     penalty, not a verdict.
    #   - that operator is already linked to a CONFIRMED rug elsewhere (see
    #     BUNDLE_OPERATOR_BLACKLIST): this is the actual high-confidence
    #     signal — a bad actor rotating dev wallets to dodge per-wallet
    #     blacklisting, caught because the operator identity persists.
    if entry.get("bundle_detected"):
        if entry.get("bundle_known_bad_operator"):
            score -= 40
            reasons.append("Bundle operator is linked to a confirmed rug elsewhere — high-confidence repeat bad actor (-40)")
        elif entry.get("bundle_repeat_operator"):
            score -= 10
            reasons.append(f"Same operator bundled {entry.get('bundle_wallet_count', 0)} wallets on another launch too — not necessarily bad, but worth extra scrutiny (-10)")
        else:
            reasons.append(f"{entry.get('bundle_wallet_count', 0)} wallets bundled by one operator at launch ({bundle_supply_pct or 0:.0f}% of supply) — common practice, not inherently a red flag")

    return max(0, min(100, score)), reasons


def compute_early_momentum_score(entry: dict[str, Any]) -> tuple[int, list[str]]:
    """A separate, much lower-floor score for tokens that haven't cleared
    compute_opportunity_score's ~$40k-mcap/real-activity floor yet — which is
    most brand-new fair launches for their first several minutes of life (see
    MIN_OPPORTUNITY_MARKET_CAP_USD). The main score asks "has this already
    proven real traction in dollar terms"; this asks "does this tiny-cap
    token show the RELATIVE signals that tend to PRECEDE that traction" — a
    deliberately earlier, noisier read, not a replacement for it. A token
    graduating out of this into a real opportunity_score is the intended
    flow, not a bug: this view exists to catch it before that happens.

    Reuses the same underlying signals/fields as compute_opportunity_score
    (dev trust, narrative, smart money, holder risk, bundle detection) but
    recalibrated for a stage where every dollar figure is inherently tiny —
    dollar-scaled bonuses like "volume activity (+20)" would always read as
    ~0 here and tell a viewer nothing, so this leans on ratios and signal
    presence instead of dollar magnitude."""
    market_cap = entry.get("market_cap", 0.0)
    volume_24h = entry.get("volume_24h", 0.0)
    txns_24h = entry.get("txns_24h", 0)
    buys_24h = entry.get("buys_24h") or 0
    sells_24h = entry.get("sells_24h") or 0

    # Minimal existence floor only — needs SOME market data and SOME real
    # trade to evaluate at all, but deliberately no dollar-amount requirement
    # (that's the whole point of this being a different, earlier view).
    if market_cap <= 0 or (volume_24h <= 0 and txns_24h <= 0):
        return 0, ["No real market data yet — too early to evaluate"]

    ticker = entry.get("ticker")
    if not ticker or ticker == "UNKNOWN":
        return 0, ["Ticker not yet resolved — not considered until a real name is known"]
    if _ticker_starts_lowercase(ticker):
        return 0, [f"Ticker \"{ticker}\" starts lowercase — low-effort naming, not considered"]

    score = 0
    reasons: list[str] = []

    # Volume/mcap ratio is the PRIMARY signal here, not a minor one — dollar
    # amounts are all tiny at this stage, but a healthy ratio (real trading
    # relative to its own size) is scale-invariant: just as meaningful on a
    # $3k token as a $300k one, and the one thing not distorted by size.
    ratio = (volume_24h / market_cap) if market_cap > 0 else 0.0
    ratio_points = min(35, round(ratio * 100))
    if ratio_points:
        score += ratio_points
        reasons.append(f"Volume/mcap ratio {ratio * 100:.0f}% — real trading relative to its size (+{ratio_points})")

    # Broad buy pressure — genuinely distinct from CONVICTION_BUY/HIVE_MIND
    # below: those only fire for wallets already in SMART_WALLETS (a small,
    # slowly-growing set), so they miss ordinary buyers entirely. This reads
    # buys-vs-sells directly off DexScreener's own txn counts, independent of
    # any tracked-wallet list — the actual "is a crowd piling into this coin
    # right now" signal, not just "did one wallet we already know about."
    # Requires a real sample size so 2 buys/0 sells on a brand-new token
    # doesn't look identical to a genuine 40-buy/8-sell wave.
    total_txns = buys_24h + sells_24h
    token_age_seconds = time.time() - (entry.get("created_at") or 0)
    if total_txns >= 10 and token_age_seconds >= BUY_PRESSURE_MIN_AGE_SECONDS:
        buy_ratio = buys_24h / total_txns
        if buy_ratio >= 0.75:
            score += 8
            reasons.append(f"Strong buy pressure: {buys_24h} buys vs {sells_24h} sells (+8)")
        elif buy_ratio >= 0.6:
            score += 4
            reasons.append(f"Buy-leaning: {buys_24h} buys vs {sells_24h} sells (+4)")

    dev_wallet = entry.get("dev_wallet", "")
    is_infra_wallet = dev_wallet.lower() in KNOWN_INFRASTRUCTURE_ADDRESSES
    if not is_infra_wallet:
        dev_rep = DEV_REPUTATION_DATABASE.get(dev_wallet)
        if dev_rep and dev_rep.get("is_blacklisted"):
            score -= 40
            reasons.append("Dev has since been blacklisted (-40)")
        elif dev_rep and dev_rep.get("failed_spams", 0) > 0:
            penalty = min(30, dev_rep["failed_spams"] * 15)
            score -= penalty
            reasons.append(f"Dev has {dev_rep['failed_spams']} prior rug(s) on record (-{penalty})")
        elif entry.get("dev_decision") == "ELITE":
            score += 15
            grad_count = dev_rep.get("successful_launches", 0) if dev_rep else 0
            reasons.append(f"Elite dev — {grad_count} prior graduation(s) on record, 0 rugs (+15)")

    # Smart money weighted HIGHER than in the main score — a tracked wallet
    # already buying into a token this small/new is a stronger "someone sees
    # something" signal than the identical buy on an already-$100k token.
    signals = entry.get("signals", [])
    signal_ts = entry.get("signal_ts", {})
    now_ts = time.time()

    def _signal_fresh(tag: str, window: float) -> bool:
        ts = signal_ts.get(tag)
        return ts is not None and (now_ts - ts) <= window

    if "CONVICTION_BUY" in signals and _signal_fresh("CONVICTION_BUY", CONVICTION_WINDOW_SECONDS):
        score += 20
        reasons.append("Conviction buy from a tracked wallet, this early (+20)")
    if "HIVE_MIND" in signals and _signal_fresh("HIVE_MIND", HIVE_MIND_WINDOW_SECONDS):
        score += 20
        reasons.append("Multiple tracked wallets already bought in, this early (+20)")

    # Narrative velocity — the actual "catch it before it's obvious" signal.
    # An emerging or accelerating cluster matters MORE pre-threshold, since
    # this is exactly the window before it becomes visible everywhere else.
    ticker_key = narrative_cluster_key(normalize_ticker(entry.get("ticker", "")))
    narrative_entry = NARRATIVE_STATUS.get(ticker_key, {}) if ticker_key else {}
    narrative_status = narrative_entry.get("status")
    if narrative_entry.get("same_operator_suspected"):
        score -= 30
        reasons.append("Cross-chain narrative looks orchestrated by one bundle operator, not organic (-30)")
    else:
        # Same reweight as compute_opportunity_score, and arguably more relevant
        # here — a copycat-cluster hype spike shows up as ACCELERATING/EMERGING
        # right as it's peaking, which is exactly the "catch it early" score's
        # blind spot: it reads a synchronized swarm reaction as genuine early
        # momentum. See _drawdown_from_peak_signal's neighbor comment above and
        # BUY_PRESSURE_MIN_AGE_SECONDS for the same backtest (n=81).
        narrative_points = {
            "ACCELERATING": 8,
            "EMERGING_CLUSTER": 0,
            "STABLE": 5,
            "DECAYING": 0,
        }.get(narrative_status, 0)
        if narrative_points:
            score += narrative_points
            sign = "+" if narrative_points > 0 else ""
            reasons.append(f"Cross-chain narrative is {narrative_status} ({sign}{narrative_points})")

    drawdown_points, drawdown_reason = _drawdown_from_peak_signal(entry)
    if drawdown_points:
        score += drawdown_points
        reasons.append(drawdown_reason)

    # Concentration/wash-trading/bundle risk checks — same weight as the main
    # score. Arguably matter MORE here: a handful of wallets is trivially
    # cheap to fake a "healthy-looking" micro-cap token with.
    top_holder_pct = entry.get("top_holder_pct")
    bundle_supply_pct = entry.get("bundle_supply_pct")
    effective_concentration_pct = max(top_holder_pct or 0.0, bundle_supply_pct or 0.0)
    have_concentration_data = top_holder_pct is not None or bundle_supply_pct is not None
    if have_concentration_data:
        if effective_concentration_pct >= 50:
            score -= 25
            reasons.append(f"Top holder(s) control {effective_concentration_pct:.0f}% of supply — one-entity rug risk (-25)")
        elif effective_concentration_pct >= 25:
            score -= 10
            reasons.append(f"Top holder(s) control {effective_concentration_pct:.0f}% of supply (-10)")

    if entry.get("top_holder_selling"):
        score -= 15
        reasons.append("Top holder's balance is shrinking — may be selling down (-15)")

    holder_transfer_count = entry.get("holder_transfer_count")
    holder_count_for_wash_check = entry.get("holder_count")
    if holder_transfer_count is not None and holder_count_for_wash_check is not None:
        if holder_count_for_wash_check < 5 and holder_transfer_count >= 20:
            score -= 15
            reasons.append(
                f"{holder_transfer_count} transfers among only {holder_count_for_wash_check} holders — looks like wash trading (-15)"
            )

    if entry.get("bundle_detected"):
        if entry.get("bundle_known_bad_operator"):
            score -= 40
            reasons.append("Bundle operator is linked to a confirmed rug elsewhere — high-confidence repeat bad actor (-40)")
        elif entry.get("bundle_repeat_operator"):
            score -= 10
            reasons.append(f"Same operator bundled {entry.get('bundle_wallet_count', 0)} wallets on another launch too — not necessarily bad, but worth extra scrutiny (-10)")
        else:
            reasons.append(f"{entry.get('bundle_wallet_count', 0)} wallets bundled by one operator at launch ({bundle_supply_pct or 0:.0f}% of supply) — common practice, not inherently a red flag")

    return max(0, min(100, score)), reasons


EVM_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
EVM_LEDGER_RESUBSCRIBE_INTERVAL_SECONDS = 30

# BNB/Robinhood have no equivalent of Solana's getProgramAccounts — an
# ERC-20/BEP-20 balance lives in the token contract's own storage, not as a
# separately-queryable account, so there's no direct "list every holder" RPC
# call. The natural alternative — replay every Transfer log since a token's
# creation block — was tried and confirmed dead on this project's current
# Chainstack plan: eth_getLogs errors with "Archive, Debug and Trace requests
# are not available on your current plan" for anything older than roughly
# 100-500 blocks back on both the BNB and Robinhood endpoints. So instead of
# backfilling, this keeps one persistent Transfer-log subscription covering
# every currently-tracked token per chain and accumulates a live balance
# ledger going forward from whenever each token enters the watchlist — no
# archive access needed. Trade-off: misses any transfers in the (typically
# sub-second) gap between the token's creation and our own listener catching
# it. The running transfer count is a free bonus signal out of the same
# ledger: a token with very few holders but a high transfer count reads as
# wash trading / bot churn, not organic distinct participation.
EVM_HOLDER_LEDGER: dict[str, dict[str, int]] = {}
EVM_HOLDER_TRANSFER_COUNT: dict[str, int] = {}
# token_address -> {tx_hash: {recipient addresses that got tokens in that tx}}
# — a single transaction paying out to several distinct wallets at once is
# exactly what a bundler contract does to fake simultaneous "independent buys".
EVM_TX_RECIPIENTS: dict[str, dict[str, set[str]]] = {}
EVM_BUNDLE_MIN_RECIPIENTS = 3  # distinct wallets funded in one tx before it counts as a bundle


def _evm_watched_addresses(chain: str) -> set[str]:
    return {
        addr for addr, info in TOKEN_WATCHLIST.items()
        if info.get("chain") == chain and info.get("status") in ("WATCHING", "GRADUATED", "RUGGED")
    }


def _apply_evm_transfer_log_to_ledger(log: dict) -> None:
    token_address = (log.get("address") or "").lower()
    if not token_address:
        return
    topics = log.get("topics", [])
    if len(topics) < 3:
        return
    frm = "0x" + topics[1][-40:]
    to = "0x" + topics[2][-40:]
    try:
        value = int(log.get("data") or "0x0", 16)
    except ValueError:
        return
    ledger = EVM_HOLDER_LEDGER.setdefault(token_address, {})
    if frm != EVM_ZERO_ADDRESS:
        ledger[frm] = ledger.get(frm, 0) - value
    if to != EVM_ZERO_ADDRESS:
        ledger[to] = ledger.get(to, 0) + value
        tx_hash = log.get("transactionHash")
        if tx_hash:
            EVM_TX_RECIPIENTS.setdefault(token_address, {}).setdefault(tx_hash, set()).add(to)
    EVM_HOLDER_TRANSFER_COUNT[token_address] = EVM_HOLDER_TRANSFER_COUNT.get(token_address, 0) + 1


def _detect_evm_bundle_tx(token_address: str) -> Optional[tuple[str, set[str]]]:
    """Largest single transaction that paid out to >= EVM_BUNDLE_MIN_RECIPIENTS
    distinct wallets for this token, if any — purely derived from the live
    Transfer ledger already being kept, no extra RPC calls."""
    tx_map = EVM_TX_RECIPIENTS.get(token_address) or {}
    best: Optional[tuple[str, set[str]]] = None
    for tx_hash, recipients in tx_map.items():
        if len(recipients) >= EVM_BUNDLE_MIN_RECIPIENTS and (best is None or len(recipients) > len(best[1])):
            best = (tx_hash, recipients)
    return best


async def _resolve_evm_tx_sender(chain: str, tx_hash: str) -> Optional[str]:
    """One-off eth_getTransactionByHash to identify who actually submitted a
    detected bundle transaction — the operator behind it. Only ever called
    once per newly-detected bundle (result is cached in TOKEN_BUNDLE_INFO), and
    for a transaction our own listener just observed live, so unlike
    eth_getLogs backfill this isn't an archive/trace call — plain recent-tx
    lookup, unrestricted on this plan."""
    ws_url = {"bnb": BNB_WS_RPC_URL, "robinhood": ROBINHOOD_WS_RPC_URL}.get(chain)
    if not ws_url:
        return None
    client = JsonRpcWsClient(ws_url)
    try:
        await client.connect()
        resp = await client.call("eth_getTransactionByHash", [tx_hash])
        result = resp.get("result") or {}
        sender = result.get("from")
        return sender.lower() if sender else None
    except Exception as exc:
        logger.debug(f"_resolve_evm_tx_sender({tx_hash}) failed: {exc!r}")
        return None
    finally:
        await client.close()


def _evm_holder_stats_from_ledger(token_address: str) -> dict[str, Any]:
    ledger = EVM_HOLDER_LEDGER.get(token_address) or {}
    transfer_count = EVM_HOLDER_TRANSFER_COUNT.get(token_address, 0)
    holders = {addr: bal for addr, bal in ledger.items() if bal > 0}
    if not holders:
        return {"holder_count": 0, "top_holder_pct": 0.0, "top10_holder_pct": 0.0, "holder_transfer_count": transfer_count}
    total = sum(holders.values())
    sorted_items = sorted(holders.items(), key=lambda kv: kv[1], reverse=True)
    sorted_amounts = [v for _, v in sorted_items]
    top_holder_pct = (sorted_amounts[0] / total * 100.0) if total > 0 else 0.0
    top10_holder_pct = (sum(sorted_amounts[:10]) / total * 100.0) if total > 0 else 0.0
    return {
        "holder_count": len(holders),
        "top_holder_pct": top_holder_pct,
        "top10_holder_pct": top10_holder_pct,
        "holder_transfer_count": transfer_count,
        "top_holder_address": sorted_items[0][0],
        "top_holder_balance": sorted_items[0][1],
    }


async def evm_holder_ledger_listener(chain: str, ws_url: str) -> None:
    if not ws_url:
        logger.warning(f"[holder-ledger/{chain}] RPC URL not configured; listener idle.")
        await asyncio.sleep(300)
        return

    client = JsonRpcWsClient(ws_url)
    await client.connect()
    subscription_id: Optional[str] = None
    watched: set[str] = set()
    try:
        while True:
            current = _evm_watched_addresses(chain)
            if current != watched:
                if subscription_id:
                    try:
                        await client.call("eth_unsubscribe", [subscription_id])
                    except Exception:
                        pass
                for stale_addr in list(EVM_HOLDER_LEDGER.keys()) + list(EVM_HOLDER_TRANSFER_COUNT.keys()):
                    info = TOKEN_WATCHLIST.get(stale_addr)
                    if info is None or (info.get("chain") == chain and stale_addr not in current):
                        EVM_HOLDER_LEDGER.pop(stale_addr, None)
                        EVM_HOLDER_TRANSFER_COUNT.pop(stale_addr, None)
                        EVM_TX_RECIPIENTS.pop(stale_addr, None)
                watched = current
                subscription_id = (
                    await client.subscribe("eth_subscribe", ["logs", {"address": sorted(watched), "topics": [TRANSFER_EVENT_TOPIC0]}])
                    if watched else None
                )

            try:
                log = await asyncio.wait_for(client.next_notification(), timeout=EVM_LEDGER_RESUBSCRIBE_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue
            if log:
                _apply_evm_transfer_log_to_ledger(log)
    finally:
        await client.close()


async def estimate_solana_trade(client: JsonRpcWsClient, signature: str, wallet: str) -> tuple[Optional[str], float]:
    """Fetch a confirmed Solana tx and estimate USD size of the wallet's largest token balance increase."""
    try:
        resp = await client.call(
            "getTransaction",
            [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
            timeout=20,
        )
        result = resp.get("result")
        if not result:
            return None, 0.0
        meta = result.get("meta", {}) or {}
        pre = {b["accountIndex"]: b for b in meta.get("preTokenBalances", [])}
        post = {b["accountIndex"]: b for b in meta.get("postTokenBalances", [])}
        best_token, best_delta = None, 0.0
        for idx, post_bal in post.items():
            if post_bal.get("owner") != wallet:
                continue
            pre_bal = pre.get(idx)
            pre_amount = float((pre_bal["uiTokenAmount"].get("uiAmount") or 0)) if pre_bal else 0.0
            post_amount = float((post_bal["uiTokenAmount"].get("uiAmount") or 0))
            delta = post_amount - pre_amount
            if delta > best_delta:
                best_delta = delta
                best_token = post_bal.get("mint")
        if not best_token or best_delta <= 0:
            return None, 0.0
        price = await fetch_token_price_usd(best_token)
        return best_token, best_delta * price
    except Exception as exc:
        logger.warning(f"[wallet/solana] failed to estimate trade for {signature}: {exc!r}")
        return None, 0.0


# ============================================================================
# SECTION 6 — DASHBOARD BROADCAST & PERSISTENCE
# ============================================================================

async def broadcast_json(message: dict) -> None:
    dead = []
    for ws in list(CONNECTED_CLIENTS):
        try:
            await ws.send_text(json.dumps(message, default=str))
        except Exception:
            dead.append(ws)
    for ws in dead:
        CONNECTED_CLIENTS.discard(ws)


async def append_alert_log(alert: dict) -> None:
    def _write():
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(os.path.join(DATA_DIR, "alerts.log"), "a") as f:
            f.write(json.dumps(alert, default=str) + "\n")

    await asyncio.to_thread(_write)


async def broadcast_alert(alert: dict) -> None:
    alert.setdefault("id", f"{alert.get('type', 'ALERT')}-{int(time.time() * 1000)}-{random.randint(1000, 9999)}")
    alert.setdefault("timestamp", time.time())
    ALERT_HISTORY.append(alert)
    logger.info(f"ALERT {alert.get('type')}: {alert.get('title')}")
    await append_alert_log(alert)
    await broadcast_json({"kind": "alert", "payload": alert})


async def broadcast_narrative_update(payload: dict) -> None:
    await broadcast_json({"kind": "narrative", "payload": payload})


async def broadcast_smart_money_activity(payload: dict) -> None:
    await broadcast_json({"kind": "smart_money", "payload": payload})


async def broadcast_token_card(payload: dict) -> None:
    await broadcast_json({"kind": "token", "payload": payload})


def _today_key(ts: float) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ts))


async def bump_daily_stat(ts: float, field: str, amount: int = 1) -> None:
    day = _today_key(ts)
    DAILY_STATS[day][field] += amount
    if len(DAILY_STATS) > DAILY_STATS_MAX_DAYS:
        oldest = sorted(DAILY_STATS.keys())[0]
        if oldest != day:
            del DAILY_STATS[oldest]
    await broadcast_json({"kind": "stats", "payload": {"date": day, **DAILY_STATS[day]}})


def _hour_key(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H", time.gmtime(ts))


async def bump_hourly_launch_stat(ts: float) -> None:
    key = _hour_key(ts)
    HOURLY_LAUNCH_STATS[key] += 1
    days_present = sorted({k[:10] for k in HOURLY_LAUNCH_STATS})
    if len(days_present) > DAILY_STATS_MAX_DAYS:
        oldest_day = days_present[0]
        for stale_key in [k for k in HOURLY_LAUNCH_STATS if k.startswith(oldest_day)]:
            del HOURLY_LAUNCH_STATS[stale_key]
    await broadcast_json({"kind": "hourly_stats", "payload": {"key": key, "count": HOURLY_LAUNCH_STATS[key]}})


def _write_state_sync(snapshot: dict[str, Any]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, "state.json")
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(snapshot, f, indent=2, default=str)
    os.replace(tmp_path, path)


async def persist_state() -> None:
    # Copy each shared dict on the event loop (no `await` in between, so this
    # can't be interleaved with the coroutines that mutate them) before
    # handing a static snapshot to the thread. Serializing the live dicts
    # directly in a background thread let concurrent mutations (new dev
    # wallets, stat bumps) race the thread's iteration, raising
    # "dictionary changed size during iteration" and crashing whichever
    # listener task happened to be awaiting this call.
    snapshot = {
        "dev_reputation": dict(DEV_REPUTATION_DATABASE),
        "smart_wallets": dict(SMART_WALLETS),
        "dev_rug_history": dict(DEV_RUG_HISTORY),
        "daily_stats": {day: dict(counters) for day, counters in DAILY_STATS.items()},
        "hourly_launch_stats": dict(HOURLY_LAUNCH_STATS),
        "recent_graduations": list(RECENT_GRADUATIONS),
        "recent_rugs": list(RECENT_RUGS),
        "recent_opportunities": list(RECENT_OPPORTUNITIES),
        "bundle_operator_history": dict(BUNDLE_OPERATOR_HISTORY),
        "bundle_operator_blacklist": list(BUNDLE_OPERATOR_BLACKLIST),
        "long_tail_watchlist": dict(LONG_TAIL_WATCHLIST),
        "telegram_pinged_tokens": dict(TELEGRAM_PINGED_TOKENS),
        "telegram_early_pinged_tokens": dict(TELEGRAM_EARLY_PINGED_TOKENS),
        "opportunity_recorded_tokens": dict(OPPORTUNITY_RECORDED_TOKENS),
        "mock_portfolio": {addr: dict(pos) for addr, pos in MOCK_PORTFOLIO.items()},
        "wallet_track_record": dict(WALLET_TRACK_RECORD),
        "saved_at": time.time(),
    }
    await asyncio.to_thread(_write_state_sync, snapshot)


def _load_state_sync() -> None:
    path = os.path.join(DATA_DIR, "state.json")
    if not os.path.exists(path):
        return
    try:
        with open(path) as f:
            saved = json.load(f)
        loaded_devs = saved.get("dev_reputation", {})
        for k, v in loaded_devs.items():
            DEV_REPUTATION_DATABASE[k] = v
        loaded_rug_history = saved.get("dev_rug_history", {})
        for k, v in loaded_rug_history.items():
            DEV_RUG_HISTORY[k] = v
        loaded_daily_stats = saved.get("daily_stats", {})
        for day, counters in loaded_daily_stats.items():
            for field, value in counters.items():
                DAILY_STATS[day][field] = value
        loaded_hourly_launch_stats = saved.get("hourly_launch_stats", {})
        for k, v in loaded_hourly_launch_stats.items():
            HOURLY_LAUNCH_STATS[k] = v
        loaded_graduations = saved.get("recent_graduations", [])
        RECENT_GRADUATIONS.extend(loaded_graduations)
        loaded_rugs = saved.get("recent_rugs", [])
        RECENT_RUGS.extend(loaded_rugs)
        loaded_opportunities = saved.get("recent_opportunities", [])
        RECENT_OPPORTUNITIES.extend(loaded_opportunities)
        loaded_bundle_history = saved.get("bundle_operator_history", {})
        for k, v in loaded_bundle_history.items():
            BUNDLE_OPERATOR_HISTORY[k] = v
        BUNDLE_OPERATOR_BLACKLIST.update(saved.get("bundle_operator_blacklist", []))
        loaded_long_tail = saved.get("long_tail_watchlist", {})
        for k, v in loaded_long_tail.items():
            LONG_TAIL_WATCHLIST[k] = v
        loaded_telegram_pinged = saved.get("telegram_pinged_tokens", {})
        for k, v in loaded_telegram_pinged.items():
            TELEGRAM_PINGED_TOKENS[k] = v
        loaded_telegram_early_pinged = saved.get("telegram_early_pinged_tokens", {})
        for k, v in loaded_telegram_early_pinged.items():
            TELEGRAM_EARLY_PINGED_TOKENS[k] = v
        loaded_opportunity_recorded = saved.get("opportunity_recorded_tokens", {})
        for k, v in loaded_opportunity_recorded.items():
            OPPORTUNITY_RECORDED_TOKENS[k] = v
        loaded_mock_portfolio = saved.get("mock_portfolio", {})
        for k, v in loaded_mock_portfolio.items():
            MOCK_PORTFOLIO[k] = v
        loaded_wallet_track_record = saved.get("wallet_track_record", {})
        for k, v in loaded_wallet_track_record.items():
            WALLET_TRACK_RECORD[k] = v
        logger.info(
            f"Loaded persisted dev reputation state for {len(loaded_devs)} dev wallet(s), "
            f"rug history for {len(loaded_rug_history)} dev wallet(s), "
            f"{len(loaded_graduations)} recent graduation(s), {len(loaded_rugs)} recent rug(s), "
            f"{len(loaded_opportunities)} recent opportunit{'y' if len(loaded_opportunities) == 1 else 'ies'}, "
            f"{len(loaded_bundle_history)} bundle operator(s) ({len(BUNDLE_OPERATOR_BLACKLIST)} blacklisted), "
            f"{len(loaded_long_tail)} long-tail revival candidate(s), "
            f"{len(loaded_wallet_track_record)} wallet track record(s)"
        )
    except Exception as exc:
        logger.warning(f"Failed to load persisted state: {exc!r}")


async def periodic_state_snapshot() -> None:
    while True:
        await asyncio.sleep(STATE_SNAPSHOT_INTERVAL)
        await persist_state()


# ============================================================================
# SECTION 7 — STAGE A: DEVELOPER TRUST FILTER
# ============================================================================

def get_or_create_dev(dev_wallet: str, chain: str) -> dict[str, Any]:
    if dev_wallet not in DEV_REPUTATION_DATABASE:
        DEV_REPUTATION_DATABASE[dev_wallet] = {
            "alias": f"unknown-{dev_wallet[:6]}",
            "chain": chain,
            "successful_launches": 0,
            "failed_spams": 0,
            "is_blacklisted": False,
            "last_launch_time": 0.0,
        }
    return DEV_REPUTATION_DATABASE[dev_wallet]


def dev_rep_badge_fields(dev_wallet: str) -> dict[str, Any]:
    """Compact dev track-record summary attached to token cards, narrative
    launches, and alerts — so a wallet address is never the only thing shown.
    What matters is whether this dev has rugged or graduated something before,
    not the raw hex string."""
    if dev_wallet.lower() in KNOWN_INFRASTRUCTURE_ADDRESSES:
        return {"dev_alias": "shared infra (not a person)", "dev_moons": 0, "dev_rugs": 0, "dev_blacklisted": False}
    dev = DEV_REPUTATION_DATABASE.get(dev_wallet)
    if not dev:
        return {"dev_alias": None, "dev_moons": 0, "dev_rugs": 0, "dev_blacklisted": False}
    return {
        "dev_alias": dev.get("alias"),
        "dev_moons": dev.get("successful_launches", 0),
        "dev_rugs": dev.get("failed_spams", 0),
        "dev_blacklisted": dev.get("is_blacklisted", False),
    }


def _blacklist_bundle_operator_if_any(token_address: str) -> None:
    """Called wherever a token gets marked RUGGED — if it had a detected
    bundle operator behind it, that operator identity is now confirmed bad and
    goes on BUNDLE_OPERATOR_BLACKLIST, so any future launch it bundles (even
    under a dev wallet never seen before) gets caught immediately rather than
    waiting for that new wallet to individually rack up its own rug history."""
    bundle_info = TOKEN_BUNDLE_INFO.get(token_address)
    if bundle_info:
        BUNDLE_OPERATOR_BLACKLIST.add(bundle_info["operator"])


def _record_bundle_operator(operator: str, token_address: str, dev_wallet: str, chain: str, ts: float) -> bool:
    """Links a detected bundle operator identity to this token/dev — returns
    True if that operator was already on record behind a DIFFERENT dev_wallet,
    i.e. caught reusing a fresh throwaway dev identity."""
    history = BUNDLE_OPERATOR_HISTORY[operator]
    is_repeat_across_devs = any(h["dev_wallet"] != dev_wallet for h in history)
    history.append({"token_address": token_address, "dev_wallet": dev_wallet, "chain": chain, "timestamp": ts})
    return is_repeat_across_devs


def stage_a_dev_trust(dev_wallet: str, chain: str, ts: float) -> dict[str, Any]:
    if dev_wallet.lower() in KNOWN_INFRASTRUCTURE_ADDRESSES:
        # A shared router/multicall contract, not a trackable individual — never
        # accumulate blacklist/elite reputation on it. The alternative (treating
        # it as one identity) would let one bad actor's multicall-routed rug
        # blacklist every unrelated future launch that happens to route through
        # the same generic infrastructure.
        return {"decision": "PASS", "reason": "SHARED_INFRASTRUCTURE", "dev": {"alias": "shared-infrastructure", "is_blacklisted": False, "successful_launches": 0}}

    dev = get_or_create_dev(dev_wallet, chain)

    recent_spam = [t for t in DEV_SPAM_LOG[dev_wallet] if ts - t <= SPAM_WINDOW_SECONDS]
    DEV_SPAM_LOG[dev_wallet] = recent_spam

    if dev["is_blacklisted"]:
        return {"decision": "SKIP", "reason": "BLACKLISTED", "dev": dev}

    if len(recent_spam) >= SPAM_THRESHOLD:
        dev["is_blacklisted"] = True
        return {"decision": "SKIP", "reason": "SERIAL_RUGGER_THRESHOLD", "dev": dev}

    dev["last_launch_time"] = ts

    if dev["successful_launches"] >= 1:
        return {"decision": "ELITE", "reason": "PROVEN_DEV", "dev": dev}

    return {"decision": "PASS", "reason": "NEUTRAL", "dev": dev}


# ============================================================================
# SECTION 8 — STAGE B: CROSS-CHAIN NARRATIVE CLUSTERING & VELOCITY DECAY
# ============================================================================

def _real_unique_devs(events: list[tuple[float, str, str, str, str]]) -> set[str]:
    """Unique dev wallets behind a narrative cluster, excluding known shared
    infrastructure (e.g. Multicall3) — a batch/router contract showing up as
    the on-chain 'deployer' for one launch doesn't mean a second, unrelated
    human coordinated anything; it's just shared plumbing, not a real actor."""
    return {e[3] for e in events if e[3].lower() not in KNOWN_INFRASTRUCTURE_ADDRESSES}


def _narrative_same_operator_suspected(launches: list[dict[str, Any]]) -> bool:
    """True if two or more of this cluster's distinct dev_wallets were
    bundled (see _maybe_check_bundle) by the same operator — i.e. what looks
    like independent devs jumping on the same trend is one entity wearing
    multiple masks, not a real organic narrative."""
    operator_by_dev: dict[str, str] = {}
    for launch in launches:
        bundle_info = TOKEN_BUNDLE_INFO.get(launch["token_address"])
        if bundle_info:
            operator_by_dev[launch["dev_wallet"]] = bundle_info["operator"]
    devs_per_operator: dict[str, set[str]] = defaultdict(set)
    for dev_wallet, operator in operator_by_dev.items():
        devs_per_operator[operator].add(dev_wallet)
    return any(len(devs) >= 2 for devs in devs_per_operator.values())


def _narrative_launches(events: list[tuple[float, str, str, str, str]]) -> list[dict[str, Any]]:
    """Attach live status/market cap/links for every token behind a narrative cluster,
    so an alert can point at exactly which copycats to go look at, not just abstract
    counts. Falls back gracefully if a token dropped out of TOKEN_FEED."""
    launches = []
    for ts, chain, platform, dev_wallet, token_address in events:
        feed_entry = TOKEN_FEED.get(token_address, {})
        launches.append({
            "token_address": token_address,
            "chain": chain,
            "platform": platform,
            "dev_wallet": dev_wallet,
            "ticker": feed_entry.get("ticker", ""),
            "timestamp": ts,
            "status": feed_entry.get("status", "WATCHING"),
            "market_cap": feed_entry.get("market_cap", 0.0),
            "volume_24h": feed_entry.get("volume_24h", 0.0),
            "links": feed_entry.get("links") or build_token_links(chain, token_address),
            **dev_rep_badge_fields(dev_wallet),
        })
    # A narrative cluster is only actionable once you know which copycat the
    # volume actually went to — surface that one explicitly instead of making
    # a viewer eyeball every chip's mcap themselves, and sort it to the front.
    if launches:
        leader = max(launches, key=lambda l: l["market_cap"])
        if leader["market_cap"] > 0:
            leader["is_leader"] = True
    launches.sort(key=lambda l: l["market_cap"], reverse=True)
    return launches


async def stage_b_narrative_clustering(
    ticker: str, chain: str, platform: str, dev_wallet: str, token_address: str, ts: float
) -> dict[str, Any]:
    events = NARRATIVE_CACHE[ticker]
    events.append((ts, chain, platform, dev_wallet, token_address))
    events.sort(key=lambda e: e[0])

    unique_devs = _real_unique_devs(events)
    launches = _narrative_launches(events)
    result: dict[str, Any] = {
        "ticker": ticker,
        "qualifies": len(unique_devs) >= 2,
        "unique_dev_count": len(unique_devs),
        "total_launches": len(events),
        "chains_involved": sorted({e[1] for e in events}),
        "status": "FORMING",
        "alert": None,
        "launches": launches,
        "same_operator_suspected": _narrative_same_operator_suspected(launches),
    }

    if len(unique_devs) < 2:
        NARRATIVE_STATUS[ticker] = result
        return result

    if len(events) >= 3:
        delta_1 = events[-2][0] - events[-3][0]
        delta_2 = events[-1][0] - events[-2][0]
        result["delta_1"] = delta_1
        result["delta_2"] = delta_2

        if delta_2 >= delta_1 * NARRATIVE_ACCEL_RATIO or (ts - events[-2][0]) > NARRATIVE_DECAY_SECONDS:
            result["status"] = "DECAYING"
            result["alert"] = None  # suppressed per spec — no flashy alert on decay
        elif delta_2 < delta_1:
            result["status"] = "ACCELERATING"
            result["alert"] = "HIGH_VELOCITY_CLUSTER"
        else:
            result["status"] = "STABLE"
    else:
        result["status"] = "EMERGING_CLUSTER"

    NARRATIVE_STATUS[ticker] = result
    return result


async def narrative_decay_sweeper() -> None:
    """Periodically re-checks narratives with no new activity so decay is caught even
    when no new token event arrives to trigger stage_b_narrative_clustering."""
    while True:
        await asyncio.sleep(30)
        now = time.time()
        for ticker, events in list(NARRATIVE_CACHE.items()):
            if not events:
                continue
            last_ts = events[-1][0]
            current_status = NARRATIVE_STATUS.get(ticker, {}).get("status")
            if now - last_ts > NARRATIVE_DECAY_SECONDS and current_status != "DECAYING":
                unique_devs = _real_unique_devs(events)
                launches = _narrative_launches(events)
                update = {
                    "ticker": ticker,
                    "qualifies": len(unique_devs) >= 2,
                    "unique_dev_count": len(unique_devs),
                    "total_launches": len(events),
                    "chains_involved": sorted({e[1] for e in events}),
                    "status": "DECAYING",
                    "alert": None,
                    "launches": launches,
                    "same_operator_suspected": _narrative_same_operator_suspected(launches),
                }
                NARRATIVE_STATUS[ticker] = update
                if update["qualifies"]:
                    await broadcast_narrative_update(update)
                    await _rescore_narrative_launches(update)


# ============================================================================
# SECTION 9 — STAGE C: SMART MONEY CONVICTION & HIVE MIND CROSS-OVER
# ============================================================================

async def stage_c_smart_money(wallet_address: str, token_address: str, chain: str, trade_size_usd: float, ts: float) -> dict[str, Any]:
    result: dict[str, Any] = {"conviction_alert": False, "hive_mind_alert": False, "hive_mind_wallets": []}
    wallet_info = SMART_WALLETS.get(wallet_address)
    if wallet_info is None:
        return result

    creation_ts = TOKEN_CREATION_TIME.get(token_address)
    if creation_ts is not None and (ts - creation_ts) <= CONVICTION_WINDOW_SECONDS:
        avg = wallet_info.get("avg_trade_size_usd", 0.0)
        if avg > 0 and trade_size_usd >= avg * CONVICTION_MULTIPLIER:
            result["conviction_alert"] = True

    HIVE_MIND_CACHE[token_address].add(wallet_address)
    HIVE_MIND_TIMESTAMPS[token_address].append((wallet_address, ts))
    HIVE_MIND_TIMESTAMPS[token_address] = [
        (w, t) for (w, t) in HIVE_MIND_TIMESTAMPS[token_address] if ts - t <= HIVE_MIND_WINDOW_SECONDS
    ]
    recent_wallets = sorted({w for w, _ in HIVE_MIND_TIMESTAMPS[token_address]})
    result["hive_mind_wallets"] = recent_wallets
    if len(recent_wallets) >= HIVE_MIND_MIN_WALLETS:
        result["hive_mind_alert"] = True

    return result


# --- Automatic smart-wallet discovery ---------------------------------------
# SMART_WALLETS ships with labeled example addresses (see its declaration) —
# without something populating it from real activity, CONVICTION_BUY and
# HIVE_MIND never fire on anything real. This credits whichever wallets were
# early into a token when it resolves (graduation = credit, rug = debit), and
# auto-promotes a wallet into SMART_WALLETS once its track record earns it.
WALLET_TRACK_RECORD: dict[str, dict[str, Any]] = {}
# Lowered from 3: the Solana "early buyer" signal is a proxy (current top
# holders, not real per-trade chronology — see _extract_early_buyers), so the
# same wallet address landing in the top-N of 3+ SEPARATE, mostly-unrelated
# token launches essentially never happens by chance. At 3 this promotion
# path was structurally close to dead weight; 2 is still a real repeat
# pattern, not a fluke, but reachable in a realistic running window.
SMART_WALLET_MIN_GRADUATIONS = 2
SMART_WALLET_MIN_WIN_RATE = 0.6
SMART_WALLET_EARLY_BUYER_COUNT = 8


def _extract_early_buyers(token_address: str, chain: str) -> list[str]:
    """Best-effort 'who was in early', for crediting/debiting wallet track
    records. EVM: the first distinct recipients in real Transfer-log arrival
    order (dict insertion order in EVM_HOLDER_LEDGER matches the order logs
    were actually received). Solana: current top holders by balance — a
    weaker approximation (a holder isn't necessarily an early buyer), used
    because there's no live per-trade feed for arbitrary tracked Solana
    tokens (see fetch_solana_holder_stats's module note)."""
    if chain in ("bnb", "robinhood"):
        ledger = EVM_HOLDER_LEDGER.get(token_address) or {}
        return list(ledger.keys())[:SMART_WALLET_EARLY_BUYER_COUNT]
    if chain == "solana":
        balances = SOLANA_LAST_HOLDER_BALANCES.get(token_address) or {}
        top = sorted(balances.items(), key=lambda kv: kv[1], reverse=True)[:SMART_WALLET_EARLY_BUYER_COUNT]
        return [w for w, _ in top]
    return []


def _maybe_promote_smart_wallet(wallet: str, record: dict[str, Any]) -> None:
    if wallet in SMART_WALLETS:
        return
    if wallet in BUNDLE_OPERATOR_BLACKLIST or wallet in BUNDLE_OPERATOR_HISTORY:
        # This wallet is itself a bundle operator (see _maybe_check_bundle) —
        # its "graduations" may just be its own bundled pump-and-dumps
        # crossing the graduation bar, not real early-buyer conviction. Never
        # let a bundle operator earn smart-money status on that basis.
        return
    total = record["graduations"] + record["rugs"]
    if record["graduations"] < SMART_WALLET_MIN_GRADUATIONS or total == 0:
        return
    win_rate = record["graduations"] / total
    if win_rate < SMART_WALLET_MIN_WIN_RATE:
        return
    SMART_WALLETS[wallet] = {
        "alias": f"auto-{wallet[:6]}",
        "chain": record["chain"],
        # No reliable per-buy dollar amount is available from graduation/rug
        # crediting alone (would need a live per-trade $ feed on every chain,
        # which only EVM has right now) — 0 disables CONVICTION_BUY for this
        # wallet (guarded by stage_c_smart_money's `if avg > 0`) rather than
        # faking a number. It still fully participates in HIVE_MIND, which
        # needs wallet identity only, no calibration.
        "avg_trade_size_usd": 0.0,
        "win_rate": win_rate,
        "auto_discovered": True,
    }
    logger.info(
        f"[smart-wallet] auto-promoted {wallet} ({record['graduations']}/{total} graduations, {win_rate:.0%} win rate)"
    )


def _credit_early_buyers(token_address: str, chain: str, outcome: str) -> None:
    for wallet in _extract_early_buyers(token_address, chain):
        record = WALLET_TRACK_RECORD.setdefault(wallet, {"chain": chain, "graduations": 0, "rugs": 0})
        record[outcome] += 1
        _maybe_promote_smart_wallet(wallet, record)


# ============================================================================
# SECTION 10 — EVENT PIPELINE GLUE
# ============================================================================

async def process_new_token_event(
    chain: str,
    platform: str,
    token_address: str,
    ticker_raw: str,
    dev_wallet: str,
    ts: Optional[float] = None,
    extra: Optional[dict] = None,
) -> None:
    ts = ts or time.time()
    extra = extra or {}

    bonding_curve_key = (extra or {}).get("bonding_curve_key")

    TOKEN_CREATION_TIME[token_address] = ts
    TOKEN_WATCHLIST[token_address] = {
        "chain": chain,
        "platform": platform,
        "dev_wallet": dev_wallet,
        "ticker": ticker_raw,
        "created_at": ts,
        "peak_market_cap": 0.0,
        "status": "WATCHING",
        "dev_decision": "PENDING",
        "bonding_curve_key": bonding_curve_key,
    }
    token_feed_upsert(
        token_address,
        chain=chain,
        platform=platform,
        dev_wallet=dev_wallet,
        ticker=ticker_raw,
        created_at=ts,
        dev_decision="PENDING",
        market_cap=0.0,
        peak_market_cap=0.0,
        status="WATCHING",
        sparkline=[],
        signals=[],
        volume_24h=0.0,
        liquidity_usd=0.0,
        txns_24h=0,
        buys_24h=0,
        sells_24h=0,
        opportunity_score=0,
        score_reasons=[],
        links=build_token_links(chain, token_address),
        bonding_sol_raised=None,
        bonding_progress_pct=None,
        holder_count=None,
        top_holder_pct=None,
        top10_holder_pct=None,
        holder_transfer_count=None,
        bundle_detected=False,
        bundle_wallet_count=None,
        bundle_supply_pct=None,
        bundle_repeat_operator=False,
        bundle_known_bad_operator=False,
        early_momentum_score=0,
        early_momentum_reasons=[],
        image_url=None,
        **dev_rep_badge_fields(dev_wallet),
    )
    await bump_daily_stat(ts, "total_tokens")
    await bump_hourly_launch_stat(ts)

    dev_verdict = stage_a_dev_trust(dev_wallet, chain, ts)

    if dev_verdict["decision"] == "SKIP":
        TOKEN_WATCHLIST[token_address]["status"] = "SKIPPED"
        TOKEN_WATCHLIST[token_address]["dev_decision"] = "SKIPPED"
        token_feed_upsert(token_address, status="SKIPPED", dev_decision="SKIPPED")
        await broadcast_token_card(TOKEN_FEED[token_address])
        await bump_daily_stat(ts, "skipped")
        await broadcast_alert(
            {
                "type": "SKIPPED",
                "severity": "info",
                "title": "SKIPPED - SERIAL RUGGER BLACKLISTED",
                "chain": chain,
                "platform": platform,
                "token_address": token_address,
                "ticker": ticker_raw,
                "dev_wallet": dev_wallet,
                "reason": dev_verdict["reason"],
                "prior_rugs": DEV_RUG_HISTORY.get(dev_wallet, [])[-3:],
                **dev_rep_badge_fields(dev_wallet),
                "timestamp": ts,
            }
        )
        return

    ticker_norm = normalize_ticker(ticker_raw)
    # Never cluster on a placeholder ticker — StonkFun launches (and some Ember
    # ones) start out as literal "UNKNOWN" until DexScreener backfills the real
    # symbol later. Clustering on that would falsely group unrelated tokens
    # into a fake "$UNKNOWN" narrative.
    if ticker_norm and ticker_norm != "UNKNOWN":
        ticker = narrative_cluster_key(ticker_norm)
        narrative = await stage_b_narrative_clustering(ticker, chain, platform, dev_wallet, token_address, ts)
    else:
        narrative = {"status": "N/A", "alert": None, "ticker": "", "qualifies": False}

    if dev_verdict["decision"] == "ELITE":
        dev = dev_verdict["dev"]
        await bump_daily_stat(ts, "elite_launches")
        add_token_signal(token_address, "ELITE_DEV")
        await broadcast_alert(
            {
                "type": "ELITE_DEV_LAUNCH",
                "severity": "critical",
                "title": "\U0001F680 ELITE DEV LAUNCH ALERT",
                "chain": chain,
                "platform": platform,
                "token_address": token_address,
                "ticker": ticker_raw,
                "dev_wallet": dev_wallet,
                "dev_alias": dev.get("alias"),
                "successful_launches": dev.get("successful_launches"),
                "dev_moons": dev.get("successful_launches", 0),
                "dev_rugs": dev.get("failed_spams", 0),
                "timestamp": ts,
            }
        )

    if narrative.get("alert") == "HIGH_VELOCITY_CLUSTER":
        await broadcast_alert(
            {
                "type": "NARRATIVE_CLUSTER",
                "severity": "high",
                "title": "\U0001F6A8 HIGH-VELOCITY CROSS-CHAIN CLUSTER ALERT",
                "ticker": ticker,
                "chains_involved": narrative.get("chains_involved"),
                "unique_dev_count": narrative.get("unique_dev_count"),
                "delta_1": narrative.get("delta_1"),
                "delta_2": narrative.get("delta_2"),
                "launches": narrative.get("launches", []),
                "timestamp": ts,
            }
        )

    # Only surface a narrative once it actually qualifies (>=2 unique devs) —
    # a single-dev entry isn't a cross-chain trend, it's just a normal token,
    # and showing it as a "narrative" card was pure noise.
    if narrative.get("qualifies"):
        await broadcast_narrative_update(narrative)
        await _rescore_narrative_launches(narrative)

    TOKEN_WATCHLIST[token_address]["dev_decision"] = dev_verdict["decision"]
    token_feed_upsert(token_address, dev_decision=dev_verdict["decision"])
    await _rescore_token_and_maybe_ping(token_address)
    await broadcast_token_card(TOKEN_FEED[token_address])


async def process_wallet_buy_event(
    wallet_address: str,
    token_address: str,
    chain: str,
    trade_size_usd: float,
    ts: Optional[float] = None,
    extra: Optional[dict] = None,
) -> None:
    ts = ts or time.time()
    extra = extra or {}
    wallet_info = SMART_WALLETS.get(wallet_address)
    if wallet_info is None:
        return

    c_result = await stage_c_smart_money(wallet_address, token_address, chain, trade_size_usd, ts)

    await broadcast_smart_money_activity(
        {
            "wallet": wallet_address,
            "alias": wallet_info["alias"],
            "chain": chain,
            "token_address": token_address,
            "trade_size_usd": trade_size_usd,
            "timestamp": ts,
        }
    )

    if c_result["conviction_alert"]:
        add_token_signal(token_address, "CONVICTION_BUY")
        await broadcast_alert(
            {
                "type": "MAX_CONVICTION_TRADE",
                "severity": "critical",
                "title": "\U0001F525 MAX CONVICTION TRADE ALERT",
                "chain": chain,
                "wallet": wallet_address,
                "wallet_alias": wallet_info["alias"],
                "token_address": token_address,
                "trade_size_usd": trade_size_usd,
                "avg_trade_size_usd": wallet_info["avg_trade_size_usd"],
                "timestamp": ts,
            }
        )

    if c_result["hive_mind_alert"]:
        add_token_signal(token_address, "HIVE_MIND")
        await broadcast_alert(
            {
                "type": "HIVE_MIND",
                "severity": "high",
                "title": "\U0001F451 HIVE MIND ALERT",
                "chain": chain,
                "token_address": token_address,
                "wallets": c_result["hive_mind_wallets"],
                "wallet_aliases": [
                    SMART_WALLETS.get(w, {}).get("alias", w) for w in c_result["hive_mind_wallets"]
                ],
                "timestamp": ts,
            }
        )

    if c_result["conviction_alert"] or c_result["hive_mind_alert"]:
        await _rescore_token_and_maybe_ping(token_address)
        if token_address in TOKEN_FEED:
            await broadcast_token_card(TOKEN_FEED[token_address])


# ============================================================================
# SECTION 11 — SOLANA LISTENERS (Pump.fun, Stonkfun, Ember)
# ============================================================================

async def handle_pumpfun_message(data: dict) -> None:
    """Schema based on PumpPortal's documented `subscribeNewToken` payload
    (txType == 'create', mint / traderPublicKey / name / symbol / uri).
    Verify field names against current PumpPortal docs if the payload shape changes."""
    if not isinstance(data, dict) or data.get("txType") != "create":
        return
    token_address = data.get("mint") or data.get("tokenAddress")
    dev_wallet = data.get("traderPublicKey") or data.get("creator")
    ticker = data.get("symbol") or data.get("name") or "UNKNOWN"
    if not token_address or not dev_wallet:
        return
    await process_new_token_event(
        chain="solana",
        platform="pump.fun",
        token_address=token_address,
        ticker_raw=ticker,
        dev_wallet=dev_wallet,
        ts=time.time(),
        extra={"raw_name": data.get("name"), "uri": data.get("uri"), "bonding_curve_key": data.get("bondingCurveKey")},
    )


async def handle_pumpfun_migration_message(data: dict) -> None:
    """PumpPortal's `subscribeMigration` feed — confirmed live via a direct capture:
    {"txType": "migrate", "mint": "...", "pool": "pump-amm"}. This is pump.fun's own
    authoritative bonding-curve-complete signal, not a heuristic — a token has
    actually migrated off its bonding curve into a real AMM pool."""
    if not isinstance(data, dict) or data.get("txType") != "migrate":
        return
    token_address = data.get("mint")
    if not token_address:
        return
    await mark_token_graduated(token_address, source="platform_native")


async def solana_pumpfun_listener() -> None:
    async with websockets.connect(PUMPPORTAL_WS_URL, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps({"method": "subscribeNewToken"}))
        await ws.send(json.dumps({"method": "subscribeMigration"}))
        logger.info("[solana/pump.fun] subscribed to new token stream + migration events")
        async for raw in ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if data.get("txType") == "migrate":
                await handle_pumpfun_migration_message(data)
            else:
                await handle_pumpfun_message(data)


async def handle_generic_solana_launch_message(data: dict, chain: str, platform: str) -> None:
    """Best-effort parser for pump.fun-style clone platforms. Accepts common field-name
    variants; adjust to match the platform's actual payload shape once verified."""
    if not isinstance(data, dict):
        return
    token_address = data.get("mint") or data.get("token") or data.get("tokenAddress") or data.get("address")
    dev_wallet = data.get("traderPublicKey") or data.get("creator") or data.get("dev") or data.get("deployer")
    ticker = data.get("symbol") or data.get("ticker") or data.get("name") or "UNKNOWN"
    event_kind = data.get("txType") or data.get("type") or data.get("event")
    if event_kind and event_kind not in ("create", "new_token", "launch", "TokenCreated"):
        return
    if not token_address or not dev_wallet:
        return
    await process_new_token_event(
        chain=chain, platform=platform, token_address=token_address,
        ticker_raw=ticker, dev_wallet=dev_wallet, ts=time.time(), extra=data,
    )


async def handle_stonkfun_log(client: JsonRpcWsClient, value: dict) -> None:
    """StonkFun launches are `initialize_with_token_2022` instructions on Raydium's
    LaunchLab program that reference a StonkFun platform-config account. We don't
    have a verified account-index layout for that instruction, so instead of
    guessing, we fetch the full transaction and derive the fields generically:
    the fee payer (accountKeys[0]) is always the transaction signer/dev wallet,
    and the new mint is whichever token address appears in postTokenBalances but
    not preTokenBalances (the same robust technique used for wallet-buy sizing)."""
    if value.get("err"):
        return
    logs = value.get("logs", [])
    signature = value.get("signature")
    log_text = " ".join(logs)
    if "initialize_with_token_2022" not in log_text.lower():
        return
    if not signature:
        return
    try:
        resp = await client.call(
            "getTransaction",
            [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
            timeout=20,
        )
        result = resp.get("result")
        if not result:
            return
        account_keys = result.get("transaction", {}).get("message", {}).get("accountKeys", [])
        if not account_keys:
            return
        first_key = account_keys[0]
        dev_wallet = first_key.get("pubkey") if isinstance(first_key, dict) else first_key

        meta = result.get("meta", {}) or {}
        pre_mints = {b.get("mint") for b in meta.get("preTokenBalances", [])}
        post_mints = {b.get("mint") for b in meta.get("postTokenBalances", [])}
        new_mints = {m for m in (post_mints - pre_mints) if m}
        if not new_mints or not dev_wallet:
            return
        token_address = next(iter(new_mints))
    except Exception as exc:
        logger.warning(f"[solana/stonkfun] failed to resolve launch tx {signature}: {exc!r}")
        return

    await process_new_token_event(
        chain="solana",
        platform="stonkfun",
        token_address=token_address,
        ticker_raw="UNKNOWN",
        dev_wallet=dev_wallet,
        ts=time.time(),
        extra={"signature": signature},
    )


async def solana_stonkfun_listener() -> None:
    if not SOLANA_WS_RPC_URL:
        logger.warning("[solana/stonkfun] SOLANA_WS_RPC_URL not configured; listener idle.")
        await asyncio.sleep(300)
        return
    client = JsonRpcWsClient(SOLANA_WS_RPC_URL)
    await client.connect()
    try:
        for account in STONKFUN_PLATFORM_CONFIGS:
            await client.subscribe("logsSubscribe", [{"mentions": [account]}, {"commitment": "confirmed"}])
        logger.info("[solana/stonkfun] subscribed to StonkFun platform-config accounts on Raydium LaunchLab")
        while True:
            result = await client.next_notification()
            value = result.get("value", {})
            if value:
                await handle_stonkfun_log(client, value)
    finally:
        await client.close()


async def handle_ember_sse_event(data: dict) -> None:
    if not isinstance(data, dict):
        return
    kind = data.get("type") or data.get("kind") or data.get("event") or data.get("action")
    if kind == "launch":
        await handle_generic_solana_launch_message(data, chain="solana", platform="ember")
    elif kind == "graduate":
        # "graduate" is a confirmed SSE event kind per Ember's own bundle doc
        # string, but its exact field name for the token address wasn't
        # confirmed the same way the "launch" shape was — try the common ones.
        token_address = data.get("mint") or data.get("token") or data.get("tokenAddress") or data.get("address")
        if token_address:
            await mark_token_graduated(token_address, source="platform_native")


async def solana_ember_listener() -> None:
    """Ember has no WebSocket — it streams Server-Sent Events at EMBER_FEED_URL.
    Each SSE `data:` line is a JSON event; `launch` is the new-token event kind
    (confirmed directly from Ember's own production bundle — see config section)."""
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        headers = {"Accept": "text/event-stream"}
        async with session.get(EMBER_FEED_URL, headers=headers) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Ember feed returned HTTP {resp.status}")
            logger.info("[solana/ember] connected to SSE feed")
            async for raw_line in resp.content:
                line = raw_line.decode("utf-8", errors="ignore").strip()
                if not line.startswith("data:"):
                    continue
                payload = line[len("data:"):].strip()
                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                await handle_ember_sse_event(data)


# ============================================================================
# SECTION 12 — EVM LISTENERS (BNB: Four.meme / Flap.sh; Robinhood Chain:
# Pons / Long.xyz)
# ============================================================================

async def handle_evm_token_created_log(log: dict, chain: str, platform_lookup: dict[str, str]) -> None:
    address = (log.get("address") or "").lower()
    platform = platform_lookup.get(address, "unknown")
    topics = log.get("topics", [])
    data_hex = log.get("data", "0x")
    try:
        if platform == "four.meme":
            decoded = decode_four_meme_token_create(data_hex)
        else:
            # Generic best-effort decoder — used for Flap.sh (and Long.xyz if
            # configured) where the exact event ABI hasn't been verified. See
            # module docstring for per-platform confidence levels.
            decoded = decode_token_created_log(data_hex, topics)
    except Exception as exc:
        logger.warning(f"[{chain}/{platform}] failed to decode TokenCreated log: {exc!r}")
        return

    token_address = decoded.get("token")
    dev_wallet = decoded.get("creator")
    ticker = decoded.get("symbol") or decoded.get("name") or "UNKNOWN"
    if not token_address or not dev_wallet:
        return

    await process_new_token_event(
        chain=chain,
        platform=platform,
        token_address=token_address,
        ticker_raw=ticker,
        dev_wallet=dev_wallet,
        ts=time.time(),
        extra={"tx_hash": log.get("transactionHash"), "name": decoded.get("name")},
    )


async def evm_factory_listener(
    chain: str,
    ws_url: str,
    factories: list[tuple[str, str, str]],  # (address, topic0, platform_name)
    listener_label: str,
) -> None:
    if not ws_url or not factories:
        logger.warning(f"[{listener_label}] RPC URL or factory address(es) not configured; listener idle.")
        await asyncio.sleep(300)
        return

    client = JsonRpcWsClient(ws_url)
    await client.connect()
    try:
        addr_list = [a for a, _, _ in factories]
        topic_list = sorted({t for _, t, _ in factories})
        await client.subscribe("eth_subscribe", ["logs", {"address": addr_list, "topics": [topic_list]}])
        addr_platform_map = {a: p for a, _, p in factories}
        logger.info(f"[{listener_label}] subscribed to {len(factories)} factory contract(s)")
        while True:
            log = await client.next_notification()
            if log:
                await handle_evm_token_created_log(log, chain=chain, platform_lookup=addr_platform_map)
    finally:
        await client.close()


async def bnb_chain_listener() -> None:
    factories = []
    if FOUR_MEME_FACTORY_ADDRESS:
        factories.append((FOUR_MEME_FACTORY_ADDRESS.lower(), FOUR_MEME_TOPIC0, "four.meme"))
    if FLAP_FACTORY_ADDRESS:
        factories.append((FLAP_FACTORY_ADDRESS.lower(), FLAP_TOPIC0, "flap.sh"))
    await evm_factory_listener("bnb", BNB_WS_RPC_URL, factories, "bnb/four.meme+flap.sh")


async def handle_pons_token_launched_log(client: JsonRpcWsClient, log: dict) -> None:
    """Pons's TokenLaunched event indexes token/curve/deployer as topics and carries
    no name/symbol — fetch those from the token contract itself via eth_call."""
    topics = log.get("topics", [])
    if len(topics) < 4:
        return
    token_address = "0x" + topics[1][-40:]
    curve_address = "0x" + topics[2][-40:]
    dev_wallet = "0x" + topics[3][-40:]

    name, symbol = await evm_get_token_metadata(client, token_address)
    ticker = symbol or name or "UNKNOWN"

    await process_new_token_event(
        chain="robinhood",
        platform="pons",
        token_address=token_address,
        ticker_raw=ticker,
        dev_wallet=dev_wallet,
        ts=time.time(),
        extra={"curve": curve_address, "tx_hash": log.get("transactionHash")},
    )


async def handle_pons_launch_swept_log(log: dict) -> None:
    """LaunchSwept(address indexed token, uint256, uint256) — topic0 verified by
    exact keccak256 match. Treated as Pons's bonding-curve-finalization signal:
    a "sweep" of the launch curve's remaining inventory into the graduated pool."""
    topics = log.get("topics", [])
    if len(topics) < 2:
        return
    token_address = "0x" + topics[1][-40:]
    await mark_token_graduated(token_address, source="platform_native")


async def pons_listener() -> None:
    if not ROBINHOOD_WS_RPC_URL or not PONSFAMILY_FACTORY_ADDRESS:
        logger.warning("[robinhood/pons] ROBINHOOD_WS_RPC_URL or PONSFAMILY_FACTORY_ADDRESS not configured; listener idle.")
        await asyncio.sleep(300)
        return

    client = JsonRpcWsClient(ROBINHOOD_WS_RPC_URL)
    await client.connect()
    try:
        await client.subscribe(
            "eth_subscribe",
            [
                "logs",
                {
                    "address": [PONSFAMILY_FACTORY_ADDRESS.lower()],
                    "topics": [[PONSFAMILY_TOPIC0, PONS_LAUNCH_SWEPT_TOPIC0]],
                },
            ],
        )
        logger.info("[robinhood/pons] subscribed to Pons TokenLaunched + LaunchSwept events")
        while True:
            log = await client.next_notification()
            if not log:
                continue
            topics = log.get("topics", [])
            topic0 = topics[0] if topics else None
            if topic0 == PONSFAMILY_TOPIC0:
                await handle_pons_token_launched_log(client, log)
            elif topic0 == PONS_LAUNCH_SWEPT_TOPIC0:
                await handle_pons_launch_swept_log(log)
    finally:
        await client.close()


async def longxyz_listener() -> None:
    # UNRESOLVED — no verified LongLauncher factory address (see module
    # docstring). Idles until LONGXYZ_FACTORY_ADDRESS is supplied, then uses
    # the same generic best-effort decoder as Flap.sh.
    factories = []
    if LONGXYZ_FACTORY_ADDRESS:
        factories.append((LONGXYZ_FACTORY_ADDRESS.lower(), LONGXYZ_TOPIC0, "long.xyz"))
    await evm_factory_listener("robinhood", ROBINHOOD_WS_RPC_URL, factories, "robinhood/long.xyz")


# ============================================================================
# SECTION 13 — WALLET LOG MONITORING (SMART_WALLETS across all chains)
# ============================================================================

async def handle_solana_wallet_log(client: JsonRpcWsClient, value: dict, tracked_wallets: list[str]) -> None:
    if value.get("err"):
        return
    logs = value.get("logs", [])
    signature = value.get("signature")
    if not signature:
        return
    log_text = " ".join(logs)
    mentioned = [w for w in tracked_wallets if w in log_text]
    is_swap_like = any(kw in log_text for kw in ("Buy", "buy", "Swap", "swap"))
    if not mentioned or not is_swap_like:
        return
    for wallet in mentioned:
        token_address, trade_size_usd = await estimate_solana_trade(client, signature, wallet)
        if not token_address:
            continue
        await process_wallet_buy_event(
            wallet_address=wallet,
            token_address=token_address,
            chain="solana",
            trade_size_usd=trade_size_usd,
            ts=time.time(),
            extra={"signature": signature},
        )


SMART_WALLET_RESYNC_INTERVAL_SECONDS = 60  # how often the wallet monitors re-check SMART_WALLETS for newly auto-promoted entries


async def solana_wallet_monitor() -> None:
    if not SOLANA_WS_RPC_URL:
        logger.warning("[wallet/solana] SOLANA_WS_RPC_URL not configured; listener idle.")
        await asyncio.sleep(300)
        return

    client = JsonRpcWsClient(SOLANA_WS_RPC_URL)
    await client.connect()
    subscribed: set[str] = set()
    try:
        while True:
            # SMART_WALLETS can grow at runtime now (see _maybe_promote_smart_wallet)
            # — logsSubscribe supports many independent subscriptions on one
            # client, so a newly-promoted wallet just adds one, no need to
            # tear down and rebuild existing subscriptions the way EVM's
            # single combined address-list filter requires.
            current = {w for w, info in SMART_WALLETS.items() if info.get("chain") == "solana"}
            new_wallets = current - subscribed
            for wallet in new_wallets:
                await client.subscribe("logsSubscribe", [{"mentions": [wallet]}, {"commitment": "confirmed"}])
            if new_wallets:
                subscribed |= new_wallets
                logger.info(f"[wallet/solana] subscribed to {len(new_wallets)} new wallet(s), {len(subscribed)} total")

            try:
                result = await asyncio.wait_for(client.next_notification(), timeout=SMART_WALLET_RESYNC_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue
            value = result.get("value", {})
            if value:
                await handle_solana_wallet_log(client, value, list(subscribed))
    finally:
        await client.close()


async def handle_evm_transfer_log(client: JsonRpcWsClient, log: dict, chain: str, wallet_by_topic: dict[str, str]) -> None:
    topics = log.get("topics", [])
    if len(topics) < 3:
        return
    to_topic = topics[2].lower()
    wallet = wallet_by_topic.get(to_topic)
    if not wallet:
        return
    token_address = log.get("address")
    data_hex = log.get("data", "0x0")
    try:
        raw_amount = int(data_hex, 16)
    except ValueError:
        return
    decimals = await evm_get_decimals(client, token_address)
    ui_amount = raw_amount / (10 ** decimals)
    price = await fetch_token_price_usd(token_address)
    trade_size_usd = ui_amount * price
    await process_wallet_buy_event(
        wallet_address=wallet,
        token_address=token_address,
        chain=chain,
        trade_size_usd=trade_size_usd,
        ts=time.time(),
        extra={"tx_hash": log.get("transactionHash")},
    )


async def evm_wallet_monitor(chain: str, ws_url: str) -> None:
    if not ws_url:
        logger.warning(f"[wallet/{chain}] RPC URL not configured; listener idle.")
        await asyncio.sleep(300)
        return

    client = JsonRpcWsClient(ws_url)
    await client.connect()
    subscription_id: Optional[str] = None
    watched: set[str] = set()
    wallet_by_topic: dict[str, str] = {}
    try:
        while True:
            # SMART_WALLETS can grow at runtime now (see
            # _maybe_promote_smart_wallet) — eth_subscribe's combined
            # address/topic filter is static per-subscription, so a newly
            # promoted wallet means unsubscribe + resubscribe with the full
            # updated set, same pattern as evm_holder_ledger_listener.
            current = {w for w, info in SMART_WALLETS.items() if info.get("chain") == chain}
            if current != watched:
                if subscription_id:
                    try:
                        await client.call("eth_unsubscribe", [subscription_id])
                    except Exception:
                        pass
                watched = current
                wallet_by_topic = {address_to_topic(w): w for w in watched}
                subscription_id = (
                    await client.subscribe("eth_subscribe", ["logs", {"topics": [TRANSFER_EVENT_TOPIC0, None, list(wallet_by_topic.keys())]}])
                    if watched else None
                )
                if watched:
                    logger.info(f"[wallet/{chain}] subscribed to {len(watched)} wallet(s)")

            try:
                log = await asyncio.wait_for(client.next_notification(), timeout=SMART_WALLET_RESYNC_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue
            if log:
                await handle_evm_transfer_log(client, log, chain, wallet_by_topic)
    finally:
        await client.close()


# ============================================================================
# SECTION 14 — AUTOMATED POST-TRADE FEEDBACK LOOP
# ============================================================================

WATCHLIST_PRUNE_AGE_SECONDS = 2 * 3600  # drop terminal entries from the hot loop after this long
TERMINAL_POLL_INTERVAL_SECONDS = 300  # GRADUATED/RUGGED tokens keep getting mcap/volume refreshes, just less often than active WATCHING ones
HOLDER_STATS_POLL_INTERVAL_SECONDS = 60  # getProgramAccounts is a full scan — throttled separately from the 20s mcap poll

# --- Long-tail revival watch --------------------------------------------
# A token leaving the hot loop (see WATCHLIST_PRUNE_AGE_SECONDS) doesn't mean
# it's done forever — a tweet, a raid, anything can wake a "dead" graduated
# or expired coin back up, and that's invisible if we just stop watching it
# entirely. Instead of deleting, it moves here: a much larger population
# checked at a much slower cadence (bounded so DexScreener's rate limit stays
# comfortable even at LONG_TAIL_MAX_ENTRIES), until either it expires
# (LONG_TAIL_RETENTION_SECONDS) or shows a real volume breakout, at which
# point it's promoted straight back into TOKEN_WATCHLIST — not just flagged —
# so it immediately gets full active tracking again: on EVM that's the live
# Transfer ledger capturing every individual buyer from that moment on, plus
# holder/bundle re-detection, in case the "revival" is the same operator
# pumping their own bag to dump on new buyers.
LONG_TAIL_WATCHLIST: dict[str, dict[str, Any]] = {}
LONG_TAIL_MAX_ENTRIES = 500
LONG_TAIL_CHECK_INTERVAL_SECONDS = 900  # 15 min
LONG_TAIL_RETENTION_SECONDS = 7 * 24 * 3600  # give up watching for a revival after a week
LONG_TAIL_REQUEST_SPACING_SECONDS = 0.3  # paces DexScreener calls across the batch instead of bursting them
REVIVAL_MIN_VOLUME_USD = 500.0  # floor so a jump from $1 to $10 24h volume doesn't count
REVIVAL_VOLUME_MULTIPLIER = 5.0  # current 24h volume vs its baseline when it went quiet


def _migrate_to_long_tail(token_address: str, info: dict[str, Any], now: float) -> None:
    del TOKEN_WATCHLIST[token_address]
    if info["status"] == "SKIPPED":
        return  # blacklisted-dev tokens aren't worth long-term revival watching
    feed_entry = TOKEN_FEED.get(token_address, {})
    LONG_TAIL_WATCHLIST[token_address] = {
        "chain": info["chain"],
        "platform": info["platform"],
        "dev_wallet": info["dev_wallet"],
        "ticker": info["ticker"],
        "last_status": info["status"],
        "peak_market_cap": info.get("peak_market_cap", 0.0),
        "baseline_volume_24h": feed_entry.get("volume_24h", 0.0),
        "entered_cold_storage_at": now,
    }
    if len(LONG_TAIL_WATCHLIST) > LONG_TAIL_MAX_ENTRIES:
        oldest_key = next(iter(LONG_TAIL_WATCHLIST))
        if oldest_key != token_address:
            del LONG_TAIL_WATCHLIST[oldest_key]


async def _handle_token_revival(token_address: str, cold_info: dict[str, Any], dex_info: dict[str, Any], now: float) -> None:
    del LONG_TAIL_WATCHLIST[token_address]
    market_cap = dex_info.get("market_cap", 0.0)
    peak_market_cap = max(cold_info.get("peak_market_cap", 0.0), market_cap)
    TOKEN_WATCHLIST[token_address] = {
        "chain": cold_info["chain"],
        "platform": cold_info["platform"],
        "dev_wallet": cold_info["dev_wallet"],
        "ticker": cold_info["ticker"],
        "created_at": now,  # restarts WATCHLIST_PRUNE_AGE_SECONDS for this new active window
        "peak_market_cap": peak_market_cap,
        "status": "WATCHING",
        "dev_decision": "PENDING",
        "bonding_curve_key": None,
    }
    token_feed_upsert(
        token_address, status="WATCHING", market_cap=market_cap, peak_market_cap=peak_market_cap,
        volume_24h=dex_info.get("volume_24h", 0.0), liquidity_usd=dex_info.get("liquidity_usd", 0.0),
        txns_24h=dex_info.get("txns_24h", 0), buys_24h=dex_info.get("buys_24h", 0), sells_24h=dex_info.get("sells_24h", 0),
        image_url=dex_info.get("image_url"),
        **_identity_fields(token_address, TOKEN_WATCHLIST[token_address]),
    )
    await _rescore_token_and_maybe_ping(token_address)
    await broadcast_token_card(TOKEN_FEED[token_address])
    await bump_daily_stat(now, "revived")

    revival_alert = {
        "type": "REVIVAL",
        "severity": "high",
        "title": "\U0001F9DF REVIVAL DETECTED - DEAD COIN WAKING UP",
        "chain": cold_info["chain"],
        "platform": cold_info["platform"],
        "token_address": token_address,
        "ticker": cold_info["ticker"],
        "dev_wallet": cold_info["dev_wallet"],
        "last_status": cold_info["last_status"],
        "baseline_volume_24h": cold_info["baseline_volume_24h"],
        "current_volume_24h": dex_info.get("volume_24h", 0.0),
        "market_cap_usd": market_cap,
        **dev_rep_badge_fields(cold_info["dev_wallet"]),
        "timestamp": now,
    }
    await broadcast_alert(revival_alert)
    await persist_state()


async def long_tail_revival_watcher() -> None:
    while True:
        await asyncio.sleep(LONG_TAIL_CHECK_INTERVAL_SECONDS)
        now = time.time()
        for token_address, info in list(LONG_TAIL_WATCHLIST.items()):
            if token_address not in LONG_TAIL_WATCHLIST:
                continue  # a concurrent revival already removed it mid-batch
            if now - info["entered_cold_storage_at"] > LONG_TAIL_RETENTION_SECONDS:
                del LONG_TAIL_WATCHLIST[token_address]
                continue
            await asyncio.sleep(LONG_TAIL_REQUEST_SPACING_SECONDS)
            dex_info = await fetch_dexscreener_info(token_address)
            volume_24h = dex_info.get("volume_24h", 0.0)
            baseline = info["baseline_volume_24h"]
            is_revival = volume_24h >= REVIVAL_MIN_VOLUME_USD and (
                baseline == 0 or volume_24h >= baseline * REVIVAL_VOLUME_MULTIPLIER
            )
            if is_revival:
                await _handle_token_revival(token_address, info, dex_info, now)


def _update_feed_sparkline(token_address: str, market_cap: float, ts: float) -> list[tuple[float, float]]:
    entry = TOKEN_FEED.get(token_address)
    points = list(entry.get("sparkline", [])) if entry else []
    points.append((ts, market_cap))
    if len(points) > SPARKLINE_MAX_POINTS:
        points = points[-SPARKLINE_MAX_POINTS:]
    return points


def _identity_fields(token_address: str, info: dict[str, Any]) -> dict[str, Any]:
    """TOKEN_FEED is capped (TOKEN_FEED_MAX) and evicts its oldest entry when full;
    TOKEN_WATCHLIST is not, and keeps polling every still-WATCHING token regardless.
    If a token's TOKEN_FEED entry gets evicted while TOKEN_WATCHLIST is still actively
    tracking it, the next token_feed_upsert() call would otherwise silently recreate a
    bare stub missing chain/platform/ticker/dev_wallet — showing up on the dashboard as
    a blank row with an empty chain badge and a "PENDING"/"unresolved" ticker. Passing
    these identity fields on every watch-loop update makes that self-healing instead."""
    return {
        "chain": info["chain"],
        "platform": info["platform"],
        "dev_wallet": info["dev_wallet"],
        "ticker": info["ticker"],
        "created_at": info["created_at"],
        "dev_decision": info.get("dev_decision", "PASS"),
        "links": build_token_links(info["chain"], token_address),
        **dev_rep_badge_fields(info["dev_wallet"]),
    }


async def mark_token_graduated(token_address: str, source: str) -> None:
    """Graduate a token off a platform's own authoritative signal (pump.fun's
    `migrate` event, Ember's `graduate` SSE kind, Pons's `LaunchSwept` event) —
    trusted directly, without re-checking it against our own DexScreener-based
    volume/liquidity/txn gates. Those gates exist to guess whether an opaque
    mcap number is real; a platform telling us its own token graduated is
    already the ground truth, not something to second-guess."""
    info = TOKEN_WATCHLIST.get(token_address)
    if info is None or info["status"] != "WATCHING":
        return  # not tracked this session, or already resolved

    now = time.time()
    dex_info = await fetch_dexscreener_info(token_address)
    market_cap = dex_info.get("market_cap", 0.0)

    # Trusting the platform's OWN graduation signal (see docstring) still
    # assumes the event is actually ABOUT this mint — confirmed empirically
    # this session that's not always true (PumpPortal reporting "migrate" for
    # a mint that turned out to belong to an already-established, unrelated
    # token; the $187M "graduation" this caught is nowhere close to anything
    # a real bonding-curve token could reach). Same tripwire
    # _process_watchlist_token uses for the WATCHING path, applied here too
    # so a bogus event can't skip it by graduating before ever being polled.
    if info["platform"] == "pump.fun" and market_cap > PUMPFUN_IMPLAUSIBLE_WATCHING_MCAP_USD:
        await _kick_out_watchlist_token(
            token_address, info, now, "IMPLAUSIBLE_MCAP_FOR_BONDING_CURVE",
            "SKIPPED - NOT A REAL NEW LAUNCH (implausible mcap for bonding curve)",
        )
        return

    if market_cap > info["peak_market_cap"]:
        info["peak_market_cap"] = market_cap
    info["status"] = "GRADUATED"
    _credit_early_buyers(token_address, info["chain"], "graduations")

    dev = DEV_REPUTATION_DATABASE.get(info["dev_wallet"])
    if dev:
        dev["successful_launches"] += 1

    sparkline = _update_feed_sparkline(token_address, market_cap, now)
    token_feed_upsert(
        token_address, status="GRADUATED", market_cap=market_cap,
        peak_market_cap=info["peak_market_cap"], sparkline=sparkline,
        volume_24h=dex_info.get("volume_24h", 0.0), liquidity_usd=dex_info.get("liquidity_usd", 0.0),
        txns_24h=dex_info.get("txns_24h", 0), buys_24h=dex_info.get("buys_24h", 0),
        sells_24h=dex_info.get("sells_24h", 0), image_url=dex_info.get("image_url"),
        **_identity_fields(token_address, info),
    )
    # Resolved: whatever thin-volume concern an earlier WATCHING poll may
    # have flagged no longer applies now that the platform itself has
    # confirmed this as a real graduation — see remove_token_signal's
    # docstring for why a stale flag here would otherwise wrongly persist.
    remove_token_signal(token_address, "THIN_VOLUME")
    # mark_token_graduated previously never rescored at all — the
    # opportunity_score/score_reasons shown for a platform-natively-graduated
    # token would silently stay frozen at whatever they were during its last
    # WATCHING poll (e.g. not reflecting the dev's now-incremented
    # successful_launches), and it could never fire a Telegram ping either.
    await _rescore_token_and_maybe_ping(token_address)
    await broadcast_token_card(TOKEN_FEED[token_address])
    await bump_daily_stat(now, "graduated")

    grad_alert = {
        "type": "GRADUATED",
        "severity": "success",
        "title": "TOKEN GRADUATED - DEV REPUTATION UPGRADED",
        "source": source,
        "chain": info["chain"],
        "platform": info["platform"],
        "token_address": token_address,
        "ticker": info["ticker"],
        "dev_wallet": info["dev_wallet"],
        "dev_successful_launches": dev.get("successful_launches") if dev else None,
        **dev_rep_badge_fields(info["dev_wallet"]),
        "market_cap_usd": market_cap,
        "target_market_cap_usd": None,
        "volume_24h_usd": dex_info.get("volume_24h", 0.0),
        "liquidity_usd": dex_info.get("liquidity_usd", 0.0),
        "txns_24h": dex_info.get("txns_24h", 0),
        "buys_24h": dex_info.get("buys_24h", 0),
        "sells_24h": dex_info.get("sells_24h", 0),
        "signals": TOKEN_FEED.get(token_address, {}).get("signals", []),
        "timestamp": now,
    }
    RECENT_GRADUATIONS.append(grad_alert)
    await broadcast_alert(grad_alert)
    await persist_state()


def _rescore_token(token_address: str) -> None:
    entry = TOKEN_FEED.get(token_address)
    if entry is None:
        return
    score, reasons = compute_opportunity_score(entry)
    entry["opportunity_score"] = score
    entry["score_reasons"] = reasons
    # Computed unconditionally alongside the main score — pure arithmetic
    # over data already in the entry, no extra RPC/API cost either way.
    early_score, early_reasons = compute_early_momentum_score(entry)
    entry["early_momentum_score"] = early_score
    entry["early_momentum_reasons"] = early_reasons


# --- Telegram opportunity pings ---------------------------------------------
TELEGRAM_SEND_QUEUE: "asyncio.Queue[tuple[str, Optional[str]]]" = asyncio.Queue()


def _mark_telegram_pinged(token_address: str) -> None:
    TELEGRAM_PINGED_TOKENS[token_address] = time.time()
    if len(TELEGRAM_PINGED_TOKENS) > TELEGRAM_PINGED_MAX:
        oldest_key = next(iter(TELEGRAM_PINGED_TOKENS))
        if oldest_key != token_address:
            del TELEGRAM_PINGED_TOKENS[oldest_key]


def _mark_telegram_early_pinged(token_address: str) -> None:
    TELEGRAM_EARLY_PINGED_TOKENS[token_address] = time.time()
    if len(TELEGRAM_EARLY_PINGED_TOKENS) > TELEGRAM_EARLY_PINGED_MAX:
        oldest_key = next(iter(TELEGRAM_EARLY_PINGED_TOKENS))
        if oldest_key != token_address:
            del TELEGRAM_EARLY_PINGED_TOKENS[oldest_key]


def _format_telegram_opportunity_message(entry: dict[str, Any]) -> str:
    """Pulls every field this pipeline tracks for a token into one message —
    the whole point of an "opportunity ping" is not needing to tab over to
    the dashboard to see why it fired. HTML-escaped throughout: ticker/dev
    alias/platform are arbitrary strings a token's own dev fully controls,
    and Telegram's parse_mode=HTML will render unescaped markup from them."""
    def esc(v: Any) -> str:
        return html.escape(str(v)) if v is not None else ""

    ticker = entry.get("ticker") or "UNKNOWN"
    chain = entry.get("chain") or "?"
    platform = entry.get("platform") or "?"
    score = entry.get("opportunity_score", 0)
    market_cap = entry.get("market_cap") or 0.0
    volume_24h = entry.get("volume_24h") or 0.0
    liquidity_usd = entry.get("liquidity_usd") or 0.0
    buys_24h = entry.get("buys_24h") or 0
    sells_24h = entry.get("sells_24h") or 0

    holder_count = entry.get("holder_count")
    top_holder_pct = entry.get("top_holder_pct")
    if holder_count is not None:
        holder_line = f"👥 {holder_count} holders"
        if top_holder_pct is not None:
            holder_line += f" (top holder {top_holder_pct:.0f}%)"
        if entry.get("bundle_detected"):
            holder_line += f" · 🎭 bundle detected ({entry.get('bundle_wallet_count', 0)} wallets)"
        if entry.get("top_holder_selling"):
            holder_line += " · ⚠ top holder selling"
    else:
        holder_line = "👥 holder data: not yet available"

    dev_alias = entry.get("dev_alias") or f"unknown-{esc(entry.get('dev_wallet', ''))[:6]}"
    dev_moons = entry.get("dev_moons") or 0
    dev_rugs = entry.get("dev_rugs") or 0
    dev_line = f"🧑‍💻 Dev: {esc(dev_alias)} (🚀{dev_moons} graduated, 💀{dev_rugs} rugged)"

    reasons = entry.get("score_reasons") or []
    reasons_block = "\n".join(f"• {esc(r)}" for r in reasons) or "• (no individual signals — baseline score)"

    links = entry.get("links") or {}
    link_url = links.get("dexscreener") or links.get("explorer")
    link_line = f'\n🔗 <a href="{esc(link_url)}">View on DexScreener</a>' if link_url else ""

    text = (
        f"🎯 <b>Opportunity: ${esc(ticker)}</b>  —  {score}/100\n"
        f"{esc(chain)} · {esc(platform)}\n\n"
        f"💰 MCap: ${market_cap:,.0f}\n"
        f"📊 Vol 24h: ${volume_24h:,.0f}  ({buys_24h}↑/{sells_24h}↓)\n"
        f"💧 Liquidity: ${liquidity_usd:,.0f}\n"
        f"{holder_line}\n"
        f"{dev_line}\n\n"
        f"<b>Why:</b>\n{reasons_block}"
        f"{link_line}"
    )
    return text


def _format_telegram_early_momentum_message(entry: dict[str, Any]) -> str:
    """Early Momentum's own message — same field pull as the main opportunity
    ping, but built around early_momentum_score/early_momentum_reasons (ratio
    and signal-based, not dollar-scaled) so the "why" actually matches what
    fired it instead of showing the (still-computed but irrelevant) main
    score's reasons."""
    def esc(v: Any) -> str:
        return html.escape(str(v)) if v is not None else ""

    ticker = entry.get("ticker") or "UNKNOWN"
    chain = entry.get("chain") or "?"
    platform = entry.get("platform") or "?"
    score = entry.get("early_momentum_score", 0)
    market_cap = entry.get("market_cap") or 0.0
    volume_24h = entry.get("volume_24h") or 0.0
    buys_24h = entry.get("buys_24h") or 0
    sells_24h = entry.get("sells_24h") or 0

    holder_count = entry.get("holder_count")
    top_holder_pct = entry.get("top_holder_pct")
    if holder_count is not None:
        holder_line = f"👥 {holder_count} holders"
        if top_holder_pct is not None:
            holder_line += f" (top holder {top_holder_pct:.0f}%)"
        if entry.get("bundle_detected"):
            holder_line += f" · 🎭 bundle detected ({entry.get('bundle_wallet_count', 0)} wallets)"
    else:
        holder_line = "👥 holder data: not yet available"

    dev_alias = entry.get("dev_alias") or f"unknown-{esc(entry.get('dev_wallet', ''))[:6]}"
    dev_line = f"🧑‍💻 Dev: {esc(dev_alias)}"

    reasons = entry.get("early_momentum_reasons") or []
    reasons_block = "\n".join(f"• {esc(r)}" for r in reasons) or "• (no individual signals — baseline score)"

    links = entry.get("links") or {}
    link_url = links.get("dexscreener") or links.get("explorer")
    link_line = f'\n🔗 <a href="{esc(link_url)}">View on DexScreener</a>' if link_url else ""

    text = (
        f"⚡ <b>Early Momentum: ${esc(ticker)}</b>  —  {score}/100\n"
        f"{esc(chain)} · {esc(platform)}\n\n"
        f"💰 MCap: ${market_cap:,.0f}\n"
        f"📊 Vol 24h: ${volume_24h:,.0f}  ({buys_24h}↑/{sells_24h}↓)\n"
        f"{holder_line}\n"
        f"{dev_line}\n\n"
        f"<b>Why:</b>\n{reasons_block}"
        f"{link_line}"
    )
    return text


async def send_telegram_message(text: str, photo_url: Optional[str] = None) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        async with aiohttp.ClientSession() as session:
            if photo_url:
                # Telegram fetches the URL itself server-side — no need to
                # download the image ourselves. Caption is capped at 1024
                # chars (much shorter than a plain message's 4096), so a
                # long reasons list can get cut off here; fall back to a
                # plain text message (no length cap issue at our sizes) if
                # Telegram can't fetch this particular image URL at all.
                resp_photo = await session.post(
                    f"{TELEGRAM_API_BASE}/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                    json={"chat_id": TELEGRAM_CHAT_ID, "photo": photo_url, "caption": text[:1024], "parse_mode": "HTML"},
                    timeout=aiohttp.ClientTimeout(total=15),
                )
                if resp_photo.status == 200:
                    return
                body = await resp_photo.text()
                logger.debug(f"[telegram] sendPhoto failed HTTP {resp_photo.status}, falling back to text: {body[:200]}")

            resp = await session.post(
                f"{TELEGRAM_API_BASE}/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4096], "parse_mode": "HTML", "disable_web_page_preview": False},
                timeout=aiohttp.ClientTimeout(total=15),
            )
            if resp.status != 200:
                body = await resp.text()
                logger.warning(f"[telegram] sendMessage failed HTTP {resp.status}: {body[:300]}")
    except Exception as exc:
        logger.warning(f"[telegram] send failed: {exc!r}")


async def telegram_sender_worker() -> None:
    """Single consumer draining TELEGRAM_SEND_QUEUE, paced — several tokens
    could cross the opportunity threshold within the same poll cycle, and
    firing all of them as parallel unthrottled requests risks tripping
    Telegram's per-chat rate limit. Producers (see _rescore_token_and_maybe_ping)
    only ever enqueue, never call send_telegram_message directly."""
    while True:
        text, photo_url = await TELEGRAM_SEND_QUEUE.get()
        await send_telegram_message(text, photo_url)
        await asyncio.sleep(TELEGRAM_MIN_SEND_INTERVAL_SECONDS)


def _mark_opportunity_recorded(token_address: str) -> None:
    OPPORTUNITY_RECORDED_TOKENS[token_address] = time.time()
    if len(OPPORTUNITY_RECORDED_TOKENS) > OPPORTUNITY_RECORDED_MAX:
        oldest_key = next(iter(OPPORTUNITY_RECORDED_TOKENS))
        if oldest_key != token_address:
            del OPPORTUNITY_RECORDED_TOKENS[oldest_key]


def _open_mock_position(token_address: str, entry: dict[str, Any], score: int, ts: float) -> None:
    if token_address in MOCK_PORTFOLIO:
        return
    entry_market_cap = entry.get("market_cap") or 0.0
    MOCK_PORTFOLIO[token_address] = {
        "ticker": entry.get("ticker"),
        "chain": entry.get("chain"),
        "platform": entry.get("platform"),
        "dev_wallet": entry.get("dev_wallet"),
        "entry_market_cap": entry_market_cap,
        "entry_score": score,
        "entry_ts": ts,
        "links": entry.get("links") or {},
        "image_url": entry.get("image_url"),
        "last_known_market_cap": entry_market_cap,
        "last_known_status": entry.get("status"),
        "last_known_ts": ts,
        # Peak mcap SINCE this position opened (not the token's all-time peak,
        # which may have happened before it was ever flagged) — this is what
        # answers "did it actually move up from where we'd have bought," not
        # just "where is it right now" (current price can be well off the
        # peak on the way back down and still look fine on pnl_pct alone).
        "peak_market_cap": entry_market_cap,
        "peak_ts": ts,
    }
    if len(MOCK_PORTFOLIO) > MOCK_PORTFOLIO_MAX:
        oldest_key = next(iter(MOCK_PORTFOLIO))
        if oldest_key != token_address:
            del MOCK_PORTFOLIO[oldest_key]


def _mock_position_with_pnl(token_address: str, position: dict[str, Any]) -> dict[str, Any]:
    # Refresh from TOKEN_FEED whenever it's still tracked there — mcap keeps
    # updating for GRADUATED/RUGGED tokens too, not just WATCHING ones. Once a
    # token ages out of TOKEN_FEED entirely (bounded at TOKEN_FEED_MAX), the
    # position just freezes at its last known value instead of erroring.
    feed_entry = TOKEN_FEED.get(token_address)
    if feed_entry is not None and feed_entry.get("market_cap"):
        now = time.time()
        position["last_known_market_cap"] = feed_entry["market_cap"]
        position["last_known_status"] = feed_entry.get("status")
        position["last_known_ts"] = now
        if feed_entry["market_cap"] > position.get("peak_market_cap", 0.0):
            position["peak_market_cap"] = feed_entry["market_cap"]
            position["peak_ts"] = now

    entry_market_cap = position.get("entry_market_cap") or 0.0
    current_market_cap = position.get("last_known_market_cap") or entry_market_cap
    peak_market_cap = position.get("peak_market_cap") or current_market_cap
    pnl_pct = ((current_market_cap - entry_market_cap) / entry_market_cap * 100) if entry_market_cap > 0 else 0.0
    pnl_usd = MOCK_BUY_SIZE_USD * (pnl_pct / 100)
    peak_multiple = (peak_market_cap / entry_market_cap) if entry_market_cap > 0 else 0.0
    return {
        **position,
        "token_address": token_address,
        "current_market_cap": current_market_cap,
        "pnl_pct": pnl_pct,
        "pnl_usd": pnl_usd,
        "buy_size_usd": MOCK_BUY_SIZE_USD,
        "peak_market_cap": peak_market_cap,
        "peak_multiple": peak_multiple,
    }


def _build_mock_portfolio_snapshot() -> list[dict[str, Any]]:
    return [_mock_position_with_pnl(addr, pos) for addr, pos in MOCK_PORTFOLIO.items()]


def _refresh_opportunity_history_statuses() -> list[dict[str, Any]]:
    """RECENT_OPPORTUNITIES entries are otherwise frozen at the moment a token
    was flagged — a token that goes on to graduate or rug has no way to show
    that in the Opportunity History panel, which then just looks like a
    silent list of calls with no outcome. Mutates each entry's current_status
    in place so the panel can badge "this one rugged"/"this one graduated".

    Sources from MOCK_PORTFOLIO's last_known_status rather than querying
    TOKEN_FEED directly: TOKEN_FEED is in-memory only and wiped on every
    restart, so right after a redeploy every historical opportunity would
    show no badge at all. MOCK_PORTFOLIO's last_known_status IS persisted and
    already falls back gracefully to its last-known value when TOKEN_FEED no
    longer has the entry (see _mock_position_with_pnl) — call this AFTER
    _build_mock_portfolio_snapshot()/refreshing MOCK_PORTFOLIO in the same
    cycle so it reads this cycle's values, not last cycle's."""
    for opp in RECENT_OPPORTUNITIES:
        position = MOCK_PORTFOLIO.get(opp.get("token_address"))
        if position is not None and position.get("last_known_status") is not None:
            opp["current_status"] = position["last_known_status"]
    return list(RECENT_OPPORTUNITIES)


MOCK_PORTFOLIO_BROADCAST_INTERVAL_SECONDS = 20


async def mock_portfolio_broadcaster() -> None:
    while True:
        await asyncio.sleep(MOCK_PORTFOLIO_BROADCAST_INTERVAL_SECONDS)
        if MOCK_PORTFOLIO:
            await broadcast_json({"kind": "mock_portfolio", "payload": _build_mock_portfolio_snapshot()})
        if RECENT_OPPORTUNITIES:
            await broadcast_json({"kind": "opportunity_history", "payload": _refresh_opportunity_history_statuses()})


async def _rescore_token_and_maybe_ping(token_address: str) -> None:
    """Drop-in replacement for _rescore_token at every call site — rescoring
    is the one operation guaranteed to run every time any signal that feeds
    the opportunity score changes (dev trust, narrative, holders, bundle,
    momentum, volume...), which makes it the correct single choke point to
    hang the ping check off of rather than duplicating the check at every
    place a score-affecting field gets updated."""
    _rescore_token(token_address)
    entry = TOKEN_FEED.get(token_address)
    if not entry:
        return
    # Only act while it's still an actionable, live opportunity — not a
    # token that's already graduated/rugged/skipped by the time it crossed
    # the bar (rescoring can happen well after the fact, e.g. via narrative
    # cluster updates touching every token in a cluster regardless of status).
    if entry.get("status") != "WATCHING":
        return
    score = entry.get("opportunity_score", 0)

    # Record into the permanent history log the first time this token is ever
    # good enough to show in the Top Opportunities panel (score > 0 — same bar
    # the dashboard uses), independent of the much higher Telegram bar below.
    # Without this, a token that briefly appeared there and then graduated,
    # rugged, or dipped back under the hard $ floor just vanishes with no
    # record it was ever flagged.
    if score > 0 and token_address not in OPPORTUNITY_RECORDED_TOKENS:
        _mark_opportunity_recorded(token_address)
        opp_alert = {
            "type": "OPPORTUNITY_FLAGGED",
            "severity": "info",
            "title": "OPPORTUNITY FLAGGED",
            "chain": entry.get("chain"),
            "platform": entry.get("platform"),
            "token_address": token_address,
            "ticker": entry.get("ticker"),
            "dev_wallet": entry.get("dev_wallet"),
            "opportunity_score": score,
            "score_reasons": entry.get("score_reasons") or [],
            "market_cap_usd": entry.get("market_cap") or 0.0,
            "volume_24h_usd": entry.get("volume_24h") or 0.0,
            "image_url": entry.get("image_url"),
            "links": entry.get("links") or {},
            "timestamp": time.time(),
        }
        RECENT_OPPORTUNITIES.append(opp_alert)
        await broadcast_alert(opp_alert)
        await bump_daily_stat(opp_alert["timestamp"], "opportunities_flagged")
        _open_mock_position(token_address, entry, score, opp_alert["timestamp"])
        await persist_state()

    if token_address not in TELEGRAM_PINGED_TOKENS and score >= TELEGRAM_OPPORTUNITY_SCORE_THRESHOLD:
        _mark_telegram_pinged(token_address)
        text = _format_telegram_opportunity_message(entry)
        TELEGRAM_SEND_QUEUE.put_nowait((text, entry.get("image_url")))

    # Early Momentum's own independent ping — different score field, lower
    # threshold, but gated to the same mcap band the dashboard's Early
    # Momentum panel filters to, so Telegram never fires on a ratio spike on
    # a token too small/new to actually act on.
    early_score = entry.get("early_momentum_score", 0)
    market_cap = entry.get("market_cap") or 0.0
    if (
        token_address not in TELEGRAM_EARLY_PINGED_TOKENS
        and early_score >= TELEGRAM_EARLY_MOMENTUM_SCORE_THRESHOLD
        and EARLY_MOMENTUM_PING_MIN_MCAP_USD <= market_cap <= EARLY_MOMENTUM_PING_MAX_MCAP_USD
    ):
        _mark_telegram_early_pinged(token_address)
        early_text = _format_telegram_early_momentum_message(entry)
        TELEGRAM_SEND_QUEUE.put_nowait((early_text, entry.get("image_url")))


TOP_HOLDER_SELL_DROP_RATIO = 0.9  # same top holder's balance falling below 90% of its last-seen value counts as "selling down"


async def _kick_out_watchlist_token(token_address: str, info: dict[str, Any], now: float, reason: str, title: str) -> None:
    """Shared removal path for a token that should stop being tracked/scored
    entirely rather than just take a scoring penalty — used for structural
    disqualifiers (mint authority active, implausible data) where the
    problem isn't "this looks a bit risky," it's "this shouldn't be
    evaluated as a live opportunity at all." Same SKIPPED treatment a
    blacklisted dev gets at launch time, just triggered later once the
    disqualifying fact becomes known (mid-tracking, not at creation)."""
    info["status"] = "SKIPPED"
    info["dev_decision"] = "SKIPPED"
    token_feed_upsert(token_address, status="SKIPPED", dev_decision="SKIPPED")
    await broadcast_token_card(TOKEN_FEED[token_address])
    await bump_daily_stat(now, "skipped")
    await broadcast_alert(
        {
            "type": "SKIPPED",
            "severity": "info",
            "title": title,
            "chain": info["chain"],
            "platform": info["platform"],
            "token_address": token_address,
            "ticker": info["ticker"],
            "dev_wallet": info["dev_wallet"],
            "reason": reason,
            **dev_rep_badge_fields(info["dev_wallet"]),
            "timestamp": now,
        }
    )


async def _maybe_refresh_holder_stats(token_address: str, info: dict[str, Any], now: float) -> None:
    next_due = info.get("next_holder_poll_at", 0.0)
    if now < next_due:
        return
    info["next_holder_poll_at"] = now + HOLDER_STATS_POLL_INTERVAL_SECONDS

    chain = info["chain"]
    if chain == "solana":
        holder_stats = await fetch_solana_holder_stats(token_address)
    elif chain in ("bnb", "robinhood"):
        holder_stats = _evm_holder_stats_from_ledger(token_address)
    else:
        return

    if holder_stats.get("mint_authority_active") and info["status"] == "WATCHING":
        await _kick_out_watchlist_token(token_address, info, now, "MINTABLE", "SKIPPED - MINT AUTHORITY NOT RENOUNCED")
        return

    if holder_stats:
        # A dropping top_holder_pct alone is ambiguous — new buyers diluting
        # an unchanged whale balance looks identical to the whale actually
        # selling. Tracking the SAME address's raw balance across polls
        # disambiguates: only a real reduction in their own holdings counts
        # as "selling down," not dilution from organic new demand.
        prev_entry = TOKEN_FEED.get(token_address) or {}
        prev_addr = prev_entry.get("top_holder_address")
        prev_balance = prev_entry.get("top_holder_balance")
        new_addr = holder_stats.get("top_holder_address")
        new_balance = holder_stats.get("top_holder_balance")
        top_holder_selling = bool(
            prev_addr and new_addr and prev_addr == new_addr
            and prev_balance and new_balance is not None
            and new_balance < prev_balance * TOP_HOLDER_SELL_DROP_RATIO
        )
        holder_stats["top_holder_selling"] = top_holder_selling
        token_feed_upsert(token_address, **holder_stats)


BUNDLE_CHECK_INTERVAL_SECONDS = 300  # bundle detection is RPC-heavier (Solana) / needs enough tx history (EVM) than a plain stats refresh


async def _maybe_check_bundle(token_address: str, info: dict[str, Any], now: float) -> None:
    if token_address in TOKEN_BUNDLE_INFO:
        return  # already confirmed — no need to keep re-checking
    next_due = info.get("next_bundle_check_at", 0.0)
    if now < next_due:
        return
    info["next_bundle_check_at"] = now + BUNDLE_CHECK_INTERVAL_SECONDS

    chain = info["chain"]
    bundle_wallets: Optional[list[str]] = None
    operator: Optional[str] = None
    if chain == "solana":
        result = await _detect_solana_bundle(token_address)
        if result:
            operator, bundle_wallets = result
    elif chain in ("bnb", "robinhood"):
        tx_bundle = _detect_evm_bundle_tx(token_address)
        if tx_bundle:
            tx_hash, wallets = tx_bundle
            operator = await _resolve_evm_tx_sender(chain, tx_hash)
            bundle_wallets = sorted(wallets)
    else:
        return

    if not bundle_wallets or not operator:
        return

    dev_wallet = info["dev_wallet"]
    is_repeat_operator = _record_bundle_operator(operator, token_address, dev_wallet, chain, now)
    is_known_bad_operator = operator in BUNDLE_OPERATOR_BLACKLIST

    # Recompute concentration treating the whole bundle as one entity — a fake
    # "50 holders" shouldn't read as healthy distribution when 40 of them are
    # one operator's own wallets.
    raw_balances = (
        SOLANA_LAST_HOLDER_BALANCES.get(token_address) if chain == "solana" else EVM_HOLDER_LEDGER.get(token_address)
    ) or {}
    total_supply_seen = sum(v for v in raw_balances.values() if v > 0)
    bundle_supply = sum(raw_balances.get(w, 0) for w in bundle_wallets if raw_balances.get(w, 0) > 0)
    bundle_supply_pct = (bundle_supply / total_supply_seen * 100.0) if total_supply_seen > 0 else 0.0

    TOKEN_BUNDLE_INFO[token_address] = {
        "operator": operator,
        "wallet_count": len(bundle_wallets),
        "supply_pct": bundle_supply_pct,
        "is_repeat_operator": is_repeat_operator,
        "is_known_bad_operator": is_known_bad_operator,
    }

    if is_known_bad_operator:
        # This operator is already linked to a confirmed rug elsewhere — treat
        # this dev_wallet the same way a direct rug would, even though this
        # specific wallet has no rug of its own on record yet. The whole point
        # of this registry: a serial bundler rotating dev wallets doesn't get
        # a clean slate just because the wallet itself is new.
        dev = DEV_REPUTATION_DATABASE.get(dev_wallet)
        if dev:
            dev["is_blacklisted"] = True

    token_feed_upsert(
        token_address,
        bundle_detected=True,
        bundle_wallet_count=len(bundle_wallets),
        bundle_supply_pct=bundle_supply_pct,
        # Kept as two separate flags, not merged — "seen behind another dev
        # wallet before" and "linked to a confirmed rug" are very different
        # confidence levels and get different treatment everywhere they're
        # read (compute_opportunity_score, the dashboard badge/alert).
        bundle_repeat_operator=is_repeat_operator,
        bundle_known_bad_operator=is_known_bad_operator,
    )
    await _rescore_token_and_maybe_ping(token_address)
    await broadcast_token_card(TOKEN_FEED[token_address])

    # Plain first-time bundling is common, legitimate launch practice — not
    # worth an alert at all. Only surface an alert once there's an actual
    # track record making it notable (repeat operator, or worse, one already
    # tied to a confirmed rug).
    if is_repeat_operator or is_known_bad_operator:
        bundle_alert = {
            "type": "BUNDLE_DETECTED",
            "severity": "danger" if is_known_bad_operator else "info",
            "title": "\U0001F3AD REPEAT BUNDLE OPERATOR — LINKED TO A CONFIRMED RUG" if is_known_bad_operator else "\U0001F3AD SAME OPERATOR BUNDLED ANOTHER LAUNCH",
            "chain": chain,
            "platform": info["platform"],
            "token_address": token_address,
            "ticker": info["ticker"],
            "dev_wallet": dev_wallet,
            "operator": operator,
            "bundle_wallet_count": len(bundle_wallets),
            "bundle_supply_pct": bundle_supply_pct,
            "is_repeat_operator": is_repeat_operator,
            "is_known_bad_operator": is_known_bad_operator,
            **dev_rep_badge_fields(dev_wallet),
            "timestamp": now,
        }
        await broadcast_alert(bundle_alert)
        await persist_state()


async def _rescore_narrative_launches(narrative: dict[str, Any]) -> None:
    # compute_opportunity_score reads NARRATIVE_STATUS live, but a score is only
    # ever recomputed when something else touches that specific token — so
    # without this, only the single newest launch that triggered a status flip
    # (e.g. into ACCELERATING) ever picks up the narrative bonus/penalty; every
    # earlier token in the same cluster keeps whatever stale score it had from
    # before the cluster's velocity changed. Re-touch all of them here instead.
    for launch in narrative.get("launches", []):
        token_address = launch.get("token_address")
        if token_address and token_address in TOKEN_FEED:
            await _rescore_token_and_maybe_ping(token_address)
            await broadcast_token_card(TOKEN_FEED[token_address])


POST_TRADE_CONCURRENCY = 20  # bounds simultaneous per-token DexScreener/RPC calls so a large tracked population doesn't hammer providers, while running far faster than one-at-a-time

# Debug/observability only (see /api/debug/sweep) — lets a live "why hasn't
# this token's data updated" report distinguish "the sweep loop itself is
# stalled/slow" from "this specific token has a per-token bug," instead of
# guessing from log silence alone.
POST_TRADE_SWEEP_STATS: dict[str, Any] = {"sweep_count": 0, "last_sweep_started_at": 0.0, "last_sweep_finished_at": 0.0, "last_sweep_duration": 0.0, "last_sweep_token_count": 0}


async def post_trade_feedback_worker() -> None:
    semaphore = asyncio.Semaphore(POST_TRADE_CONCURRENCY)

    async def _guarded(token_address: str, info: dict[str, Any], now: float) -> None:
        async with semaphore:
            try:
                await _process_watchlist_token(token_address, info, now)
            except Exception as exc:
                # One token's transient failure (network blip, provider
                # hiccup) must not take down the whole batch — every other
                # token's fan-out task is independent.
                logger.warning(f"[post-trade-feedback] error processing {token_address}: {exc!r}")

    while True:
        await asyncio.sleep(POST_TRADE_POLL_INTERVAL)
        now = time.time()
        tokens = list(TOKEN_WATCHLIST.items())
        POST_TRADE_SWEEP_STATS["last_sweep_started_at"] = now
        POST_TRADE_SWEEP_STATS["last_sweep_token_count"] = len(tokens)
        await asyncio.gather(*(
            _guarded(token_address, info, now)
            for token_address, info in tokens
        ))
        finished = time.time()
        POST_TRADE_SWEEP_STATS["sweep_count"] += 1
        POST_TRADE_SWEEP_STATS["last_sweep_finished_at"] = finished
        POST_TRADE_SWEEP_STATS["last_sweep_duration"] = finished - now


async def _process_watchlist_token(token_address: str, info: dict[str, Any], now: float) -> None:
    """The per-token body of post_trade_feedback_worker's old sequential
    for-loop, extracted so it can be run concurrently across the whole
    tracked population instead of one token at a time. With a large,
    growing WATCHING population (hundreds of tokens), the old sequential
    version's per-token DexScreener/holder/bundle awaits meant a full pass
    could take far longer than POST_TRADE_POLL_INTERVAL — confirmed
    empirically: most tokens sat at market_cap=0 well after creation,
    simply because their turn in a 300+-token sequential loop hadn't come
    up yet, not because of any scoring or data-fetch bug."""
    info["last_swept_at"] = now
    if info["status"] not in ("WATCHING", "GRADUATED", "RUGGED"):
        if now - info["created_at"] > WATCHLIST_PRUNE_AGE_SECONDS:
            _migrate_to_long_tail(token_address, info, now)
        return

    is_terminal = info["status"] != "WATCHING"
    if is_terminal:
        last_polled = info.get("terminal_last_polled_at", 0.0)
        if now - last_polled < TERMINAL_POLL_INTERVAL_SECONDS:
            if now - info["created_at"] > WATCHLIST_PRUNE_AGE_SECONDS:
                _migrate_to_long_tail(token_address, info, now)
            return
        info["terminal_last_polled_at"] = now

    dex_info = await fetch_dexscreener_info(token_address)
    market_cap = dex_info.get("market_cap", 0.0)
    volume_24h = dex_info.get("volume_24h", 0.0)
    liquidity_usd = dex_info.get("liquidity_usd", 0.0)
    txns_24h = dex_info.get("txns_24h", 0)
    buys_24h = dex_info.get("buys_24h", 0)
    sells_24h = dex_info.get("sells_24h", 0)
    image_url = dex_info.get("image_url")
    if market_cap > info["peak_market_cap"]:
        info["peak_market_cap"] = market_cap

    sparkline = _update_feed_sparkline(token_address, market_cap, now)
    await _maybe_refresh_holder_stats(token_address, info, now)
    await _maybe_check_bundle(token_address, info, now)
    if info.get("platform") == "pump.fun" and not info.get("pumpfun_image_checked"):
        # Fire-and-forget — an unofficial third-party API's latency
        # (or an outage) has no business blocking every OTHER
        # tracked token's mcap/holder/bundle refresh in this same loop.
        asyncio.create_task(_maybe_fetch_pumpfun_image(token_address, info))

    if is_terminal:
        # Already GRADUATED or RUGGED — don't re-run the graduation gates,
        # just keep mcap/volume/liquidity fresh so a graduated token that
        # wasn't listed yet (or a rug's aftermath) doesn't freeze on the
        # dashboard at whatever it read the instant it resolved.
        token_feed_upsert(
            token_address, market_cap=market_cap,
            peak_market_cap=info["peak_market_cap"], sparkline=sparkline,
            volume_24h=volume_24h, liquidity_usd=liquidity_usd, txns_24h=txns_24h,
            buys_24h=buys_24h, sells_24h=sells_24h, image_url=image_url,
            **_identity_fields(token_address, info),
        )
        # Without this, opportunity_score/score_reasons would freeze
        # at whatever they were the moment the token graduated/rugged
        # while the mcap/volume/liquidity numbers right above it keep
        # updating every cycle — a growing, visible mismatch between
        # the displayed score and the displayed numbers it's supposed
        # to be scoring. (No ping fires — status isn't WATCHING.)
        await _rescore_token_and_maybe_ping(token_address)
        await broadcast_token_card(TOKEN_FEED[token_address])

        # "Graduated" only ever meant "crossed the mcap/volume/liquidity
        # bar once" — it was never a rug-immunity guarantee. Nothing
        # previously re-checked a graduated token for a post-graduation
        # dump, so a dev could graduate, get credited as a proven/ELITE
        # dev, then rug the exact same token and keep that credit
        # forever. Apply the same drawdown-from-peak test graduated
        # tokens would otherwise never get, and if it fires, undo the
        # "successful launch" credit along with the usual rug bookkeeping.
        if info["status"] == "GRADUATED":
            token_age = now - info["created_at"]
            peak = info["peak_market_cap"]
            if token_age >= RUG_WINDOW_SECONDS and peak > 0 and market_cap <= peak * (1 - RUG_DRAWDOWN_PCT):
                info["status"] = "RUGGED"
                _blacklist_bundle_operator_if_any(token_address)
                _credit_early_buyers(token_address, info["chain"], "rugs")
                dev = DEV_REPUTATION_DATABASE.get(info["dev_wallet"])
                if dev:
                    dev["failed_spams"] += 1
                    dev["is_blacklisted"] = True
                    if dev.get("successful_launches", 0) > 0:
                        dev["successful_launches"] -= 1
                DEV_SPAM_LOG[info["dev_wallet"]].append(now)
                drawdown_pct = ((peak - market_cap) / peak * 100.0) if peak > 0 else 0.0
                history = DEV_RUG_HISTORY[info["dev_wallet"]]
                history.append({
                    "token_address": token_address,
                    "chain": info["chain"],
                    "ticker": info["ticker"],
                    "timestamp": now,
                    "peak_market_cap": peak,
                    "drawdown_pct": drawdown_pct,
                })
                if len(history) > DEV_RUG_HISTORY_MAX_PER_DEV:
                    del history[0]
                token_feed_upsert(token_address, status="RUGGED", **_identity_fields(token_address, info))
                # This branch never rescored either — without it, a
                # rugged-after-graduation token's displayed
                # opportunity_score would stay frozen at its last
                # WATCHING-era number instead of reflecting the dev
                # now being blacklisted. (No ping fires here — the
                # wrapper only pings while status is WATCHING.)
                await _rescore_token_and_maybe_ping(token_address)
                await broadcast_token_card(TOKEN_FEED[token_address])
                await bump_daily_stat(now, "rugged")
                rug_alert = {
                    "type": "RUGGED",
                    "severity": "danger",
                    "title": "RUGGED AFTER GRADUATION - DEV REPUTATION REVOKED",
                    "chain": info["chain"],
                    "platform": info["platform"],
                    "token_address": token_address,
                    "ticker": info["ticker"],
                    "dev_wallet": info["dev_wallet"],
                    "peak_market_cap_usd": peak,
                    "current_market_cap_usd": market_cap,
                    "drawdown_pct": drawdown_pct,
                    "rugged_after_graduation": True,
                    "signals": TOKEN_FEED.get(token_address, {}).get("signals", []),
                    **dev_rep_badge_fields(info["dev_wallet"]),
                    "timestamp": now,
                }
                RECENT_RUGS.append(rug_alert)
                await broadcast_alert(rug_alert)
                await persist_state()
        return

    if info["platform"] == "pump.fun" and market_cap > PUMPFUN_IMPLAUSIBLE_WATCHING_MCAP_USD:
        await _kick_out_watchlist_token(
            token_address, info, now, "IMPLAUSIBLE_MCAP_FOR_BONDING_CURVE",
            "SKIPPED - NOT A REAL NEW LAUNCH (implausible mcap for bonding curve)",
        )
        return

    # Free alternative to PumpPortal's paid per-trade subscription: poll
    # the token's own on-chain bonding curve account directly (decoder
    # verified empirically — see fetch_pumpfun_bonding_curve_state).
    bonding_curve_key = info.get("bonding_curve_key")
    if info["platform"] == "pump.fun" and bonding_curve_key:
        bonding_state = await fetch_pumpfun_bonding_curve_state(bonding_curve_key)
        if bonding_state:
            token_feed_upsert(
                token_address,
                bonding_sol_raised=bonding_state.get("bonding_sol_raised"),
                bonding_progress_pct=bonding_state.get("bonding_progress_pct"),
                bonding_complete=bonding_state.get("bonding_complete"),
            )

    # Backfill a ticker that showed up as UNKNOWN at creation (StonkFun
    # never has one; some Ember events lack it too) once DexScreener
    # has indexed the pair and can tell us the real symbol.
    resolved_symbol = dex_info.get("symbol")
    if resolved_symbol and (not info["ticker"] or info["ticker"] == "UNKNOWN"):
        info["ticker"] = resolved_symbol
        token_feed_upsert(token_address, ticker=resolved_symbol)

    token_age = now - info["created_at"]
    hit_mcap_target = market_cap >= TARGET_MARKET_CAP_USD
    volume_to_mcap_ratio = (volume_24h / market_cap) if market_cap > 0 else 0.0
    buy_sell_skew = (buys_24h / sells_24h) if sells_24h > 0 else float(buys_24h)
    # Dollar volume alone can come from a handful of large/wash trades —
    # an $8M mcap token with 30 total transactions is exactly the
    # "bloated, not real" pattern this is meant to catch. Require real
    # transaction count, a healthy volume/mcap ratio (mcap can be
    # inflated by price alone without real turnover), a minimum age
    # (an instant graduation is bots racing the firehose, not organic
    # demand), and a buy/sell ratio that isn't absurdly one-sided
    # (everyone still holding = no one has proven they can exit).
    has_real_activity = (
        volume_24h >= MIN_GRADUATION_VOLUME_USD
        and liquidity_usd >= MIN_GRADUATION_LIQUIDITY_USD
        and txns_24h >= MIN_GRADUATION_TXNS
        and volume_to_mcap_ratio >= MIN_GRADUATION_VOLUME_TO_MCAP_RATIO
        and token_age >= MIN_GRADUATION_AGE_SECONDS
        and buy_sell_skew <= MAX_GRADUATION_BUY_SELL_SKEW
    )

    uses_native_graduation = info["platform"] in PLATFORMS_WITH_NATIVE_GRADUATION

    if hit_mcap_target and not has_real_activity and not uses_native_graduation:
        # Crossed the mcap bar but volume/liquidity don't back it up —
        # exactly the "bloated, fake-looking graduation" pattern. Flag
        # it and keep watching instead of crowning it GRADUATED.
        add_token_signal(token_address, "THIN_VOLUME")
        token_feed_upsert(
            token_address, status="WATCHING", market_cap=market_cap,
            peak_market_cap=info["peak_market_cap"], sparkline=sparkline,
            volume_24h=volume_24h, liquidity_usd=liquidity_usd, txns_24h=txns_24h,
            buys_24h=buys_24h, sells_24h=sells_24h, image_url=image_url,
            **_identity_fields(token_address, info),
        )
        await _rescore_token_and_maybe_ping(token_address)
        await broadcast_token_card(TOKEN_FEED[token_address])
        return

    if hit_mcap_target and has_real_activity and not uses_native_graduation:
        info["status"] = "GRADUATED"
        _credit_early_buyers(token_address, info["chain"], "graduations")
        dev = DEV_REPUTATION_DATABASE.get(info["dev_wallet"])
        if dev:
            dev["successful_launches"] += 1
        token_feed_upsert(
            token_address, status="GRADUATED", market_cap=market_cap,
            peak_market_cap=info["peak_market_cap"], sparkline=sparkline,
            volume_24h=volume_24h, liquidity_usd=liquidity_usd, txns_24h=txns_24h,
            buys_24h=buys_24h, sells_24h=sells_24h, image_url=image_url,
            **_identity_fields(token_address, info),
        )
        # Resolved: this branch only reaches GRADUATED when
        # has_real_activity is true, i.e. the earlier thin-volume
        # concern (if this token ever had one) no longer applies —
        # see remove_token_signal's docstring.
        remove_token_signal(token_address, "THIN_VOLUME")
        # This graduation path previously never rescored at all —
        # opportunity_score/score_reasons would silently stay frozen
        # at whatever they were during the last WATCHING poll, and it
        # could never fire a Telegram ping either.
        await _rescore_token_and_maybe_ping(token_address)
        await broadcast_token_card(TOKEN_FEED[token_address])
        await bump_daily_stat(now, "graduated")
        grad_alert = {
            "type": "GRADUATED",
            "severity": "success",
            "title": "TOKEN GRADUATED - DEV REPUTATION UPGRADED",
            "source": "heuristic",
            "chain": info["chain"],
            "platform": info["platform"],
            "token_address": token_address,
            "ticker": info["ticker"],
            "dev_wallet": info["dev_wallet"],
            "dev_successful_launches": dev.get("successful_launches") if dev else None,
            **dev_rep_badge_fields(info["dev_wallet"]),
            "market_cap_usd": market_cap,
            "target_market_cap_usd": TARGET_MARKET_CAP_USD,
            "volume_24h_usd": volume_24h,
            "liquidity_usd": liquidity_usd,
            "txns_24h": txns_24h,
            "buys_24h": buys_24h,
            "sells_24h": sells_24h,
            "signals": TOKEN_FEED.get(token_address, {}).get("signals", []),
            "timestamp": now,
        }
        RECENT_GRADUATIONS.append(grad_alert)
        await broadcast_alert(grad_alert)
        await persist_state()
        return

    if token_age >= RUG_WINDOW_SECONDS:
        peak = info["peak_market_cap"]
        if peak > 0 and market_cap <= peak * (1 - RUG_DRAWDOWN_PCT):
            info["status"] = "RUGGED"
            _blacklist_bundle_operator_if_any(token_address)
            _credit_early_buyers(token_address, info["chain"], "rugs")
            dev = DEV_REPUTATION_DATABASE.get(info["dev_wallet"])
            if dev:
                dev["failed_spams"] += 1
                dev["is_blacklisted"] = True
            DEV_SPAM_LOG[info["dev_wallet"]].append(now)
            drawdown_pct_for_history = ((peak - market_cap) / peak * 100.0) if peak > 0 else 0.0
            history = DEV_RUG_HISTORY[info["dev_wallet"]]
            history.append({
                "token_address": token_address,
                "chain": info["chain"],
                "ticker": info["ticker"],
                "timestamp": now,
                "peak_market_cap": peak,
                "drawdown_pct": drawdown_pct_for_history,
            })
            if len(history) > DEV_RUG_HISTORY_MAX_PER_DEV:
                del history[0]
            token_feed_upsert(
                token_address, status="RUGGED", market_cap=market_cap,
                peak_market_cap=peak, sparkline=sparkline,
                volume_24h=volume_24h, liquidity_usd=liquidity_usd, txns_24h=txns_24h,
            buys_24h=buys_24h, sells_24h=sells_24h, image_url=image_url,
                **_identity_fields(token_address, info),
            )
            await _rescore_token_and_maybe_ping(token_address)
            await broadcast_token_card(TOKEN_FEED[token_address])
            await bump_daily_stat(now, "rugged")
            drawdown_pct = ((peak - market_cap) / peak * 100.0) if peak > 0 else 0.0
            rug_alert = {
                "type": "RUGGED",
                "severity": "danger",
                "title": "RUGPULL DETECTED - DEV AUTO-BLACKLISTED",
                "chain": info["chain"],
                "platform": info["platform"],
                "token_address": token_address,
                "ticker": info["ticker"],
                "dev_wallet": info["dev_wallet"],
                "peak_market_cap_usd": peak,
                "current_market_cap_usd": market_cap,
                "drawdown_pct": drawdown_pct,
                "signals": TOKEN_FEED.get(token_address, {}).get("signals", []),
                **dev_rep_badge_fields(info["dev_wallet"]),
                "timestamp": now,
            }
            RECENT_RUGS.append(rug_alert)
            await broadcast_alert(rug_alert)
            await persist_state()
        else:
            info["status"] = "EXPIRED_WATCH"
            token_feed_upsert(
                token_address, status="EXPIRED_WATCH", market_cap=market_cap,
                peak_market_cap=peak, sparkline=sparkline,
                volume_24h=volume_24h, liquidity_usd=liquidity_usd, txns_24h=txns_24h,
            buys_24h=buys_24h, sells_24h=sells_24h, image_url=image_url,
                **_identity_fields(token_address, info),
            )
            await _rescore_token_and_maybe_ping(token_address)
            await broadcast_token_card(TOKEN_FEED[token_address])
            await bump_daily_stat(now, "expired")
    else:
        token_feed_upsert(
            token_address, status="WATCHING", market_cap=market_cap,
            peak_market_cap=info["peak_market_cap"], sparkline=sparkline,
            volume_24h=volume_24h, liquidity_usd=liquidity_usd, txns_24h=txns_24h,
            buys_24h=buys_24h, sells_24h=sells_24h, image_url=image_url,
            **_identity_fields(token_address, info),
        )
        await _rescore_token_and_maybe_ping(token_address)
        await broadcast_token_card(TOKEN_FEED[token_address])




# ============================================================================
# SECTION 15 — FASTAPI APPLICATION & DASHBOARD WEBSOCKET API
# ============================================================================

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


def _restore_tracking_from_mock_portfolio() -> int:
    """TOKEN_WATCHLIST/TOKEN_FEED are in-memory only (never persisted) — every
    restart silently drops every still-live token out of the hot polling
    loop, since nothing re-adds an ALREADY-known token back into
    TOKEN_WATCHLIST (only a genuinely NEW "create" event does that). A
    WATCHING/GRADUATED token from before the restart then just freezes
    forever at whatever mcap/peak it last had — confirmed empirically this
    session (a token's mock-portfolio peak silently stopped moving right at
    a restart, well below its later real peak). MOCK_PORTFOLIO IS persisted
    and already has everything needed to rebuild a plausible TOKEN_WATCHLIST
    entry, so re-seed the hot loop from it on startup instead of losing
    tracking on every redeploy."""
    restored = 0
    for token_address, position in MOCK_PORTFOLIO.items():
        if token_address in TOKEN_WATCHLIST:
            continue
        status = position.get("last_known_status")
        if status not in ("WATCHING", "GRADUATED", "RUGGED"):
            continue
        chain = position.get("chain")
        platform = position.get("platform")
        dev_wallet = position.get("dev_wallet")
        ticker = position.get("ticker")
        if not chain or not platform or not dev_wallet:
            continue
        created_at = position.get("entry_ts") or time.time()
        peak_market_cap = position.get("peak_market_cap") or 0.0
        market_cap = position.get("last_known_market_cap") or 0.0
        TOKEN_WATCHLIST[token_address] = {
            "chain": chain,
            "platform": platform,
            "dev_wallet": dev_wallet,
            "ticker": ticker,
            "created_at": created_at,
            "peak_market_cap": peak_market_cap,
            "status": status,
            "dev_decision": "PENDING",
            "bonding_curve_key": None,
        }
        token_feed_upsert(
            token_address,
            chain=chain, platform=platform, dev_wallet=dev_wallet, ticker=ticker,
            created_at=created_at, status=status, market_cap=market_cap,
            peak_market_cap=peak_market_cap, image_url=position.get("image_url"),
            links=position.get("links") or {},
            **dev_rep_badge_fields(dev_wallet),
        )
        restored += 1
    return restored


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_state_sync()
    restored_count = _restore_tracking_from_mock_portfolio()
    if restored_count:
        logger.info(f"Restored {restored_count} token(s) from mock_portfolio into the active hot loop after restart")

    tasks_spec = [
        (solana_pumpfun_listener, "solana/pump.fun"),
        (solana_stonkfun_listener, "solana/stonkfun"),
        (solana_ember_listener, "solana/ember"),
        (bnb_chain_listener, "bnb/four.meme+flap.sh"),
        (pons_listener, "robinhood/pons"),
        (longxyz_listener, "robinhood/long.xyz"),
        (solana_wallet_monitor, "wallet-monitor/solana"),
        (lambda: evm_wallet_monitor("bnb", BNB_WS_RPC_URL), "wallet-monitor/bnb"),
        (lambda: evm_wallet_monitor("robinhood", ROBINHOOD_WS_RPC_URL), "wallet-monitor/robinhood"),
        (lambda: evm_holder_ledger_listener("bnb", BNB_WS_RPC_URL), "holder-ledger/bnb"),
        (lambda: evm_holder_ledger_listener("robinhood", ROBINHOOD_WS_RPC_URL), "holder-ledger/robinhood"),
        (narrative_decay_sweeper, "narrative-decay-sweeper"),
        (post_trade_feedback_worker, "post-trade-feedback"),
        (long_tail_revival_watcher, "long-tail-revival-watcher"),
        (telegram_sender_worker, "telegram-sender"),
        (mock_portfolio_broadcaster, "mock-portfolio-broadcaster"),
        (periodic_state_snapshot, "state-snapshot"),
    ]
    for factory, name in tasks_spec:
        BACKGROUND_TASKS.append(asyncio.create_task(run_forever(factory, name), name=name))
    logger.info(f"Started {len(BACKGROUND_TASKS)} background tasks")

    try:
        yield
    finally:
        for task in BACKGROUND_TASKS:
            task.cancel()
        await asyncio.gather(*BACKGROUND_TASKS, return_exceptions=True)
        await persist_state()


app = FastAPI(title="Multi-Chain Fair-Launch Tracker", lifespan=lifespan)

if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    path = os.path.join(STATIC_DIR, "dashboard.html")
    if not os.path.exists(path):
        return HTMLResponse("<h1>Dashboard not found</h1>", status_code=404)
    with open(path) as f:
        return HTMLResponse(f.read())


@app.get("/api/health")
async def health() -> dict:
    return {
        "status": "ok",
        "connected_clients": len(CONNECTED_CLIENTS),
        "tracked_tokens": len(TOKEN_WATCHLIST),
        "alerts_logged": len(ALERT_HISTORY),
    }


@app.get("/api/state")
async def api_state() -> dict:
    return {
        "smart_wallets": SMART_WALLETS,
        "dev_reputation": DEV_REPUTATION_DATABASE,
        "narratives": NARRATIVE_STATUS,
        "watchlist_size": len(TOKEN_WATCHLIST),
        "alert_count": len(ALERT_HISTORY),
    }


@app.get("/api/debug/sweep")
async def api_debug_sweep() -> dict:
    now = time.time()
    return {
        **POST_TRADE_SWEEP_STATS,
        "seconds_since_sweep_started": now - POST_TRADE_SWEEP_STATS["last_sweep_started_at"],
        "watchlist_size": len(TOKEN_WATCHLIST),
        "token_feed_size": len(TOKEN_FEED),
    }


@app.get("/api/debug/token/{token_address}")
async def api_debug_token(token_address: str) -> dict:
    watchlist_entry = TOKEN_WATCHLIST.get(token_address)
    feed_entry = TOKEN_FEED.get(token_address)
    mock_position = MOCK_PORTFOLIO.get(token_address)
    now = time.time()
    return {
        "in_watchlist": watchlist_entry is not None,
        "watchlist_entry": {k: v for k, v in (watchlist_entry or {}).items() if k != "sparkline"},
        "seconds_since_last_swept": (now - watchlist_entry["last_swept_at"]) if watchlist_entry and "last_swept_at" in watchlist_entry else None,
        "in_token_feed": feed_entry is not None,
        "feed_entry": {k: v for k, v in (feed_entry or {}).items() if k != "sparkline"},
        "in_long_tail": token_address in LONG_TAIL_WATCHLIST,
        "mock_position": mock_position,
    }


@app.websocket("/ws/dashboard")
async def dashboard_ws(websocket: WebSocket) -> None:
    await websocket.accept()
    CONNECTED_CLIENTS.add(websocket)
    try:
        snapshot = {
            "kind": "snapshot",
            "payload": {
                "alerts": list(ALERT_HISTORY),
                "smart_wallets": SMART_WALLETS,
                "dev_reputation": DEV_REPUTATION_DATABASE,
                "narratives": {k: v for k, v in NARRATIVE_STATUS.items() if v.get("qualifies")},
                # Full TOKEN_FEED (up to TOKEN_FEED_MAX) stays server-side for live
                # tracking, but the reconnect snapshot only sends the most recent
                # slice — with sparkline/links/signals on every entry, sending all
                # of it verges on multi-MB and needlessly slows every page load.
                "tokens": list(TOKEN_FEED.values())[-SNAPSHOT_TOKEN_LIMIT:],
                "today_stats": {"date": _today_key(time.time()), **DAILY_STATS[_today_key(time.time())]},
                # Full multi-day history (bounded to DAILY_STATS_MAX_DAYS) for
                # the Weekly Progress panel — today_stats above only carries
                # the single current day, which can't show day-over-day or
                # week-over-week trends.
                "daily_stats_history": {day: dict(counters) for day, counters in DAILY_STATS.items()},
                "hourly_launch_stats": dict(HOURLY_LAUNCH_STATS),
                "recent_graduations": list(RECENT_GRADUATIONS),
                "recent_rugs": list(RECENT_RUGS),
                "mock_portfolio": _build_mock_portfolio_snapshot(),
                "recent_opportunities": _refresh_opportunity_history_statuses(),
            },
        }
        await websocket.send_text(json.dumps(snapshot, default=str))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.debug(f"dashboard_ws closed: {exc!r}")
    finally:
        CONNECTED_CLIENTS.discard(websocket)


# ============================================================================
# SECTION 16 — ENTRYPOINT
# ============================================================================

if __name__ == "__main__":
    uvicorn.run(app, host=DASHBOARD_HOST, port=DASHBOARD_PORT)
