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
import hashlib
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
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
import websockets
from Crypto.Hash import keccak as _keccak
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
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
SOLANA_FALLBACK_WS_RPC_URL = os.getenv(
    "SOLANA_FALLBACK_WS_RPC_URL",
    SOLANA_FALLBACK_RPC_URL.replace("https://", "wss://").replace("http://", "ws://"),
)

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

# --- Birdeye (Solana) — rate-limited enrichment/fallback --------------------
# DexScreener stays PRIMARY (free/unlimited) for price/mcap/liquidity. Birdeye
# fills two gaps for Solana coins: (1) price/mcap/liquidity when DexScreener has
# nothing yet, and (2) holder count + top-holder concentration (DexScreener
# gives no holder data, and the RPC holder scan is often rate-limited).
# Free tier is ~1 req/s, so calls are globally throttled + only made for gated
# candidates. Blank key = disabled (feature no-ops, never blocks the pipeline).
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")
BIRDEYE_API_BASE = os.getenv("BIRDEYE_API_BASE", "https://public-api.birdeye.so")
BIRDEYE_ENABLED = bool(BIRDEYE_API_KEY)
BIRDEYE_MIN_INTERVAL_SECONDS = float(os.getenv("BIRDEYE_MIN_INTERVAL_SECONDS", "1.2"))  # global throttle
BIRDEYE_TIMEOUT = float(os.getenv("BIRDEYE_TIMEOUT", "12"))
BIRDEYE_MAX_RETRIES = int(os.getenv("BIRDEYE_MAX_RETRIES", "2"))  # retry on 429 with backoff
# Only re-check a given coin via Birdeye this often — caches the "DexScreener
# had nothing" fallback so the same coin doesn't burn quota every 20s poll.
BIRDEYE_RECHECK_SECONDS = float(os.getenv("BIRDEYE_RECHECK_SECONDS", "600"))  # 10 min

# --- Discovery scanner ------------------------------------------------------
# The launch listeners only catch coins at BIRTH. A coin that was missed at
# launch but is now trading well (like $JEV) stays invisible forever. The
# discovery scanner periodically pulls trending/high-volume Solana coins from
# DexScreener (free) + Birdeye and INJECTS qualifying ones into the watchlist
# so they enter the normal pipeline + Jev gate. Gated by mcap/volume floors +
# the same stock/ticker filters so it doesn't flood with junk. Solscan's free
# tier can't help (every endpoint 401s), so it isn't used.
DISCOVERY_ENABLED = os.getenv("DISCOVERY_ENABLED", "true").lower() == "true"
DISCOVERY_INTERVAL_SECONDS = float(os.getenv("DISCOVERY_INTERVAL_SECONDS", "150"))  # every 2.5 min
DISCOVERY_MIN_MCAP = float(os.getenv("DISCOVERY_MIN_MCAP", "10000"))  # match the Jev eval gate
DISCOVERY_MIN_VOLUME_24H = float(os.getenv("DISCOVERY_MIN_VOLUME_24H", "8000"))
DISCOVERY_MAX_PER_SCAN = int(os.getenv("DISCOVERY_MAX_PER_SCAN", "30"))  # cap injections per cycle

# --- Telegram opportunity pings ---------------------------------------------
# Bot token from @BotFather; chat ID is whichever chat/user/channel should
# receive pings — Telegram gives no way to discover it from the token alone,
# it has to come from a getUpdates call after the target chat has sent the
# bot at least one message. Both blank = feature silently disabled (every
# send site already no-ops in that case), not a startup requirement.
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API_BASE = "https://api.telegram.org"
TELEGRAM_OPPORTUNITY_SCORE_THRESHOLD = float(os.getenv("TELEGRAM_OPPORTUNITY_SCORE_THRESHOLD", "20"))
TELEGRAM_MIN_SEND_INTERVAL_SECONDS = 1.5  # keeps sends under Telegram's per-chat rate limit even if several tokens cross threshold at once
TELEGRAM_SEND_EARLY_MOMENTUM = os.getenv("TELEGRAM_SEND_EARLY_MOMENTUM", "false").lower() in ("true", "1", "yes")
# Early Momentum gets its own, independent ping — same mechanics, different
# score field and threshold. Gated to under $100k matching user's opportunity ceiling.
TELEGRAM_EARLY_MOMENTUM_SCORE_THRESHOLD = float(os.getenv("TELEGRAM_EARLY_MOMENTUM_SCORE_THRESHOLD", "20"))
TELEGRAM_EARLY_MOMENTUM_PING_MIN_MCAP_USD = float(os.getenv("EARLY_MOMENTUM_PING_MIN_MCAP_USD", "15000"))
TELEGRAM_EARLY_MOMENTUM_PING_MAX_MCAP_USD = float(os.getenv("EARLY_MOMENTUM_PING_MAX_MCAP_USD", "100000"))

# Market cap ceiling for opportunity consideration & calls: strictly under 100k ONLY for thestonkboard.com / StonkFun
MAX_OPPORTUNITY_MARKET_CAP_USD = float(os.getenv("MAX_OPPORTUNITY_MARKET_CAP_USD", "100000"))

# Only evaluate tokens launched from recognized launchpads and/or contracts ending in 7777, pump, 4444
QUALIFYING_CONTRACT_SUFFIXES = ("7777", "pump", "4444")
# --- TheStonkBoard token detection & sync -------------------------------------
# The user specified that the <$100k market cap ceiling applies strictly to
# coins associated with thestonkboard.com (StonkFun native launches and coins
# indexed on TheStonkBoard). Other launchpads (pump.fun, etc.) are NOT capped at 100k.
STONKBOARD_COIN_ADDRESSES: set[str] = set()
STONKBOARD_LAST_SYNC_TS: float = 0.0

# StonkFun pairs a new fair-launch memecoin (base token, e.g. NIU) against an
# established stock token or crypto asset (quote token, e.g. ZEC, STONK, WBTC, USDC).
# The user strictly wants the FIRST one (base token) — quote tokens must NEVER be
# tracked, scored, recorded as big runners, or alerted on.
STONKFUN_KNOWN_QUOTE_MINTS: set[str] = {
    "XsbEhLAtcf6HdfpFZ5xEMdqW8nfAvcsP5bdudRLJzJp",  # AAPLx
    "9cRCn9rGT8V2imeM2BaKs13yhMEais3ruM3rPvTGpump",  # ANSEM
    "Pren1FvFX6J3E4kXhJuCiAD5aDmGEb7qJRncwA8Lkhw",  # ANTHROPIC
    "bioJ9JTqW62MLz7UKHU69gtKhPpGi1BQhccj2kmSvUJ",  # BIO
    "BPxxfRCXkUVhig4HS1Lh7kZqV6SPJhzfEk4x6fVBjPCy",  # BP
    "CARDSccUMFKoPRZxt5vt3ksUbxEFEcnZ3H2pd3dKxYjp",  # CARDS
    "Xs2yquAgsHByNzx68WJC55WHjHBvG9JsMB7CWjTLyPy",  # DFDV
    "DKNGQFNGQmoBdXSRGKJ8tTu7uPDasw5JDcfMmWniNfow",  # DKNG
    "DoGEV7LASBkQbibMc5k5vKnTZoMg423GpJ5QtJEGfm7R",  # DOGE
    "FLWSojG1gB5VStYR3Sb4nQFRt43UBYkqih1j2CpVLqgd",  # FLWS
    "Xsv9hRk1z5ystj9MhnA7Lq4vjSsLwzL2nxrwmwtD3re",  # GLDX
    "GPRR2u6NS5yBQHWGauoJ9HXgjrTH8dDsrBfTV5zAYvDH",  # GPRO
    "GRNDYDpqwpCm6jVxpbh4xT5AM4r3p391qYsKTHqgaET2",  # GRND
    "98sMhvDwXj1RQi5c5Mndm3vPe9cBqPrbLaufMXFNMh5g",  # HYPE
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",  # JUP
    "TKLSidmLVt3cqGaaodG8tyRzoANfQwoh67AccjmubeZ",  # KALSHI
    "XsaBXg8dU5cPM6ehmVctMkVqoiRG2ZjMo1cyBJ3AykQ",  # KOX
    "EicWvteVi2fWepEzS3FYWsnuPoP6caZfjnKqNvydLjCH",  # LIT
    "XsqE9cRRpzxcGKDXj1BJ7Xmg4GRhZoyY1KpmGSxAWT2",  # MCDX
    "XspzcW1PRtgf6Wj92HCiZdjzKCyFekVD8P5Ueh3dRMX",  # MSFTX
    "XsP7xzNPvEHS1m6qfanPUGjNmdnmsLKEoNAnHjdxxyZ",  # MSTRX
    "MUxEsUKSMACyw5fZf68wxf5FLnZVhtU9CwH8uNNGay1",  # MU
    "ATBR4i19gcQ31Rfr7ymA2XvkCQEAkNFGBtVKTmdqpump",  # Machi
    "3ZLekZYq2qkZiSpnSvabjit34tUkjSwD1JFuW9as9wBG",  # NEAR
    "oPAiAikWTaFj9RYoRFD35ccfwhnMcB3ThgBZRHSkjTZ",  # OPENAI
    "oreoU2P8bN6jkk3jbaiVxYnG1dCXcYxwhwyK9jSybcp",  # ORE
    "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv",  # PENGU
    "Pre8AREmFPtoJFT8mQSXQLh56cwJmM7CFDRuoGBZiUP",  # POLYMARKET
    "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn",  # PUMP
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",  # RAY
    "RDDTGbhHwVXfyCvQMXzzowKjf5qrYBZAnehoXW83ooh",  # RDDT
    "BoTx8y9ynfdxf5ZjWtCoBVkff52qKA82ysaLU8ZM6d8T",  # ROBOSTRATEGY
    "SiLVFMgD3eD2rgK628NbTBq9MnuJF5FW2CRaVyTB35L",  # SILVER
    "SNDKbwMUQvZhnLnxLduradgLHG5KrPuKwpnrkkGRhfH",  # SNDK
    "So11111111111111111111111111111111111111112",  # SOL
    "Xs3oZwbHvqis4NYcf4YKWmEia2eC84wSiVrcYcTqpH8",  # SPCXX
    "J3NKxxXZcnNiMjKw9hYb2K4LUxgwB6t1FtPtQVsv3KFr",  # SPX
    "6GmAFSYs4gk3FDao5FzzySQpPZaWsa4rUJHacpMpUNgx",  # STONK
    "Xs78JED6PFZxWc2wCEPspZW9kL3Se5J7L5TChKgsidH",  # STRCX
    "SV151D5pjygAKA8aJJcKzm4wFnRX5G92Fye94jQJk7g",  # SV151
    "taoC6xyv2v8tDLcev4uaGUgV4vdQsWJrGft2kcBRrBY",  # TAO
    "TTWofwAge91oFhZs7kpQdyrVRkmevgM88xijGvQFbKo",  # TTWO
    "uniHfuPhEQSrtpzXpJZDCSq53yaejKKpNhFUiKoHKHV",  # UNI
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Dz9mQ9NzkBcCsuGPFJ3r1bS4wgqKMHBPiVuniW8Mbonk",  # USELESS
    "XsfCC9VL4DamVGNgdJpfLXB3sBVa158Gbx8sh7NzmTk",  # VIDAX
    "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh",  # WBTC
    "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs",  # WETH
    "2zCo6bUowJMvr89ajxuWsPadAqJ2F9akCkxumNsSdgsL",  # XBTC
    "WXMRyRZhsa19ety5erZhHg4N3xj3EVN92u94422teJp",  # XMR
    "A7bdiYdS5GjqGFtxf17ppRHtDKPkkRqbKtR27dxvQXaS",  # ZEC
    "Ce2gx9KGXJ6C9Mp5b5x1sn9Mg87JwEbrQby4Zqo3pump",  # neet
    "6UpQcMAb5xMzxc7ZfPaVMgx3KqsvKZdT5U718BzD5We2",  # wXRP
    "4sWNB8zGWHkh6UnmwiEtzNxL4XrN7uK9tosbESbJFfVs",  # xSOL
}
STONKFUN_QUOTE_MINTS: set[str] = set(STONKFUN_KNOWN_QUOTE_MINTS)

def purge_stonkfun_quote_tokens() -> int:
    """Purges all StonkFun quote assets (collateral stock/crypto tokens like ZEC, STONK,
    WBTC, USDC, SOL, etc.) from all active tracking structures.
    StonkFun pairs fair-launch coins against these quote assets (e.g. NIU/ZEC) — we only ever want
    the base token (first one), never the quote token (second one)."""
    purged = 0
    wl = globals().get("TOKEN_WATCHLIST")
    feed = globals().get("TOKEN_FEED")
    mock = globals().get("MOCK_PORTFOLIO")
    stonk_addrs = globals().get("STONKBOARD_COIN_ADDRESSES")
    long_tail = globals().get("LONG_TAIL_WATCHLIST")
    seen = globals().get("BIG_RUNNERS_SEEN")
    jev_log = globals().get("JEV_JUDGMENT_LOG")
    jev_eval = globals().get("JEV_EVALUATED_TOKENS")
    jev_screen = globals().get("JEV_SCREENED_TOKENS")
    tg_pinged = globals().get("TELEGRAM_PINGED_TOKENS")
    opp_rec = globals().get("OPPORTUNITY_RECORDED_TOKENS")

    for qm in list(STONKFUN_QUOTE_MINTS):
        if wl is not None and qm in wl:
            del wl[qm]
            purged += 1
        if feed is not None and qm in feed:
            del feed[qm]
            purged += 1
        if mock is not None and qm in mock:
            del mock[qm]
            purged += 1
        if stonk_addrs is not None and qm in stonk_addrs:
            stonk_addrs.discard(qm)
            purged += 1
        if long_tail is not None and qm in long_tail:
            del long_tail[qm]
            purged += 1
        if seen is not None and qm in seen:
            seen.discard(qm)
            purged += 1
        if jev_log is not None and qm in jev_log:
            del jev_log[qm]
            purged += 1
        if jev_eval is not None and qm in jev_eval:
            del jev_eval[qm]
            purged += 1
        if jev_screen is not None and qm in jev_screen:
            del jev_screen[qm]
            purged += 1
        if tg_pinged is not None and qm in tg_pinged:
            del tg_pinged[qm]
            purged += 1
        if opp_rec is not None and qm in opp_rec:
            del opp_rec[qm]
            purged += 1

    # Clean from BIG_RUNNERS across all chains
    big_runners = globals().get("BIG_RUNNERS")
    if big_runners is not None:
        for ch in ("solana", "bnb", "robinhood"):
            runners = big_runners.get(ch, [])
            new_runners = [r for r in runners if r.get("token_address") not in STONKFUN_QUOTE_MINTS]
            if len(new_runners) != len(runners):
                purged += (len(runners) - len(new_runners))
                big_runners[ch] = new_runners

    # Clean from RECENT_OPPORTUNITIES
    recent_opps = globals().get("RECENT_OPPORTUNITIES")
    if recent_opps is not None:
        new_opps = deque([o for o in recent_opps if o.get("token_address") not in STONKFUN_QUOTE_MINTS], maxlen=recent_opps.maxlen)
        if len(new_opps) != len(recent_opps):
            purged += (len(recent_opps) - len(new_opps))
            recent_opps.clear()
            recent_opps.extend(new_opps)

    if purged > 0:
        logger.info(f"[stonkboard] Purged {purged} quote token leak(s) ($ZEC, $STONK, etc.) from active memory")
    return purged

def is_stonkboard_token(token_address: str, platform: Optional[str] = None, links: Optional[dict[str, Any]] = None) -> bool:
    """Checks if a coin is associated with TheStonkBoard (thestonkboard.com) —
    either native to StonkFun, listed in TheStonkBoard cache, or has a verified StonkBoard link.
    CRITICAL: Quote/collateral assets (e.g. ZEC, STONK, WBTC, USDC) are NEVER StonkBoard fair-launch coins."""
    if not token_address:
        return False
    if token_address in STONKFUN_QUOTE_MINTS:
        return False
    plat = (platform or "").lower().strip()
    if "stonkfun" in plat or "stonk" in plat:
        return True
    if token_address in STONKBOARD_COIN_ADDRESSES:
        return True
    if links and bool(links.get("stonkboard")):
        return True
    return False

KNOWN_LAUNCHPAD_PLATFORMS = {
    "pump.fun",
    "stonkfun",
    "four.meme",
    "flap.sh",
    "pons",
    "ember",
    "long.xyz",
}

def infer_launchpad_platform(platform: Optional[str], token_address: str) -> Optional[str]:
    """Infers or normalizes the launchpad platform name based on contract suffix and platform string:
    - ends in 'pump' -> 'pump.fun'
    - ends in '4444' -> 'four.meme'
    - ends in '7777' -> 'flap.sh'
    - stonkboard / stonkfun -> 'stonkfun'
    """
    if is_stonkboard_token(token_address, platform):
        return "stonkfun"
    addr = (token_address or "").lower()
    if addr.endswith("pump"):
        return "pump.fun"
    if addr.endswith("4444"):
        return "four.meme"
    if addr.endswith("7777"):
        return "flap.sh"
    plat = (platform or "").lower().strip()
    if "pump" in plat:
        return "pump.fun"
    if "four" in plat or "4meme" in plat:
        return "four.meme"
    if "flap" in plat:
        return "flap.sh"
    if "stonk" in plat:
        return "stonkfun"
    return platform if platform and platform not in ("?", "unknown") else None


def is_launchpad_or_target_suffix(platform: Optional[str], token_address: str) -> tuple[bool, str]:
    """Checks if a token was launched from a recognized launchpad (pump.fun, four.meme, flap.sh,
    stonkfun, pons, ember) and/or its contract address ends in 7777, pump, or 4444:
    - ends with pump: pump.fun launchpad
    - ends with 4444: four.meme launchpad
    - ends with 7777: flap.sh launchpad
    - stonkfun / thestonkboard.com: stonkfun launchpad"""
    addr = (token_address or "").lower()
    inferred = infer_launchpad_platform(platform, token_address)

    if addr.endswith("pump"):
        return True, "pump.fun launchpad (contract suffix 'pump')"
    if addr.endswith("4444"):
        return True, "four.meme launchpad (contract suffix '4444')"
    if addr.endswith("7777"):
        return True, "flap.sh launchpad (contract suffix '7777')"
    if inferred == "stonkfun" or is_stonkboard_token(token_address, platform):
        return True, "StonkFun launchpad"

    plat = (platform or inferred or "").lower().strip()
    is_launchpad = any(lp in plat for lp in KNOWN_LAUNCHPAD_PLATFORMS)
    if is_launchpad:
        return True, f"Launchpad ({inferred or platform})"

    return False, f"Not from a launchpad ({platform or 'unknown'}) & contract does not end in 7777/pump/4444"

async def sync_stonkboard_coins() -> None:
    """Scrapes token roster from https://thestonkboard.com so we know exactly
    which tokens are visible on TheStonkBoard and ingests any newly discovered StonkFun coins.
    CRITICAL: StonkFun pools are pairs of (baseToken, quoteToken). The baseToken is the
    fair-launch meme coin (e.g. NIU), while quoteToken is the established stock/crypto collateral (e.g. ZEC).
    We strictly ingest the baseToken (the first one) and record quote tokens to block them."""
    global STONKBOARD_LAST_SYNC_TS
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get("https://thestonkboard.com", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    coins: list[dict[str, Any]] = []
                    scripts = re.findall(r"<script[^>]*>(.*?)</script>", text, re.DOTALL)
                    for s in scripts:
                        if "initialSnapshot" in s:
                            try:
                                data = json.loads(s)
                                snapshot = data.get("initialSnapshot", {})
                                coins = snapshot.get("coins", [])
                                if coins:
                                    break
                            except Exception:
                                pass

                    now = time.time()
                    newly_ingested = 0

                    if coins:
                        base_mints = {c.get("mint") for c in coins if c.get("mint")}
                        # 1. Update STONKFUN_QUOTE_MINTS with pure quote collateral tokens (tokens that are NOT base launches)
                        for c in coins:
                            q = c.get("quote", {})
                            qm = q.get("mint")
                            if qm and qm not in base_mints:
                                STONKFUN_QUOTE_MINTS.add(qm)

                        # 2. Ingest strictly the base tokens (c['mint'] — the FIRST token)
                        for c in coins:
                            mint = c.get("mint")
                            if not mint:
                                continue
                            # Never ingest a pure quote asset (like ZEC, STONK, WBTC)
                            if mint in STONKFUN_QUOTE_MINTS:
                                continue

                            STONKBOARD_COIN_ADDRESSES.add(mint)
                            logo_path = c.get("logo")
                            logo_url = f"https://thestonkboard.com{logo_path}" if logo_path else f"https://thestonkboard.com/api/logos/{mint}"
                            coin_name = c.get("name")

                            # Ensure any active tracked instances in memory are stamped with stonkfun and logo
                            if mint in TOKEN_WATCHLIST:
                                TOKEN_WATCHLIST[mint]["platform"] = "stonkfun"
                                if not TOKEN_WATCHLIST[mint].get("image_url"):
                                    TOKEN_WATCHLIST[mint]["image_url"] = logo_url
                                if coin_name and not TOKEN_WATCHLIST[mint].get("name"):
                                    TOKEN_WATCHLIST[mint]["name"] = coin_name
                            if mint in TOKEN_FEED:
                                TOKEN_FEED[mint]["platform"] = "stonkfun"
                                TOKEN_FEED[mint]["is_stonkboard"] = True
                                if not TOKEN_FEED[mint].get("image_url"):
                                    TOKEN_FEED[mint]["image_url"] = logo_url
                                if coin_name and not TOKEN_FEED[mint].get("name"):
                                    TOKEN_FEED[mint]["name"] = coin_name

                            existing_wl = TOKEN_WATCHLIST.get(mint)
                            if existing_wl and existing_wl.get("status") == "SKIPPED":
                                dev_w = existing_wl.get("dev_wallet", "")
                                if dev_w.startswith("stonkboard_"):
                                    existing_wl["status"] = "WATCHING"
                                    existing_wl["dev_decision"] = "PASS"
                                    if mint in TOKEN_FEED:
                                        TOKEN_FEED[mint]["status"] = "WATCHING"
                            elif mint not in TOKEN_FEED and mint not in TOKEN_WATCHLIST:
                                sym = (c.get("symbol") or "UNKNOWN").strip().upper()
                                if sym and is_stock_style_ticker(sym):
                                    continue
                                if sym and ticker_is_invalid(sym)[0]:
                                    continue
                                await process_new_token_event(
                                    chain="solana",
                                    platform="stonkfun",
                                    token_address=mint,
                                    ticker_raw=sym,
                                    dev_wallet=f"stonkboard_{mint[:8]}",
                                    ts=now,
                                    extra={
                                        "source": "thestonkboard.com",
                                        "name": coin_name,
                                        "image_url": logo_url,
                                    },
                                )
                                newly_ingested += 1
                    else:
                        # Fallback regex if SSR JSON shape changed
                        token_objs = re.findall(r'\{[^{}]*\"mint\":\"([1-9A-HJ-NP-Za-km-z]{32,44})\"[^{}]*\"symbol\":\"([^\"]+)\"[^{}]*\}', text)
                        for mint, sym_raw in token_objs:
                            if mint in STONKFUN_QUOTE_MINTS:
                                continue
                            STONKBOARD_COIN_ADDRESSES.add(mint)
                            sym = sym_raw.strip().upper()
                            if sym and is_stock_style_ticker(sym):
                                continue
                            if sym and ticker_is_invalid(sym)[0]:
                                continue
                            logo_url = f"https://thestonkboard.com/api/logos/{mint}"
                            if mint not in TOKEN_FEED and mint not in TOKEN_WATCHLIST:
                                await process_new_token_event(
                                    chain="solana",
                                    platform="stonkfun",
                                    token_address=mint,
                                    ticker_raw=sym,
                                    dev_wallet=f"stonkboard_{mint[:8]}",
                                    ts=now,
                                    extra={
                                        "source": "thestonkboard.com",
                                        "image_url": logo_url,
                                    },
                                )
                                newly_ingested += 1

                    # Purge any quote tokens that may have leaked in
                    purge_stonkfun_quote_tokens()

                    STONKBOARD_LAST_SYNC_TS = now
                    if newly_ingested > 0:
                        logger.info(f"[stonkboard] Synced {len(STONKBOARD_COIN_ADDRESSES)} base tokens; ingested {newly_ingested} new StonkFun base coins into live tracker & PvP pipeline")
    except Exception as exc:
        logger.debug(f"[stonkboard] Sync failed: {exc!r}")

async def stonkboard_sync_worker() -> None:
    while True:
        await sync_stonkboard_coins()
        await asyncio.sleep(180)


# Paper-trading size for the Mock Portfolio panel — purely a display multiplier
# (pnl_pct * this / 100), no real funds involved. Answers "how much would I be
# up" without needing per-position custom stake sizing.
MOCK_BUY_SIZE_USD = float(os.getenv("MOCK_BUY_SIZE_USD", "100"))

# --- TypeSafe "Jev" semantic reasoner ---------------------------------------
# Jev (TypeSafe's System One model) is used here as a per-coin SEMANTIC layer:
# it judges the qualitative things this pipeline's arithmetic can't compute —
# is the name/narrative coherent or low-effort garbage, does it look like a
# serious launch, is it impersonating an established asset, does the theme have
# staying power. It returns calibrated, typed judgments (not generated text);
# code owns the workflow and blends the judgment into the opportunity score as
# confidence-scaled, fully-attributed points (see compute_opportunity_score).
#
# IMPORTANT — Jev does NOT learn on its own. It's a stateless reasoner. The
# "learning" lives in THIS system: every judgment is logged next to the token's
# eventual outcome (mooned/rugged/flat, from the mock portfolio + graduation
# history), and JEV_CORRELATION stats show which dimensions actually track
# winners on YOUR data. That's the loop that improves opportunity-finding.
#
# Blank key = feature silently disabled (idles like every other listener),
# so the stack runs fine with no TypeSafe account at all.
TYPESAFE_API_KEY = os.getenv("TYPESAFE_API_KEY", "")
TYPESAFE_API_BASE = os.getenv("TYPESAFE_API_BASE", "https://api.typesafe.ai")
TYPESAFE_MODEL = os.getenv("TYPESAFE_MODEL", "jev-latest")
JEV_ENABLED = bool(TYPESAFE_API_KEY)
JEV_REQUEST_TIMEOUT = float(os.getenv("JEV_REQUEST_TIMEOUT", "20"))
JEV_MAX_RETRIES = int(os.getenv("JEV_MAX_RETRIES", "3"))  # retried only on 429/529, exp backoff

# --- Jev COST CONTROL (small credit budgets: Jev calls cost tokens) ---------
# Jev is NEVER called on every candidate the firehose produces — that would
# drain a small credit balance in minutes. It's gated so only genuinely
# promising, identifiable candidates ever cost a call, evaluated once each and
# cached, and hard-capped per day and lifetime. When any cap is hit, Jev
# silently stops calling and tokens score on the deterministic signals only
# (identical to the no-key path). Defaults are deliberately conservative.
JEV_MIN_SCORE_TO_EVALUATE = float(os.getenv("JEV_MIN_SCORE_TO_EVALUATE", "40"))  # deterministic opp-score bar before spending a call
JEV_MIN_MCAP_TO_EVALUATE = float(os.getenv("JEV_MIN_MCAP_TO_EVALUATE", "30000"))
JEV_MAX_CALLS_PER_DAY = int(os.getenv("JEV_MAX_CALLS_PER_DAY", "400"))
JEV_MAX_CALLS_TOTAL = int(os.getenv("JEV_MAX_CALLS_TOTAL", "3000"))  # lifetime safety cap across restarts (persisted)
# Blend weights — points contributed to the 0-100 opportunity score at FULL
# confidence; the actual contribution is scaled by Jev's own confidence, so an
# uncertain judgment moves the score little (docs' confidence-gating pattern).
# Kept in code (not sent to Jev) so you can retune from outcomes without any
# re-inference (docs: "changing a weight need not rerun inference").
JEV_WEIGHT_NARRATIVE_QUALITY = float(os.getenv("JEV_WEIGHT_NARRATIVE_QUALITY", "12"))
JEV_WEIGHT_LEGITIMACY = float(os.getenv("JEV_WEIGHT_LEGITIMACY", "12"))
JEV_WEIGHT_DURABILITY = float(os.getenv("JEV_WEIGHT_DURABILITY", "10"))
# Impersonation is a Noul (prob 0..1); above this it HARD-VETOES the score to 0,
# matching the existing blacklist/implausible-mcap hard floors.
JEV_IMPERSONATION_VETO = float(os.getenv("JEV_IMPERSONATION_VETO", "0.6"))
# Trap/rug risk (Noul over the enriched on-chain state): negative points scaled
# by probability up to this weight; above the veto it hard-zeros the score.
JEV_WEIGHT_TRAP_RISK = float(os.getenv("JEV_WEIGHT_TRAP_RISK", "20"))
JEV_TRAP_RISK_VETO = float(os.getenv("JEV_TRAP_RISK_VETO", "0.85"))

# --- PvP same-name comparative choice + >$500k runner history ---------------
# When several coins share a name (old + new, across launchpads — the "PvP"
# situation, e.g. a narrative spike spawning copycats of $OPTIMUS), Jev can be
# asked to PICK which coin in that cohort is the actual play, grounded in a
# per-chain history of coins that previously ran above BIG_RUNNER_MCAP_USD.
BIG_RUNNER_MCAP_USD = float(os.getenv("BIG_RUNNER_MCAP_USD", "500000"))
BIG_RUNNERS_MAX_PER_CHAIN = int(os.getenv("BIG_RUNNERS_MAX_PER_CHAIN", "200"))
# Minimum distinct same-name coins before a PvP comparative call is worth it.
JEV_PVP_MIN_COHORT = int(os.getenv("JEV_PVP_MIN_COHORT", "2"))
# PvP is expensive and was over-firing — gate it: at least one cohort member
# must have real mcap, and don't re-run the same narrative within the cooldown
# even if the cohort drifts a little (prevents churn re-spend).
JEV_PVP_MIN_MCAP = float(os.getenv("JEV_PVP_MIN_MCAP", "10000"))
JEV_PVP_COOLDOWN_SECONDS = float(os.getenv("JEV_PVP_COOLDOWN_SECONDS", "1800"))  # 30 min per narrative
# Points from the primary 'moon potential' judgment (confidence-scaled). Jev
# judges upside-from-here for any LIVE coin (big or small); only already-run
# GRADUATED/RUGGED coins are excluded from candidacy (see assemble_same_name_cohort).
JEV_WEIGHT_MOON_POTENTIAL = float(os.getenv("JEV_WEIGHT_MOON_POTENTIAL", "18"))

# --- Real-trajectory outcome model (replaces graduation=moon) ---------------
# A Jev-flagged coin is tracked from its flag mcap. Outcomes are graded on the
# ACTUAL move, not graduation:
#   MOONED  = reached a tier multiple AND held >= sustain minutes
#   RUGGED  = instant/sharp >= 80% drop from peak within the rug window
#   DYING   = slow bleed down (not a rug) — a separate, softer negative
#   ALIVE   = still tracking, no verdict yet
JEV_MOON_TIERS = [float(x) for x in os.getenv("JEV_MOON_TIERS", "3,5,10").split(",")]  # x-multiples
JEV_MOON_SUSTAIN_SECONDS = float(os.getenv("JEV_MOON_SUSTAIN_SECONDS", "900"))  # 15 min
JEV_RUG_DROP_PCT = float(os.getenv("JEV_RUG_DROP_PCT", "0.80"))                 # 80% from peak
JEV_RUG_WINDOW_SECONDS = float(os.getenv("JEV_RUG_WINDOW_SECONDS", "600"))      # "instant" = within 10 min of peak
JEV_DYING_DROP_PCT = float(os.getenv("JEV_DYING_DROP_PCT", "0.60"))            # >=60% down but slow = dying
JEV_TRACK_WINDOW_SECONDS = float(os.getenv("JEV_TRACK_WINDOW_SECONDS", "86400"))  # keep grading up to 24h
JEV_TRACK_POLL_SECONDS = float(os.getenv("JEV_TRACK_POLL_SECONDS", "30"))

# --- Level C: auto-tuning of blend weights from real outcomes ----------------
# Once enough coins have reached a real moon/rug/dying outcome, the system
# measures which Jev dimensions separated winners from losers and derives a
# per-dimension weight MULTIPLIER (predictive → up, useless/inverted → down).
# Gated on sample size so it never tunes on noise; multipliers are clamped so
# one weird batch can't dominate. Learned multipliers are applied on top of the
# base JEV_WEIGHT_* constants in jev_score_contribution.
JEV_AUTOTUNE_ENABLED = os.getenv("JEV_AUTOTUNE_ENABLED", "true").lower() == "true"
JEV_AUTOTUNE_MIN_OUTCOMES = int(os.getenv("JEV_AUTOTUNE_MIN_OUTCOMES", "30"))  # labeled outcomes before activating
JEV_AUTOTUNE_MIN_WINNERS = int(os.getenv("JEV_AUTOTUNE_MIN_WINNERS", "3"))     # need some moons for a real contrast
JEV_AUTOTUNE_CLAMP_LOW = float(os.getenv("JEV_AUTOTUNE_CLAMP_LOW", "0.3"))     # a dimension can't drop below 0.3x
JEV_AUTOTUNE_CLAMP_HIGH = float(os.getenv("JEV_AUTOTUNE_CLAMP_HIGH", "2.5"))   # or above 2.5x
JEV_AUTOTUNE_INTERVAL_SECONDS = float(os.getenv("JEV_AUTOTUNE_INTERVAL_SECONDS", "1800"))
# Learned multipliers per scored dimension (1.0 = base weight, updated live).
JEV_LEARNED_WEIGHTS: dict[str, float] = {}
JEV_AUTOTUNE_STATUS: dict[str, Any] = {"active": False, "reason": "not enough outcomes yet", "updated_at": None}
# Points the PvP winner gets (confidence-scaled); a coin the cohort-pick did
# NOT choose gets a small penalty so the chosen one is preferred.
JEV_WEIGHT_PVP_PICK = float(os.getenv("JEV_WEIGHT_PVP_PICK", "15"))
# Optional seed of known past big runners so the history is useful on day one,
# before the tracker has observed its own. Format: "chain:TICKER:platform"
# comma-separated, e.g. "solana:OPTIMUS:pump.fun,solana:PEPE:pump.fun".
BIG_RUNNERS_SEED = os.getenv("BIG_RUNNERS_SEED", "")

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
    # --- user smart wallets (bulk-loaded) ---
    "Dj59QJvGrRJZfAbGAUdrv11TZm7d1K4qawvw7V59v6So": {"alias": "Tuna", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "EyDiU3AWmav8dkGLFRmjeVckmw9uZ3B43QtAkdURGAky": {"alias": "kalm", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "5zVedvk9ffJKwaoueQWKEAAAa1nutXKhaqmFxzwmvVkW": {"alias": "AJC", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "Gw4eZYJNpf7eqMk97tWx5XjnBQ9QzegLxwnsQPTfTAEU": {"alias": "Theo", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "9QWZhySZ3UqXR6pEfk43w3w8c8udGeLv8jcvWjgn1cCH": {"alias": "iykyk", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "BSM7obo97xfVUPSPEuU6kURoPvTYUNVspQSKJm5jpPUq": {"alias": "jg", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "GEyyNQPCykZ4MzazG9xja4vRiKUmvvFnFKt2YkvftmQp": {"alias": "eric", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "6pVvYMQgSUETvtUc4mBFZPxKW43k7R46jszFidbCKmWP": {"alias": "GrantLiu", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "EmFaYsznEzQ1awoTyNxU9bi7wH9z2Xp2ufTRdY8pbYYQ": {"alias": "cipher", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "Gf3MCpRezf6kFMRTuoXjMa21Lsj9HrEgSGarxi6EK5AG": {"alias": "Clark", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "7mRxZ7yAk6KZxdre9s59odHSR5Xq2XVj46j3jyjafkDp": {"alias": "Hammy", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "Bhkhub5XjRTjcoG4FnCgfNvymdhan8ZXTTbQYXfhf9rv": {"alias": "hihi33", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "588deeKuwhHddybmi6H9c7R9SXFidMs43GpjpgKsPnXN": {"alias": "User2", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},    "7CMeePt8sKLnndc65hbBe7Ye7VZuAFBHcFSyMGnpK3C1": {"alias": "moneysurfers", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "A5SEXYJY4jTEi6sjMLfZs5KAP8SVFvLDPDV67GgSSZSk": {"alias": "frank", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "DsoMrMSYzcMuUAR9WP3qjyPj3uo6oVcwjjQDfD8S4hgV": {"alias": "BIGWARZ", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "9GUeC76XUTbct39mdNy6kHs7YwfrvwdBSe8tummTm5sT": {"alias": "BS", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "D14CPUBvncprFxofp8jq2dmavW9kpobLGvMHN6vbDhNT": {"alias": "DegenCapitalLLC", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "2Kj9rdrLkU44neC3CwXuBfN1oz4VycZEDuwnD8fZAU4v": {"alias": "lilbro", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "8QmE9tvK8jaB1E2azGHySv8wACpnPMSuLd6jVTKAuMve": {"alias": "ShillPill", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "DATMkmVrFbZt7isGoxyDSpAuF9vJycpkDKwhbf978eUw": {"alias": "game", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "9QcRTLHsVzLNFgxCNDVR9M6ZkA1FGkVDAcAefQaHgDN3": {"alias": "seb", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "AN5qesUgQDDUgVGVqEErxhNbgQbLEqBmXANrEnauwd5D": {"alias": "Yodel", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "6CKfpKL3nNHyZ5mStHaDcKAUiLzajrJPruYCpWXtsn6P": {"alias": "kiss", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "6Buy9uMErVDFZfMq7mGDfnY88wxt4FETcCx8g4xD6Gox": {"alias": "Bruce", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "7AY1paSDAwdUjo2vQGz5mU6LQuy2ESYJ1s46qJrLkrZr": {"alias": "cjuggz", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "zn535ZgYyoMPoixDG6LH5S7E2ucF5fZmFjF9s2uw9Ak": {"alias": "dietrying", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "HYmWmpbJbjzsSGu19qBKBjBaXrSrFiGzX3EFtaoPos9F": {"alias": "DipWheeler", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "3hrre4tP36Hgy43a2GrzcixVPbb5JEz6gVHJ2B4dh3uU": {"alias": "chrollo", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "4WhFtDKcJGLoLAYbQm7iByw5h2sNQTTZF99v4y21T28L": {"alias": "remus", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "AyqBKrYYF4C3GpUrMm15xMf8KP6hz1jNiMzugHj4urVt": {"alias": "ton", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "7TAMgUTRR2J8iUpPVuevFRFq8oX1kmhKTx4Y4BUNF9Eb": {"alias": "RC", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "8kK1P9Fuqx3whGjjZpFFPTuJ7jKGSPBTcWLeaQ32vpuk": {"alias": "se", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "CJDjRAjigb3UdsjxaV7AiMchhJV6qAcc3SjrSLFyytE3": {"alias": "bigbabbajohnson", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "9u9KvMKSrwNeWbgyU3dErGFvVzxmc3QQzh2nH7YdA7dz": {"alias": "Levis", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "3Jh89g9WSztmVHryfWrWM455FvMLMWuvhpkPDXCuqnUc": {"alias": "BTheBezel", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "CUjK7UkVughqGpv8H5jabZvMMx5pFe7a3GSLUEEH3hnz": {"alias": "Nach", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "7Nah6th1W2cKzGXVL24uNBfEKCzEtrkBc2fmEbkdkLAR": {"alias": "Red", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "Bj6u3bLyd7CciYev1SkBtDdsjz8p7knjNxByFAH8MyfY": {"alias": "Qwerty", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "2kDuF558okYKx4P3va7nCigFSBtN3uCjRgERQqoNnxtx": {"alias": "TheBoggartt", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "8nwzLRBBQyW9BndxZn3YLaEVVoNp91iAxMpuWgczGZB7": {"alias": "dns", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "6FYwko4PVrbTuVaPyqcD8e6gquvx5yXnEBjvUMcfKv9b": {"alias": "fomopumpguy", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "CxVjCVvSw5W8Sc8fXARCd5cAna1xNGopDmV6D8oh42B1": {"alias": "Tekkerrss", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "3QGeGcN3atNeBehp9TWKDPvvroNrK6uQZieSPa4qPfqQ": {"alias": "Sneaky", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "7CCJbLegkjpJNNbSe7zrkFaTt7mHoakFz6yecbQhV8Rk": {"alias": "arbitrary", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "6a1ADPXoWdFyHNBw6ECVANq4TtcGFe2aMHgQDYW45cPw": {"alias": "reese", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "7b3GfEL2ofEchjaMpt7LMDUh4dZqBvVHSw4z6i85VCai": {"alias": "bird", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "6H5fSkG1WyYNRuqQJsjUG5saxYdfuYZwukTX1YywUSQV": {"alias": "nahyeR", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "3V6Cqe2iwCaRbZ8jB34qDWCtoHKgvdmY8M1DgW8Dxnm6": {"alias": "Slurp", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "F2VrjQDtdNC1dvnz4UaZX1LZd5Ra8vYJE4EnZX6rEfgg": {"alias": "willwin", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "EhD4iG1UYBxHVXByQ9Xgy8i99wcZWRrtp6tKDYBneRur": {"alias": "irio", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "37HfT8ESMHoyqUPXCXWDhtaiw4wN922vZx2Gbh1VPgLA": {"alias": "Bitman", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "6PH7KwBsMC9Aq9um5YrQPA4hx34opEA4iLTeMGq543VL": {"alias": "31337", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "GJcMxxQf27cYv2KLA26H6VTZdPfrneZ2HmvCdvMxxZfi": {"alias": "fibs", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "ENgh9jC8T49tKCgLEmj2h4tryQxB7XYb9sNQsfmaiKKg": {"alias": "Don", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "CRaP9mv2Ws84phVq2xH8SeqMv2NkR1HvALrxpR9Le6Hy": {"alias": "kch3n", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "FF93UeCJ8Gf5w5nH3n7ZzQhiP7QykwbNpM3No2n7YbXC": {"alias": "px", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "FdmJL6ApGjeyKSymASrjtUu9cxQCfMzVHSCzHxQSQTPZ": {"alias": "bobloblaw", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "GWcBJ6BUPw4w87GyiSiKgzrRBGBXypmC98rqzMJqA2dJ": {"alias": "yanineko", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "6zMPHJMEnaS5toiVy59KL8k5SvVYCdEAZgbf4RCa8vBE": {"alias": "Dedrater", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "4t5nPKrKC2BX7RBLv7Gm5MghFMvgoXTQFg8KSy4QdM6V": {"alias": "lefty", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "usvNm7AraV9bjfJs3bRs7KjgfbYDZKvpn1GMsVX2fb1": {"alias": "RT", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "FJDy9FDRy6bwGEUKuAC98bUtN8MkpE2pT7Dj7HE3Z7Q1": {"alias": "Unipcs", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "AS6XTzRBBKe15u7c6hTbzjru22MQRWhRRM91QzshYzya": {"alias": "Kaduna", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "5amw8o962mrUQq1hWTjUa6WMcyBPcNdWRe6rWM6zo4x6": {"alias": "ebaniludik", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "7rpkCAMyNnpGxsbRKszqawgPDe8eRwho9jgfzMZfsfEK": {"alias": "EricCryptoman", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "2r8Z99rdDQNuiGHBoK1bbfVvifiYUwofc8e7wUz7S7Qm": {"alias": "bluntz", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "B8tB2a5uRHLSTg6hSoc8UDJaL6LeJYCaEVhfq6DEG5FF": {"alias": "generationalfumble", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "HZrxCXCms81ryxwvYNycwcPmynXmPgcKV4C2FeDJA86e": {"alias": "ericeth", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "BS5KRbtnAkqQyDgEcjr97A2ZeNv6DMTZgq149paGagWC": {"alias": "DuckSoldier", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "48M5Q8gy9TyfPrMjPEqHWm61DsQ2393JwGhnuRXj8rzH": {"alias": "neo", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "ELXarhHXtFydYLJ3cZ3NbEFRsP85TSfkW9YXhQtWnWgc": {"alias": "DeeZe", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "AvDcwxqbVvUYBvSeUR88JxvmgGJuwayNvAdboDv4rTCM": {"alias": "kimjongun", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "8ks6a6bj1Lq78TsdDjA12VkD8nFgJtP5FFiyX7o39WCW": {"alias": "Unicorn", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "BDuKtmc5ssnZhWLQbux7VLnTA6BdBHFs4ThiMfFj9u4P": {"alias": "zaceth", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "EArbLvWf5xo6JZfVpfk7ANUThZEwMNhYcptP7eKg5qgs": {"alias": "Badabeep", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "CGcvCcpRLxYs9ZKd15wbFStg4HBuwstdXiw5Jgwxq27d": {"alias": "Value", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "ARCRywUNjhjNMAQCRxKXna1KH5HMzx1C8kN8EppD9D4U": {"alias": "Albus", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "4v2t4fn7EuZaUtGymVyEqCediePfdoE1v3azozTw16T4": {"alias": "topickcrypto", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "9ad2xvzHWbjui5NyUaRkC8ycsX4ZufJpLegQ8texV9Ao": {"alias": "DueRivalWren", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "6ug9m4JajZrkJ5kB31VQ6m6kQsWBkCs3x2TAvUFqJD19": {"alias": "GiganticCheeseRetard", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "HbqKbT4UXvqWdEyCs54PzjrrKysFBirq9sqx1avrLmwQ": {"alias": "LevelsDennis", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "GM8u1GgckUpsWEtGpdztLEzPyF5fQUBTUrnQsa32WVVc": {"alias": "Frosty", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "AZJ4G1bRA3zT8BkqyyTPppqCUY7j92arwYCjcR3KaMfQ": {"alias": "mk4", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "6fs1GrNW9176CmBTDrtC2su7FQe1XjntUnRwGyNqYowU": {"alias": "lyx", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "2GKpFFehqF2VknP6TNrh5pnbpGhFA9aiNGaLWbwRLcGS": {"alias": "cringelord", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "CckZNRTevhQizNyU4Nv1BXJtEyRo9RqnnSjbcRkHFszf": {"alias": "au2", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "HTpumppos77vJbxekoSEFsi6HejDLknn6tRngFNptDzR": {"alias": "plottwist", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "D5NAtCzYikf67zNVpg6uDc6w27ZRQdRM66iQmpuZFKuU": {"alias": "Miyamoto", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "9nKxuozxn9ozXYx32nU8EhzuqUjijqfjvrQkQe5sqiE4": {"alias": "TreasureGoblin", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "EMSxJrFaB9A2LSp9tbUsjmBNNnkNZ8euDcRvdLEhDxik": {"alias": "Monkey", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "853yxZmJZVnPoWzUsQiGkk9DWgrA8v2uFN8fJvWNo1JN": {"alias": "downhorrendously", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "BoBdB7Zi4Vvjg4zVEgTwasQPhuU3GrVzEFkLhiAzTpy5": {"alias": "ice", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "Gt6MM3JA2HeJRU7x6tp5yUBVZrTFeMscs6S5gg8BpRqd": {"alias": "Ethermonk", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "2FZWheWt5r9zDSa7y8aB5A5iHT65NKMXVWrvnPYWN13h": {"alias": "dougfunnie", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "7VwwdxZXECjp8Dxex5DTvFcXAtsjQMFddnyHVbMGGAtP": {"alias": "auspicious", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "2zU6ASCZJSDHRvUvJvJg4aP454ioq9CRz9DdcBsaJbZp": {"alias": "Conviction", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "GsJhJ9zo19vWbQMBaMGPhMfkNzVAB16bCUWM7D8bD191": {"alias": "picadura", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
    "51hE7oK7rRsG7FZESY4pXuZfgP4mNN31L9LqbzSYGm4V": {"alias": "TubifexPupa", "chain": "solana", "avg_trade_size_usd": 1000.0, "win_rate": 0.5},
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

# History of tokens launched by a developer wallet (used to warn users what dev previously launched)
DEV_LAUNCH_HISTORY: dict[str, list[dict[str, Any]]] = defaultdict(list)
DEV_LAUNCH_HISTORY_MAX_PER_DEV = 10


def record_dev_launch_event(
    dev_wallet: str,
    token_address: str,
    ticker: str,
    chain: str,
    platform: str,
    ts: float,
    peak_market_cap: float = 0.0,
) -> list[dict[str, Any]]:
    """Records a token launch under the dev wallet and returns prior launched tokens."""
    if not dev_wallet or dev_wallet.lower() in KNOWN_INFRASTRUCTURE_ADDRESSES or dev_wallet.lower().startswith("stonkboard") or dev_wallet.lower().startswith("discovered:"):
        return []
    history = DEV_LAUNCH_HISTORY[dev_wallet]
    prior = [h for h in history if h.get("token_address") != token_address]
    existing = next((h for h in history if h.get("token_address") == token_address), None)
    if existing is None:
        history.append({
            "token_address": token_address,
            "ticker": ticker or "UNKNOWN",
            "chain": chain,
            "platform": platform,
            "timestamp": ts,
            "peak_market_cap": float(peak_market_cap or 0.0),
        })
        if len(history) > DEV_LAUNCH_HISTORY_MAX_PER_DEV:
            del history[0]
    elif peak_market_cap > float(existing.get("peak_market_cap") or 0.0):
        existing["peak_market_cap"] = float(peak_market_cap)
    return prior


def record_token_peak_mcap(token_address: str, peak_mcap: float, dev_wallet: Optional[str] = None) -> None:
    """Updates peak market cap in DEV_LAUNCH_HISTORY for matching token_address."""
    if not token_address or peak_mcap <= 0:
        return
    wallets = [dev_wallet] if dev_wallet else list(DEV_LAUNCH_HISTORY.keys())
    for w in wallets:
        if not w:
            continue
        for item in DEV_LAUNCH_HISTORY.get(w, []):
            if item.get("token_address") == token_address:
                if peak_mcap > float(item.get("peak_market_cap") or 0.0):
                    item["peak_market_cap"] = float(peak_mcap)
                break


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

# --- Jev (TypeSafe) runtime state -------------------------------------------
# Which tokens have already been sent to Jev (once-per-candidate cache). Value
# is the timestamp of the evaluation; presence alone means "don't spend another
# call on this token." Bounded so long uptime can't grow it unbounded.
JEV_EVALUATED_TOKENS: dict[str, float] = {}
JEV_EVALUATED_MAX = 4000

# Stage-1 Early Momentum screening cache (gated, 2-question screening before full eval)
JEV_SCREENED_TOKENS: dict[str, float] = {}
JEV_SCREENED_MAX = 4000

# Semantic content-hash cache for invariant qualitative questions (dedup across identical memes)
JEV_SEMANTIC_CACHE: dict[str, dict[str, Any]] = {}
JEV_SEMANTIC_CACHE_MAX = 2000
JEV_SEMANTIC_CACHE_TTL = 86400.0  # 24 hours


def _compute_semantic_hash(ticker: Optional[str], name: Optional[str], description: Optional[str]) -> str:
    t = (ticker or "").upper().strip()
    n = (name or "").lower().strip()
    d = (description or "").lower().strip()[:300]
    raw = f"{t}|{n}|{d}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

# Live spend/usage accounting so the budget is never a black box. Persisted so
# the lifetime cap and today's counts survive restarts. day_key rolls over.
JEV_USAGE: dict[str, Any] = {
    "calls_total": 0,          # lifetime successful calls (counts against JEV_MAX_CALLS_TOTAL)
    "calls_today": 0,
    "input_tokens_total": 0,
    "output_tokens_total": 0,
    "errors": 0,
    "last_error": None,
    "skipped_not_ready": 0,  # routine "token not ready" skips (not shown as events)
    "day_key": "",             # date these _today counters belong to
    "budget_exhausted": False, # set True once a cap is hit; surfaced in UI
    "disabled_reason": None,    # human-readable why Jev isn't calling (no key / cap hit)
}
# The learning-loop ledger: one record per Jev judgment, with the token's
# eventual outcome filled in later (mooned / rugged / flat) so JEV_CORRELATION
# can show which dimensions actually track winners on YOUR data. Bounded.
JEV_JUDGMENT_LOG: dict[str, dict[str, Any]] = {}
JEV_JUDGMENT_LOG_MAX = 4000
# PvP learning ledger: one record per comparative pick, keyed by narrative, with
# the picked coin + the full cohort + which coin (if any) actually mooned. Lets
# us measure "when Jev picked between same-name coins, was it right?" Bounded.
JEV_PVP_LOG: dict[str, dict[str, Any]] = {}
JEV_PVP_LOG_MAX = 2000
# Reverse index: token_address -> narrative_key(s) it belongs to in a PvP pick,
# so a token's terminal outcome can update the right PvP record(s).
JEV_PVP_TOKEN_INDEX: dict[str, set[str]] = defaultdict(set)
# A rich, display-ready feed of the most recent judgments (full dimensions,
# score contribution, reasons, verdict) so the dashboard can show, in detail,
# exactly what Jev looked at and concluded — not just aggregate counts.
RECENT_JEV_JUDGMENTS: "deque" = deque(maxlen=60)
# A UNIFIED event log of EVERYTHING Jev does — every evaluation, every skip
# (with the exact reason), every veto, every API error, every PvP pick/skip.
# The point: no Jev action is invisible. Broadcast live + shown as a stream.
RECENT_JEV_EVENTS: "deque" = deque(maxlen=200)
# Tokens that have already emitted a budget-block event today (dedup so a
# capped day doesn't flood the log with the same token every poll).
JEV_BUDGET_BLOCKED_SEEN: set[str] = set()

# Per-chain history of coins that ran above BIG_RUNNER_MCAP_USD — the reference
# set Jev compares a new same-name cohort against. Keyed by chain -> list of
# {ticker, narrative_key, platform, peak_mcap, first_seen, recorded_at}. This
# is the "check coins that ran >$500k on this chain" evidence, supplied to Jev
# as state (Jev can't fetch history itself). Populated live + seedable.
BIG_RUNNERS: dict[str, list[dict[str, Any]]] = defaultdict(list)
BIG_RUNNERS_SEEN: set[str] = set()  # token addresses already recorded, dedup

# Cohort-signature cache so a PvP comparative call isn't re-spent every poll —
# only when the same-name cohort materially changes (new member / leader flip).
JEV_PVP_CACHE: dict[str, dict[str, Any]] = {}
JEV_PVP_CACHE_MAX = 500
# Narratives with a PvP call currently in flight — prevents an async race where
# several cohort members rescored at the same instant each fire a duplicate call.
JEV_PVP_INFLIGHT: set[str] = set()

# --- #3 Question-proposal & measurement loop --------------------------------
# Candidate questions being TESTED (not yet part of the trusted scoring set).
# Each is asked alongside the core questions, its answers logged per token, and
# scored by how well it separates winners from losers. A generative LLM can
# auto-propose these (if a key is set); otherwise they're added manually via the
# /api/jev/propose endpoint. Winners can be promoted into the core set.
# Shape per id: {type, instructions, criteria, active, status, proposed_by,
#                proposed_at, answers: {token: value}}
JEV_PROPOSED_QUESTIONS: dict[str, dict[str, Any]] = {}
JEV_PROPOSED_MAX = 12  # cap active proposals so test calls don't bloat token cost

# --- Semantic narrative themes (DeepSeek-tagged, replaces regex clustering) --
# For each gated coin, DeepSeek assigns a canonical theme slug (e.g. "ai-agents",
# "politics", "dog-meme"). Coins are then grouped by THEME across different
# tickers — real semantic narratives, not just identical ticker strings.
NARRATIVE_THEMES: dict[str, dict[str, Any]] = {}   # theme_slug -> {label, coins:[...], first_seen, chains}
TOKEN_THEME: dict[str, str] = {}                    # token_address -> theme_slug
JEV_THEME_TAGGED: dict[str, float] = {}             # dedup: token -> tagged_at
JEV_THEME_TAGGED_MAX = 4000
# Tokens the discovery scanner has already injected (dedup so it doesn't
# re-add the same coin every scan). Bounded.
DISCOVERED_TOKENS: dict[str, float] = {}
DISCOVERED_TOKENS_MAX = 4000
# Free social data from DexScreener token-profiles (twitter/telegram/website
# links + description). Rolling cache keyed by token address, refreshed by a
# periodic worker. Real X sentiment isn't free, but "does it have socials" is.
TOKEN_PROFILES: dict[str, dict[str, Any]] = {}   # addr -> {socials:[types], description, link_count}
TOKEN_PROFILES_MAX = 3000
# Live pump.fun description feed + rolling trending-word aggregation. Each new
# coin description is pushed here; words are counted with a last-seen timestamp
# so a word drops off the trending pill bar after TRENDING_WORD_TTL_SECONDS of
# not reappearing (a decaying, live "what narrative is hot right now" view).
RECENT_DESCRIPTIONS: "deque" = deque(maxlen=80)
TRENDING_WORDS: dict[str, dict[str, Any]] = {}   # word -> {count, last_seen}
TRENDING_WORD_TTL_SECONDS = 180.0                # 3 min without reappearing = dies
TRENDING_STOPWORDS = {
    "the","a","an","to","of","and","for","in","on","is","it","its","this","that","with",
    "coin","token","meme","memecoin","solana","sol","pump","fun","launched","stonkfun",
    "via","by","new","first","official","community","your","you","we","are","be","will",
    "was","has","have","from","at","as","or","not","no","all","just","now","get","let",
    "fees","cameo","usepaid","http","https","com","www","t","co","x",
}
# Generative LLM for the auto-writer (optional). Blank => manual queue only.
JEV_PROPOSER_LLM_KEY = os.getenv("JEV_PROPOSER_LLM_KEY", "") or os.getenv("OPENAI_API_KEY", "") or os.getenv("ANTHROPIC_API_KEY", "")
JEV_PROPOSER_LLM_URL = os.getenv("JEV_PROPOSER_LLM_URL", "https://api.deepseek.com/chat/completions")
JEV_PROPOSER_LLM_MODEL = os.getenv("JEV_PROPOSER_LLM_MODEL", "deepseek-chat")
# How often the auto-proposer runs, and how many new candidates it may add per
# round. Conservative: proposals cost Jev tokens on every subsequent eval.
JEV_PROPOSER_INTERVAL_SECONDS = float(os.getenv("JEV_PROPOSER_INTERVAL_SECONDS", "3600"))
JEV_PROPOSER_MAX_NEW_PER_ROUND = int(os.getenv("JEV_PROPOSER_MAX_NEW_PER_ROUND", "2"))
# A proposal with clearly negative separation after this many labeled outcomes
# is auto-retired (it predicts the wrong way or nothing).
JEV_PROPOSAL_MIN_LABELS_TO_JUDGE = int(os.getenv("JEV_PROPOSAL_MIN_LABELS_TO_JUDGE", "8"))

# Bounded, dashboard-facing token feed (survives a browser refresh via the
# /ws/dashboard snapshot). Insertion order is creation order; updates mutate
# in place without moving position. Capped so a 24/7 deployment doesn't leak
# memory indefinitely.
TOKEN_FEED: dict[str, dict[str, Any]] = {}
TOKEN_FEED_MAX = 1500
SNAPSHOT_TOKEN_LIMIT = 80  # cap on how many tokens a reconnect snapshot sends at once
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

    # Never overwrite an existing valid image_url/name/description with None or empty
    for k in ("image_url", "name", "description"):
        if k in fields and not fields[k] and entry.get(k):
            fields[k] = entry[k]

    entry.update(fields)
    inferred_plat = infer_launchpad_platform(entry.get("platform"), token_address)
    if inferred_plat:
        entry["platform"] = inferred_plat
    if is_stonkboard_token(token_address, entry.get("platform")):
        entry["platform"] = "stonkfun"
        entry["is_stonkboard"] = True
        if not entry.get("image_url"):
            entry["image_url"] = f"https://thestonkboard.com/api/logos/{token_address}"
    return entry


def format_mcap_compact(mcap: float) -> str:
    if not mcap or mcap <= 0:
        return ""
    if mcap >= 1_000_000:
        val = mcap / 1_000_000
        return f"${val:.2f}M".replace(".00M", "M")
    elif mcap >= 1_000:
        val = mcap / 1_000
        return f"${val:.1f}k".replace(".0k", "k")
    else:
        return f"${mcap:.0f}"


def get_known_token_peak_mcap(token_address: str) -> float:
    """Finds highest known peak market cap for a token address across in-memory tracking stores."""
    if not token_address:
        return 0.0
    peaks = []
    if token_address in MOCK_PORTFOLIO:
        peaks.append(float(MOCK_PORTFOLIO[token_address].get("peak_market_cap") or 0.0))
        peaks.append(float(MOCK_PORTFOLIO[token_address].get("last_known_market_cap") or 0.0))
    if token_address in TOKEN_WATCHLIST:
        peaks.append(float(TOKEN_WATCHLIST[token_address].get("peak_market_cap") or 0.0))
        peaks.append(float(TOKEN_WATCHLIST[token_address].get("market_cap") or 0.0))
    if token_address in TOKEN_FEED:
        peaks.append(float(TOKEN_FEED[token_address].get("peak_market_cap") or 0.0))
        peaks.append(float(TOKEN_FEED[token_address].get("market_cap") or 0.0))
    for chain_runners in BIG_RUNNERS.values():
        for r in chain_runners:
            if r.get("token_address") == token_address and r.get("peak_mcap"):
                peaks.append(float(r["peak_mcap"]))
    return max(peaks, default=0.0)


import re as _re_desc


def record_description(ticker: Optional[str], name: Optional[str], description: Optional[str],
                      platform: Optional[str], token_address: str) -> bool:
    """Push a coin's description into the live feed and fold its words into the
    rolling trending-word counter. Deduped per token. Returns True if recorded."""
    if not description or len(description.strip()) < 3:
        return False
    # dedup: don't re-record the same token's description
    if any(d.get("token_address") == token_address for d in RECENT_DESCRIPTIONS):
        return False
    now = time.time()
    RECENT_DESCRIPTIONS.append({
        "token_address": token_address, "ticker": ticker, "name": name,
        "description": description.strip()[:200], "platform": platform, "ts": now,
    })
    # tokenize name + description into words for trending aggregation
    text = f"{name or ''} {description}".lower()
    words = _re_desc.findall(r"[a-z0-9$#]{3,20}", text)
    seen_this = set()
    for w in words:
        w = w.strip("$#")
        if len(w) < 3 or w in TRENDING_STOPWORDS or w.isdigit():
            continue
        if w in seen_this:
            continue  # count each word once per coin
        seen_this.add(w)
        rec = TRENDING_WORDS.get(w)
        if rec:
            rec["count"] += 1
            rec["last_seen"] = now
        else:
            TRENDING_WORDS[w] = {"count": 1, "last_seen": now}
    return True


def build_trending_words(limit: int = 12) -> list[dict[str, Any]]:
    """Current trending words — those seen within the TTL window, by count.
    Words not reappearing within TRENDING_WORD_TTL_SECONDS drop off (die)."""
    now = time.time()
    # prune dead words
    for w in list(TRENDING_WORDS.keys()):
        if now - TRENDING_WORDS[w]["last_seen"] > TRENDING_WORD_TTL_SECONDS:
            del TRENDING_WORDS[w]
    live = [
        {"word": w, "count": r["count"], "age_s": round(now - r["last_seen"])}
        for w, r in TRENDING_WORDS.items() if r["count"] >= 2  # need >=2 coins to "trend"
    ]
    live.sort(key=lambda x: (-x["count"], x["age_s"]))
    return live[:limit]


def build_description_feed(limit: int = 40) -> dict[str, Any]:
    """Recent descriptions (newest first) + current trending words for the UI."""
    return {
        "descriptions": list(RECENT_DESCRIPTIONS)[-limit:][::-1],
        "trending_words": build_trending_words(),
    }

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
RECENT_SMART_MONEY: deque = deque(maxlen=100)


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
            err_msg = str(exc)
            logger.warning(f"[{name}] error: {exc!r}")
            # If the RPC quota is exhausted or access is forbidden (HTTP 403),
            # back off for 30 minutes instead of rapidly spamming logs every 60s.
            if "403" in err_msg or "quota" in err_msg.lower() or "forbidden" in err_msg.lower():
                logger.warning(f"[{name}] RPC quota exhausted or forbidden (403). Pausing connection attempts for 30 minutes.")
                await asyncio.sleep(1800)
                continue
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

    def __init__(self, url: str, fallback_url: Optional[str] = None):
        self.url = url
        self.fallback_url = fallback_url
        self.active_url = url
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._next_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._notifications: asyncio.Queue = asyncio.Queue()
        self._reader_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        urls_to_try = [self.url] if self.url else []
        if self.fallback_url and self.fallback_url not in urls_to_try:
            urls_to_try.append(self.fallback_url)
        if not urls_to_try:
            raise ValueError("No WS RPC URL provided")

        last_exc: Optional[Exception] = None
        for i, target_url in enumerate(urls_to_try):
            try:
                self.ws = await websockets.connect(
                    target_url, ping_interval=20, ping_timeout=20, max_size=2 ** 23
                )
                self.active_url = target_url
                self._reader_task = asyncio.create_task(self._reader())
                if i > 0:
                    logger.info(f"[JsonRpcWsClient] Connected to fallback WS RPC: {target_url}")
                return
            except Exception as exc:
                last_exc = exc
                if i == 0 and len(urls_to_try) > 1:
                    logger.warning(f"[JsonRpcWsClient] Primary WS RPC failed: {exc!r}. Retrying on fallback: {urls_to_try[1]}")
        if last_exc:
            raise last_exc

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
        out = result if isinstance(result, dict) else {}
        # expose the subscription id so callers can map a notification back to
        # which subscription (e.g. which watched wallet) produced it.
        if isinstance(out, dict) and "subscription" in params:
            out = dict(out)
            out["_subscription"] = params.get("subscription")
        return out

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

# --- Birdeye rate-limited enrichment (Solana) -------------------------------
_BIRDEYE_LOCK = asyncio.Lock()
_BIRDEYE_LAST_CALL = 0.0


async def _birdeye_get(path: str) -> Optional[dict[str, Any]]:
    """One throttled Birdeye GET. Serializes calls through a lock + global
    min-interval so the free tier's ~1 req/s isn't exceeded. Retries 429 with
    backoff. Returns the JSON 'data' object, or None on any failure (fail-safe)."""
    if not BIRDEYE_ENABLED:
        return None
    global _BIRDEYE_LAST_CALL
    url = f"{BIRDEYE_API_BASE.rstrip('/')}/{path}"
    headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": "solana", "Accept-Encoding": "gzip, deflate"}
    backoff = 1.5
    for attempt in range(1, BIRDEYE_MAX_RETRIES + 1):
        try:
            async with _BIRDEYE_LOCK:
                # global throttle: space calls out
                wait = BIRDEYE_MIN_INTERVAL_SECONDS - (time.time() - _BIRDEYE_LAST_CALL)
                if wait > 0:
                    await asyncio.sleep(wait)
                timeout = aiohttp.ClientTimeout(total=BIRDEYE_TIMEOUT)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, headers=headers) as resp:
                        _BIRDEYE_LAST_CALL = time.time()
                        if resp.status == 200:
                            body = await resp.json()
                            return body.get("data") if isinstance(body, dict) else None
                        if resp.status == 429 and attempt < BIRDEYE_MAX_RETRIES:
                            pass  # fall through to backoff below (outside lock)
                        else:
                            return None
            await asyncio.sleep(backoff)
            backoff *= 2
        except Exception as exc:
            logger.debug(f"[birdeye] {path} failed: {exc!r}")
            return None
    return None


async def fetch_birdeye_overview(token_address: str) -> dict[str, Any]:
    """Price / market cap / liquidity from Birdeye — fallback when DexScreener
    has no data yet. Returns normalized dict (same keys as DexScreener) or {}."""
    data = await _birdeye_get(f"defi/token_overview?address={token_address}")
    if not data:
        return {}
    return {
        "market_cap": float(data.get("marketCap") or data.get("mc") or 0.0),
        "price_usd": float(data.get("price") or 0.0),
        "liquidity_usd": float(data.get("liquidity") or 0.0),
        "volume_24h": float((data.get("v24hUSD") or data.get("v24h") or 0.0) or 0.0),
        "symbol": (data.get("symbol") or "").strip(),
        "name": (data.get("name") or "").strip(),
        "_source": "birdeye",
    }


async def fetch_birdeye_holders(token_address: str) -> dict[str, Any]:
    """Holder count + top-holder concentration from Birdeye — fills the gap
    DexScreener doesn't cover and the RPC holder scan often can't. Returns
    {holder_count, top_holder_pct, top10_holder_pct} or {}."""
    data = await _birdeye_get(f"defi/v3/token/holder?address={token_address}&offset=0&limit=10")
    if not data:
        return {}
    out: dict[str, Any] = {}
    if data.get("holder") is not None:
        out["holder_count"] = int(data.get("holder") or 0)
    if data.get("top10_hold_percent") is not None:
        out["top10_holder_pct"] = round(float(data["top10_hold_percent"]), 2)
    items = data.get("items") or []
    if items:
        # top holder % ≈ largest holder's share; Birdeye gives raw amounts, and
        # top10% — approximate the single top holder from the first item vs total
        # of the returned items is unreliable, so only set top10 (authoritative).
        try:
            top1 = float(items[0].get("amount") or 0)
            # amount is raw; use it only relative to nothing reliable → skip top_holder_pct
        except (ValueError, TypeError, IndexError):
            pass
    return out


# --- Discovery sources: find already-trading Solana coins we missed at launch --
async def refresh_token_profiles() -> int:
    """Pull DexScreener token-profiles (free, no key) — the 30 newest coins that
    submitted a profile, with their social links (twitter/telegram/website) +
    description. Cache by address so Jev can be told whether a coin has real
    social presence. Real X sentiment isn't free, but this presence signal is.
    Returns count cached. Fail-safe."""
    try:
        url = f"{DEXSCREENER_API_BASE}/token-profiles/latest/v1"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return 0
                data = await resp.json()
    except Exception as exc:
        logger.debug(f"[profiles] fetch failed: {exc!r}")
        return 0
    n = 0
    for item in (data if isinstance(data, list) else []):
        addr = item.get("tokenAddress")
        if not addr:
            continue
        links = item.get("links") or []
        socials = sorted({(l.get("type") or "").lower() for l in links if l.get("type")})
        TOKEN_PROFILES[addr] = {
            "socials": socials,
            "link_count": len(links),
            "description": (item.get("description") or "").strip()[:400],
            "cached_at": time.time(),
        }
        n += 1
    # bound the cache
    while len(TOKEN_PROFILES) > TOKEN_PROFILES_MAX:
        TOKEN_PROFILES.pop(next(iter(TOKEN_PROFILES)), None)
    return n


async def token_profiles_worker() -> None:
    """Refresh the token-profiles social cache periodically (free endpoint)."""
    await asyncio.sleep(15)
    while True:
        try:
            await refresh_token_profiles()
        except Exception as exc:
            logger.warning(f"[profiles] worker error: {exc!r}")
        await asyncio.sleep(120)  # every 2 min; endpoint is a rolling newest-30


async def fetch_dexscreener_boosted_solana() -> list[str]:
    """DexScreener token-boosts (free, no key): actively-promoted tokens. Returns
    Solana token addresses. Fail-safe."""
    out: list[str] = []
    for path in ("token-boosts/top/v1", "token-boosts/latest/v1"):
        try:
            url = f"{DEXSCREENER_API_BASE}/{path}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
            for item in (data if isinstance(data, list) else []):
                if item.get("chainId") == "solana" and item.get("tokenAddress"):
                    out.append(item["tokenAddress"])
        except Exception as exc:
            logger.debug(f"[discovery] dexscreener boosts {path} failed: {exc!r}")
    return list(dict.fromkeys(out))  # dedup, keep order


async def fetch_birdeye_top_volume_solana(limit: int = 30) -> list[str]:
    """Birdeye v3 token list sorted by 24h volume — 'what's moving on Solana'.
    Rate-limited via the shared Birdeye throttle. Fail-safe."""
    if not BIRDEYE_ENABLED:
        return []
    data = await _birdeye_get(f"defi/v3/token/list?sort_by=volume_24h_usd&sort_type=desc&offset=0&limit={limit}")
    if not data:
        return []
    items = data.get("items") or []
    out = []
    for it in items:
        addr = it.get("address")
        # skip SOL/stables/wrapped — only want tradeable memecoins
        if addr and not addr.startswith("So1111") and it.get("symbol") not in ("SOL", "USDC", "USDT"):
            out.append(addr)
    return out


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
    if token_address in STONKFUN_QUOTE_MINTS:
        return {}
    url = f"{DEXSCREENER_API_BASE}/latest/dex/tokens/{token_address}"
    # DexScreener rate-limits high-frequency polling and intermittently returns
    # 429 or an empty pairs list even when the token HAS data — which used to get
    # stored as mcap 0 and drop the coin out of the gate forever. Retry a couple
    # of times with backoff so a transient throttle doesn't zero a real coin.
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(0.6 * (attempt + 1))
                        continue
                    if resp.status != 200:
                        return {}
                    payload = await resp.json()
                    pairs = payload.get("pairs") or []
                    if not pairs:
                        if attempt < 2:
                            await asyncio.sleep(0.6 * (attempt + 1))
                            continue
                        return {}
                    break
        except Exception:
            if attempt < 2:
                await asyncio.sleep(0.6 * (attempt + 1))
                continue
            return {}
    else:
        return {}
    try:
        # Prefer pairs where token_address is the baseToken (we strictly want the first token of the pair!)
        matching_base_pairs = [p for p in pairs if ((p.get("baseToken") or {}).get("address") or "").lower() == token_address.lower()]
        if matching_base_pairs:
            matching_base_pairs.sort(key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0), reverse=True)
            best = matching_base_pairs[0]
        else:
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
        raw_socials = info.get("socials") or []
        raw_websites = info.get("websites") or []
        boosts_data = best.get("boosts") or {}
        active_boosts = int(boosts_data.get("active") or 0)

        twitter_url = next((s.get("url") for s in raw_socials if (s.get("type") or "").lower() == "twitter"), None)
        telegram_url = next((s.get("url") for s in raw_socials if (s.get("type") or "").lower() == "telegram"), None)
        website_url = (raw_websites[0].get("url") if raw_websites else None) or next(
            (s.get("url") for s in raw_socials if (s.get("type") or "").lower() in ("website", "web")), None
        )

        social_dict = {
            "twitter": twitter_url,
            "telegram": telegram_url,
            "website": website_url,
            "has_twitter": bool(twitter_url),
            "has_telegram": bool(telegram_url),
            "has_website": bool(website_url),
            "social_count": sum(1 for x in [twitter_url, telegram_url, website_url] if x),
            "active_boosts": active_boosts,
        }

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
            "socials": social_dict,
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


async def ensure_prior_launches_peak_mcap(entry: dict[str, Any]) -> None:
    """Ensures prior launched tokens have their peak market caps populated before alerts are sent."""
    dev_wallet = entry.get("dev_wallet", "")
    if not dev_wallet or dev_wallet.lower().startswith("stonkboard") or dev_wallet.lower().startswith("discovered:"):
        return
    launches = entry.get("dev_prior_launches") or DEV_LAUNCH_HISTORY.get(dev_wallet, [])
    current_addr = entry.get("token_address")
    for h in launches:
        t_addr = h.get("token_address")
        if not t_addr or t_addr == current_addr:
            continue
        peak = float(h.get("peak_market_cap") or 0.0)
        if peak <= 0:
            peak = get_known_token_peak_mcap(t_addr)
            if peak > 0:
                h["peak_market_cap"] = peak
                record_token_peak_mcap(t_addr, peak, dev_wallet)
            else:
                try:
                    mcap = await fetch_token_market_cap_usd(t_addr)
                    if mcap > 0:
                        h["peak_market_cap"] = mcap
                        record_token_peak_mcap(t_addr, mcap, dev_wallet)
                except Exception:
                    pass



CHAIN_EXPLORER_URLS = {
    "solana": "https://solscan.io/token/{addr}",
    "bnb": "https://bscscan.com/token/{addr}",
    "robinhood": "https://robinhoodchain.blockscout.com/token/{addr}",
}
# DexScreener chain slugs — "solana"/"bsc" are certain; "robinhood" is a
# reasonable guess for a brand-new chain and may 404 if not indexed yet, but
# that's a harmless dead link, not a functional risk.
DEXSCREENER_CHAIN_SLUGS = {"solana": "solana", "bnb": "bsc", "robinhood": "robinhood"}


def build_token_links(chain: str, token_address: str, platform: Optional[str] = None) -> dict[str, str]:
    links = {}
    explorer_tpl = CHAIN_EXPLORER_URLS.get(chain)
    if explorer_tpl:
        links["explorer"] = explorer_tpl.format(addr=token_address)
    slug = DEXSCREENER_CHAIN_SLUGS.get(chain)
    if slug:
        links["dexscreener"] = f"https://dexscreener.com/{slug}/{token_address}"
    chain_lower = (chain or "solana").lower()
    fomo_chain = chain_lower if chain_lower in ("solana", "bnb", "base", "robinhood") else ("bnb" if chain_lower in ("bsc", "binance") else "solana")
    links["fomo"] = f"https://fomo.family/tokens/{fomo_chain}/{token_address}"
    addr_lower = (token_address or "").lower()
    if chain == "solana":
        if is_stonkboard_token(token_address, platform):
            links["stonkboard"] = f"https://thestonkboard.com/coin/{token_address}"
        else:
            links["pumpfun"] = f"https://pump.fun/{token_address}"
        links["birdeye"] = f"https://birdeye.so/token/{token_address}?chain=solana"
        links["rugcheck"] = f"https://rugcheck.xyz/tokens/{token_address}"
    if not is_stonkboard_token(token_address, platform) and (addr_lower.endswith("pump") or (platform and "pump" in platform.lower())):
        links["pumpfun"] = f"https://pump.fun/{token_address}"
    if addr_lower.endswith("4444") or (platform and ("four" in platform.lower() or "4meme" in platform.lower())):
        links["fourmeme"] = f"https://four.meme/token/{token_address}"
    if addr_lower.endswith("7777") or (platform and "flap" in platform.lower()):
        links["flap"] = f"https://flap.sh/{token_address}"
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

# Established/stock coins keep leaking in as if freshly launched (HYPE, MSTRx,
# ZEC, xStocks like AAPLx/TSLAx). These are NOT fair-launches — block them.
# 1) A cross-platform mcap tripwire: a genuinely brand-new fair launch cannot
#    already be sitting at this mcap the first time we see it, on ANY platform
#    (the pump.fun-only guard above missed StonkFun/Ember/EVM arrivals).
IMPLAUSIBLE_NEW_LAUNCH_MCAP_USD = float(os.getenv("IMPLAUSIBLE_NEW_LAUNCH_MCAP_USD", "1000000"))
# 2) Known established token symbols to hard-block (comma-separated, case-insensitive).
_default_established = "HYPE,ZEC,BTC,ETH,SOL,BNB,XRP,DOGE,SHIB,PEPE,WIF,BONK,USDC,USDT,LINK,ADA,AVAX,TRX,TON,SUI,APT,ARB,OP,MSTR,JNJ,AAPL,TSLA,NVDA,MSFT,AMZN,META,GOOG,GOOGL,COIN,AMD,NFLX,SPY,QQQ,GME,AMC,PLTR,BABA,DIS,PYPL,SQ,HOOD,KO,PEP,MCD,NKE,WMT,JPM,BAC,V,MA,PFE,XOM,CVX,RDDT,SNAP,RBLX,RIVN,SOFI,ROKU,ABNB,UBER,LYFT,INTC,ORCL,CRM,ADBE,AVGO,QCOM,MU,BA,GE,F,GM,T,VZ,WFC,GS,MS,C,SBUX,LULU,MRNA,SHOP,SQ,DASH,RIOT,MARA,SMCI,ARM,DELL,IBM,CSCO"
ESTABLISHED_SYMBOL_BLOCKLIST = {
    s.strip().upper() for s in os.getenv("ESTABLISHED_SYMBOL_BLOCKLIST", _default_established).split(",") if s.strip()
}


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
    if not (SOLANA_WS_RPC_URL or SOLANA_FALLBACK_RPC_URL) or not bonding_curve_key:
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


async def fetch_pumpfun_image(mint_address: str) -> Optional[dict[str, Any]]:
    """pump.fun's own coin API — undocumented but confirmed real. Returns the
    coin's image_uri, name, and description (the description is the coin's own
    pitch — exactly the narrative context Jev needs to judge it). Never used for
    scoring-relevant numeric fields (mcap/bonding), only descriptive metadata."""
    url = f"{PUMPFUN_COIN_API_BASE}/coins/{mint_address}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=10), headers={"User-Agent": "Mozilla/5.0"}
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                return {
                    "image_uri": data.get("image_uri") or None,
                    "name": (data.get("name") or "").strip() or None,
                    "description": (data.get("description") or "").strip() or None,
                    "twitter": data.get("twitter") or None,
                    "telegram": data.get("telegram") or None,
                    "website": data.get("website") or None,
                }
    except Exception as exc:
        logger.debug(f"fetch_pumpfun_image({mint_address}) failed: {exc!r}")
        return None


PUMPFUN_IMAGE_MAX_ATTEMPTS = 5  # ~5 poll cycles — pump.fun's own backend can lag a few seconds behind a mint actually existing on-chain


async def _maybe_fetch_pumpfun_image(token_address: str, info: dict[str, Any]) -> None:
    if info.get("pumpfun_image_checked"):
        return
    fe = TOKEN_FEED.get(token_address) or {}
    if info.get("platform") != "pump.fun" or (fe.get("image_url") and fe.get("description")):
        info["pumpfun_image_checked"] = True
        return
    meta = await fetch_pumpfun_image(token_address)
    if meta and (meta.get("image_uri") or meta.get("name") or meta.get("description")):
        info["pumpfun_image_checked"] = True
        upd = {}
        if meta.get("image_uri"):
            upd["image_url"] = meta["image_uri"]
            info["image_url"] = meta["image_uri"]
        if meta.get("name") and not fe.get("name"):
            upd["name"] = meta["name"]
            info["name"] = meta["name"]
        if meta.get("description") and not fe.get("description"):
            # the coin's own pitch — the narrative context Jev was missing
            upd["description"] = meta["description"][:1000]
            info["description"] = meta["description"][:1000]
        if meta.get("twitter") or meta.get("telegram") or meta.get("website"):
            soc = dict(info.get("socials") or {})
            if meta.get("twitter"):
                soc["twitter"] = meta["twitter"]
                soc["has_twitter"] = True
            if meta.get("telegram"):
                soc["telegram"] = meta["telegram"]
                soc["has_telegram"] = True
            if meta.get("website"):
                soc["website"] = meta["website"]
                soc["has_website"] = True
            soc["social_count"] = sum(1 for x in [soc.get("twitter"), soc.get("telegram"), soc.get("website")] if x)
            upd["socials"] = soc
            info["socials"] = soc
        if upd:
            token_feed_upsert(token_address, **upd)
            await broadcast_token_card(TOKEN_FEED[token_address])
        # feed the live description stream + trending-word aggregator
        if meta.get("description"):
            fe2 = TOKEN_FEED.get(token_address, {})
            if record_description(fe2.get("ticker"), meta.get("name"),
                                  meta.get("description"), "pump.fun", token_address):
                await broadcast_json({"kind": "description_feed", "payload": build_description_feed()})
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
SOLANA_MINT_SECURITY_CACHE: dict[str, dict[str, Any]] = {}
SOLANA_TOKEN_SUPPLY_CACHE: dict[str, float] = {}


async def fetch_solana_holder_stats(mint_address: str, force_full_scan: bool = False) -> dict[str, Any]:
    """Holder count and concentration using lightweight indexed Solana RPC calls.
    Uses getTokenLargestAccounts + getMultipleAccounts (2 cheap indexed credits)
    instead of heavy getProgramAccounts table scans to aggressively conserve Helius quota."""
    if not (SOLANA_WS_RPC_URL or SOLANA_FALLBACK_RPC_URL):
        return {}

    sec = SOLANA_MINT_SECURITY_CACHE.get(mint_address)
    try:
        async with aiohttp.ClientSession() as session:
            if not sec:
                mint_result = await _solana_rpc_post(session, "getAccountInfo", [mint_address, {"encoding": "jsonParsed"}])
                if mint_result is None:
                    return {}
                mint_value = mint_result.get("value") or {}
                owner_program = mint_value.get("owner")
                parsed_info = ((mint_value.get("data") or {}).get("parsed") or {}).get("info") or {}
                mint_authority_active = parsed_info.get("mintAuthority") is not None
                freeze_authority_active = parsed_info.get("freezeAuthority") is not None

                # Inspect Token-2022 extensions for transfer hooks, default frozen accounts, or permanent delegates
                transfer_hook_active = False
                default_account_frozen = False
                permanent_delegate_active = False
                extensions = parsed_info.get("extensions") or []
                if isinstance(extensions, list):
                    for ext in extensions:
                        if not isinstance(ext, dict):
                            continue
                        ename = ext.get("extension")
                        estate = ext.get("state") or {}
                        if ename == "transferHook":
                            prog = estate.get("programId")
                            if prog and prog != "11111111111111111111111111111111":
                                transfer_hook_active = True
                        elif ename == "defaultAccountState":
                            if estate.get("accountState") == "frozen":
                                default_account_frozen = True
                        elif ename == "permanentDelegate":
                            if estate.get("delegate"):
                                permanent_delegate_active = True

                is_honeypot = bool(freeze_authority_active or transfer_hook_active or default_account_frozen or permanent_delegate_active)
                sell_whitelist = bool(transfer_hook_active or default_account_frozen)
                honeypot_reason = ""
                if transfer_hook_active:
                    honeypot_reason = "Token-2022 transfer hook active (sells whitelisted)"
                elif default_account_frozen:
                    honeypot_reason = "Token-2022 default account state is frozen (whitelisted sells only)"
                elif freeze_authority_active:
                    honeypot_reason = "Freeze authority active (honeypot risk)"
                elif permanent_delegate_active:
                    honeypot_reason = "Permanent delegate active (honeypot risk)"

                sec = {
                    "owner_program": owner_program,
                    "mint_authority_active": mint_authority_active,
                    "freeze_authority_active": freeze_authority_active,
                    "transfer_hook_active": transfer_hook_active,
                    "default_account_frozen": default_account_frozen,
                    "permanent_delegate_active": permanent_delegate_active,
                    "is_honeypot": is_honeypot,
                    "sell_whitelist": sell_whitelist,
                    "honeypot_reason": honeypot_reason,
                }
                SOLANA_MINT_SECURITY_CACHE[mint_address] = sec

            if sec.get("owner_program") not in (SPL_TOKEN_PROGRAM_ID, SPL_TOKEN_2022_PROGRAM_ID):
                return {}

            if sec.get("is_honeypot") or sec.get("sell_whitelist") or sec.get("mint_authority_active") or sec.get("freeze_authority_active"):
                return {
                    "holder_count": 0, "top_holder_pct": 100.0, "top10_holder_pct": 100.0,
                    "top_holder_address": None, "top_holder_balance": 0.0,
                    **sec,
                }

            # Cache token total supply
            supply_val = SOLANA_TOKEN_SUPPLY_CACHE.get(mint_address)
            if supply_val is None:
                supply_res = await _solana_rpc_post(session, "getTokenSupply", [mint_address], timeout=10.0)
                supply_val = float(((supply_res or {}).get("value") or {}).get("uiAmount") or 1_000_000_000.0)
                if supply_val > 0:
                    SOLANA_TOKEN_SUPPLY_CACHE[mint_address] = supply_val

            # Indexed top holder lookup (1 cheap credit)
            largest_res = await _solana_rpc_post(session, "getTokenLargestAccounts", [mint_address], timeout=10.0)
            largest_accounts = (largest_res or {}).get("value") or []
            balances: dict[str, float] = {}

            if largest_accounts:
                token_acc_addrs = [item["address"] for item in largest_accounts[:20] if item.get("address")]
                if token_acc_addrs:
                    multi_res = await _solana_rpc_post(
                        session, "getMultipleAccounts", [token_acc_addrs, {"encoding": "jsonParsed"}], timeout=10.0
                    )
                    vals = (multi_res or {}).get("value") or []
                    for item, acc_info in zip(largest_accounts[:20], vals):
                        owner = (((acc_info or {}).get("data") or {}).get("parsed") or {}).get("info", {}).get("owner")
                        addr = owner or item.get("address")
                        ui_amt = float(item.get("uiAmount") or 0.0)
                        if addr and ui_amt > 0:
                            balances[addr] = balances.get(addr, 0.0) + ui_amt

            if not balances and force_full_scan:
                # Heavy scan fallback only when requested
                filters: list[dict[str, Any]] = [{"memcmp": {"offset": 0, "bytes": mint_address}}]
                if sec.get("owner_program") == SPL_TOKEN_PROGRAM_ID:
                    filters.insert(0, {"dataSize": SPL_TOKEN_ACCOUNT_SIZE})
                accounts = await _solana_rpc_post(
                    session, "getProgramAccounts",
                    [sec.get("owner_program"), {"encoding": "jsonParsed", "filters": filters}],
                    timeout=20.0,
                )
                if accounts:
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

            SOLANA_LAST_HOLDER_BALANCES[mint_address] = balances

            if not balances:
                return {
                    "holder_count": 0, "top_holder_pct": 0.0, "top10_holder_pct": 0.0,
                    "top_holder_address": None, "top_holder_balance": 0.0,
                    **sec,
                }

            sorted_items = sorted(balances.items(), key=lambda kv: kv[1], reverse=True)
            sorted_amounts = [v for _, v in sorted_items]
            top_holder_pct = (sorted_amounts[0] / supply_val * 100.0) if supply_val > 0 else 0.0
            top10_holder_pct = (sum(sorted_amounts[:10]) / supply_val * 100.0) if supply_val > 0 else 0.0
            return {
                "holder_count": len(balances),
                "top_holder_pct": top_holder_pct,
                "top10_holder_pct": top10_holder_pct,
                "top_holder_address": sorted_items[0][0],
                "top_holder_balance": sorted_items[0][1],
                **sec,
            }
    except Exception as exc:
        logger.debug(f"fetch_solana_holder_stats({mint_address}) failed: {exc!r}")
        return {}


TOKEN_SECURITY_CACHE: dict[str, dict[str, Any]] = {}
TOKEN_SECURITY_CACHE_MAX = 5000

async def check_token_honeypot_and_whitelist(chain: str, token_address: str, entry: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Inspects on-chain state and GoPlus Security APIs to detect whether a token has
    sell whitelisting, non-transferable rules, transfer hooks, freeze authority,
    or honeypot behavior. Deemed as honeypot if any sell restriction or whitelist is found."""
    if not token_address:
        return {"safe": False, "is_honeypot": False, "sell_whitelist": False, "reason": "", "summary": "Unknown CA", "ts": 0}

    now = time.time()
    cached = TOKEN_SECURITY_CACHE.get(token_address)
    if cached and (now - cached.get("ts", 0)) < 300:
        if entry is not None:
            entry["goplus"] = cached
            token_feed_upsert(token_address, goplus=cached)
        return cached

    # 1. Trading chart honeypot check (fast local heuristic)
    buys = (entry.get("buys_24h") if entry else 0) or 0
    sells = (entry.get("sells_24h") if entry else 0) or 0
    if (buys >= 4 and sells == 0) or (buys >= 15 and sells <= 1):
        res = {
            "safe": False,
            "is_honeypot": True,
            "sell_whitelist": True,
            "mintable": False,
            "freezable": False,
            "transfer_hook": False,
            "buy_tax": 0.0,
            "sell_tax": 0.0,
            "holder_count": None,
            "reason": f"Chart honeypot: {buys} buys vs {sells} sells (sells blocked/whitelisted)",
            "summary": "🚨 Honeypot Chart (Sells Blocked)",
            "ts": now,
        }
        TOKEN_SECURITY_CACHE[token_address] = res
        if entry is not None:
            entry["goplus"] = res
            entry["is_honeypot"] = True
            entry["sell_whitelist"] = True
            entry["honeypot_reason"] = res["reason"]
            token_feed_upsert(token_address, goplus=res, is_honeypot=True, sell_whitelist=True)
        return res

    # 2. Existing entry flags
    if entry:
        if entry.get("freeze_authority_active") is True:
            res = {
                "safe": False,
                "is_honeypot": True,
                "sell_whitelist": True,
                "mintable": False,
                "freezable": True,
                "transfer_hook": False,
                "buy_tax": 0.0,
                "sell_tax": 0.0,
                "holder_count": entry.get("holder_count"),
                "reason": "Freeze authority active (honeypot / sell freeze risk)",
                "summary": "🚨 Freeze Authority Active",
                "ts": now,
            }
            TOKEN_SECURITY_CACHE[token_address] = res
            entry["goplus"] = res
            entry["is_honeypot"] = True
            entry["sell_whitelist"] = True
            entry["honeypot_reason"] = res["reason"]
            token_feed_upsert(token_address, goplus=res, is_honeypot=True, sell_whitelist=True)
            return res
        if entry.get("sell_whitelist") or entry.get("is_honeypot"):
            hp_r = entry.get("honeypot_reason") or "Sell whitelist / honeypot detected"
            res = {
                "safe": False,
                "is_honeypot": True,
                "sell_whitelist": True,
                "mintable": False,
                "freezable": False,
                "transfer_hook": False,
                "buy_tax": 0.0,
                "sell_tax": 0.0,
                "holder_count": entry.get("holder_count"),
                "reason": hp_r,
                "summary": f"🚨 {hp_r}",
                "ts": now,
            }
            TOKEN_SECURITY_CACHE[token_address] = res
            entry["goplus"] = res
            return res

    chain_norm = (chain or "solana").lower()
    is_honeypot = False
    sell_whitelist = False
    mintable = False
    freezable = False
    transfer_hook = False
    buy_tax_pct = 0.0
    sell_tax_pct = 0.0
    holder_count = None
    reasons = []

    # 3. External Security API check (GoPlus)
    try:
        async with aiohttp.ClientSession() as session:
            if chain_norm == "solana":
                url = f"https://api.gopluslabs.io/api/v1/solana/token_security?contract_addresses={token_address}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        sec = (data.get("result") or {}).get(token_address) or {}
                        # Mintable
                        m_st = str((sec.get("mintable") or {}).get("status", "0"))
                        if m_st == "1":
                            mintable = True
                            reasons.append("Mintable")
                        # Freezable
                        f_st = str((sec.get("freezable") or {}).get("status", "0"))
                        if f_st == "1":
                            freezable = True
                            is_honeypot = True
                            reasons.append("Freeze authority active")
                        # Transfer hook
                        th = sec.get("transfer_hook")
                        if th and isinstance(th, list) and len(th) > 0:
                            transfer_hook = True
                            is_honeypot = True
                            sell_whitelist = True
                            reasons.append("Transfer hook active (sell whitelist)")
                        # Non-transferable
                        if str(sec.get("non_transferable", "0")) == "1":
                            is_honeypot = True
                            sell_whitelist = True
                            reasons.append("Non-transferable")
                        # Default account state (2 = frozen)
                        if str(sec.get("default_account_state", "1")) == "2":
                            is_honeypot = True
                            reasons.append("Default account state frozen")
                        # Transfer fee
                        tf = sec.get("transfer_fee") or {}
                        cfr = tf.get("current_fee_rate") or {}
                        fee_val = cfr.get("fee_rate")
                        if fee_val is not None:
                            try:
                                sell_tax_pct = round(float(fee_val) * 100, 1)
                                buy_tax_pct = sell_tax_pct
                            except Exception:
                                pass
                        hc_val = sec.get("holder_count")
                        if hc_val is not None:
                            try:
                                holder_count = int(hc_val)
                            except Exception:
                                pass
            elif chain_norm in ("bnb", "base", "bsc"):
                cid = "56" if chain_norm in ("bnb", "bsc") else "8453"
                url = f"https://api.gopluslabs.io/api/v1/token_security/{cid}?contract_addresses={token_address}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        sec = (data.get("result") or {}).get(token_address.lower()) or {}
                        if str(sec.get("is_whitelisted", "0")) == "1":
                            is_honeypot = True
                            sell_whitelist = True
                            reasons.append("Sell/transfer whitelist enabled")
                        if str(sec.get("cannot_sell_all", "0")) == "1":
                            is_honeypot = True
                            sell_whitelist = True
                            reasons.append("Cannot sell all")
                        if str(sec.get("is_honeypot", "0")) == "1":
                            is_honeypot = True
                            reasons.append("GoPlus verified honeypot")
                        if str(sec.get("is_mintable", "0")) == "1":
                            mintable = True
                            reasons.append("Mintable")
                        try:
                            buy_tax_pct = round(float(sec.get("buy_tax") or 0.0) * 100, 1)
                            sell_tax_pct = round(float(sec.get("sell_tax") or 0.0) * 100, 1)
                        except Exception:
                            pass
                        hc_val = sec.get("holder_count")
                        if hc_val is not None:
                            try:
                                holder_count = int(hc_val)
                            except Exception:
                                pass
    except Exception as exc:
        logger.debug(f"check_token_honeypot_and_whitelist({token_address}) API error: {exc!r}")

    # Tax does not disqualify a token from being safe; only honeypots, whitelists, freezable, mintable do
    safe = not (is_honeypot or sell_whitelist or freezable or mintable)
    reason = "; ".join(reasons)

    if is_honeypot or sell_whitelist:
        summary = f"🚨 Honeypot ({reason or 'Sells restricted'})"
    elif freezable or mintable:
        summary = f"⚠️ {reason}"
    elif buy_tax_pct > 0 or sell_tax_pct > 0:
        summary = f"🛡️ Buy {buy_tax_pct:g}% / Sell {sell_tax_pct:g}% Tax"
    else:
        summary = "🛡️ Clean · 0% Tax · Renounced"

    res = {
        "safe": safe,
        "is_honeypot": is_honeypot,
        "sell_whitelist": sell_whitelist,
        "mintable": mintable,
        "freezable": freezable,
        "transfer_hook": transfer_hook,
        "buy_tax": buy_tax_pct,
        "sell_tax": sell_tax_pct,
        "holder_count": holder_count,
        "reason": reason,
        "summary": summary,
        "ts": now,
    }
    TOKEN_SECURITY_CACHE[token_address] = res
    if len(TOKEN_SECURITY_CACHE) > TOKEN_SECURITY_CACHE_MAX:
        TOKEN_SECURITY_CACHE.pop(next(iter(TOKEN_SECURITY_CACHE)), None)

    if entry is not None:
        entry["goplus"] = res
        if is_honeypot or sell_whitelist:
            entry["is_honeypot"] = True
            entry["sell_whitelist"] = True
            entry["honeypot_reason"] = reason
        token_feed_upsert(token_address, goplus=res, is_honeypot=is_honeypot, sell_whitelist=sell_whitelist)
    return res


DEBOT_API_BASES = ["https://app.debot.ai", "https://debot.ai"]
DEBOT_STORY_CACHE: dict[str, tuple[float, Optional[dict[str, Any]]]] = {}
DEBOT_CACHE_TTL = 1800.0  # 30 minutes cache


async def fetch_debot_story(token_address: str) -> Optional[dict[str, Any]]:
    """Fetches narrative origin, source tweet, and AI rating from DeBot AI.
    Free unauthenticated public API used by Fomo Lens. Returns structured dict or None."""
    if not token_address:
        return None
    now = time.time()
    cached = DEBOT_STORY_CACHE.get(token_address)
    if cached and (now - cached[0]) < DEBOT_CACHE_TTL:
        return cached[1]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    ca_param = token_address.lower() if token_address.startswith("0x") else token_address

    result = None
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            for base in DEBOT_API_BASES:
                url = f"{base}/api/v1/nitter/story/latest?ca_address={ca_param}"
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            body = await resp.json()
                            if body.get("code") == 0:
                                history = body.get("data", {}).get("history", {})
                                story_en = history.get("story_en") or {}
                                story_zh = history.get("story") or {}
                                story = story_en if story_en.get("background") else story_zh
                                origin = story.get("background", {}).get("origin", {})
                                rating = story.get("rating", {})
                                distrib = story.get("distribution", {})
                                bot_check = distrib.get("community_participation", {}).get("text")
                                celeb_check = distrib.get("celebrity_support", {}).get("text")

                                origin_text = origin.get("text") or ""
                                if not origin_text and story_zh:
                                    origin_text = story_zh.get("background", {}).get("origin", {}).get("text") or ""

                                result = {
                                    "narrative_type": story.get("narrative_type") or story_zh.get("narrative_type"),
                                    "origin_text": origin_text.strip() if origin_text else None,
                                    "origin_ref": origin.get("ref") or story_zh.get("background", {}).get("origin", {}).get("ref") or "",
                                    "rating_score": rating.get("score"),
                                    "rating_reason": rating.get("reason"),
                                    "bot_participation": bot_check if bot_check and bot_check.lower() != "none" else None,
                                    "celebrity_support": celeb_check if celeb_check and celeb_check.lower() != "none" else None,
                                }
                                break
                            elif body.get("code") == -1:
                                result = None
                                break
                except Exception as exc:
                    logger.debug(f"[debot] fetch from {base} for {token_address} failed: {exc!r}")
                    continue
    except Exception as exc:
        logger.debug(f"[debot] fetch_debot_story({token_address}) failed: {exc!r}")

    DEBOT_STORY_CACHE[token_address] = (now, result)
    if len(DEBOT_STORY_CACHE) > 500:
        DEBOT_STORY_CACHE.pop(next(iter(DEBOT_STORY_CACHE)), None)
    return result


TWITTER_VERIFY_CACHE: dict[str, dict[str, Any]] = {}
TWITTER_VERIFY_CACHE_MAX = 1000
TWITTER_VERIFY_CACHE_TTL = 21600.0  # 6 hours


def extract_twitter_handle(url_or_handle: Optional[str]) -> Optional[str]:
    """Extracts clean Twitter/X handle from a URL or raw string."""
    if not url_or_handle:
        return None
    s = str(url_or_handle).strip()
    m = re.search(r"(?:twitter\.com|x\.com)/([A-Za-z0-9_]{1,15})(?:/|\?|$)", s, re.IGNORECASE)
    if m:
        h = m.group(1)
        if h.lower() not in ("i", "intent", "home", "explore", "search", "hashtag"):
            return h
    if s.startswith("@") and len(s) <= 16:
        return s[1:]
    if re.match(r"^[A-Za-z0-9_]{1,15}$", s):
        return s
    return None


async def verify_twitter_quality(twitter_input: Optional[str]) -> tuple[bool, str, dict[str, Any]]:
    """Verifies Twitter account criteria required for opportunity admission:
    1. If no Twitter link is provided, passes (tokens without Twitter can still qualify).
    2. If a Twitter link is provided, the account must be:
       - Created within the last year (<= 365 days old)
       - Have 0 historical username changes (verified via memory.lol)
    Returns (is_valid, reason, metadata_dict)."""
    if not twitter_input:
        return True, "NO_TWITTER", {}

    handle = extract_twitter_handle(twitter_input)
    if not handle:
        return False, "INVALID_TWITTER_LINK", {}

    cache_key = handle.lower()
    now = time.time()
    cached = TWITTER_VERIFY_CACHE.get(cache_key)
    if cached and (now - cached.get("cached_at", 0) < TWITTER_VERIFY_CACHE_TTL):
        return cached["ok"], cached["reason"], cached.get("meta", {})

    meta: dict[str, Any] = {"handle": handle}
    timeout = aiohttp.ClientTimeout(total=6)
    headers = {"User-Agent": "curl/8.5.0"}

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        # 1. Check account creation age via vxTwitter
        try:
            async with session.get(f"https://api.vxtwitter.com/{handle}") as resp:
                if resp.status == 404:
                    res = (False, "TWITTER_ACCOUNT_NOT_FOUND_OR_SUSPENDED", meta)
                    TWITTER_VERIFY_CACHE[cache_key] = {"ok": res[0], "reason": res[1], "meta": meta, "cached_at": now}
                    return res
                if resp.status == 200:
                    data = await resp.json()
                    created_str = data.get("created_at")
                    if created_str:
                        dt = datetime.strptime(created_str, "%a %b %d %H:%M:%S %z %Y")
                        days_old = (datetime.now(timezone.utc) - dt).days
                        meta["days_old"] = days_old
                        meta["created_at"] = dt.strftime("%b %Y")
                        meta["followers"] = data.get("followers_count", 0)

                        if days_old > 365:
                            reason = f"TWITTER_ACCOUNT_TOO_OLD ({days_old}d > 365d, created {meta['created_at']})"
                            res = (False, reason, meta)
                            TWITTER_VERIFY_CACHE[cache_key] = {"ok": res[0], "reason": res[1], "meta": meta, "cached_at": now}
                            return res
        except Exception as exc:
            logger.debug(f"[twitter] age check for {handle} failed: {exc!r}")

        # 2. Check username change history via memory.lol
        try:
            async with session.get(f"https://api.memory.lol/v1/tw/{handle}") as resp_mem:
                if resp_mem.status == 200:
                    data_mem = await resp_mem.json()
                    accounts = data_mem.get("accounts") or []
                    for acc in accounts:
                        snames = acc.get("screen_names") or {}
                        if len(snames) > 1:
                            prior = ", ".join(snames.keys())
                            reason = f"TWITTER_USERNAME_CHANGED ({len(snames)-1} changes: {prior})"
                            res = (False, reason, meta)
                            TWITTER_VERIFY_CACHE[cache_key] = {"ok": res[0], "reason": res[1], "meta": meta, "cached_at": now}
                            return res
        except Exception as exc:
            logger.debug(f"[twitter] memory.lol check for {handle} failed: {exc!r}")

    res = (True, "TWITTER_CLEAN", meta)
    TWITTER_VERIFY_CACHE[cache_key] = {"ok": res[0], "reason": res[1], "meta": meta, "cached_at": now}
    if len(TWITTER_VERIFY_CACHE) > TWITTER_VERIFY_CACHE_MAX:
        TWITTER_VERIFY_CACHE.pop(next(iter(TWITTER_VERIFY_CACHE)), None)
    return res



SOLANA_BUNDLE_MIN_WALLETS = 2  # holders sharing one funder before it counts as a bundle
SOLANA_BUNDLE_MAX_HOLDERS_TO_CHECK = 10  # bounds RPC cost — bundle wallets are near-always among the top holders
SOLANA_WALLET_FUNDER_CACHE: dict[str, Optional[str]] = {}
SOLANA_WALLET_FUNDER_CACHE_MAX = 10000


async def _resolve_solana_wallet_funder(wallet: str) -> Optional[str]:
    """Best-effort: walk to this wallet's OLDEST transaction and look for a
    System Program transfer landing in it, returning who sent it.
    Results are permanently cached in SOLANA_WALLET_FUNDER_CACHE to prevent
    repeated RPC queries for known holders."""
    if not (SOLANA_WS_RPC_URL or SOLANA_FALLBACK_RPC_URL):
        return None
    if wallet in SOLANA_WALLET_FUNDER_CACHE:
        return SOLANA_WALLET_FUNDER_CACHE[wallet]
    try:
        async with aiohttp.ClientSession() as session:
            sigs = await _solana_rpc_post(session, "getSignaturesForAddress", [wallet, {"limit": 1000}], timeout=15.0)
            if not sigs:
                if len(SOLANA_WALLET_FUNDER_CACHE) < SOLANA_WALLET_FUNDER_CACHE_MAX:
                    SOLANA_WALLET_FUNDER_CACHE[wallet] = None
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
                        src = info["source"]
                        if len(SOLANA_WALLET_FUNDER_CACHE) < SOLANA_WALLET_FUNDER_CACHE_MAX:
                            SOLANA_WALLET_FUNDER_CACHE[wallet] = src
                        return src
    except Exception as exc:
        logger.debug(f"_resolve_solana_wallet_funder({wallet}) failed: {exc!r}")
    if len(SOLANA_WALLET_FUNDER_CACHE) < SOLANA_WALLET_FUNDER_CACHE_MAX:
        SOLANA_WALLET_FUNDER_CACHE[wallet] = None
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


def ticker_is_invalid(ticker: Optional[str]) -> tuple[bool, str]:
    """Reject low-effort/malformed tickers. Valid = first character is a letter,
    either full-caps (FOMO) or Title-case (Fomo). Invalid:
      - starts with '$' (e.g. "$DOGE") — user rule, no-go
      - first alphabetic character is lowercase ("fomobrain") — low-effort
    UNKNOWN / not-yet-resolved is not judged (returns valid so it isn't dropped
    before DexScreener backfills the real symbol)."""
    if not ticker or ticker == "UNKNOWN":
        return False, ""
    t = ticker.strip()
    if t.startswith("$"):
        return True, f"ticker \"{ticker}\" starts with '$' — rejected"
    if _ticker_starts_lowercase(t):
        return True, f"ticker \"{ticker}\" starts lowercase — low-effort naming, rejected"
    return False, ""


def is_stock_style_ticker(ticker: Optional[str]) -> bool:
    """Tokenized-equity 'xStocks' pattern: an uppercase stock symbol with a
    trailing lowercase 'x' (AAPLx, TSLAx, MSTRx, COINx, MCDx, NVDAx). These are
    real-world equity mirrors, not fair-launch memecoins — the user doesn't
    want them. Matches 2-5 uppercase letters/digits followed by a single 'x'."""
    if not ticker or ticker == "UNKNOWN":
        return False
    return bool(re.fullmatch(r"[A-Z]{1,5}[0-9]?x", ticker.strip()))


def is_probably_established_or_stock(
    ticker: Optional[str],
    market_cap: float = 0.0,
    platform: Optional[str] = None,
    token_address: Optional[str] = None,
) -> tuple[bool, str]:
    """True + reason if this looks like an ALREADY-ESTABLISHED coin, a StonkFun quote collateral token,
    or a tokenized stock rather than a genuine new fair-launch. Four signals:
      - token address is a known StonkFun quote/collateral asset (ZEC, STONK, WBTC, USDC, etc.)
      - a known established symbol (HYPE, ZEC, BTC, ...) — case-insensitive (blocked unless fresh StonkFun base token)
      - the xStocks tokenized-equity pattern (MSTRx, AAPLx, ...)
      - an implausibly high mcap for something we're seeing as 'new'
    NOTE: StonkFun pairs fair-launch coins against quote stock tokens (e.g. NIU/ZEC). The user ONLY
    wants the first one (base token). Quote collateral tokens are 100% blocked regardless of platform.
    """
    if token_address and token_address in STONKFUN_QUOTE_MINTS:
        return True, f"'{ticker or 'token'}' ({token_address}) is a StonkFun quote collateral token (paired stock coin), not the fair-launch base token"
    if ticker:
        sym = ticker.strip().upper()
        if is_stock_style_ticker(ticker):
            return True, f"'{ticker}' looks like a tokenized stock (xStocks pattern), not a memecoin"
        if (platform or "").lower() != "stonkfun" and sym in ESTABLISHED_SYMBOL_BLOCKLIST:
            return True, f"'{ticker}' is a known established coin, not a new launch"
    if market_cap and market_cap >= IMPLAUSIBLE_NEW_LAUNCH_MCAP_USD:
        return True, f"mcap ${market_cap:,.0f} is implausibly high for a genuinely new launch — likely an established coin mislabeled as new"
    return False, ""


def purge_established_from_jev() -> int:
    """Remove established-coin / tokenized-stock records that leaked in. HARDENED:
    a coin is NEVER purged if it has a real outcome (mooned/rugged/dying) or has
    actually moved (peak_multiple > 1.2) — we only clear obvious, unlabeled,
    non-moving stock/established leaks so we can't destroy real learning data."""
    removed = 0
    def _has_real_history(rec_or_a):
        # protect anything with an outcome or a genuine price move
        if rec_or_a.get("outcome") in ("mooned", "rugged", "dying"):
            return True
        pm = rec_or_a.get("peak_multiple")
        return pm is not None and pm > 1.2
    def _is_stock(rec_or_a):
        est, _ = is_probably_established_or_stock(
            rec_or_a.get("ticker"),
            rec_or_a.get("mcap_at_eval") or 0.0,
            platform=rec_or_a.get("platform"),
            token_address=rec_or_a.get("token_address"),
        )
        if est:
            return True
        # only the STRONG signal (xStocks pattern / blocklist via est above);
        # do NOT purge on impersonation_risk alone — that risked deleting real
        # coins Jev merely found name-similar. Ticker match is the safe gate.
        return False
    # recent activity feed
    keep = deque(maxlen=RECENT_JEV_JUDGMENTS.maxlen)
    for a in RECENT_JEV_JUDGMENTS:
        if a.get("kind") != "pvp" and _is_stock(a) and not _has_real_history(a):
            removed += 1
            continue
        keep.append(a)
    RECENT_JEV_JUDGMENTS.clear()
    RECENT_JEV_JUDGMENTS.extend(keep)
    # judgment log (learning ledger)
    for addr in list(JEV_JUDGMENT_LOG.keys()):
        rec = JEV_JUDGMENT_LOG[addr]
        if _is_stock(rec) and not _has_real_history(rec):
            del JEV_JUDGMENT_LOG[addr]
            removed += 1
    if removed:
        logger.info(f"[jev] purged {removed} established/stock record(s) from Jev feed/log")
    return removed


def extract_social_presence(entry: dict[str, Any]) -> dict[str, Any]:
    """Derives free social presence data from DexScreener token info/profiles,
    pump.fun coin metadata, and link URLs. Requires zero paid API keys."""
    token_address = entry.get("token_address") or ""

    soc = dict(entry.get("socials") or {})
    has_twitter = bool(soc.get("has_twitter") or soc.get("twitter") or entry.get("twitter"))
    has_telegram = bool(soc.get("has_telegram") or soc.get("telegram") or entry.get("telegram"))
    has_website = bool(soc.get("has_website") or soc.get("website") or entry.get("website"))
    active_boosts = int(soc.get("active_boosts") or entry.get("active_boosts") or 0)

    # Check TOKEN_PROFILES cache (free background DexScreener profile scraper)
    prof = TOKEN_PROFILES.get(token_address) or {}
    if prof:
        prof_soc = prof.get("socials") or []
        if "twitter" in prof_soc:
            has_twitter = True
        if "telegram" in prof_soc:
            has_telegram = True
        if "website" in prof_soc:
            has_website = True
        if prof.get("boosts"):
            active_boosts = max(active_boosts, int(prof.get("boosts")))

    # Check links dict on entry
    links = entry.get("links") or {}
    for k, v in links.items():
        v_str = str(v).lower()
        if "twitter.com" in v_str or "x.com" in v_str:
            has_twitter = True
        elif "t.me" in v_str or "telegram" in v_str:
            has_telegram = True
        elif "http" in v_str and not any(x in v_str for x in ["dexscreener", "solscan", "bscscan", "rugcheck", "fomo", "thestonkboard"]):
            has_website = True

    channels = []
    if has_twitter:
        channels.append("Twitter/X")
    if has_telegram:
        channels.append("Telegram")
    if has_website:
        channels.append("Website")

    return {
        "has_twitter": has_twitter,
        "has_telegram": has_telegram,
        "has_website": has_website,
        "channel_count": len(channels),
        "channels": channels,
        "active_boosts": active_boosts,
    }


def compute_opportunity_score(entry: dict[str, Any]) -> tuple[int, list[str]]:
    """A heuristic 0-100 composite of everything this pipeline already knows about
    a token, so a viewer isn't left manually cross-referencing raw numbers to
    guess whether something looks promising. This is NOT a predictive model or
    financial advice — it's a transparent weighted sum of the same signals shown
    elsewhere in the UI, and every point is attributed so it's never a black box."""
    market_cap = entry.get("market_cap", 0.0)
    volume_24h = entry.get("volume_24h", 0.0)
    volume_to_mcap_ratio = (volume_24h / market_cap) if market_cap > 0 else 0.0

    token_address = entry.get("token_address") or ""
    platform = entry.get("platform")

    # Hard ceiling: strictly under 100k ONLY for thestonkboard.com / StonkFun coins
    is_stonk = is_stonkboard_token(token_address, platform, entry.get("links"))
    if is_stonk and market_cap > MAX_OPPORTUNITY_MARKET_CAP_USD:
        return 0, [
            f"TheStonkBoard market cap ${market_cap:,.0f} exceeds ${MAX_OPPORTUNITY_MARKET_CAP_USD:,.0f} ceiling (under 100k only)"
        ]
    if market_cap > IMPLAUSIBLE_NEW_LAUNCH_MCAP_USD:
        return 0, [f"Market cap ${market_cap:,.0f} exceeds implausible new launch threshold"]

    # Hard floor: dust-level mcap/volume disqualifies a token outright,
    # regardless of what other signals fired. No dev-trust or narrative flag
    # should be able to outrank "this barely has any real activity."
    if market_cap < MIN_OPPORTUNITY_MARKET_CAP_USD or volume_24h < MIN_OPPORTUNITY_VOLUME_USD:
        return 0, [
            f"Below minimum floor (mcap {market_cap:.0f} / vol {volume_24h:.0f}) "
            f"— too little real activity to be considered"
        ]

    # Launchpad & Contract suffix filter (7777, pump, 4444)
    qualifies_launch, launch_reason = is_launchpad_or_target_suffix(platform, token_address)
    if not qualifies_launch:
        return 0, [launch_reason]


    ticker = entry.get("ticker")
    if not ticker or ticker == "UNKNOWN":
        return 0, ["Ticker not yet resolved — not considered until a real name is known"]
    if ticker_is_invalid(ticker)[0]:
        return 0, [ticker_is_invalid(ticker)[1]]

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

    # Honeypot, sell whitelisting & freeze authority check
    buys_24h = entry.get("buys_24h") or 0
    sells_24h = entry.get("sells_24h") or 0
    if entry.get("is_honeypot") or entry.get("sell_whitelist"):
        return 0, [f"Honeypot / sell whitelisting detected ({entry.get('honeypot_reason', 'sells restricted')}) — discarded"]
    if entry.get("freeze_authority_active") is True:
        return 0, ["Freeze authority active (honeypot risk) — discarded"]
    if (buys_24h >= 4 and sells_24h == 0) or (buys_24h >= 15 and sells_24h <= 1):
        return 0, [f"Honeypot chart detected ({buys_24h} buys / {sells_24h} sells) — no sells possible (discarded)"]

    dev_wallet = entry.get("dev_wallet", "")
    is_infra_wallet = dev_wallet.lower() in KNOWN_INFRASTRUCTURE_ADDRESSES or dev_wallet.lower().startswith("stonkboard") or dev_wallet.lower().startswith("discovered:")

    if is_infra_wallet:
        reasons.append("Dev field is shared launchpad infrastructure, not a trackable individual — reputation neutral")
    else:
        dev_rep = DEV_REPUTATION_DATABASE.get(dev_wallet)
        dev_total = dev_rep.get("total_launches", 0) if dev_rep else (entry.get("dev_total_launches") or 0)
        dev_rugs = dev_rep.get("failed_spams", 0) if dev_rep else (entry.get("dev_rugs") or 0)
        is_bl = (dev_rep.get("is_blacklisted") if dev_rep else False) or bool(entry.get("dev_blacklisted"))

        if is_bl:
            return 0, ["Dev is blacklisted — discarded"]
        if dev_rugs > 0 or len(DEV_RUG_HISTORY.get(dev_wallet, [])) > 0:
            return 0, [f"Dev has prior rug history ({dev_rugs} rugs) — serial rugger discarded"]

        if dev_total > 1:
            prior_launches = entry.get("dev_prior_launches") or [
                h for h in DEV_LAUNCH_HISTORY.get(dev_wallet, [])
                if h.get("token_address") != entry.get("token_address")
            ]
            dedup_map: dict[str, dict[str, Any]] = {}
            for h in prior_launches:
                t = h.get("ticker")
                if not t or t == "UNKNOWN":
                    continue
                peak = float(h.get("peak_market_cap") or 0.0) or get_known_token_peak_mcap(h.get("token_address", ""))
                if t not in dedup_map or peak > float(dedup_map[t].get("peak_market_cap") or 0.0):
                    dedup_map[t] = {"ticker": t, "peak_market_cap": peak}
            if dedup_map:
                prior_strs = []
                for d in list(dedup_map.values())[:3]:
                    if d["peak_market_cap"] > 0:
                        prior_strs.append(f"${d['ticker']} peak {format_mcap_compact(d['peak_market_cap'])}")
                    else:
                        prior_strs.append(f"${d['ticker']}")
                prior_str = f" ({', '.join(prior_strs)})"
            elif entry.get("dev_prior_tickers"):
                prior_str = f" (${', $'.join(entry.get('dev_prior_tickers')[:3])})"
            else:
                prior_str = ""
            reasons.append(f"⚠️ Multi-launch dev: {dev_total} launches on record{prior_str} (-5)")
            score -= 5

        dev_moons = dev_rep.get("successful_launches", 0) if dev_rep else (entry.get("dev_moons") or 0)
        if dev_moons > 0:
            moons_bonus = min(15, dev_moons * 5)
            score += moons_bonus
            reasons.append(f"🏆 Proven dev track record: {dev_moons} prior graduated/mooned coin(s) (+{moons_bonus})")

    # Free social presence & community engagement score (DexScreener/PumpPortal/TokenProfiles)
    soc_data = extract_social_presence(entry)
    social_count = soc_data["channel_count"]
    channels_str = ", ".join(soc_data["channels"])
    active_boosts = soc_data["active_boosts"]

    if social_count >= 3:
        score += 15
        reasons.append(f"Full social footprint ({channels_str}) (+15)")
    elif social_count == 2:
        score += 10
        reasons.append(f"Established socials ({channels_str}) (+10)")
    elif social_count == 1:
        score += 5
        reasons.append(f"Active social link ({channels_str}) (+5)")
    else:
        reasons.append("No official socials found (0)")

    if active_boosts > 0:
        boost_pts = min(5, active_boosts)
        score += boost_pts
        reasons.append(f"DexScreener community boosts ({active_boosts} active, +{boost_pts})")

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

    # --- Jev semantic layer (see SECTION 5.5) -------------------------------
    # Blended LAST so it adjusts an otherwise-complete deterministic score.
    # A high impersonation-risk judgment hard-vetoes to 0, matching the other
    # hard floors above. All other Jev dimensions are confidence-scaled points.
    jev_points, jev_reasons, jev_veto = jev_score_contribution(entry)
    if jev_veto:
        return 0, reasons + jev_reasons
    if jev_points:
        score += jev_points
    reasons.extend(jev_reasons)

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

    token_address = entry.get("token_address") or ""
    platform = entry.get("platform")

    is_stonk = is_stonkboard_token(token_address, platform, entry.get("links"))
    if is_stonk and market_cap > MAX_OPPORTUNITY_MARKET_CAP_USD:
        return 0, [f"TheStonkBoard coin market cap ${market_cap:,.0f} exceeds $100k ceiling"]
    if market_cap > IMPLAUSIBLE_NEW_LAUNCH_MCAP_USD:
        return 0, [f"Market cap ${market_cap:,.0f} exceeds implausible new launch threshold"]

    # Launchpad & Contract suffix filter (7777, pump, 4444)
    qualifies_launch, launch_reason = is_launchpad_or_target_suffix(platform, token_address)
    if not qualifies_launch:
        return 0, [launch_reason]


    ticker = entry.get("ticker")
    if not ticker or ticker == "UNKNOWN":
        return 0, ["Ticker not yet resolved — not considered until a real name is known"]
    if ticker_is_invalid(ticker)[0]:
        return 0, [ticker_is_invalid(ticker)[1]]

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

    # Honeypot chart & freeze authority check
    if entry.get("is_honeypot") or entry.get("sell_whitelist"):
        return 0, [f"Honeypot / sell whitelisting detected ({entry.get('honeypot_reason', 'sells restricted')}) — discarded"]
    if entry.get("freeze_authority_active") is True:
        return 0, ["Freeze authority active (honeypot risk) — discarded"]
    if (buys_24h >= 4 and sells_24h == 0) or (buys_24h >= 15 and sells_24h <= 1):
        return 0, [f"Honeypot chart detected ({buys_24h} buys / {sells_24h} sells) — no sells possible (discarded)"]

    dev_wallet = entry.get("dev_wallet", "")
    is_infra_wallet = dev_wallet.lower() in KNOWN_INFRASTRUCTURE_ADDRESSES or dev_wallet.lower().startswith("stonkboard") or dev_wallet.lower().startswith("discovered:")
    if not is_infra_wallet:
        dev_rep = DEV_REPUTATION_DATABASE.get(dev_wallet)
        dev_total = dev_rep.get("total_launches", 0) if dev_rep else (entry.get("dev_total_launches") or 0)
        dev_rugs = dev_rep.get("failed_spams", 0) if dev_rep else (entry.get("dev_rugs") or 0)
        is_bl = (dev_rep.get("is_blacklisted") if dev_rep else False) or bool(entry.get("dev_blacklisted"))

        if is_bl:
            return 0, ["Dev is blacklisted — discarded"]
        if dev_rugs > 0 or len(DEV_RUG_HISTORY.get(dev_wallet, [])) > 0:
            return 0, [f"Dev has prior rug history ({dev_rugs} rugs) — serial rugger discarded"]

        if dev_total > 1:
            prior_launches = entry.get("dev_prior_launches") or [
                h for h in DEV_LAUNCH_HISTORY.get(dev_wallet, [])
                if h.get("token_address") != entry.get("token_address")
            ]
            dedup_map: dict[str, dict[str, Any]] = {}
            for h in prior_launches:
                t = h.get("ticker")
                if not t or t == "UNKNOWN":
                    continue
                peak = float(h.get("peak_market_cap") or 0.0) or get_known_token_peak_mcap(h.get("token_address", ""))
                if t not in dedup_map or peak > float(dedup_map[t].get("peak_market_cap") or 0.0):
                    dedup_map[t] = {"ticker": t, "peak_market_cap": peak}
            if dedup_map:
                prior_strs = []
                for d in list(dedup_map.values())[:3]:
                    if d["peak_market_cap"] > 0:
                        prior_strs.append(f"${d['ticker']} peak {format_mcap_compact(d['peak_market_cap'])}")
                    else:
                        prior_strs.append(f"${d['ticker']}")
                prior_str = f" ({', '.join(prior_strs)})"
            elif entry.get("dev_prior_tickers"):
                prior_str = f" (${', $'.join(entry.get('dev_prior_tickers')[:3])})"
            else:
                prior_str = ""
            reasons.append(f"⚠️ Multi-launch dev: {dev_total} launches on record{prior_str}")

    # Free early social presence score
    soc_data = extract_social_presence(entry)
    social_count = soc_data["channel_count"]
    channels_str = ", ".join(soc_data["channels"])
    if social_count >= 2:
        score += 15
        reasons.append(f"Early verified socials ({channels_str}) (+15)")
    elif social_count == 1:
        score += 8
        reasons.append(f"Early social link ({channels_str}) (+8)")

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

    # Stage-1 Jev qualitative screening bonus/penalty (narrative coherence & originality)
    screen = entry.get("jev_screen") or (entry.get("jev", {}).get("dimensions"))
    if screen:
        nq = screen.get("narrative_quality", {})
        nq_score = nq.get("score")
        if nq_score is not None:
            nq_conf = nq.get("confidence")
            nq_conf = 1.0 if nq_conf is None else max(0.0, min(1.0, nq_conf))
            # Rubric levels: 0=Gibberish, 1=Low-effort copy, 2=Coherent, 3=Distinctive
            if nq_score >= 3:
                pts = round(12 * nq_conf)
                score += pts
                reasons.append(f"Jev screened: distinctive narrative (+{pts})")
            elif nq_score == 2:
                pts = round(6 * nq_conf)
                score += pts
                reasons.append(f"Jev screened: coherent narrative (+{pts})")
            elif nq_score == 1:
                pts = round(6 * nq_conf)
                score -= pts
                reasons.append(f"Jev screened: low-effort copycat meme (-{pts})")
            elif nq_score == 0:
                pts = round(15 * nq_conf)
                score -= pts
                reasons.append(f"Jev screened: gibberish/spam launch (-{pts})")

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


async def estimate_solana_trade(*args, **kwargs) -> tuple[Optional[str], float]:
    """Fetch a confirmed Solana tx via HTTPS RPC and estimate USD size of the wallet's largest token balance increase."""
    if len(args) == 3:
        _, signature, wallet = args
    elif len(args) == 2:
        signature, wallet = args
    else:
        signature = kwargs.get("signature", "")
        wallet = kwargs.get("wallet", "")

    if not signature or not wallet:
        return None, 0.0

    try:
        async with aiohttp.ClientSession() as session:
            result = await _solana_rpc_post(
                session,
                "getTransaction",
                [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}],
                timeout=12.0,
            )
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

        trade_size_usd = 0.0
        price = await fetch_token_price_usd(best_token)
        if price > 0:
            trade_size_usd = best_delta * price
        else:
            # Fallback: estimate trade size from wallet's net SOL spent
            try:
                keys = result.get("transaction", {}).get("message", {}).get("accountKeys", [])
                wallet_idx = None
                for i, k in enumerate(keys):
                    pk = k.get("pubkey") if isinstance(k, dict) else str(k)
                    if pk == wallet:
                        wallet_idx = i
                        break
                if wallet_idx is not None:
                    pre_lamports = meta.get("preBalances", [])[wallet_idx] if wallet_idx < len(meta.get("preBalances", [])) else 0
                    post_lamports = meta.get("postBalances", [])[wallet_idx] if wallet_idx < len(meta.get("postBalances", [])) else 0
                    sol_spent = max(0.0, (pre_lamports - post_lamports) / 1e9)
                    if sol_spent > 0.005:  # more than a network fee
                        trade_size_usd = sol_spent * 150.0  # approximate SOL USD price
            except Exception:
                pass

        return best_token, trade_size_usd
    except Exception as exc:
        logger.warning(f"[wallet/solana] failed to estimate trade for {signature}: {exc!r}")
        return None, 0.0



# ============================================================================
# SECTION 5.5 — TYPESAFE "JEV" SEMANTIC REASONER + LEARNING LOOP
# ============================================================================
# Jev judges the qualitative things the arithmetic above can't: is the launch
# coherent/serious, is it impersonating something, does the theme have staying
# power. One batched request per candidate (docs: batching every question into
# one call is ~12x cheaper / ~10x faster with identical answers). Everything
# here fails safe: any error, missing key, or exhausted budget → the token
# still scores normally on the deterministic signals, Jev just contributes 0.

# The questions Jev answers about a token. Score levels/Noul criteria are
# concrete and self-standing (docs: "Score levels must describe concrete
# situations and stand on their own"). Ask one narrow judgment per question;
# all run in parallel over the same state.
JEV_QUESTIONS: dict[str, Any] = {
    "moon_potential": {
        "type": "score",
        "instructions": (
            "How much realistic potential does this coin have to run SIGNIFICANTLY higher from its "
            "CURRENT market cap? Judge upside from where it is now, using its name/narrative, launchpad, "
            "market cap, volume, holders and momentum in `market`, `holders`, `trust` and `momentum`. "
            "A small coin with a strong, spreading narrative has high upside; a coin that has already "
            "had its big run and stalled has little left. Size alone isn't the answer — a larger coin "
            "that's still climbing on a strong narrative can still have room, a tiny dead one does not."
        ),
        "criteria": [
            "No potential — dead/spam/rug pattern, or already ran and stalled with nothing left",
            "Weak — little to suggest further upside, likely fades",
            "Some potential — a plausible setup but nothing standout",
            "Strong potential — coherent narrative + real demand, room to run further",
            "Exceptional — the profile of a coin that could run big from here (distinctive narrative, accelerating real demand)",
        ],
    },
    "narrative_quality": {
        "type": "score",
        "instructions": (
            "Judging only by the token's name, ticker, platform and any description, how much "
            "genuine effort and coherence does this memecoin/fair-launch project show?"
        ),
        "criteria": [
            "Gibberish or spam — random characters, obvious throwaway, no discernible idea",
            "Low-effort copy — a generic or derivative meme with nothing distinctive",
            "Coherent — a clear, recognizable theme executed competently",
            "Distinctive — an original or clever angle that stands out from typical launches",
        ],
    },
    "legitimacy": {
        "type": "noul",
        "instructions": "Does this look like a serious launch rather than a low-effort cash-grab or scam? Consider its social presence in `socials` (a real Twitter/website/Telegram is a mild positive; none at all is typical of throwaway launches).",
        "criteria": {
            "true": "Presents as a genuine project someone put thought into",
            "false": "Looks like a throwaway cash-grab, rug setup, or spam",
        },
    },
    "narrative_durability": {
        "type": "score",
        "instructions": (
            "How likely is this token's theme/narrative to have staying power beyond a brief hype "
            "spike, rather than being a fleeting copycat of a trend that will die within hours?"
        ),
        "criteria": [
            "Pure fad — tied to a momentary trend that will be forgotten almost immediately",
            "Short-lived — mild interest but little reason to persist",
            "Some staying power — a theme people may keep caring about",
            "Durable — a concept with real, lasting appeal",
        ],
    },
    "impersonation_risk": {
        "type": "noul",
        "instructions": (
            "Does this appear to impersonate, or be confusable with, an established token, brand, "
            "company, or real-world asset (e.g. tickers mimicking real stocks or major coins)?"
        ),
        "criteria": {
            "true": "Name/ticker mimics an established asset, brand, or well-known token",
            "false": "Clearly its own identity, not riding on an established name",
        },
    },
    "trap_risk": {
        "type": "noul",
        "instructions": (
            "Considering the on-chain picture in `holders`, `trust`, `market` and `momentum` — "
            "holder concentration, whether real ($1k+) positions exist, bundled wallets, dev rug "
            "history, and the buy/sell pattern — does this look like a coordinated pump, bundle, "
            "or rug trap rather than organic demand from genuine buyers?"
        ),
        "criteria": {
            "true": "Signals point to a trap: concentrated supply, bundled/sybil wallets, prior-rug dev, or one-sided bot buying with no real positions",
            "false": "Looks like organic participation: distributed holders, real positions, no rug fingerprints",
        },
    },
    "sufficient_info": {
        "type": "noul",
        "instructions": (
            "Setting aside the coin's quality — did you have ENOUGH information here to judge its "
            "potential well, or is important context missing (e.g. no description, no holder data, "
            "no volume/momentum, unclear what the project actually is)?"
        ),
        "criteria": {
            "true": "There was enough context to make a confident judgment",
            "false": "Key information was missing — the judgment is a guess with important gaps",
        },
    },
}


def _jev_reset_day_if_needed() -> None:
    """Roll the per-day counters when the date changes."""
    today = _today_key(time.time())
    if JEV_USAGE.get("day_key") != today:
        JEV_USAGE["day_key"] = today
        JEV_USAGE["calls_today"] = 0
        JEV_BUDGET_BLOCKED_SEEN.clear()
        # a new day re-opens the daily gate; the lifetime cap still applies
        if JEV_USAGE["calls_total"] < JEV_MAX_CALLS_TOTAL:
            JEV_USAGE["budget_exhausted"] = False


def jev_emit_event(event_type: str, *, ticker: Optional[str] = None,
                   token_address: Optional[str] = None, reason: str = "",
                   detail: Optional[dict[str, Any]] = None) -> None:
    """Record ONE Jev action of any kind and live-broadcast it, so nothing Jev
    does is invisible. event_type is one of: evaluated, skipped, vetoed, error,
    pvp_pick, pvp_skip. Safe to call from sync code — the broadcast is fired as
    a background task when a running loop exists, else it's just logged+stored."""
    evt = {
        "type": event_type,
        "ticker": ticker,
        "token_address": token_address,
        "reason": reason,
        "detail": detail or {},
        "ts": time.time(),
    }
    RECENT_JEV_EVENTS.append(evt)
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(broadcast_json({"kind": "jev_event", "payload": evt}))
    except RuntimeError:
        pass  # no running loop (e.g. unit test) — stored + will show on snapshot
    logger.debug(f"[jev/event] {event_type} {ticker or ''} {reason}")


# --- >$500k big-runner history (the reference set for PvP judgments) --------
def record_big_runner_if_qualified(token_address: str, entry: dict[str, Any], peak_mcap: float) -> None:
    """Called on peak updates. Once a coin's peak crosses BIG_RUNNER_MCAP_USD,
    record it (once) into the per-chain history Jev compares new cohorts to."""
    if token_address in STONKFUN_QUOTE_MINTS:
        return
    if peak_mcap < BIG_RUNNER_MCAP_USD or token_address in BIG_RUNNERS_SEEN:
        return
    chain = entry.get("chain") or "?"
    ticker = entry.get("ticker") or ""
    if not ticker or ticker == "UNKNOWN":
        return
    BIG_RUNNERS_SEEN.add(token_address)
    runners = BIG_RUNNERS[chain]
    runners.append({
        "token_address": token_address,
        "ticker": ticker,
        "narrative_key": narrative_cluster_key(normalize_ticker(ticker)),
        "platform": entry.get("platform") or "?",
        "peak_mcap": round(peak_mcap),
        "first_seen": entry.get("created_at"),
        "recorded_at": time.time(),
    })
    # keep newest / highest, bounded
    if len(runners) > BIG_RUNNERS_MAX_PER_CHAIN:
        runners.sort(key=lambda r: r.get("peak_mcap", 0), reverse=True)
        del runners[BIG_RUNNERS_MAX_PER_CHAIN:]
    logger.info(f"[big-runner] recorded {ticker} on {chain}/{entry.get('platform')} @ ${peak_mcap:,.0f}")


def _seed_big_runners() -> None:
    """Load BIG_RUNNERS_SEED ("chain:TICKER:platform,...") so history is useful
    before the tracker has observed its own big runs."""
    if not BIG_RUNNERS_SEED.strip():
        return
    for item in BIG_RUNNERS_SEED.split(","):
        parts = [p.strip() for p in item.split(":")]
        if len(parts) < 2 or not parts[0] or not parts[1]:
            continue
        chain, ticker = parts[0], parts[1].upper()
        platform = parts[2] if len(parts) > 2 else "?"
        BIG_RUNNERS[chain].append({
            "token_address": f"seed:{chain}:{ticker}",
            "ticker": ticker,
            "narrative_key": narrative_cluster_key(normalize_ticker(ticker)),
            "platform": platform,
            "peak_mcap": None,   # unknown for seeds
            "first_seen": None,
            "recorded_at": time.time(),
            "seed": True,
        })
    logger.info(f"[big-runner] seeded {sum(len(v) for v in BIG_RUNNERS.values())} reference runner(s)")


def big_runners_for_narrative(chain: str, narrative_key: str) -> list[dict[str, Any]]:
    """Past >$500k runners on this chain, same-name first, then the rest — the
    evidence block handed to Jev for a PvP comparison."""
    same_name = [r for r in BIG_RUNNERS.get(chain, []) if r.get("narrative_key") == narrative_key]
    others = [r for r in BIG_RUNNERS.get(chain, []) if r.get("narrative_key") != narrative_key]
    others.sort(key=lambda r: (r.get("peak_mcap") or 0), reverse=True)
    return same_name + others[:8]  # cap context size


def jev_budget_status() -> tuple[bool, Optional[str]]:
    """(can_call, reason_if_not). Central gate for all Jev spending."""
    if not JEV_ENABLED:
        return False, "TypeSafe API key not configured — Jev disabled"
    _jev_reset_day_if_needed()
    if JEV_USAGE["calls_total"] >= JEV_MAX_CALLS_TOTAL:
        return False, f"Lifetime call cap reached ({JEV_MAX_CALLS_TOTAL})"
    if JEV_USAGE["calls_today"] >= JEV_MAX_CALLS_PER_DAY:
        return False, f"Daily call cap reached ({JEV_MAX_CALLS_PER_DAY}/day)"
    # Not capped — clear any stale disabled_reason from an earlier cap/restart.
    if JEV_USAGE.get("disabled_reason"):
        JEV_USAGE["disabled_reason"] = None
    return True, None


async def jev_call_system_one(state: Any, questions: dict[str, Any]) -> Optional[dict[str, Any]]:
    """POST one batched evaluation to Jev. Returns the parsed JSON body, or None
    on ANY failure (never raises into the hot loop). Retries 429/529 with
    exponential backoff, as the docs recommend. Records token usage on success."""
    url = f"{TYPESAFE_API_BASE.rstrip('/')}/v1/systemone"
    headers = {
        "Authorization": f"Bearer {TYPESAFE_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {"state": state, "model": TYPESAFE_MODEL, "questions": questions}
    backoff = 1.0
    for attempt in range(1, JEV_MAX_RETRIES + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=JEV_REQUEST_TIMEOUT)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, headers=headers, json=payload) as resp:
                    if resp.status == 200:
                        body = await resp.json()
                        usage = body.get("usage", {}) or {}
                        JEV_USAGE["calls_total"] += 1
                        JEV_USAGE["calls_today"] += 1
                        JEV_USAGE["input_tokens_total"] += int(usage.get("input_tokens", 0) or 0)
                        JEV_USAGE["output_tokens_total"] += int(usage.get("output_tokens", 0) or 0)
                        if JEV_USAGE["calls_total"] >= JEV_MAX_CALLS_TOTAL:
                            JEV_USAGE["budget_exhausted"] = True
                            JEV_USAGE["disabled_reason"] = f"Lifetime call cap reached ({JEV_MAX_CALLS_TOTAL})"
                        return body
                    if resp.status in (429, 529) and attempt < JEV_MAX_RETRIES:
                        logger.warning(f"[jev] {resp.status} (attempt {attempt}) — backing off {backoff:.1f}s")
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        continue
                    # 401/422/other — non-retryable, log and fail safe
                    text = (await resp.text())[:300]
                    JEV_USAGE["errors"] += 1
                    JEV_USAGE["last_error"] = f"HTTP {resp.status}: {text}"
                    logger.warning(f"[jev] non-OK response {resp.status}: {text}")
                    return None
        except Exception as exc:
            JEV_USAGE["errors"] += 1
            JEV_USAGE["last_error"] = repr(exc)
            if attempt < JEV_MAX_RETRIES:
                await asyncio.sleep(backoff)
                backoff *= 2
                continue
            logger.warning(f"[jev] request failed after {attempt} attempts: {exc!r}")
            return None
    return None


def _jev_build_state(entry: dict[str, Any]) -> dict[str, Any]:
    """Assemble the token metadata + on-chain reality Jev reasons over. Named
    JSON fields, as the docs advise for multi-part context. Only observed
    facts. This is the big upgrade: Jev now sees holder quality, concentration,
    dev history, buy/sell pressure and momentum — not just the name."""
    state: dict[str, Any] = {
        "name": entry.get("name") or entry.get("token_name") or "",
        "ticker": entry.get("ticker") or "",
        "chain": entry.get("chain") or "",
        "platform": entry.get("platform") or "",
    }
    desc = entry.get("description") or entry.get("token_description")
    # free social/profile data from DexScreener token-profiles cache
    prof = TOKEN_PROFILES.get(entry.get("token_address") or "")
    if not desc and prof and prof.get("description"):
        desc = prof["description"]   # fall back to the profile's description
    if desc:
        state["description"] = str(desc)[:1000]
    # social presence — a coin with real socials (twitter/website/telegram) is
    # more legit than one with none. Not sentiment (X API isn't free), but a
    # genuine presence signal Jev can weigh.
    if prof is not None:
        state["socials"] = {
            "has_twitter": "twitter" in prof.get("socials", []),
            "has_telegram": "telegram" in prof.get("socials", []),
            "has_website": ("website" in prof.get("socials", []) or "web" in prof.get("socials", [])),
            "link_count": prof.get("link_count", 0),
        }
    else:
        state["socials"] = {"has_twitter": False, "has_telegram": False, "has_website": False, "link_count": 0, "note": "no social profile found"}

    mcap = entry.get("market_cap") or 0.0
    vol = entry.get("volume_24h") or 0.0
    market = {
        "market_cap_usd": round(mcap),
        "volume_24h_usd": round(vol),
        "volume_to_mcap_ratio": round(vol / mcap, 3) if mcap > 0 else None,
        "liquidity_usd": round(entry.get("liquidity_usd") or 0),
        "age_minutes": round((time.time() - (entry.get("created_at") or time.time())) / 60, 1),
    }
    buys = entry.get("buys_24h") or 0
    sells = entry.get("sells_24h") or 0
    if buys or sells:
        market["buys_vs_sells"] = f"{buys} buys / {sells} sells"
    state["market"] = market

    # Holder quality — including the substantial-holder ($1k+ position) signal.
    holders: dict[str, Any] = {}
    if entry.get("holder_count") is not None:
        holders["holder_count"] = entry.get("holder_count")
    if entry.get("top_holder_pct") is not None:
        holders["top_holder_pct_of_supply"] = round(entry.get("top_holder_pct"), 1)
    if entry.get("substantial_holders_1k") is not None:
        holders["holders_with_1k_plus_position"] = entry.get("substantial_holders_1k")
        holders["holders_with_10k_plus_position"] = entry.get("substantial_holders_10k")
    if entry.get("top_holder_selling"):
        holders["top_holder_is_selling_down"] = True
    if holders:
        state["holders"] = holders

    # Bundle / dev-trust facts.
    trust: dict[str, Any] = {}
    if entry.get("bundle_detected"):
        trust["bundled_wallets_at_launch"] = entry.get("bundle_wallet_count", 0)
        trust["bundle_supply_pct"] = round(entry.get("bundle_supply_pct") or 0, 1)
        if entry.get("bundle_known_bad_operator"):
            trust["bundle_operator_linked_to_prior_rug"] = True
    if entry.get("dev_rugs"):
        trust["dev_prior_rugs"] = entry.get("dev_rugs")
    if entry.get("dev_moons"):
        trust["dev_prior_graduations"] = entry.get("dev_moons")
    if trust:
        state["trust"] = trust

    # Momentum trajectory from the sparkline (mcap over recent polls).
    spark = entry.get("sparkline") or []
    if len(spark) >= 2:
        first, last = spark[0][1], spark[-1][1]
        if first and first > 0:
            state["momentum"] = {
                "mcap_trajectory_usd": [round(p[1]) for p in spark[-6:]],
                "pct_change_recent": round((last - first) / first * 100),
            }
    return state


def _jev_parse_answers(body: dict[str, Any]) -> dict[str, Any]:
    """Turn the raw API response into a compact, display-ready judgment dict:
    per-dimension value + confidence + probabilities, kept for the learning loop."""
    answers = body.get("answers", {}) or {}
    parsed: dict[str, Any] = {
        "model": body.get("model"),
        "evaluated_at": time.time(),
        "dimensions": {},
        "usage": body.get("usage", {}) or {},
    }
    for qid, ans in answers.items():
        atype = ans.get("type")
        if atype == "score":
            parsed["dimensions"][qid] = {
                "type": "score",
                "score": ans.get("score"),
                "legend": ans.get("legend", {}),
                "probabilities": ans.get("probabilities", {}),
                "confidence": ans.get("confidence"),
                "n_levels": len(ans.get("legend", {}) or {}),
            }
        elif atype == "noul":
            parsed["dimensions"][qid] = {
                "type": "noul",
                "noul": ans.get("noul"),
            }
    return parsed


async def maybe_screen_early_momentum_with_jev(token_address: str, entry: dict[str, Any]) -> None:
    """Stage-1 Jev Screening Cascade: A fast, lightweight 2-question pre-evaluation
    (impersonation_risk + narrative_quality) on early micro-cap tokens ($15k-$30k mcap)
    that show early traction before clearing the full $30k/40-score opportunity gate.
    Catches viral narratives 10 minutes earlier, vetoes disguised impersonators before
    they reach the main feed, and primes the semantic cache."""
    if not JEV_ENABLED or entry.get("status") != "WATCHING":
        return
    if token_address in JEV_SCREENED_TOKENS or token_address in JEV_EVALUATED_TOKENS:
        return

    ticker = entry.get("ticker")
    if not ticker or ticker == "UNKNOWN" or ticker_is_invalid(ticker)[0]:
        return

    mcap = entry.get("market_cap") or 0.0
    early_score = entry.get("early_momentum_score") or 0
    # Gate: micro-cap with early relative momentum ($15k-$30k band, score >= 35)
    if mcap < 15000 or mcap >= JEV_MIN_MCAP_TO_EVALUATE or early_score < 35:
        return

    can_call, reason = jev_budget_status()
    if not can_call:
        return

    # Check semantic cache first (instant 0-token resolution!)
    sem_hash = _compute_semantic_hash(ticker, entry.get("name"), entry.get("description"))
    cached = JEV_SEMANTIC_CACHE.get(sem_hash)
    if cached and (time.time() - cached.get("cached_at", 0)) < JEV_SEMANTIC_CACHE_TTL:
        dims = dict(cached.get("dimensions", {}))
        imp = dims.get("impersonation_risk", {}).get("noul")
        if imp is not None and imp >= JEV_IMPERSONATION_VETO:
            info = TOKEN_WATCHLIST.get(token_address)
            if info is not None:
                await _kick_out_watchlist_token(
                    token_address, info, time.time(), "JEV_IMPERSONATION",
                    f"SKIPPED - NOT A NEW LAUNCH (Jev cached: {imp:.0%} impersonation risk)",
                )
            jev_emit_event("vetoed", ticker=ticker, token_address=token_address,
                           reason=f"Stage-1 cached screen veto — {imp:.0%} impersonation risk")
            return
        entry["jev_screen"] = dims
        JEV_SCREENED_TOKENS[token_address] = time.time()
        _rescore_token(token_address)
        return

    # Not in cache — perform a 2-question Stage-1 call
    JEV_SCREENED_TOKENS[token_address] = time.time()
    if len(JEV_SCREENED_TOKENS) > JEV_SCREENED_MAX:
        JEV_SCREENED_TOKENS.pop(next(iter(JEV_SCREENED_TOKENS)), None)

    # Ensure description is checked if pump.fun
    if entry.get("platform") == "pump.fun" and not entry.get("description"):
        try:
            meta = await fetch_pumpfun_image(token_address)
            if meta:
                upd = {}
                if meta.get("image_uri") and not entry.get("image_url"):
                    upd["image_url"] = meta["image_uri"]
                if meta.get("name") and not entry.get("name"):
                    upd["name"] = meta["name"]
                if meta.get("description"):
                    upd["description"] = meta["description"][:1000]
                if upd:
                    token_feed_upsert(token_address, **upd)
                    entry.update(upd)
                    if token_address in TOKEN_WATCHLIST:
                        TOKEN_WATCHLIST[token_address].update(upd)
        except Exception:
            pass

    state = _jev_build_state(entry)
    screening_questions = {
        "impersonation_risk": JEV_QUESTIONS["impersonation_risk"],
        "narrative_quality": JEV_QUESTIONS["narrative_quality"],
    }
    body = await jev_call_system_one(state, screening_questions)
    if not body:
        JEV_SCREENED_TOKENS.pop(token_address, None)
        return

    judgment = _jev_parse_answers(body)
    dims = judgment.get("dimensions", {})

    # Populate semantic cache with invariant dimensions
    JEV_SEMANTIC_CACHE[sem_hash] = {
        "dimensions": dims,
        "cached_at": time.time(),
        "ticker": ticker,
    }
    if len(JEV_SEMANTIC_CACHE) > JEV_SEMANTIC_CACHE_MAX:
        JEV_SEMANTIC_CACHE.pop(next(iter(JEV_SEMANTIC_CACHE)), None)

    # Check impersonation veto
    imp = dims.get("impersonation_risk", {}).get("noul")
    if imp is not None and imp >= JEV_IMPERSONATION_VETO:
        info = TOKEN_WATCHLIST.get(token_address)
        if info is not None:
            await _kick_out_watchlist_token(
                token_address, info, time.time(), "JEV_IMPERSONATION",
                f"SKIPPED - NOT A NEW LAUNCH (Jev Stage-1: {imp:.0%} impersonation risk)",
            )
        jev_emit_event("vetoed", ticker=ticker, token_address=token_address,
                       reason=f"Stage-1 screen veto — Jev flagged {imp:.0%} impersonation risk")
        return

    entry["jev_screen"] = dims
    _rescore_token(token_address)
    logger.info(f"[jev/screen] Stage-1 screened {ticker} ({token_address[:8]}) — narrative_quality={dims.get('narrative_quality', {}).get('score')}")


async def maybe_evaluate_token_with_jev(token_address: str, entry: dict[str, Any]) -> None:
    """Gate + call + cache. Called from the rescore path. Spends AT MOST one Jev
    call per token, and only for candidates that clear the cost gate. Mutates
    entry['jev'] in place and logs the judgment for the learning loop."""
    if token_address in JEV_EVALUATED_TOKENS:
        return  # already judged once — reuse the cached entry['jev'] (no event: not an action)

    ticker = entry.get("ticker")
    can_call, reason = jev_budget_status()
    if not can_call:
        JEV_USAGE["disabled_reason"] = reason
        # Emit a budget-block event only ONCE per token — otherwise a capped day
        # floods the log with the same token every poll. Count the rest silently.
        if token_address not in JEV_BUDGET_BLOCKED_SEEN:
            JEV_BUDGET_BLOCKED_SEEN.add(token_address)
            jev_emit_event("skipped", ticker=ticker, token_address=token_address,
                           reason=f"budget/disabled: {reason}")
        else:
            JEV_USAGE["skipped_budget"] = JEV_USAGE.get("skipped_budget", 0) + 1
        return  # fail-safe / budget guard: token scores on deterministic signals only

    # Cost gate: only genuinely promising, identifiable candidates cost a call.
    # NOTE: "not ready yet" skips (no ticker / mcap or score below gate) are the
    # overwhelming majority and are NOT meaningful Jev decisions — they'd drown
    # the event log. We count them in a summary counter but don't emit an event.
    if not ticker or ticker == "UNKNOWN":
        JEV_USAGE["skipped_not_ready"] = JEV_USAGE.get("skipped_not_ready", 0) + 1
        return
    # Safety net: never evaluate established coins / tokenized stocks even if one
    # leaked into the feed — these aren't new fair-launches (user doesn't want them).
    est, est_reason = is_probably_established_or_stock(
        ticker,
        entry.get("market_cap") or 0.0,
        platform=entry.get("platform"),
        token_address=token_address,
    )
    if est:
        jev_emit_event("skipped", ticker=ticker, token_address=token_address,
                       reason=f"not a new launch: {est_reason}")
        return
    if (entry.get("market_cap") or 0) < JEV_MIN_MCAP_TO_EVALUATE:
        JEV_USAGE["skipped_not_ready"] = JEV_USAGE.get("skipped_not_ready", 0) + 1
        return
    if (entry.get("opportunity_score") or 0) < JEV_MIN_SCORE_TO_EVALUATE:
        JEV_USAGE["skipped_not_ready"] = JEV_USAGE.get("skipped_not_ready", 0) + 1
        return

    # Mark BEFORE the call so a slow/failed call can't cause a duplicate spend
    # if this token is rescored again while the request is in flight.
    JEV_EVALUATED_TOKENS[token_address] = time.time()
    if len(JEV_EVALUATED_TOKENS) > JEV_EVALUATED_MAX:
        JEV_EVALUATED_TOKENS.pop(next(iter(JEV_EVALUATED_TOKENS)), None)

    # Make sure Jev actually SEES the coin's description before judging it.
    # The pump.fun description is normally fetched fire-and-forget by the poll
    # loop, which often hasn't completed by the time a coin clears the gate — so
    # Jev was judging narrative/quality blind. Fetch it now (awaited, fail-safe)
    # so the description informs the judgment, then rebuild state with it.
    if entry.get("platform") == "pump.fun" and not entry.get("description"):
        try:
            meta = await fetch_pumpfun_image(token_address)
            if meta:
                upd = {}
                if meta.get("image_uri") and not entry.get("image_url"):
                    upd["image_url"] = meta["image_uri"]
                if meta.get("name") and not entry.get("name"):
                    upd["name"] = meta["name"]
                if meta.get("description"):
                    upd["description"] = meta["description"][:1000]
                if upd:
                    token_feed_upsert(token_address, **upd)
                    entry.update(upd)
                    if token_address in TOKEN_WATCHLIST:
                        TOKEN_WATCHLIST[token_address].update(upd)
        except Exception:
            pass

    # Semantic deduplication check: reuse invariant qualitative dimensions if cached
    sem_hash = _compute_semantic_hash(ticker, entry.get("name"), entry.get("description"))
    cached_sem = JEV_SEMANTIC_CACHE.get(sem_hash)
    cached_dims: dict[str, Any] = {}
    if cached_sem and (time.time() - cached_sem.get("cached_at", 0)) < JEV_SEMANTIC_CACHE_TTL:
        cached_dims = dict(cached_sem.get("dimensions", {}))
        # If cached impersonation is already a veto, execute veto immediately with 0 API tokens spent!
        cached_imp = cached_dims.get("impersonation_risk", {}).get("noul")
        if cached_imp is not None and cached_imp >= JEV_IMPERSONATION_VETO:
            info = TOKEN_WATCHLIST.get(token_address)
            if info is not None:
                await _kick_out_watchlist_token(
                    token_address, info, time.time(), "JEV_IMPERSONATION",
                    f"SKIPPED - NOT A NEW LAUNCH (Jev cached: {cached_imp:.0%} impersonation risk)",
                )
            jev_emit_event("vetoed", ticker=ticker, token_address=token_address,
                           reason=f"Cached veto — {cached_imp:.0%} impersonation risk")
            return

    state = _jev_build_state(entry)
    # Merge in any candidate (proposed) questions being tested — they ride the
    # same single call at no extra request cost (docs: batch everything).
    proposed = jev_active_proposed_questions()

    # Invariant questions (narrative quality/durability/impersonation) can be skipped if already cached
    questions_sent: dict[str, Any] = {}
    for qid, q in JEV_QUESTIONS.items():
        if qid in cached_dims and qid in ("narrative_quality", "narrative_durability", "impersonation_risk"):
            continue
        questions_sent[qid] = q
    questions_sent.update(proposed)

    body = await jev_call_system_one(state, questions_sent)
    if body is None:
        # Failed — allow a future retry by clearing the mark (still budget-gated).
        JEV_EVALUATED_TOKENS.pop(token_address, None)
        jev_emit_event("error", ticker=ticker, token_address=token_address,
                       reason=JEV_USAGE.get("last_error") or "API call failed")
        return

    judgment = _jev_parse_answers(body)
    # Merge cached invariant dimensions back into judgment
    if cached_dims:
        for k, v in cached_dims.items():
            if k not in judgment["dimensions"]:
                judgment["dimensions"][k] = v

    # Update semantic cache with invariant dimensions
    new_invariants = {
        k: judgment["dimensions"][k]
        for k in ("narrative_quality", "narrative_durability", "impersonation_risk")
        if k in judgment["dimensions"]
    }
    if new_invariants:
        JEV_SEMANTIC_CACHE[sem_hash] = {
            "dimensions": new_invariants,
            "cached_at": time.time(),
            "ticker": ticker,
        }
        if len(JEV_SEMANTIC_CACHE) > JEV_SEMANTIC_CACHE_MAX:
            JEV_SEMANTIC_CACHE.pop(next(iter(JEV_SEMANTIC_CACHE)), None)

    # GATEKEEPER: if Jev flags high impersonation risk, this is an established
    # coin / tokenized stock / brand impersonator (e.g. RBLX, NVDA, RDDT) that
    # slipped past the cheap pre-filters. Purge it — don't show it as an
    # evaluation, don't log it, kick it from the watchlist. Jev IS the real
    # gatekeeper here; the hardcoded blocklist is just a cheap first pass.
    imp = (judgment.get("dimensions", {}).get("impersonation_risk") or {}).get("noul")
    if imp is not None and imp >= JEV_IMPERSONATION_VETO:
        JEV_JUDGMENT_LOG.pop(token_address, None)
        info = TOKEN_WATCHLIST.get(token_address)
        if info is not None:
            await _kick_out_watchlist_token(
                token_address, info, time.time(), "JEV_IMPERSONATION",
                f"SKIPPED - NOT A NEW LAUNCH (Jev: {imp:.0%} impersonation risk)",
            )
        jev_emit_event("vetoed", ticker=ticker, token_address=token_address,
                       reason=f"purged — Jev flagged {imp:.0%} impersonation risk (established/stock/brand)")
        await broadcast_json({"kind": "jev_stats", "payload": build_jev_stats()})
        logger.info(f"[jev] purged {ticker} ({token_address[:8]}) — impersonation {imp:.0%}")
        return
    entry["jev"] = judgment
    _jev_log_judgment(token_address, entry, judgment)
    # Record proposed-question answers for measurement against outcome.
    if proposed:
        _jev_record_proposed_answers(token_address, judgment.get("dimensions", {}))
    # Recompute so the Jev contribution lands in the visible score immediately.
    _rescore_token(token_address)
    # Build a rich, display-ready activity record and push it to the dashboard
    # so the operator can see EXACTLY what Jev looked at and concluded — including
    # the RAW request (state + questions) that was sent.
    activity = _jev_build_activity_record(token_address, entry, judgment)
    activity["raw_request"] = {"state": state, "questions": questions_sent}
    RECENT_JEV_JUDGMENTS.append(activity)
    jev_emit_event("vetoed" if activity["veto"] else "evaluated", ticker=ticker,
                   token_address=token_address,
                   reason=("VETO — " + (activity["reasons"][0] if activity["reasons"] else "")) if activity["veto"]
                          else f"contribution {activity['contribution']:+d} pts",
                   detail={"contribution": activity["contribution"], "dimensions": activity["dimensions"],
                           "links": entry.get("links") or {}})
    await broadcast_json({"kind": "jev_judgment", "payload": activity})
    await broadcast_json({"kind": "jev_stats", "payload": build_jev_stats()})
    logger.info(
        f"[jev] evaluated {ticker} ({token_address[:8]}) — "
        f"{len(judgment['dimensions'])} dims, contribution {activity['contribution']:+d}"
        f"{' VETO' if activity['veto'] else ''}"
    )


def _jev_build_activity_record(token_address: str, entry: dict[str, Any], judgment: dict[str, Any]) -> dict[str, Any]:
    """Flatten one judgment into everything the UI needs to show it in full:
    the input state Jev saw, each dimension's value/confidence/probabilities,
    the points it contributed, the reasons, and the verdict."""
    pts, reasons, veto = jev_score_contribution(entry)
    dims_out: dict[str, Any] = {}
    for qid, d in judgment.get("dimensions", {}).items():
        if d.get("type") == "score":
            dims_out[qid] = {
                "type": "score",
                "score": d.get("score"),
                "n_levels": d.get("n_levels"),
                "confidence": d.get("confidence"),
                "legend": d.get("legend", {}),
                "probabilities": d.get("probabilities", {}),
            }
        elif d.get("type") == "noul":
            dims_out[qid] = {"type": "noul", "noul": d.get("noul")}
    return {
        "token_address": token_address,
        "ticker": entry.get("ticker"),
        "chain": entry.get("chain"),
        "platform": entry.get("platform"),
        "model": judgment.get("model"),
        "evaluated_at": judgment.get("evaluated_at"),
        "state_seen": _jev_build_state(entry),   # exactly what Jev was shown
        "mcap_at_eval": entry.get("market_cap"),
        "opportunity_score": entry.get("opportunity_score"),
        "dimensions": dims_out,
        "contribution": int(pts),
        "reasons": reasons,
        "veto": veto,
        "usage": judgment.get("usage", {}),
        "image_url": entry.get("image_url"),
        "links": entry.get("links") or {},
    }


# --- PvP: same-name cohort assembly + comparative CHOICE --------------------
def assemble_same_name_cohort(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """Gather same-name candidates from NARRATIVE_CACHE — the coins competing
    for this narrative. Only coins that have ALREADY had their run are excluded:
    GRADUATED (the move already happened) and RUGGED (dead). A coin that's still
    live is kept even if it's already sizable — a big-but-climbing coin with a
    strong narrative can still be a play, and Jev's moon_potential judgment
    decides that, not a blunt mcap cutoff."""
    ticker = entry.get("ticker") or ""
    key = narrative_cluster_key(normalize_ticker(ticker))
    if not key:
        return []
    events = NARRATIVE_CACHE.get(key, [])
    cohort: dict[str, dict[str, Any]] = {}
    now = time.time()
    for ts, chain, platform, dev_wallet, token_address in events:
        fe = TOKEN_FEED.get(token_address, {})
        status = fe.get("status", "WATCHING")
        mcap = fe.get("market_cap", 0.0)
        # Only exclude coins whose run is already over — not big live ones.
        if status in ("GRADUATED", "RUGGED"):
            continue
        cohort[token_address] = {
            "token_address": token_address,
            "ticker": fe.get("ticker") or ticker,
            "chain": chain,
            "platform": platform,
            "age_seconds": now - (fe.get("created_at") or ts),
            "market_cap": mcap,
            "volume_24h": fe.get("volume_24h", 0.0),
            "status": status,
            "links": fe.get("links") or build_token_links(chain, token_address),
        }
    return sorted(cohort.values(), key=lambda c: c.get("market_cap", 0), reverse=True)


def _cohort_signature(cohort: list[dict[str, Any]]) -> str:
    """Stable signature so a PvP call is re-spent only when the cohort changes
    materially (membership or leader). Buckets mcap so tiny drifts don't churn."""
    parts = []
    for c in sorted(cohort, key=lambda x: x["token_address"]):
        parts.append(f"{c['token_address'][:10]}:{int((c.get('market_cap') or 0) // 50000)}")
    return "|".join(parts)


def _build_pvp_state(entry: dict[str, Any], cohort: list[dict[str, Any]], history: list[dict[str, Any]]) -> dict[str, Any]:
    """State for the comparative call: the same-name cohort as options + the
    per-chain >$500k runner history as grounding evidence."""
    return {
        "narrative": entry.get("ticker") or "",
        "chain": entry.get("chain"),
        "candidates": [
            {
                "id": f"opt_{i}",
                "platform": c["platform"],
                "chain": c["chain"],
                "age_minutes": round((c["age_seconds"] or 0) / 60, 1),
                "market_cap_usd": round(c.get("market_cap") or 0),
                "volume_24h_usd": round(c.get("volume_24h") or 0),
                "status": c["status"],
                "is_oldest": False,  # set below
            }
            for i, c in enumerate(cohort)
        ],
        "past_500k_runners_same_name": [
            {"platform": r["platform"], "peak_mcap_usd": r.get("peak_mcap")}
            for r in history if r.get("narrative_key") == narrative_cluster_key(normalize_ticker(entry.get("ticker") or ""))
        ],
        "past_500k_runners_this_chain": [
            {"ticker": r["ticker"], "platform": r["platform"], "peak_mcap_usd": r.get("peak_mcap")}
            for r in history
        ],
    }


async def maybe_run_pvp_choice(token_address: str, entry: dict[str, Any]) -> None:
    """When a same-name cohort exists (PvP), ask Jev to PICK which coin is the
    best play, grounded in the chain's >$500k runner history. Cost-gated +
    cached per cohort signature so it only spends when the field changes."""
    can_call, reason = jev_budget_status()
    if not can_call:
        return
    cohort = assemble_same_name_cohort(entry)
    if len(cohort) < JEV_PVP_MIN_COHORT:
        return  # not a PvP situation — a single coin needs no comparison (normal, no event)

    # Gate: at least one cohort member must have real mcap — no point spending a
    # comparative call to pick between several dust coins.
    if max((c.get("market_cap") or 0) for c in cohort) < JEV_PVP_MIN_MCAP:
        return

    sig = _cohort_signature(cohort)
    key = narrative_cluster_key(normalize_ticker(entry.get("ticker") or ""))
    cached = JEV_PVP_CACHE.get(key)
    if cached and cached.get("signature") == sig:
        return  # cohort unchanged since last pick — reuse (cached, no re-spend, no event)
    # Cooldown: even if the cohort drifted, don't re-run the same narrative too
    # often — a churning cohort was re-spending PvP calls every poll.
    if cached and (time.time() - (cached.get("evaluated_at") or 0)) < JEV_PVP_COOLDOWN_SECONDS:
        return
    # RACE GUARD: the Jev call below is awaited (~1-2s). Without reserving the
    # key NOW, several cohort members rescored in the same instant all pass the
    # checks above (cache still empty) and each fires a duplicate call for the
    # SAME narrative. Reserve synchronously before any await.
    if key in JEV_PVP_INFLIGHT:
        return
    JEV_PVP_INFLIGHT.add(key)

    # mark the oldest candidate (the "old coin might have the edge" case)
    if cohort:
        oldest_idx = max(range(len(cohort)), key=lambda i: cohort[i]["age_seconds"])
    else:
        oldest_idx = -1
    chain = entry.get("chain") or "?"
    history = big_runners_for_narrative(chain, key)
    state = _build_pvp_state(entry, cohort, history)
    if oldest_idx >= 0 and oldest_idx < len(state["candidates"]):
        state["candidates"][oldest_idx]["is_oldest"] = True

    # Build a dynamic CHOICE: one option per cohort member + an explicit "none".
    criteria: dict[str, Any] = {}
    for i, c in enumerate(cohort):
        criteria[f"opt_{i}"] = (
            f"{c['platform']} on {c['chain']}, "
            f"{'OLDEST/pre-existing' if i == oldest_idx else 'newer'}, "
            f"mcap ${round(c.get('market_cap') or 0):,}, vol ${round(c.get('volume_24h') or 0):,}"
        )
    criteria["none"] = "None is a clear pick yet — too early or none stands out"
    questions = {
        "best_pick": {
            "type": "choice",
            "instructions": (
                "Several coins share this narrative/name across launchpads (platform-vs-platform). "
                "Considering each candidate's launchpad, age, market cap and volume — and the history "
                "of coins that previously ran above $500k on this chain — which single coin is the best "
                "play? Note that an older, pre-existing coin of the same name often captures the "
                "attention better than a fresh copycat."
            ),
            "criteria": criteria,
        }
    }
    body = await jev_call_system_one(state, questions)
    if body is None:
        JEV_PVP_INFLIGHT.discard(key)
        jev_emit_event("error", ticker=entry.get("ticker"), token_address=token_address,
                       reason=f"PvP call failed: {JEV_USAGE.get('last_error') or 'unknown'}")
        return
    ans = (body.get("answers") or {}).get("best_pick", {})
    picked_id = ans.get("choice")
    picked_idx = int(picked_id.split("_")[1]) if (picked_id or "").startswith("opt_") else None
    picked_addr = cohort[picked_idx]["token_address"] if picked_idx is not None and picked_idx < len(cohort) else None

    # Jev's CHOICE returns typed answers, not prose — so derive the reasoning
    # from WHAT distinguished the pick: age (old-coin edge), mcap lead, and
    # whether same-name history exists on this chain, plus the probability gap.
    probs = ans.get("probabilities", {})
    reasoning = ""
    if picked_idx is not None and picked_idx < len(cohort):
        pk = cohort[picked_idx]
        factors = []
        if oldest_idx == picked_idx:
            factors.append("it's the OLDEST/pre-existing coin of this name (attention tends to flow to the established one, not fresh copycats)")
        leader = max(range(len(cohort)), key=lambda i: cohort[i].get("market_cap") or 0)
        if leader == picked_idx:
            factors.append(f"it has the highest market cap of the cohort (${round(pk.get('market_cap') or 0):,})")
        same_name_hist = [r for r in history if r.get("narrative_key") == key]
        if same_name_hist:
            factors.append(f"this name has run >$500k before on {chain} ({len(same_name_hist)} time(s))")
        # probability margin over runner-up
        sorted_p = sorted((v for v in probs.values()), reverse=True)
        if len(sorted_p) >= 2:
            factors.append(f"Jev assigned it {sorted_p[0]*100:.0f}% vs {sorted_p[1]*100:.0f}% for the next best")
        reasoning = (
            f"Jev picked the {pk['platform']} coin"
            + (" — " + "; ".join(factors) if factors else "")
            + "."
        )
    elif picked_id == "none":
        reasoning = "Jev judged no coin in this cohort a clear pick yet (too early or none stands out)."

    result = {
        "signature": sig,
        "model": body.get("model"),
        "evaluated_at": time.time(),
        "narrative_key": key,
        "chain": chain,
        "picked_id": picked_id,
        "picked_address": picked_addr,
        "probabilities": probs,
        "confidence": ans.get("confidence"),
        "reasoning": reasoning,
        "cohort": cohort,
        "state_seen": state,
        "usage": body.get("usage", {}),
    }
    JEV_PVP_CACHE[key] = result
    _jev_log_pvp(key, result)
    if len(JEV_PVP_CACHE) > JEV_PVP_CACHE_MAX:
        JEV_PVP_CACHE.pop(next(iter(JEV_PVP_CACHE)), None)

    # Tag every cohort member's feed entry with the pick so scoring + UI see it.
    for i, c in enumerate(cohort):
        fe = TOKEN_FEED.get(c["token_address"])
        if fe is not None:
            fe["jev_pvp"] = {
                "is_pick": (c["token_address"] == picked_addr),
                "picked_ticker": entry.get("ticker"),
                "picked_platform": (cohort[picked_idx]["platform"] if picked_idx is not None and picked_idx < len(cohort) else None),
                "confidence": result["confidence"],
                "cohort_size": len(cohort),
                "probability": result["probabilities"].get(f"opt_{i}"),
            }
        _rescore_token(c["token_address"])

    RECENT_JEV_JUDGMENTS.append({
        "kind": "pvp",
        "token_address": picked_addr or token_address,
        "ticker": entry.get("ticker"),
        "chain": chain,
        "model": result["model"],
        "evaluated_at": result["evaluated_at"],
        "picked_platform": (cohort[picked_idx]["platform"] if picked_idx is not None and picked_idx < len(cohort) else "none"),
        "picked_address": picked_addr,
        "confidence": result["confidence"],
        "reasoning": result["reasoning"],
        "probabilities": result["probabilities"],
        "cohort": cohort,
        "history_count": len(history),
        "usage": result["usage"],
        "raw_request": {"state": state, "questions": questions},
        "links": (TOKEN_FEED.get(picked_addr) or {}).get("links") or (entry.get("links") or {}),
    })
    picked_platform = (cohort[picked_idx]["platform"] if picked_idx is not None and picked_idx < len(cohort) else "none")
    jev_emit_event("pvp_pick", ticker=entry.get("ticker"), token_address=picked_addr or token_address,
                   reason=f"picked {picked_platform} out of {len(cohort)} same-name coins (conf {(result['confidence'] or 0):.0%})",
                   detail={"cohort_size": len(cohort), "picked_platform": picked_platform,
                           "probabilities": result["probabilities"],
                           "links": (TOKEN_FEED.get(picked_addr) or {}).get("links") or {}})
    await broadcast_json({"kind": "jev_pvp", "payload": RECENT_JEV_JUDGMENTS[-1]})
    await broadcast_json({"kind": "jev_stats", "payload": build_jev_stats()})
    logger.info(
        f"[jev/pvp] {entry.get('ticker')} cohort={len(cohort)} → pick={picked_id} "
        f"({'none' if not picked_addr else picked_addr[:8]}) conf={result['confidence']}"
    )
    JEV_PVP_INFLIGHT.discard(key)


def jev_score_contribution(entry: dict[str, Any]) -> tuple[int, list[str], bool]:
    """Convert a cached Jev judgment into signed, confidence-scaled opportunity
    points + human-readable reasons. Returns (points, reasons, hard_veto).
    Policy (weights, veto) lives here in code, not in the model."""
    jev = entry.get("jev")
    pvp = entry.get("jev_pvp")
    # Nothing from Jev at all → no contribution. But a token can have a PvP
    # pick without its own per-token judgment (it's part of a cohort), so we
    # only bail when BOTH are absent.
    if (not jev or not jev.get("dimensions")) and not pvp:
        return 0, [], False
    dims = (jev or {}).get("dimensions", {}) if jev else {}
    points = 0
    reasons: list[str] = []

    # Impersonation is the hard-veto path (like blacklist / implausible mcap).
    imp = dims.get("impersonation_risk", {})
    imp_p = imp.get("noul")
    if imp_p is not None and imp_p >= JEV_IMPERSONATION_VETO:
        reasons.append(f"Jev: high impersonation risk ({imp_p:.0%}) — looks like it mimics an established asset (VETO)")
        return 0, reasons, True

    # Trap/rug risk — a second hard-veto path, grounded in the enriched
    # on-chain state; below the veto it contributes scaled negative points.
    trap = dims.get("trap_risk", {})
    trap_p = trap.get("noul")
    if trap_p is not None:
        if trap_p >= JEV_TRAP_RISK_VETO:
            reasons.append(f"Jev: high trap/rug risk ({trap_p:.0%}) — on-chain signals look like a coordinated pump/rug (VETO)")
            return 0, reasons, True
        if trap_p > 0.5:
            pts = round((trap_p - 0.5) * 2 * JEV_WEIGHT_TRAP_RISK * JEV_LEARNED_WEIGHTS.get("trap_risk", 1.0))
            if pts:
                points -= pts
                reasons.append(f"Jev: elevated trap/rug risk ({trap_p:.0%}) (-{pts})")

    def _score_points(qid: str, weight: float, label: str) -> None:
        nonlocal points
        d = dims.get(qid)
        if not d or d.get("score") is None:
            return
        n_levels = d.get("n_levels") or 0
        if n_levels < 2:
            return
        # Normalize the 0..(n-1) level to a -1..+1 axis (bottom levels negative,
        # top levels positive), then scale by weight, learned multiplier, and conf.
        norm = (d["score"] / (n_levels - 1)) * 2 - 1  # -1..+1
        conf = d.get("confidence")
        conf = 1.0 if conf is None else max(0.0, min(1.0, conf))
        mult = JEV_LEARNED_WEIGHTS.get(qid, 1.0)  # Level C auto-tune (1.0 until learned)
        pts = round(norm * weight * mult * conf)
        if pts:
            points += pts
            sign = "+" if pts > 0 else ""
            mtxt = f", ×{mult:g} learned" if mult != 1.0 else ""
            reasons.append(
                f"Jev: {label} {d['score']:.1f}/{n_levels - 1} (conf {conf:.0%}{mtxt}) ({sign}{pts})"
            )

    _score_points("narrative_quality", JEV_WEIGHT_NARRATIVE_QUALITY, "narrative quality")
    _score_points("narrative_durability", JEV_WEIGHT_DURABILITY, "narrative durability")
    _score_points("moon_potential", JEV_WEIGHT_MOON_POTENTIAL, "moon potential")

    # Legitimacy is a Noul (0..1): map to a -weight..+weight band around 0.5.
    legit = dims.get("legitimacy", {})
    legit_p = legit.get("noul")
    if legit_p is not None:
        _lm = JEV_LEARNED_WEIGHTS.get("legitimacy", 1.0)
        pts = round((legit_p - 0.5) * 2 * JEV_WEIGHT_LEGITIMACY * _lm)
        if pts:
            points += pts
            sign = "+" if pts > 0 else ""
            reasons.append(f"Jev: legitimacy {legit_p:.0%} ({sign}{pts})")

    # PvP comparative pick (see maybe_run_pvp_choice): if this coin is part of a
    # same-name cohort, reward it for being Jev's pick and lightly penalize the
    # ones it wasn't — confidence-scaled.
    pvp = entry.get("jev_pvp")
    if pvp and pvp.get("cohort_size", 0) >= 2:
        conf = pvp.get("confidence")
        conf = 1.0 if conf is None else max(0.0, min(1.0, conf))
        if pvp.get("is_pick"):
            pts = round(JEV_WEIGHT_PVP_PICK * conf)
            if pts:
                points += pts
                reasons.append(
                    f"Jev PvP: best pick of {pvp['cohort_size']} same-name coins "
                    f"(conf {conf:.0%}) (+{pts})"
                )
        else:
            pts = round(JEV_WEIGHT_PVP_PICK * 0.5 * conf)
            if pts:
                points -= pts
                pick = pvp.get("picked_platform") or "another platform"
                reasons.append(
                    f"Jev PvP: not the pick — {pick} coin favored for this name (-{pts})"
                )

    # Data context sufficiency (sufficient_info Noul) confidence-gating:
    # If the model judged that key context was missing (unindexed, no description, sparse data),
    # scale down the qualitative contribution or suppress it if critically low (TypeSafe confidence-routing pattern).
    suff = dims.get("sufficient_info", {})
    suff_p = suff.get("noul")
    if suff_p is not None and suff_p < 0.5:
        if suff_p <= 0.2:
            reasons.append(f"Jev: critical context missing ({suff_p:.0%} sufficiency) — qualitative points suppressed")
            return 0, reasons, False
        discount_factor = max(0.2, suff_p)
        discounted = round(points * discount_factor)
        diff = points - discounted
        points = discounted
        reasons.append(f"Jev: sparse context ({suff_p:.0%} sufficiency) — points discounted by {round((1 - discount_factor) * 100)}% (-{diff} pts)")

    return points, reasons, False


# --- Learning loop (Level A + B) --------------------------------------------
# Jev itself does NOT learn. This is where the system learns: log every judgment
# with the token's eventual outcome, then report which dimensions track winners.

def _jev_log_judgment(token_address: str, entry: dict[str, Any], judgment: dict[str, Any]) -> None:
    """Persist a judgment record; outcome filled in later by jev_record_outcome."""
    dims = judgment.get("dimensions", {})
    record = {
        "token": token_address,
        "ticker": entry.get("ticker"),
        "chain": entry.get("chain"),
        "platform": entry.get("platform"),
        "evaluated_at": judgment.get("evaluated_at"),
        "mcap_at_eval": entry.get("market_cap"),
        "opportunity_score_at_eval": entry.get("opportunity_score"),
        # flatten the numeric signals for correlation
        "moon_potential": (dims.get("moon_potential") or {}).get("score"),
        "narrative_quality": (dims.get("narrative_quality") or {}).get("score"),
        "narrative_durability": (dims.get("narrative_durability") or {}).get("score"),
        "legitimacy": (dims.get("legitimacy") or {}).get("noul"),
        "impersonation_risk": (dims.get("impersonation_risk") or {}).get("noul"),
        "trap_risk": (dims.get("trap_risk") or {}).get("noul"),
        "sufficient_info": (dims.get("sufficient_info") or {}).get("noul"),
        # --- real-trajectory tracking (graded over time, see jev_update_trajectory) ---
        "flag_mcap": entry.get("market_cap") or 0.0,   # mcap when Jev flagged it
        "peak_mcap": entry.get("market_cap") or 0.0,
        "peak_at": judgment.get("evaluated_at") or time.time(),
        "last_mcap": entry.get("market_cap") or 0.0,
        "peak_multiple": 1.0,                           # best mcap/flag_mcap seen
        "tier_reached": 0.0,                            # highest tier multiple touched
        "tier_reached_at": None,
        "tier_sustained": 0.0,                          # highest tier held >= sustain
        "outcome": None,          # 'mooned' | 'rugged' | 'dying' | 'flat' — set later
        "outcome_at": None,
        "tracking_done": False,
    }
    JEV_JUDGMENT_LOG[token_address] = record
    if len(JEV_JUDGMENT_LOG) > JEV_JUDGMENT_LOG_MAX:
        JEV_JUDGMENT_LOG.pop(next(iter(JEV_JUDGMENT_LOG)), None)


def jev_record_outcome(token_address: str, outcome: str) -> None:
    """Label a logged judgment with its eventual outcome. Idempotent-ish: a
    terminal outcome (mooned/rugged) is not overwritten by a later 'flat'."""
    rec = JEV_JUDGMENT_LOG.get(token_address)
    if rec is None:
        return
    # terminal outcomes (mooned/rugged/dying) aren't downgraded to 'flat'
    if rec.get("outcome") in ("mooned", "rugged", "dying") and outcome == "flat":
        return
    rec["outcome"] = outcome
    rec["outcome_at"] = time.time()
    # Also stamp the visible activity-feed card so the UI shows which coin
    # mooned/rugged/faded (not just an unlabeled evaluation).
    for a in RECENT_JEV_JUDGMENTS:
        if a.get("kind") != "pvp" and a.get("token_address") == token_address:
            if a.get("outcome") in ("mooned", "rugged", "dying") and outcome == "flat":
                continue
            a["outcome"] = outcome


def jev_update_trajectory(token_address: str, market_cap: float) -> None:
    """Called on every poll for a coin Jev flagged. Tracks the real mcap move
    from the flag point and grades the outcome:
      MOONED = held a tier multiple (3x/5x/10x) for >= sustain minutes
      RUGGED = instant >=80% crash from peak (within the rug window of the peak)
      DYING  = >=60% down from peak but slowly (not a sharp crash)
    Terminal outcomes stick; ALIVE coins keep being graded until the window ends."""
    rec = JEV_JUDGMENT_LOG.get(token_address)
    if rec is None or rec.get("tracking_done"):
        return
    if not market_cap or market_cap <= 0:
        return
    now = time.time()
    flag = rec.get("flag_mcap") or 0.0
    rec["last_mcap"] = market_cap

    # update peak
    if market_cap > (rec.get("peak_mcap") or 0.0):
        rec["peak_mcap"] = market_cap
        rec["peak_at"] = now

    if flag > 0:
        mult = market_cap / flag
        if mult > (rec.get("peak_multiple") or 1.0):
            rec["peak_multiple"] = round(mult, 2)
        # tier touched
        touched = max([t for t in JEV_MOON_TIERS if mult >= t], default=0.0)
        if touched > (rec.get("tier_reached") or 0.0):
            rec["tier_reached"] = touched
            rec["tier_reached_at"] = now
        # --- continuous tier-hold tracking (fix): track how long the coin has
        # stayed AT/ABOVE the lowest tier without dropping below it. Reset the
        # hold-start whenever it falls under. A moon = held the tier continuously
        # for >= sustain, judged from this hold-start (not a single-poll snapshot,
        # which almost always missed fast movers).
        lowest_tier = min(JEV_MOON_TIERS) if JEV_MOON_TIERS else 3.0
        if mult >= lowest_tier:
            if not rec.get("tier_hold_start"):
                rec["tier_hold_start"] = now
        else:
            rec["tier_hold_start"] = None  # dropped below tier — reset the clock

    peak = rec.get("peak_mcap") or 0.0
    drawdown = (peak - market_cap) / peak if peak > 0 else 0.0
    secs_since_peak = now - (rec.get("peak_at") or now)

    # already terminal? keep peak/last updated but don't reclassify away from a win
    if rec.get("outcome") in ("mooned", "rugged"):
        return

    # MOON: held a tier continuously for >= sustain. Checked FIRST (before rug)
    # so a coin that sustained a real run then later dumped is still credited the
    # moon. Uses the continuous hold-start, and grades against the HIGHEST tier
    # the current mcap still satisfies.
    hold_start = rec.get("tier_hold_start")
    if hold_start and flag > 0 and (now - hold_start) >= JEV_MOON_SUSTAIN_SECONDS:
        mult_now = market_cap / flag
        sustained_tier = max([t for t in JEV_MOON_TIERS if mult_now >= t], default=0.0)
        if sustained_tier > 0:
            rec["tier_sustained"] = sustained_tier
            jev_record_outcome(token_address, "mooned")
            jev_record_pvp_outcome(token_address, "mooned")
            jev_record_proposed_outcome(token_address, "mooned")
            # don't mark tracking_done — a mooned coin can still be watched for a rug after
            return

    # RUG: sharp, large drop shortly after the peak
    if drawdown >= JEV_RUG_DROP_PCT and secs_since_peak <= JEV_RUG_WINDOW_SECONDS:
        jev_record_outcome(token_address, "rugged")
        jev_record_pvp_outcome(token_address, "rugged")
        jev_record_proposed_outcome(token_address, "rugged")
        rec["tracking_done"] = True
        return

    # DYING: big but slow bleed (not a sharp rug) — soft negative, non-terminal
    # until the window ends so it could still recover.
    if drawdown >= JEV_DYING_DROP_PCT and secs_since_peak > JEV_RUG_WINDOW_SECONDS and rec.get("outcome") is None:
        rec["outcome"] = "dying"
        for a in RECENT_JEV_JUDGMENTS:
            if a.get("kind") != "pvp" and a.get("token_address") == token_address and a.get("outcome") is None:
                a["outcome"] = "dying"

    # window elapsed with no moon/rug: finalize
    if now - (rec.get("evaluated_at") or now) >= JEV_TRACK_WINDOW_SECONDS:
        rec["tracking_done"] = True
        if rec.get("outcome") is None:
            jev_record_outcome(token_address, "flat")
            jev_record_pvp_outcome(token_address, "flat")
            jev_record_proposed_outcome(token_address, "flat")


def build_jev_correlation_stats() -> dict[str, Any]:
    """Level B: for each labeled dimension, mean value among winners vs losers,
    so the system tells you which Jev signals actually predict on YOUR data.
    Pure arithmetic over JEV_JUDGMENT_LOG — no API cost."""
    labeled = [r for r in JEV_JUDGMENT_LOG.values() if r.get("outcome")]
    winners = [r for r in labeled if r["outcome"] == "mooned"]
    losers = [r for r in labeled if r["outcome"] == "rugged"]
    non_winners = [r for r in labeled if r["outcome"] in ("rugged", "flat", "dying")]  # everything that didn't moon
    dims = ["moon_potential", "narrative_quality", "narrative_durability", "legitimacy", "impersonation_risk", "trap_risk"]

    def _mean(rows: list[dict[str, Any]], key: str) -> Optional[float]:
        vals = [r[key] for r in rows if r.get(key) is not None]
        return round(sum(vals) / len(vals), 3) if vals else None

    per_dim = {}
    for d in dims:
        per_dim[d] = {
            "winner_avg": _mean(winners, d),
            "loser_avg": _mean(losers, d),
            "non_winner_avg": _mean(non_winners, d),  # rugged+flat: 'didn't win'
            "overall_avg": _mean(labeled, d),
        }
    return {
        "judgments_total": len(JEV_JUDGMENT_LOG),
        "labeled": len(labeled),
        "winners": len(winners),
        "losers": len(losers),
        "flat": sum(1 for r in labeled if r["outcome"] == "flat"),
        "dying": sum(1 for r in labeled if r["outcome"] == "dying"),
        "tiers": {  # how many mooned coins hit each tier (sustained)
            f"{int(t)}x": sum(1 for r in winners if (r.get("tier_sustained") or 0) >= t)
            for t in JEV_MOON_TIERS
        },
        "best_multiple": round(max([r.get("peak_multiple") or 1.0 for r in JEV_JUDGMENT_LOG.values()], default=1.0), 2),
        "per_dimension": per_dim,
    }


# --- Level C: auto-tune weight multipliers from outcomes --------------------
def jev_recompute_learned_weights() -> dict[str, Any]:
    """Derive a per-dimension weight MULTIPLIER from how well each dimension
    separated winners (mooned) from non-winners (rugged/dying/flat). Gated on
    sample size. Score dims use 0..(n-1); noul dims use 0..1 — both normalized
    so separation is comparable. Predictive dims (winners score higher) get
    multiplier >1; useless or inverted dims get <1. Clamped. Updates
    JEV_LEARNED_WEIGHTS + JEV_AUTOTUNE_STATUS in place."""
    if not JEV_AUTOTUNE_ENABLED:
        JEV_AUTOTUNE_STATUS.update({"active": False, "reason": "auto-tune disabled"})
        return JEV_AUTOTUNE_STATUS
    labeled = [r for r in JEV_JUDGMENT_LOG.values() if r.get("outcome")]
    winners = [r for r in labeled if r["outcome"] == "mooned"]
    non_winners = [r for r in labeled if r["outcome"] in ("rugged", "dying", "flat")]
    if len(labeled) < JEV_AUTOTUNE_MIN_OUTCOMES or len(winners) < JEV_AUTOTUNE_MIN_WINNERS:
        JEV_AUTOTUNE_STATUS.update({
            "active": False,
            "reason": f"need ≥{JEV_AUTOTUNE_MIN_OUTCOMES} outcomes & ≥{JEV_AUTOTUNE_MIN_WINNERS} moons "
                      f"(have {len(labeled)} outcomes, {len(winners)} moons)",
            "labeled": len(labeled), "winners": len(winners), "updated_at": time.time(),
        })
        return JEV_AUTOTUNE_STATUS

    # dimension -> (is it a "higher is better" signal? and its value range)
    # score dims are graded 0..(n-1); we normalize by dividing by a nominal max.
    score_dims = {"moon_potential": 4, "narrative_quality": 3, "narrative_durability": 3}
    noul_pos = ["legitimacy"]                 # higher = better
    noul_neg = ["trap_risk", "impersonation_risk"]  # higher = worse

    def _norm_mean(rows, key, denom):
        vals = [r[key] for r in rows if r.get(key) is not None]
        return (sum(vals) / len(vals) / denom) if vals else None

    learned = {}
    detail = {}
    for d, maxlvl in score_dims.items():
        w = _norm_mean(winners, d, maxlvl); l = _norm_mean(non_winners, d, maxlvl)
        if w is None or l is None:
            continue
        sep = w - l  # -1..+1; positive => predictive of moons
        mult = max(JEV_AUTOTUNE_CLAMP_LOW, min(JEV_AUTOTUNE_CLAMP_HIGH, 1.0 + sep * 2.0))
        learned[d] = round(mult, 3); detail[d] = {"winner": round(w,3), "loser": round(l,3), "sep": round(sep,3), "mult": round(mult,3)}
    for d in noul_pos:
        w = _norm_mean(winners, d, 1); l = _norm_mean(non_winners, d, 1)
        if w is None or l is None: continue
        sep = w - l
        mult = max(JEV_AUTOTUNE_CLAMP_LOW, min(JEV_AUTOTUNE_CLAMP_HIGH, 1.0 + sep * 2.0))
        learned[d] = round(mult, 3); detail[d] = {"winner": round(w,3), "loser": round(l,3), "sep": round(sep,3), "mult": round(mult,3)}
    for d in noul_neg:
        # for "bad" signals, predictive means LOSERS score higher; flip the sep
        w = _norm_mean(winners, d, 1); l = _norm_mean(non_winners, d, 1)
        if w is None or l is None: continue
        sep = l - w
        mult = max(JEV_AUTOTUNE_CLAMP_LOW, min(JEV_AUTOTUNE_CLAMP_HIGH, 1.0 + sep * 2.0))
        learned[d] = round(mult, 3); detail[d] = {"winner": round(w,3), "loser": round(l,3), "sep": round(sep,3), "mult": round(mult,3)}

    changed = learned != JEV_LEARNED_WEIGHTS
    JEV_LEARNED_WEIGHTS.clear(); JEV_LEARNED_WEIGHTS.update(learned)
    JEV_AUTOTUNE_STATUS.update({
        "active": True, "reason": "active", "labeled": len(labeled), "winners": len(winners),
        "detail": detail, "updated_at": time.time(),
    })
    if changed:
        jev_emit_event("proposed", reason=f"auto-tuned weights from {len(labeled)} outcomes: "
                       + ", ".join(f"{k}×{v}" for k, v in learned.items()),
                       detail={"learned_weights": learned})
        logger.info(f"[jev/autotune] learned weights updated: {learned}")
    return JEV_AUTOTUNE_STATUS


def _jev_log_pvp(narrative_key: str, result: dict[str, Any]) -> None:
    """Record a PvP pick into the learning ledger so its correctness can be
    graded once cohort members reach terminal outcomes."""
    cohort = result.get("cohort", [])
    record = {
        "narrative_key": narrative_key,
        "ticker": (cohort[0]["ticker"] if cohort else None),
        "picked_address": result.get("picked_address"),
        "picked_platform": None,
        "confidence": result.get("confidence"),
        "evaluated_at": result.get("evaluated_at"),
        "members": [
            {"token_address": c["token_address"], "platform": c["platform"],
             "chain": c["chain"], "mcap_at_pick": round(c.get("market_cap") or 0),
             "outcome": None}
            for c in cohort
        ],
        "pick_was_correct": None,   # True/False once a member moons, else None
        "resolved_at": None,
    }
    # fill picked_platform
    for m in record["members"]:
        if m["token_address"] == record["picked_address"]:
            record["picked_platform"] = m["platform"]
    JEV_PVP_LOG[narrative_key] = record
    if len(JEV_PVP_LOG) > JEV_PVP_LOG_MAX:
        JEV_PVP_LOG.pop(next(iter(JEV_PVP_LOG)), None)
    # index every member so a later outcome finds this record
    for m in record["members"]:
        JEV_PVP_TOKEN_INDEX[m["token_address"]].add(narrative_key)


def jev_record_pvp_outcome(token_address: str, outcome: str) -> None:
    """A cohort member reached a terminal outcome — update the PvP record(s) it
    belongs to. The pick is scored 'correct' if the moon'd coin is the one Jev
    picked; 'incorrect' if a coin it rejected moon'd instead."""
    keys = JEV_PVP_TOKEN_INDEX.get(token_address)
    if not keys:
        return
    for key in list(keys):
        rec = JEV_PVP_LOG.get(key)
        if rec is None:
            continue
        for m in rec["members"]:
            if m["token_address"] == token_address:
                # keep terminal outcome, don't let 'flat' overwrite moon/rug
                if m["outcome"] in ("mooned", "rugged") and outcome == "flat":
                    continue
                m["outcome"] = outcome
        # score the pick the first time ANY member moons
        if rec.get("pick_was_correct") is None:
            mooned = [m for m in rec["members"] if m["outcome"] == "mooned"]
            if mooned:
                rec["pick_was_correct"] = any(m["token_address"] == rec["picked_address"] for m in mooned)
                rec["resolved_at"] = time.time()


def build_jev_pvp_stats() -> dict[str, Any]:
    """How well the PvP picks are doing: resolved count + hit rate."""
    resolved = [r for r in JEV_PVP_LOG.values() if r.get("pick_was_correct") is not None]
    correct = sum(1 for r in resolved if r["pick_was_correct"])
    return {
        "picks_total": len(JEV_PVP_LOG),
        "resolved": len(resolved),
        "correct": correct,
        "hit_rate": round(correct / len(resolved), 3) if resolved else None,
    }


def _build_jev_evaluated_coins(limit: int = 40) -> list[dict[str, Any]]:
    """Every coin Jev evaluated, with its REAL peak multiple + outcome, ranked by
    peak multiple — so the operator can see which coins ran (e.g. 5.5x) even if
    they didn't formally 'moon' (a big wick that didn't hold = flat, not mooned).
    Includes still-tracking coins so in-progress runners are visible."""
    rows = []
    for addr, r in JEV_JUDGMENT_LOG.items():
        pm = r.get("peak_multiple") or 1.0
        rows.append({
            "token_address": addr,
            "ticker": r.get("ticker"),
            "peak_multiple": round(pm, 2),
            "tier_sustained": r.get("tier_sustained") or 0,
            "outcome": r.get("outcome") or "tracking",
            "flag_mcap": round(r.get("flag_mcap") or 0),
            "peak_mcap": round(r.get("peak_mcap") or 0),
            "last_mcap": round(r.get("last_mcap") or 0),
            "chain": r.get("chain"),
            "evaluated_at": r.get("evaluated_at"),
        })
    rows.sort(key=lambda x: -x["peak_multiple"])
    return rows[:limit]


# --- #3 Question-proposal & measurement -------------------------------------
def _slug(name: str) -> str:
    base = "".join(c if c.isalnum() else "_" for c in (name or "").lower()).strip("_")
    return ("prop_" + base)[:40] or "prop_q"


def jev_add_proposed_question(instructions: str, qtype: str = "noul",
                              criteria: Any = None, proposed_by: str = "manual",
                              name: Optional[str] = None) -> tuple[bool, str]:
    """Add a candidate question to be tested live. Returns (ok, id_or_error)."""
    if qtype not in ("noul", "score"):
        return False, "type must be 'noul' or 'score'"
    if not instructions or len(instructions) < 8:
        return False, "instructions too short"
    active = [q for q in JEV_PROPOSED_QUESTIONS.values() if q.get("active")]
    if len(active) >= JEV_PROPOSED_MAX:
        return False, f"max {JEV_PROPOSED_MAX} active proposals — retire one first"
    qid = _slug(name or instructions[:24])
    n = 2
    base = qid
    while qid in JEV_PROPOSED_QUESTIONS:
        qid = f"{base}_{n}"; n += 1
    if qtype == "score" and not isinstance(criteria, list):
        criteria = ["Not at all", "Somewhat", "Strongly"]
    if qtype == "noul" and not isinstance(criteria, dict):
        criteria = {"true": "yes", "false": "no"}
    JEV_PROPOSED_QUESTIONS[qid] = {
        "type": qtype, "instructions": instructions, "criteria": criteria,
        "active": True, "status": "testing", "proposed_by": proposed_by,
        "proposed_at": time.time(), "answers": {},
    }
    logger.info(f"[jev/propose] added candidate question {qid} by {proposed_by}")
    return True, qid


def jev_active_proposed_questions() -> dict[str, Any]:
    """The proposed questions to include in the batched Jev call right now."""
    out = {}
    for qid, q in JEV_PROPOSED_QUESTIONS.items():
        if q.get("active"):
            out[qid] = {"type": q["type"], "instructions": q["instructions"], "criteria": q["criteria"]}
    return out


def _jev_record_proposed_answers(token_address: str, answers: dict[str, Any]) -> None:
    """Store each proposed question's answer for this token so its predictive
    value can be measured against the token's eventual outcome."""
    for qid, q in JEV_PROPOSED_QUESTIONS.items():
        if not q.get("active"):
            continue
        a = answers.get(qid)
        if not isinstance(a, dict):
            continue
        val = a.get("noul") if a.get("type") == "noul" else a.get("score")
        if val is not None:
            q["answers"][token_address] = {"val": val, "outcome": None}


def jev_record_proposed_outcome(token_address: str, outcome: str) -> None:
    """Label proposed-question answers for this token with its outcome."""
    for q in JEV_PROPOSED_QUESTIONS.values():
        rec = q.get("answers", {}).get(token_address)
        if rec is not None:
            if rec.get("outcome") in ("mooned", "rugged") and outcome == "flat":
                continue
            rec["outcome"] = outcome


def build_jev_proposal_stats() -> list[dict[str, Any]]:
    """For each proposed question: how well its answer separates winners from
    losers (the empirical 'is this question useful?' measurement)."""
    out = []
    for qid, q in JEV_PROPOSED_QUESTIONS.items():
        labeled = [(r["val"], r["outcome"]) for r in q.get("answers", {}).values() if r.get("outcome") in ("mooned", "rugged")]
        winners = [v for v, o in labeled if o == "mooned"]
        losers = [v for v, o in labeled if o == "rugged"]
        wa = round(sum(winners)/len(winners), 3) if winners else None
        la = round(sum(losers)/len(losers), 3) if losers else None
        separation = round(wa - la, 3) if (wa is not None and la is not None) else None
        out.append({
            "id": qid, "type": q["type"], "instructions": q["instructions"],
            "status": q["status"], "active": q["active"], "proposed_by": q["proposed_by"],
            "answered": len(q.get("answers", {})), "labeled": len(labeled),
            "winner_avg": wa, "loser_avg": la, "separation": separation,
        })
    return out


# --- Semantic narrative theme tagging (DeepSeek) ----------------------------
def _theme_slug(s: str) -> str:
    s = "".join(c if (c.isalnum() or c == "-") else "-" for c in (s or "").lower()).strip("-")
    while "--" in s:
        s = s.replace("--", "-")
    return s[:32] or "other"


async def jev_tag_narrative_theme(token_address: str, entry: dict[str, Any]) -> Optional[str]:
    """Ask DeepSeek for the coin's canonical NARRATIVE THEME (a short slug like
    'ai-agents', 'politics', 'dog-meme'), then group it with other coins of the
    same theme across different tickers. This is the semantic replacement for
    the regex ticker-suffix clustering. Gated: only called for coins that have
    already passed the Jev gate, once each. Fail-safe (returns None on error)."""
    if not _jev_proposer_configured():
        return None
    if token_address in JEV_THEME_TAGGED:
        return TOKEN_THEME.get(token_address)
    JEV_THEME_TAGGED[token_address] = time.time()
    if len(JEV_THEME_TAGGED) > JEV_THEME_TAGGED_MAX:
        JEV_THEME_TAGGED.pop(next(iter(JEV_THEME_TAGGED)), None)

    name = entry.get("name") or ""
    ticker = entry.get("ticker") or ""
    desc = (entry.get("description") or "")[:400]
    # known themes so DeepSeek reuses existing slugs instead of inventing dupes
    known = sorted(NARRATIVE_THEMES.keys())[:40]
    prompt = (
        "Classify this newly-launched memecoin into a single canonical NARRATIVE THEME — the broad "
        "meme/cultural category it belongs to, so coins sharing a theme group together even with "
        "different names. Return STRICT JSON: {\"slug\": \"kebab-case-theme\", \"label\": \"Human Label\"}.\n"
        "Reuse an existing slug if it fits.\n"
        f"Existing themes: {', '.join(known) or '(none yet)'}\n\n"
        f"Coin — name: {name!r}, ticker: {ticker!r}, description: {desc!r}\n"
        "Examples of good slugs: ai-agents, politics-trump, dog-meme, cat-meme, celebrity, "
        "tech-parody, finance-parody, sports, gaming, tokenized-culture. JSON only."
    )
    payload = {"model": JEV_PROPOSER_LLM_MODEL, "messages": [{"role": "user", "content": prompt}],
               "temperature": 0.3, "max_tokens": 120, "response_format": {"type": "json_object"}}
    headers = {"Authorization": f"Bearer {JEV_PROPOSER_LLM_KEY}", "Content-Type": "application/json",
               "Accept-Encoding": "gzip, deflate"}
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(JEV_PROPOSER_LLM_URL, headers=headers, json=payload) as resp:
                if resp.status != 200:
                    return None
                body = await resp.json()
        content = (((body.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        content = re.sub(r"^```(?:json)?|```$", "", content, flags=re.MULTILINE).strip()
        data = json.loads(content)
        slug = _theme_slug(data.get("slug") or "")
        label = (data.get("label") or slug.replace("-", " ").title())[:48]
        if not slug:
            return None
        # register / group
        theme = NARRATIVE_THEMES.setdefault(slug, {"label": label, "coins": [], "first_seen": time.time(), "chains": []})
        if token_address not in theme["coins"]:
            theme["coins"].append(token_address)
        ch = entry.get("chain")
        if ch and ch not in theme["chains"]:
            theme["chains"].append(ch)
        TOKEN_THEME[token_address] = slug
        # tag the feed entry + judgment record so UI/learning can use it
        fe = TOKEN_FEED.get(token_address)
        if fe is not None:
            fe["narrative_theme"] = slug
            fe["narrative_theme_label"] = label
        rec = JEV_JUDGMENT_LOG.get(token_address)
        if rec is not None:
            rec["narrative_theme"] = slug
        # emit an event if this theme now has multiple coins (a real cluster)
        if len(theme["coins"]) >= 2:
            jev_emit_event("proposed", ticker=ticker, token_address=token_address,
                           reason=f"narrative theme '{label}' now has {len(theme['coins'])} coins across {len(theme['chains'])} chain(s)",
                           detail={"theme": slug})
        logger.info(f"[jev/theme] {ticker} -> {slug} ({len(theme['coins'])} in theme)")
        return slug
    except Exception as exc:
        logger.warning(f"[jev/theme] failed: {exc!r}")
        return None


def build_jev_theme_stats() -> list[dict[str, Any]]:
    """Semantic narrative clusters, biggest first — for the dashboard."""
    out = []
    for slug, th in NARRATIVE_THEMES.items():
        live = [addr for addr in th["coins"] if (TOKEN_FEED.get(addr, {}).get("status") == "WATCHING")]
        out.append({
            "slug": slug, "label": th.get("label", slug),
            "coin_count": len(th["coins"]), "live_count": len(live),
            "chains": th.get("chains", []),
            "tickers": [TOKEN_FEED.get(a, {}).get("ticker") for a in th["coins"][-8:]],
        })
    out.sort(key=lambda t: t["coin_count"], reverse=True)
    return out[:25]


# --- Auto-proposer: a generative LLM (e.g. DeepSeek) invents new questions ---
def _jev_proposer_configured() -> bool:
    return bool(JEV_PROPOSER_LLM_KEY and JEV_PROPOSER_LLM_URL and JEV_PROPOSER_LLM_MODEL)


async def jev_auto_propose() -> list[str]:
    """Ask the generative LLM to propose NEW candidate questions, informed by
    the current question set and which ones actually separate winners from
    losers on our data. Returns list of added question ids. Fail-safe."""
    if not _jev_proposer_configured():
        return []
    # Build the brief: current questions + measured performance so far.
    current = [f"- {qid} ({q['type']}): {q['instructions']}" for qid, q in JEV_QUESTIONS.items()]
    stats = build_jev_proposal_stats()
    perf_lines = []
    for s in stats:
        if s["labeled"] >= 1:
            perf_lines.append(f"- {s['id']}: separation={s['separation']} over {s['labeled']} labeled")
    corr = build_jev_correlation_stats()
    active_props = [q["instructions"] for q in JEV_PROPOSED_QUESTIONS.values() if q.get("active")]

    # Extract concrete prediction misses (TypeSafe Autoresearch Feature Discovery cookbook pattern):
    # - False positives: predicted high potential / cleared gate, but RUGGED
    # - False negatives: predicted low potential / overlooked, but MOONED
    false_positives = []
    false_negatives = []
    for addr, rec in list(JEV_JUDGMENT_LOG.items()):
        outcome = rec.get("outcome")
        if not outcome:
            continue
        dims = rec.get("judgment", {}).get("dimensions", {})
        moon_score = dims.get("moon_potential", {}).get("score")
        ticker_name = f"${rec.get('ticker', 'UNKNOWN')}"
        if outcome == "rugged" and moon_score is not None and moon_score >= 3:
            false_positives.append(f"{ticker_name} (Jev moon_potential={moon_score}/4, outcome=RUGGED)")
        elif outcome == "mooned" and moon_score is not None and moon_score <= 1:
            false_negatives.append(f"{ticker_name} (Jev moon_potential={moon_score}/4, outcome=MOONED)")

    misses_section = ""
    if false_positives:
        misses_section += f"\nRecent False Positives (predicted high potential, but RUGGED):\n" + "\n".join(f"- {fp}" for fp in false_positives[-4:]) + "\n"
    if false_negatives:
        misses_section += f"\nRecent False Negatives (predicted low potential, but MOONED):\n" + "\n".join(f"- {fn}" for fn in false_negatives[-4:]) + "\n"

    prompt = (
        "You design yes/no (noul) and graded (score) questions for a fast judgment model called Jev "
        "that rates newly-launched crypto memecoins for their potential to run higher. Jev sees, per "
        "coin: name, ticker, launchpad, market cap, volume, liquidity, holder count/concentration, "
        "whether holders have real ($1k+) positions, bundle/dev-rug flags, and recent mcap momentum. "
        "Propose NEW questions that would add predictive signal NOT already covered.\n\n"
        f"Existing questions:\n{chr(10).join(current)}\n\n"
        f"Currently-testing questions:\n{chr(10).join('- '+a for a in active_props) or '(none)'}\n\n"
        f"Measured performance (separation = winner_avg - loser_avg, higher=better):\n{chr(10).join(perf_lines) or '(no labeled outcomes yet)'}\n\n"
        f"Outcome data so far: {corr['winners']} mooned, {corr['losers']} rugged.\n"
        f"{misses_section}\n"
        "Following the TypeSafe Autoresearch pattern, propose questions that specifically target the blind spots "
        "revealed by the false positives and false negatives above.\n\n"
        f"Return STRICT JSON: {{\"questions\": [{{\"name\": short_id, \"type\": \"noul\"|\"score\", "
        "\"instructions\": the question text, \"criteria\": for noul {\"true\":..,\"false\":..} or for "
        "score an array of 3-5 ordered level descriptions}}]}. "
        f"Propose at most {JEV_PROPOSER_MAX_NEW_PER_ROUND} questions. Each must be answerable from the "
        "data Jev sees, vary across coins, and be genuinely different from existing ones. JSON only."
    )
    payload = {
        "model": JEV_PROPOSER_LLM_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7,
        "max_tokens": 900,
        "response_format": {"type": "json_object"},
    }
    headers = {"Authorization": f"Bearer {JEV_PROPOSER_LLM_KEY}", "Content-Type": "application/json",
               "Accept-Encoding": "gzip, deflate"}  # not 'br' — aiohttp can't decode brotli without the extra dep
    added: list[str] = []
    try:
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(JEV_PROPOSER_LLM_URL, headers=headers, json=payload) as resp:
                if resp.status != 200:
                    logger.warning(f"[jev/proposer] LLM {resp.status}: {(await resp.text())[:200]}")
                    return []
                body = await resp.json()
        content = (((body.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        # tolerate ```json fences
        content = re.sub(r"^```(?:json)?|```$", "", content, flags=re.MULTILINE).strip()
        data = json.loads(content)
        for q in (data.get("questions") or [])[:JEV_PROPOSER_MAX_NEW_PER_ROUND]:
            ok, res = jev_add_proposed_question(
                instructions=q.get("instructions", ""),
                qtype=q.get("type", "noul"),
                criteria=q.get("criteria"),
                proposed_by="deepseek-auto",
                name=q.get("name"),
            )
            if ok:
                added.append(res)
        if added:
            details = [{"id": qid, "type": JEV_PROPOSED_QUESTIONS[qid]["type"],
                        "instructions": JEV_PROPOSED_QUESTIONS[qid]["instructions"]}
                       for qid in added if qid in JEV_PROPOSED_QUESTIONS]
            # reason carries the actual question text so the event log/history
            # shows WHAT was proposed, not just an id.
            first_q = details[0]["instructions"] if details else ""
            reason = f"auto-proposed {len(added)}: “{first_q[:80]}{'…' if len(first_q) > 80 else ''}”"
            if len(added) > 1:
                reason += f" (+{len(added)-1} more)"
            jev_emit_event("proposed", reason=reason, detail={"ids": added, "questions": details})
            logger.info(f"[jev/proposer] added {len(added)} question(s): {added}")
    except Exception as exc:
        logger.warning(f"[jev/proposer] failed: {exc!r}")
    return added


def jev_prune_proposals() -> list[str]:
    """Auto-retire proposals that clearly don't help once they have enough
    labeled outcomes (separation <= 0). Returns retired ids."""
    retired = []
    for s in build_jev_proposal_stats():
        if not s["active"] or s["status"] == "promoted":
            continue
        if s["labeled"] >= JEV_PROPOSAL_MIN_LABELS_TO_JUDGE and s["separation"] is not None and s["separation"] <= 0:
            q = JEV_PROPOSED_QUESTIONS.get(s["id"])
            if q:
                q["active"] = False
                q["status"] = "retired-poor"
                retired.append(s["id"])
    if retired:
        jev_emit_event("proposed", reason=f"auto-retired {len(retired)} underperforming question(s): {', '.join(retired)}",
                       detail={"retired": retired})
    return retired


async def jev_auto_proposer_worker() -> None:
    """Periodic: prune poor proposals, then (if room) ask the LLM for new ones."""
    if not _jev_proposer_configured():
        logger.info("[jev/proposer] no generative LLM configured — auto-proposer idle")
        return
    # small initial delay so startup settles
    await asyncio.sleep(30)
    while True:
        try:
            jev_prune_proposals()
            active = sum(1 for q in JEV_PROPOSED_QUESTIONS.values() if q.get("active"))
            if active < JEV_PROPOSED_MAX:
                await jev_auto_propose()
        except Exception as exc:
            logger.warning(f"[jev/proposer] worker error: {exc!r}")
        await asyncio.sleep(JEV_PROPOSER_INTERVAL_SECONDS)


async def jev_trajectory_worker() -> None:
    """Independently poll the mcap of every Jev-flagged coin that's still being
    graded, and update its outcome — CRUCIALLY this continues even after the
    coin would normally expire from the main watchlist, so a moon/rug that
    happens later is still caught (the old graduation-only labeling missed these).
    Batches DexScreener lookups and respects the tracking window."""
    if not JEV_ENABLED:
        return
    await asyncio.sleep(20)
    while True:
        try:
            active = [addr for addr, rec in list(JEV_JUDGMENT_LOG.items())
                      if not rec.get("tracking_done")]
            for addr in active:
                rec = JEV_JUDGMENT_LOG.get(addr)
                if rec is None or rec.get("tracking_done"):
                    continue
                # Prefer the live feed mcap if the coin is still actively polled;
                # otherwise fetch fresh so lifecycle tracking survives expiry.
                fe = TOKEN_FEED.get(addr)
                mcap = (fe.get("market_cap") if fe else 0.0) or 0.0
                if mcap <= 0 or (fe is None):
                    try:
                        mcap = await fetch_token_market_cap_usd(addr)
                    except Exception:
                        mcap = mcap or 0.0
                if mcap and mcap > 0:
                    jev_update_trajectory(addr, mcap)
                await asyncio.sleep(0.15)  # gentle pacing on DexScreener
        except Exception as exc:
            logger.warning(f"[jev/trajectory] worker error: {exc!r}")
        await asyncio.sleep(JEV_TRACK_POLL_SECONDS)


async def jev_autotune_worker() -> None:
    """Periodically recompute learned weight multipliers from accumulated
    outcomes. No-op (stays at 1.0x) until enough moons/rugs exist."""
    if not (JEV_ENABLED and JEV_AUTOTUNE_ENABLED):
        return
    await asyncio.sleep(60)
    while True:
        try:
            jev_recompute_learned_weights()
        except Exception as exc:
            logger.warning(f"[jev/autotune] worker error: {exc!r}")
        await asyncio.sleep(JEV_AUTOTUNE_INTERVAL_SECONDS)


async def run_discovery_scan() -> int:
    """Find already-trading Solana coins the launch listeners missed and inject
    qualifying ones into the watchlist. Sources: DexScreener boosts + Birdeye
    top-volume. Filters: not already tracked, Solana, mcap/volume floors, valid
    non-stock ticker. Returns count injected. Fail-safe."""
    if not DISCOVERY_ENABLED:
        return 0
    try:
        boosted = await fetch_dexscreener_boosted_solana()
        top_vol = await fetch_birdeye_top_volume_solana(limit=30)
    except Exception as exc:
        logger.warning(f"[discovery] source fetch failed: {exc!r}")
        return 0
    # merge + dedup, skip anything already known
    # also fold in coins that recently submitted a DexScreener profile (free,
    # already cached) — another stream of active coins, useful when Birdeye's
    # volume list is rate-limited/empty.
    profile_addrs = list(TOKEN_PROFILES.keys())
    candidates = list(dict.fromkeys(boosted + top_vol + profile_addrs))
    injected = 0
    for addr in candidates:
        if injected >= DISCOVERY_MAX_PER_SCAN:
            break
        if (addr in TOKEN_WATCHLIST or addr in TOKEN_FEED or addr in DISCOVERED_TOKENS
                or addr in LONG_TAIL_WATCHLIST):
            continue
        # verify via DexScreener that it clears the floors + is real
        info = await fetch_dexscreener_info(addr)
        mcap = info.get("market_cap") or 0
        vol = info.get("volume_24h") or 0
        symbol = (info.get("symbol") or "").strip()
        if addr in STONKFUN_QUOTE_MINTS:
            continue
        if mcap < DISCOVERY_MIN_MCAP or vol < DISCOVERY_MIN_VOLUME_24H:
            continue
        if is_stonkboard_token(addr) and mcap > MAX_OPPORTUNITY_MARKET_CAP_USD:
            continue
        if mcap > IMPLAUSIBLE_NEW_LAUNCH_MCAP_USD:
            continue

        # Discovered coins are outside launchpad streams, so require qualifying contract suffix
        if not any(addr.lower().endswith(s) for s in QUALIFYING_CONTRACT_SUFFIXES):
            continue
        if not symbol or symbol == "UNKNOWN":
            continue
        if ticker_is_invalid(symbol)[0]:
            continue
        est, _ = is_probably_established_or_stock(symbol, mcap, token_address=addr)
        if est:
            continue
        # inject into the normal pipeline. dev_wallet unknown for discovered
        # coins (we didn't see the launch) — use a sentinel so dev-trust treats
        # it as neutral/unknown rather than crediting/blaming a real wallet.
        DISCOVERED_TOKENS[addr] = time.time()
        if len(DISCOVERED_TOKENS) > DISCOVERED_TOKENS_MAX:
            DISCOVERED_TOKENS.pop(next(iter(DISCOVERED_TOKENS)), None)
        try:
            await process_new_token_event(
                chain="solana", platform="discovered", token_address=addr,
                ticker_raw=symbol, dev_wallet=f"discovered:{addr[:8]}", ts=time.time(),
                extra={"discovered": True},
            )
            # seed its mcap/name immediately so it isn't stuck at 0 for a cycle
            token_feed_upsert(addr, market_cap=mcap, volume_24h=vol,
                              liquidity_usd=info.get("liquidity_usd", 0.0),
                              price_usd=info.get("price_usd", 0.0),
                              name=info.get("name") or "")
            injected += 1
        except Exception as exc:
            logger.warning(f"[discovery] inject {symbol} failed: {exc!r}")
        await asyncio.sleep(0.2)  # gentle pacing on DexScreener
    if injected:
        logger.info(f"[discovery] injected {injected} already-trading coin(s) into the watchlist")
    return injected


async def discovery_worker() -> None:
    """Periodic discovery scan for missed-at-launch coins."""
    if not DISCOVERY_ENABLED:
        logger.info("[discovery] disabled")
        return
    await asyncio.sleep(45)  # let launch listeners settle first
    while True:
        try:
            await run_discovery_scan()
        except Exception as exc:
            logger.warning(f"[discovery] worker error: {exc!r}")
        await asyncio.sleep(DISCOVERY_INTERVAL_SECONDS)


def build_jev_stats() -> dict[str, Any]:
    """Aggregate Jev usage + learning stats for the dashboard/API."""
    _jev_reset_day_if_needed()
    can_call, reason = jev_budget_status()
    return {
        "enabled": JEV_ENABLED,
        "model": TYPESAFE_MODEL,
        "can_call": can_call,
        "reason": reason,
        "usage": dict(JEV_USAGE),
        "caps": {
            "per_day": JEV_MAX_CALLS_PER_DAY,
            "total": JEV_MAX_CALLS_TOTAL,
            "min_score_to_evaluate": JEV_MIN_SCORE_TO_EVALUATE,
            "min_mcap_to_evaluate": JEV_MIN_MCAP_TO_EVALUATE,
        },
        "evaluated_tokens": len(JEV_EVALUATED_TOKENS),
        "correlation": build_jev_correlation_stats(),
        "pvp_learning": build_jev_pvp_stats(),
        "evaluated_coins": _build_jev_evaluated_coins(),
        "data_gap": _build_jev_data_gap_stats(),
        "autotune": {**JEV_AUTOTUNE_STATUS, "learned_weights": dict(JEV_LEARNED_WEIGHTS),
                     "min_outcomes": JEV_AUTOTUNE_MIN_OUTCOMES, "min_winners": JEV_AUTOTUNE_MIN_WINNERS},
        "gate_funnel": _build_jev_gate_funnel(),
        "questions": _build_jev_questions_view(),
        "proposals": build_jev_proposal_stats(),
        "proposer_llm": bool(JEV_PROPOSER_LLM_KEY and JEV_PROPOSER_LLM_URL and JEV_PROPOSER_LLM_MODEL),
        "recent_judgments": list(RECENT_JEV_JUDGMENTS)[-15:],
        "recent_events": list(RECENT_JEV_EVENTS)[-20:],
    }


def _build_jev_data_gap_stats() -> dict[str, Any]:
    """How often Jev felt it LACKED enough info to judge — i.e. what data it
    needs more of. Aggregated from the sufficient_info self-assessment."""
    vals = [r.get("sufficient_info") for r in JEV_JUDGMENT_LOG.values() if r.get("sufficient_info") is not None]
    if not vals:
        return {"judged": 0, "avg_sufficiency": None, "low_info_count": 0, "low_info_pct": None}
    low = sum(1 for v in vals if v < 0.5)
    return {
        "judged": len(vals),
        "avg_sufficiency": round(sum(vals) / len(vals), 3),
        "low_info_count": low,
        "low_info_pct": round(low / len(vals) * 100, 1),
    }


def _build_jev_questions_view() -> list[dict[str, Any]]:
    """The exact questions Jev is being asked, for display in the UI. Combines
    the fixed JEV_QUESTIONS with any active proposed questions (see #3)."""
    out = []
    for qid, q in JEV_QUESTIONS.items():
        out.append({
            "id": qid,
            "type": q.get("type"),
            "instructions": q.get("instructions"),
            "criteria": q.get("criteria"),
            "source": "core",
        })
    for qid, q in JEV_PROPOSED_QUESTIONS.items():
        if q.get("active"):
            out.append({
                "id": qid,
                "type": q.get("type"),
                "instructions": q.get("instructions"),
                "criteria": q.get("criteria"),
                "source": "proposed",
                "status": q.get("status"),
            })
    return out


def _build_jev_gate_funnel() -> dict[str, Any]:
    """Live snapshot of WHY Jev is or isn't evaluating right now: of the tokens
    currently WATCHING, how many pass each gate stage. Makes 'no activity'
    self-explanatory instead of a mystery."""
    watching = [e for e in TOKEN_FEED.values() if e.get("status") == "WATCHING"]
    with_ticker = [e for e in watching if e.get("ticker") and e.get("ticker") != "UNKNOWN"]
    over_mcap = [e for e in with_ticker if (e.get("market_cap") or 0) >= JEV_MIN_MCAP_TO_EVALUATE]
    over_score = [e for e in over_mcap if (e.get("opportunity_score") or 0) >= JEV_MIN_SCORE_TO_EVALUATE]
    return {
        "watching": len(watching),
        "ticker_resolved": len(with_ticker),
        "over_mcap_gate": len(over_mcap),
        "qualifies_for_jev": len(over_score),
        "mcap_gate": JEV_MIN_MCAP_TO_EVALUATE,
        "score_gate": JEV_MIN_SCORE_TO_EVALUATE,
        "skipped_not_ready_total": JEV_USAGE.get("skipped_not_ready", 0),
    }


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
    # Keep a rolling backup of the PREVIOUS good state before overwriting, so a
    # bad save/purge can be recovered. Only back up if the current state still
    # had learning data (don't let an already-empty state clobber a good backup).
    try:
        if os.path.exists(path):
            prev = json.load(open(path))
            if prev.get("jev_judgment_log") or prev.get("dev_reputation"):
                os.replace(path, path + ".bak")
    except Exception:
        pass
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
        "dev_launch_history": dict(DEV_LAUNCH_HISTORY),
        "daily_stats": {day: dict(counters) for day, counters in DAILY_STATS.items()},
        "hourly_launch_stats": dict(HOURLY_LAUNCH_STATS),
        "recent_graduations": list(RECENT_GRADUATIONS),
        "recent_rugs": list(RECENT_RUGS),
        "recent_opportunities": list(RECENT_OPPORTUNITIES),
        "recent_smart_money": list(RECENT_SMART_MONEY),
        "bundle_operator_history": dict(BUNDLE_OPERATOR_HISTORY),
        "bundle_operator_blacklist": list(BUNDLE_OPERATOR_BLACKLIST),
        "long_tail_watchlist": dict(LONG_TAIL_WATCHLIST),
        "telegram_pinged_tokens": dict(TELEGRAM_PINGED_TOKENS),
        "telegram_early_pinged_tokens": dict(TELEGRAM_EARLY_PINGED_TOKENS),
        "opportunity_recorded_tokens": dict(OPPORTUNITY_RECORDED_TOKENS),
        "mock_portfolio": {addr: dict(pos) for addr, pos in MOCK_PORTFOLIO.items()},
        "wallet_track_record": dict(WALLET_TRACK_RECORD),
        "jev_usage": dict(JEV_USAGE),
        "jev_judgment_log": {addr: dict(rec) for addr, rec in JEV_JUDGMENT_LOG.items()},
        "jev_evaluated_tokens": dict(JEV_EVALUATED_TOKENS),
        "jev_screened_tokens": dict(JEV_SCREENED_TOKENS),
        "jev_semantic_cache": dict(JEV_SEMANTIC_CACHE),
        "jev_recent_judgments": list(RECENT_JEV_JUDGMENTS),
        "jev_recent_events": list(RECENT_JEV_EVENTS)[-80:],
        "jev_pvp_log": {k: v for k, v in JEV_PVP_LOG.items()},
        "jev_pvp_token_index": {k: list(v) for k, v in JEV_PVP_TOKEN_INDEX.items()},
        "jev_proposed_questions": {k: v for k, v in JEV_PROPOSED_QUESTIONS.items()},
        "narrative_themes": {k: v for k, v in NARRATIVE_THEMES.items()},
        "token_theme": dict(TOKEN_THEME),
        "jev_learned_weights": dict(JEV_LEARNED_WEIGHTS),
        "big_runners": {chain: list(rs) for chain, rs in BIG_RUNNERS.items()},
        "big_runners_seen": list(BIG_RUNNERS_SEEN),
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
        loaded_launch_history = saved.get("dev_launch_history", {})
        for k, v in loaded_launch_history.items():
            DEV_LAUNCH_HISTORY[k] = v
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
        loaded_smart_money = saved.get("recent_smart_money", [])
        RECENT_SMART_MONEY.extend(loaded_smart_money)
        loaded_opportunities = saved.get("recent_opportunities", [])
        for opp in loaded_opportunities:
            addr = opp.get("token_address") or ""
            if addr in STONKFUN_QUOTE_MINTS:
                continue
            mc = opp.get("market_cap_usd") or 0.0
            plat = opp.get("platform")
            qualifies, _ = is_launchpad_or_target_suffix(plat, addr)
            is_stonk = is_stonkboard_token(addr, plat, opp.get("links"))
            mc_ok = (mc <= MAX_OPPORTUNITY_MARKET_CAP_USD) if is_stonk else (mc <= IMPLAUSIBLE_NEW_LAUNCH_MCAP_USD)
            if mc_ok and qualifies:
                RECENT_OPPORTUNITIES.append(opp)
        loaded_bundle_history = saved.get("bundle_operator_history", {})
        for k, v in loaded_bundle_history.items():
            BUNDLE_OPERATOR_HISTORY[k] = v
        BUNDLE_OPERATOR_BLACKLIST.update(saved.get("bundle_operator_blacklist", []))
        loaded_long_tail = saved.get("long_tail_watchlist", {})
        for k, v in loaded_long_tail.items():
            if k not in STONKFUN_QUOTE_MINTS:
                LONG_TAIL_WATCHLIST[k] = v
        loaded_telegram_pinged = saved.get("telegram_pinged_tokens", {})
        for k, v in loaded_telegram_pinged.items():
            if k not in STONKFUN_QUOTE_MINTS:
                TELEGRAM_PINGED_TOKENS[k] = v
        loaded_telegram_early_pinged = saved.get("telegram_early_pinged_tokens", {})
        for k, v in loaded_telegram_early_pinged.items():
            if k not in STONKFUN_QUOTE_MINTS:
                TELEGRAM_EARLY_PINGED_TOKENS[k] = v
        loaded_opportunity_recorded = saved.get("opportunity_recorded_tokens", {})
        for k, v in loaded_opportunity_recorded.items():
            if k not in STONKFUN_QUOTE_MINTS:
                OPPORTUNITY_RECORDED_TOKENS[k] = v
        loaded_mock_portfolio = saved.get("mock_portfolio", {})
        for k, v in loaded_mock_portfolio.items():
            if k in STONKFUN_QUOTE_MINTS:
                continue
            entry_mc = v.get("entry_market_cap") or 0.0
            plat = v.get("platform")
            qualifies, _ = is_launchpad_or_target_suffix(plat, k)
            is_stonk = is_stonkboard_token(k, plat, v.get("links"))
            mc_ok = (entry_mc <= MAX_OPPORTUNITY_MARKET_CAP_USD) if is_stonk else (entry_mc <= IMPLAUSIBLE_NEW_LAUNCH_MCAP_USD)
            if mc_ok and qualifies:
                MOCK_PORTFOLIO[k] = v

        loaded_wallet_track_record = saved.get("wallet_track_record", {})
        for k, v in loaded_wallet_track_record.items():
            WALLET_TRACK_RECORD[k] = v
        # Jev: restore usage (so lifetime cap survives restart) + judgment log.
        loaded_jev_usage = saved.get("jev_usage", {})
        for k, v in loaded_jev_usage.items():
            if k in JEV_USAGE:
                JEV_USAGE[k] = v
        loaded_jev_log = saved.get("jev_judgment_log", {})
        for k, v in loaded_jev_log.items():
            if k not in STONKFUN_QUOTE_MINTS:
                JEV_JUDGMENT_LOG[k] = v
        loaded_jev_evaluated = saved.get("jev_evaluated_tokens", {})
        for k, v in loaded_jev_evaluated.items():
            if k not in STONKFUN_QUOTE_MINTS:
                JEV_EVALUATED_TOKENS[k] = v
        loaded_jev_screened = saved.get("jev_screened_tokens", {})
        for k, v in loaded_jev_screened.items():
            if k not in STONKFUN_QUOTE_MINTS:
                JEV_SCREENED_TOKENS[k] = v
        loaded_jev_semantic = saved.get("jev_semantic_cache", {})
        for k, v in loaded_jev_semantic.items():
            if k not in STONKFUN_QUOTE_MINTS:
                JEV_SEMANTIC_CACHE[k] = v
        for rec in saved.get("jev_recent_judgments", []):
            if rec.get("token_address") not in STONKFUN_QUOTE_MINTS:
                RECENT_JEV_JUDGMENTS.append(rec)
        for evt in saved.get("jev_recent_events", []):
            if evt.get("token_address") not in STONKFUN_QUOTE_MINTS:
                RECENT_JEV_EVENTS.append(evt)
        for k, v in saved.get("jev_pvp_log", {}).items():
            JEV_PVP_LOG[k] = v
        for k, v in saved.get("jev_pvp_token_index", {}).items():
            JEV_PVP_TOKEN_INDEX[k] = set(v)
        for k, v in saved.get("jev_proposed_questions", {}).items():
            JEV_PROPOSED_QUESTIONS[k] = v
        for k, v in saved.get("narrative_themes", {}).items():
            NARRATIVE_THEMES[k] = v
        for k, v in saved.get("token_theme", {}).items():
            TOKEN_THEME[k] = v
        for k, v in saved.get("jev_learned_weights", {}).items():
            JEV_LEARNED_WEIGHTS[k] = v
        for chain, rs in saved.get("big_runners", {}).items():
            BIG_RUNNERS[chain] = [r for r in rs if r.get("token_address") not in STONKFUN_QUOTE_MINTS]
        BIG_RUNNERS_SEEN.update([x for x in saved.get("big_runners_seen", []) if x not in STONKFUN_QUOTE_MINTS])
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
            "total_launches": 0,
            "is_blacklisted": False,
            "last_launch_time": 0.0,
        }
    entry = DEV_REPUTATION_DATABASE[dev_wallet]
    if "total_launches" not in entry:
        entry["total_launches"] = max(
            entry.get("successful_launches", 0) + entry.get("failed_spams", 0),
            len(DEV_SPAM_LOG.get(dev_wallet, [])),
            1,
        )
    return entry


def dev_rep_badge_fields(dev_wallet: str, current_token_address: Optional[str] = None) -> dict[str, Any]:
    """Compact dev track-record summary attached to token cards, narrative
    launches, and alerts — so a wallet address is never the only thing shown.
    What matters is whether this dev has rugged or graduated something before,
    how many total coins they have launched, and what coins they previously launched."""
    if dev_wallet.lower().startswith("stonkboard"):
        return {
            "dev_alias": "StonkFun Launchpad",
            "dev_moons": 0,
            "dev_rugs": 0,
            "dev_total_launches": 1,
            "dev_blacklisted": False,
            "dev_prior_tickers": [],
            "dev_prior_launches": [],
        }
    if dev_wallet.lower() in KNOWN_INFRASTRUCTURE_ADDRESSES or dev_wallet.lower().startswith("discovered:"):
        alias = "discovered token" if dev_wallet.lower().startswith("discovered:") else "shared infra (not a person)"
        return {
            "dev_alias": alias,
            "dev_moons": 0,
            "dev_rugs": 0,
            "dev_total_launches": 0,
            "dev_blacklisted": False,
            "dev_prior_tickers": [],
            "dev_prior_launches": [],
        }
    dev = DEV_REPUTATION_DATABASE.get(dev_wallet)
    spam_count = len(DEV_SPAM_LOG.get(dev_wallet, []))
    rug_history_count = len(DEV_RUG_HISTORY.get(dev_wallet, []))
    all_launches = DEV_LAUNCH_HISTORY.get(dev_wallet, [])
    prior_launches = []
    for h in all_launches:
        if current_token_address and h.get("token_address") == current_token_address:
            continue
        launch_copy = dict(h)
        t_addr = launch_copy.get("token_address", "")
        if float(launch_copy.get("peak_market_cap") or 0.0) <= 0 and t_addr:
            known = get_known_token_peak_mcap(t_addr)
            if known > 0:
                launch_copy["peak_market_cap"] = known
                h["peak_market_cap"] = known
        prior_launches.append(launch_copy)
    prior_tickers = [h.get("ticker", "") for h in prior_launches if h.get("ticker") and h.get("ticker") != "UNKNOWN"]
    unique_prior_tickers = list(dict.fromkeys(prior_tickers))

    if not dev:
        return {
            "dev_alias": None,
            "dev_moons": 0,
            "dev_rugs": max(rug_history_count, 0),
            "dev_total_launches": max(spam_count, len(all_launches), 1),
            "dev_blacklisted": rug_history_count > 0,
            "dev_prior_tickers": unique_prior_tickers,
            "dev_prior_launches": prior_launches[-5:],
        }
    moons = dev.get("successful_launches", 0)
    rugs = max(dev.get("failed_spams", 0), rug_history_count)
    total = max(dev.get("total_launches", 0), spam_count, len(all_launches), moons + rugs, 1)
    is_bl = bool(dev.get("is_blacklisted", False) or rugs > 0 or rug_history_count > 0)
    return {
        "dev_alias": dev.get("alias"),
        "dev_moons": moons,
        "dev_rugs": rugs,
        "dev_total_launches": total,
        "dev_blacklisted": is_bl,
        "dev_prior_tickers": unique_prior_tickers,
        "dev_prior_launches": prior_launches[-5:],
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
    if dev_wallet.lower() in KNOWN_INFRASTRUCTURE_ADDRESSES or dev_wallet.lower().startswith("stonkboard") or dev_wallet.lower().startswith("discovered:"):
        alias = "StonkFun Launchpad" if dev_wallet.lower().startswith("stonkboard") else ("discovered-token" if dev_wallet.lower().startswith("discovered:") else "shared-infrastructure")
        return {"decision": "PASS", "reason": "SHARED_INFRASTRUCTURE", "dev": {"alias": alias, "is_blacklisted": False, "successful_launches": 0, "total_launches": 1}}

    dev = get_or_create_dev(dev_wallet, chain)
    dev["total_launches"] = dev.get("total_launches", 0) + 1

    recent_spam = [t for t in DEV_SPAM_LOG[dev_wallet] if ts - t <= SPAM_WINDOW_SECONDS]
    DEV_SPAM_LOG[dev_wallet] = recent_spam

    if dev["is_blacklisted"]:
        return {"decision": "SKIP", "reason": "BLACKLISTED_DEV", "dev": dev}

    if dev.get("failed_spams", 0) > 0 or len(DEV_RUG_HISTORY.get(dev_wallet, [])) > 0:
        dev["is_blacklisted"] = True
        return {"decision": "SKIP", "reason": "SERIAL_RUGGER_HISTORY", "dev": dev}

    if len(recent_spam) >= SPAM_THRESHOLD:
        dev["is_blacklisted"] = True
        return {"decision": "SKIP", "reason": "SERIAL_RUGGER_THRESHOLD", "dev": dev}

    dev["last_launch_time"] = ts

    # Multi-launch dev is allowed — warn user with prior coins
    if dev["total_launches"] > 1:
        return {"decision": "PASS", "reason": f"DEV_MULTI_LAUNCH ({dev['total_launches']} launches)", "dev": dev}

    return {"decision": "PASS", "reason": "SINGLE_LAUNCH_DEV", "dev": dev}


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
    if token_address in STONKFUN_QUOTE_MINTS:
        logger.debug(f"[token-event] Skipping StonkFun quote/collateral token {token_address} ({ticker_raw})")
        return
    ts = ts or time.time()
    extra = extra or {}

    bonding_curve_key = (extra or {}).get("bonding_curve_key")
    coin_name = (extra or {}).get("name")
    coin_desc = (extra or {}).get("description")
    img_url = (extra or {}).get("image_url")
    if not img_url and is_stonkboard_token(token_address, platform):
        img_url = f"https://thestonkboard.com/api/logos/{token_address}"

    TOKEN_CREATION_TIME[token_address] = ts
    TOKEN_WATCHLIST[token_address] = {
        "chain": chain,
        "platform": platform,
        "dev_wallet": dev_wallet,
        "ticker": ticker_raw,
        "name": coin_name,
        "description": coin_desc,
        "image_url": img_url,
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
        name=coin_name,
        description=coin_desc,
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
        links=build_token_links(chain, token_address, platform=platform),
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
        image_url=img_url,
        **dev_rep_badge_fields(dev_wallet, token_address),
    )
    record_dev_launch_event(dev_wallet, token_address, ticker_raw, chain, platform, ts)
    await bump_daily_stat(ts, "total_tokens")
    await bump_hourly_launch_stat(ts)

    dev_verdict = stage_a_dev_trust(dev_wallet, chain, ts)

    # Reject established coins / tokenized stocks at the door — these are NOT
    # new fair-launches (HYPE, ZEC, MSTRx, AAPLx...). Blocking here keeps them
    # out of narrative clustering, scoring, and Jev entirely. The mcap arm of
    # the detector fires later in the poll loop once DexScreener fills mcap in.
    est, est_reason = is_probably_established_or_stock(ticker_raw, 0.0, platform=platform, token_address=token_address)
    _tinv, _treason = ticker_is_invalid(ticker_raw)
    if est or _tinv:
        _reason = est_reason if est else _treason
        _title = "SKIPPED - NOT A NEW LAUNCH (established/stock coin)" if est else "SKIPPED - INVALID TICKER"
        TOKEN_WATCHLIST[token_address]["status"] = "SKIPPED"
        TOKEN_WATCHLIST[token_address]["dev_decision"] = "SKIPPED"
        token_feed_upsert(token_address, status="SKIPPED", dev_decision="SKIPPED")
        await broadcast_token_card(TOKEN_FEED[token_address])
        await bump_daily_stat(ts, "skipped")
        await broadcast_alert({
            "type": "SKIPPED",
            "severity": "info",
            "title": _title,
            "chain": chain, "platform": platform, "token_address": token_address,
            "ticker": ticker_raw, "dev_wallet": dev_wallet, "reason": _reason,
            "timestamp": ts,
        })
        return

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

    event_payload = {
        "wallet": wallet_address,
        "alias": wallet_info["alias"],
        "chain": chain,
        "token_address": token_address,
        "trade_size_usd": trade_size_usd,
        "timestamp": ts,
    }
    RECENT_SMART_MONEY.append(event_payload)
    await broadcast_smart_money_activity(event_payload)

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
        new_mints = {m for m in (post_mints - pre_mints) if m and m not in STONKFUN_QUOTE_MINTS}
        if not new_mints or not dev_wallet:
            return
        token_address = next(iter(new_mints))
        if token_address in STONKFUN_QUOTE_MINTS:
            return
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
    client = JsonRpcWsClient(SOLANA_WS_RPC_URL, fallback_url=SOLANA_FALLBACK_WS_RPC_URL)
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

async def handle_solana_wallet_log(client: JsonRpcWsClient, value: dict, tracked_wallets: list[str], sub_wallet: Optional[str] = None) -> None:
    if value.get("err"):
        return
    logs = value.get("logs", [])
    signature = value.get("signature")
    if not signature:
        return
    log_text = " ".join(logs)
    is_swap_like = any(kw in log_text for kw in ("Buy", "buy", "Swap", "swap", "Trade", "trade", "Route", "route", "Order", "order", "Fill", "fill"))
    if not is_swap_like:
        return
    # The wallet is resolved from the per-wallet subscription (the mentions
    # filter guarantees it's involved) — the old code tried to find the wallet
    # ADDRESS as a substring of the program LOG TEXT, which never matched (logs
    # are program messages, not account keys), so NO buy was ever detected.
    mentioned = [sub_wallet] if sub_wallet else [w for w in tracked_wallets if w in log_text]
    for wallet in mentioned:
        if not wallet:
            continue
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

    client = JsonRpcWsClient(SOLANA_WS_RPC_URL, fallback_url=SOLANA_FALLBACK_WS_RPC_URL)
    await client.connect()
    subscribed: set[str] = set()
    sub_to_wallet: dict[Any, str] = {}  # logsSubscribe subscription id -> wallet
    try:
        while True:
            current = {w for w, info in SMART_WALLETS.items() if info.get("chain") == "solana"}
            new_wallets = current - subscribed
            for wallet in new_wallets:
                sub_id = await client.subscribe("logsSubscribe", [{"mentions": [wallet]}, {"commitment": "confirmed"}])
                if sub_id is not None:
                    sub_to_wallet[sub_id] = wallet
            if new_wallets:
                subscribed |= new_wallets
                logger.info(f"[wallet/solana] subscribed to {len(new_wallets)} new wallet(s), {len(subscribed)} total")

            try:
                result = await asyncio.wait_for(client.next_notification(), timeout=SMART_WALLET_RESYNC_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue
            value = result.get("value", {})
            if value:
                # resolve which wallet this log belongs to via the subscription
                # id (the mentions filter guarantees the wallet is involved).
                sub_wallet = sub_to_wallet.get(result.get("_subscription"))
                await handle_solana_wallet_log(client, value, list(subscribed), sub_wallet)
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
HOLDER_STATS_POLL_INTERVAL_SECONDS = 120  # throttled separately from the 20s mcap poll

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
        image_url=dex_info.get("image_url") or TOKEN_WATCHLIST[token_address].get("image_url") or (TOKEN_FEED.get(token_address) or {}).get("image_url"),
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
    # Don't record transient 0/blip mcaps — a DexScreener hiccup that returns 0
    # for one poll would otherwise inject a $0 point mid-trajectory, making the
    # momentum Jev sees look nonsensical ($11k -> $0 -> $43k). Skip non-positive
    # readings if we already have real history; keep the last good curve.
    if market_cap <= 0 and points:
        return points
    points.append((ts, market_cap))
    if len(points) > SPARKLINE_MAX_POINTS:
        points = points[-SPARKLINE_MAX_POINTS:]
    return points


def _identity_fields(token_address: str, info: dict[str, Any]) -> dict[str, Any]:
    plat = infer_launchpad_platform(info.get("platform"), token_address) or info.get("platform")
    is_stonk = is_stonkboard_token(token_address, plat)
    if is_stonk:
        plat = "stonkfun"
    fields = {
        "chain": info["chain"],
        "platform": plat,
        "is_stonkboard": is_stonk,
        "dev_wallet": info["dev_wallet"],
        "ticker": info["ticker"],
        "created_at": info["created_at"],
        "dev_decision": info.get("dev_decision", "PASS"),
        "links": build_token_links(info["chain"], token_address, platform=plat),
        "socials": info.get("socials"),
        **dev_rep_badge_fields(info["dev_wallet"]),
    }
    if info.get("name"):
        fields["name"] = info["name"]
    if info.get("description"):
        fields["description"] = info["description"]
    return fields


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
    record_token_peak_mcap(token_address, info["peak_market_cap"], info.get("dev_wallet"))
    record_big_runner_if_qualified(token_address, TOKEN_FEED.get(token_address, info), info["peak_market_cap"])
    info["status"] = "GRADUATED"
    _credit_early_buyers(token_address, info["chain"], "graduations")

    if token_address in MOCK_PORTFOLIO:
        mock_pos = MOCK_PORTFOLIO[token_address]
        if market_cap > 0:
            mock_pos["last_known_market_cap"] = market_cap
            if market_cap > mock_pos.get("peak_market_cap", 0.0):
                mock_pos["peak_market_cap"] = market_cap
                mock_pos["peak_ts"] = now
        mock_pos["last_known_status"] = "GRADUATED"
        mock_pos["last_known_ts"] = now

    dev = DEV_REPUTATION_DATABASE.get(info["dev_wallet"])
    if dev:
        dev["successful_launches"] += 1

    sparkline = _update_feed_sparkline(token_address, market_cap, now)
    token_feed_upsert(
        token_address, status="GRADUATED", market_cap=market_cap,
        peak_market_cap=info["peak_market_cap"], sparkline=sparkline,
        volume_24h=dex_info.get("volume_24h", 0.0), liquidity_usd=dex_info.get("liquidity_usd", 0.0),
        txns_24h=dex_info.get("txns_24h", 0), buys_24h=dex_info.get("buys_24h", 0),
        sells_24h=dex_info.get("sells_24h", 0),
        image_url=dex_info.get("image_url") or info.get("image_url") or (TOKEN_FEED.get(token_address) or {}).get("image_url"),
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
    # NOTE: graduation is NO LONGER treated as "mooned" — a coin can graduate
    # and still rug. Outcome is graded on real mcap trajectory (see
    # jev_update_trajectory). We just record graduation as a milestone.
    rec_grad = JEV_JUDGMENT_LOG.get(token_address)
    if rec_grad is not None:
        rec_grad["graduated"] = True
        rec_grad["graduated_at"] = time.time()
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


def _clean_signal_reason(r: Any) -> str:
    """Strips score adjustments like (+15) or (-25) for clean, readable bullet points."""
    return re.sub(r"\s*\([+-]\d+\)$", "", str(r).strip())


def _fit_telegram_caption(text: str, max_len: int = 1024) -> str:
    """Guarantees a Telegram HTML message or photo caption stays within max_len (1024).
    Trims narrative text or secondary signals if needed, NEVER drops Research links or CA."""
    text = text.strip()
    if len(text) <= max_len:
        return text

    # If over length, locate Narrative block and shorten it
    if "📖 <b>Narrative:</b>" in text:
        paras = text.split("\n\n")
        for idx, p in enumerate(paras):
            if "📖 <b>Narrative:</b>" in p:
                over = len(text) - max_len
                if len(p) > over + 35:
                    if "· <a" in p:
                        prefix, link = p.split("· <a", 1)
                        link = "· <a" + link
                        trim_amount = over + 5
                        new_prefix = prefix[:max(20, len(prefix) - trim_amount)].rsplit(' ', 1)[0] + "..."
                        paras[idx] = f"{new_prefix} {link}"
                    else:
                        paras[idx] = p[:len(p) - over - 5].rsplit(' ', 1)[0] + "..."
                else:
                    paras.pop(idx)
                break
        res = "\n\n".join(paras).strip()
        if len(res) <= max_len:
            return res

    # If still over, drop 1 signal bullet if more than 1 bullet exists
    if "💡 <b>Key Signals:</b>" in text:
        paras = text.split("\n\n")
        for idx, p in enumerate(paras):
            if "💡 <b>Key Signals:</b>" in p:
                lines = p.split("\n")
                if len(lines) > 2:
                    paras[idx] = "\n".join(lines[:2])
                break
        res = "\n\n".join(paras).strip()
        if len(res) <= max_len:
            return res

    return text[:max_len]


def _format_telegram_opportunity_message(entry: dict[str, Any]) -> str:
    """Ultra-clean, high-signal Telegram format for opportunity calls.
    Features a dedicated 1-tap copy/paste CA box, clean metrics, security,
    DeBot narrative origin, trade signals, and organized research/social links."""
    def esc(v: Any) -> str:
        return html.escape(str(v)) if v is not None else ""

    ticker = entry.get("ticker") or "UNKNOWN"
    token_address = entry.get("token_address") or ""
    chain = (entry.get("chain") or "?").upper()
    platform = infer_launchpad_platform(entry.get("platform"), token_address) or entry.get("platform") or "?"

    market_cap = float(entry.get("market_cap") or entry.get("market_cap_usd") or 0.0)
    volume_24h = float(entry.get("volume_24h") or entry.get("volume_24h_usd") or entry.get("volume") or 0.0)
    liquidity_usd = float(entry.get("liquidity_usd") or entry.get("liquidity") or 0.0)
    buys_24h = entry.get("buys_24h") or 0
    sells_24h = entry.get("sells_24h") or 0

    mcap_str = f"${market_cap/1_000:.1f}K" if market_cap < 1_000_000 else f"${market_cap/1_000_000:.2f}M"
    vol_str = f"${volume_24h/1_000:.1f}K" if volume_24h < 1_000_000 else f"${volume_24h/1_000_000:.2f}M"
    liq_str = f"${liquidity_usd/1_000:.1f}K" if liquidity_usd < 1_000_000 else f"${liquidity_usd/1_000_000:.2f}M"

    holder_count = entry.get("holder_count")
    top_holder_pct = entry.get("top_holder_pct")
    if holder_count is not None:
        holders_str = f"{holder_count}"
        if top_holder_pct is not None:
            if top_holder_pct > 5.0:
                holders_str += f" (⚠️ top {top_holder_pct:.1f}%)"
            else:
                holders_str += f" (top {top_holder_pct:.1f}%)"
    else:
        holders_str = "n/a"

    dev_alias = entry.get("dev_alias") or (esc(entry.get("dev_wallet", ""))[:6] + "..." if entry.get("dev_wallet") else "Unknown")
    dev_moons = entry.get("dev_moons") or 0
    dev_rugs = entry.get("dev_rugs") or 0
    dev_total = entry.get("dev_total_launches") or 1
    dev_wallet = entry.get("dev_wallet", "")

    priors = entry.get("dev_prior_tickers") or [
        h.get("ticker") for h in DEV_LAUNCH_HISTORY.get(dev_wallet, [])
        if h.get("ticker") and h.get("ticker") != "UNKNOWN" and h.get("token_address") != token_address
    ]
    unique_priors = list(dict.fromkeys(priors))

    dev_str = f"{esc(dev_alias)} (🚀{dev_moons}/💀{dev_rugs})"
    if dev_total > 1:
        dev_str += f" · ⚠️ {dev_total}x"

    dev_warn_line = ""
    if dev_total > 1:
        prior_launches = entry.get("dev_prior_launches") or [
            h for h in DEV_LAUNCH_HISTORY.get(dev_wallet, [])
            if h.get("token_address") != token_address
        ]
        dedup_map: dict[str, dict[str, Any]] = {}
        for h in prior_launches:
            t = h.get("ticker")
            if not t or t == "UNKNOWN":
                continue
            peak = float(h.get("peak_market_cap") or 0.0) or get_known_token_peak_mcap(h.get("token_address", ""))
            if t not in dedup_map or peak > float(dedup_map[t].get("peak_market_cap") or 0.0):
                dedup_map[t] = {"ticker": t, "peak_market_cap": peak}
        if dedup_map:
            prior_items = []
            for d in list(dedup_map.values())[:4]:
                p_mcap = d["peak_market_cap"]
                if p_mcap > 0:
                    prior_items.append(f"${esc(d['ticker'])} (peak {format_mcap_compact(p_mcap)})")
                else:
                    prior_items.append(f"${esc(d['ticker'])}")
            if len(dedup_map) > 4:
                prior_items.append(f"(+{len(dedup_map)-4} more)")
            dev_warn_line = f"⚠️ <b>Dev Previously Launched ({dev_total}x):</b> {', '.join(prior_items)}\n"
        elif unique_priors:
            prior_display = ", ".join(f"${esc(t)}" for t in unique_priors[:4])
            if len(unique_priors) > 4:
                prior_display += f" (+{len(unique_priors)-4} more)"
            dev_warn_line = f"⚠️ <b>Dev Previously Launched ({dev_total}x):</b> {prior_display}\n"
        else:
            dev_warn_line = f"⚠️ <b>Dev History:</b> {dev_total} launches on record\n"

    # GoPlus security status
    gp = entry.get("goplus") or {}
    gp_raw = gp.get("summary")
    if not gp_raw:
        if entry.get("freeze_authority_active") is False:
            gp_raw = "Clean · Freeze Renounced"
        else:
            gp_raw = "Clean"
    clean_gp = gp_raw.replace("🛡️", "").strip() or "Clean"

    buy_tax = float(gp.get("buy_tax") or entry.get("buy_tax") or 0.0)
    sell_tax = float(gp.get("sell_tax") or entry.get("sell_tax") or 0.0)
    if buy_tax > 0 or sell_tax > 0:
        tax_str = f"Buy {buy_tax:g}% / Sell {sell_tax:g}%"
    else:
        tax_str = "0% / 0%"

    is_stonk = (platform.lower() == "stonkfun") or is_stonkboard_token(token_address)
    plat_display = "📈 StonkFun" if is_stonk else esc(platform)
    title_badge = " <i>[📈 StonkFun]</i>" if is_stonk else ""

    created_at = entry.get("created_at") or entry.get("timestamp")
    timing_parts = []
    if created_at:
        age_sec = max(0, int(time.time() - created_at))
        if age_sec < 60:
            timing_parts.append(f"{age_sec}s ago")
        elif age_sec < 3600:
            timing_parts.append(f"{age_sec // 60}m ago")
        elif age_sec < 86400:
            timing_parts.append(f"{age_sec // 3600}h {(age_sec % 3600) // 60}m ago")
        else:
            timing_parts.append(f"{age_sec // 86400}d ago")
    now_utc = datetime.fromtimestamp(entry.get("timestamp") or time.time(), tz=timezone.utc).strftime("%H:%M UTC")
    timing_parts.append(now_utc)
    timing_str = " · ".join(timing_parts)

    header_block = (
        f"🎯 <b>${esc(ticker)}</b>{title_badge}\n"
        f"🪐 <b>Platform:</b> {esc(chain)} • {plat_display}\n\n"
        f"📋 <b>CA:</b> <i>(tap to copy)</i>\n"
        f"<pre><code>{esc(token_address)}</code></pre>\n\n"
        f"💰 <b>MCap:</b> {mcap_str}  │  💧 <b>Liq:</b> {liq_str}\n"
        f"📊 <b>Vol 24h:</b> {vol_str}  <i>({buys_24h}B / {sells_24h}S)</i>\n"
        f"👥 <b>Holders:</b> {holders_str}  │  🧑‍💻 <b>Dev:</b> {dev_str}\n"
        + dev_warn_line +
        f"🛡️ <b>Security:</b> {esc(clean_gp)}  │  💸 <b>Tax:</b> {esc(tax_str)}\n"
        f"⏱️ <b>Timing:</b> {esc(timing_str)}\n\n"
    )

    # Key driving signals as neat bullet points (top 2 for high signal & guaranteed fit)
    reasons = entry.get("score_reasons") or []
    signal_bullets = []
    active_boosts = int((entry.get("socials") or {}).get("active_boosts") or 0)
    if active_boosts > 0:
        signal_bullets.append(f"• ⚡ Paid DexScreener Boosts ({active_boosts}x)")
    for r in reasons:
        cleaned = _clean_signal_reason(r)
        if not cleaned or any(x in cleaned for x in ["Ticker not yet resolved", "Volume/mcap ratio", "Contract suffix", "Not from a launchpad"]):
            continue
        signal_bullets.append(f"• {esc(cleaned)}")
    if not signal_bullets:
        signal_bullets.append("• Strong launch momentum & volume activity")
    signals_block = "💡 <b>Key Signals:</b>\n" + "\n".join(signal_bullets[:2]) + "\n\n"

    # Dedicated link block (FOMO only, with StonkBoard for StonkFun)
    links = entry.get("links") or {}
    fomo_url = links.get("fomo")
    if not fomo_url:
        chain_lower = (chain or "solana").lower()
        fomo_chain = chain_lower if chain_lower in ("solana", "bnb", "base", "robinhood") else ("bnb" if chain_lower in ("bsc", "binance") else "solana")
        fomo_url = f"https://fomo.family/tokens/{fomo_chain}/{token_address}"
    stonk_url = links.get("stonkboard") or (f"https://thestonkboard.com/coin/{token_address}" if (platform.lower() == "stonkfun" or is_stonkboard_token(token_address)) else None)

    link_items = [f'<a href="{esc(fomo_url)}">🦄 FOMO.family</a>']
    if stonk_url:
        link_items.append(f'<a href="{esc(stonk_url)}">📈 StonkBoard</a>')
    research_str = " │ ".join(link_items)
    research_block = f"🔗 <b>Link:</b> {research_str}"

    # Social channels (only appended if at least one exists)
    soc_links = entry.get("socials") or {}
    soc_items = []
    tw_url = soc_links.get("twitter")
    tg_url = soc_links.get("telegram")
    web_url = soc_links.get("website")
    if tw_url:
        tw_meta = entry.get("twitter_meta") or {}
        tw_tag = f" ({tw_meta['created_at']})" if tw_meta.get("created_at") else ""
        soc_items.append(f'<a href="{esc(tw_url)}">🐦 X{tw_tag}</a>')
    if tg_url:
        soc_items.append(f'<a href="{esc(tg_url)}">✈️ TG</a>')
    if web_url:
        soc_items.append(f'<a href="{esc(web_url)}">🌐 Web</a>')
    socials_str = " │ ".join(soc_items) if soc_items else ""
    socials_block = f"\n🌐 <b>Socials:</b> {socials_str}" if socials_str else ""

    # Dynamically budget space for DeBot Narrative so Research and Socials NEVER get squeezed out
    fixed_len = len(header_block) + len(signals_block) + len(research_block) + len(socials_block)
    max_total = 1010
    narrative_budget = max_total - fixed_len

    debot = entry.get("debot") or {}
    origin_text = (debot.get("origin_text") or "").strip()
    narrative_type = debot.get("narrative_type")
    ref_link = f' · <a href="{esc(debot["origin_ref"])}">🔗 Source</a>' if debot.get("origin_ref") else ""

    if not origin_text:
        # Fallback 1: Project's own description / lore (from pump.fun or StonkBoard metadata)
        desc_raw = (entry.get("description") or (TOKEN_WATCHLIST.get(token_address) or {}).get("description") or "").strip()
        if desc_raw:
            origin_text = desc_raw
            narrative_type = "Creator Lore"
            ref_link = ""
        elif entry.get("score_reasons"):
            # Fallback 2: Check for AI narrative / lore observations in reasons
            for r in entry.get("score_reasons", []):
                if any(k in r.lower() for k in ["narrative", "lore", "concept", "theme"]):
                    cleaned_r = _clean_signal_reason(r)
                    if cleaned_r:
                        origin_text = cleaned_r
                        narrative_type = "AI Intel"
                        ref_link = ""
                        break

    debot_line = ""
    if origin_text and narrative_budget > 60:
        type_tag = f"<i>[{esc(narrative_type)}]</i> " if narrative_type else ""
        prefix = f"📖 <b>Narrative:</b> {type_tag}"
        suffix = f"{ref_link}\n\n"
        max_orig_len = narrative_budget - len(prefix) - len(suffix)
        if max_orig_len > 15:
            if len(origin_text) > max_orig_len:
                clean_orig = origin_text[:max_orig_len - 3].rsplit(' ', 1)[0] + "..."
            else:
                clean_orig = origin_text
            debot_line = f"{prefix}{esc(clean_orig)}{suffix}"

    text = header_block + debot_line + signals_block + research_block + socials_block
    return _fit_telegram_caption(text.strip(), max_len=1024)


def _format_telegram_early_momentum_message(entry: dict[str, Any]) -> str:
    """Ultra-clean Telegram format for early breakout momentum."""
    def esc(v: Any) -> str:
        return html.escape(str(v)) if v is not None else ""

    ticker = entry.get("ticker") or "UNKNOWN"
    token_address = entry.get("token_address") or ""
    chain = (entry.get("chain") or "?").upper()
    platform = infer_launchpad_platform(entry.get("platform"), token_address) or entry.get("platform") or "?"

    market_cap = float(entry.get("market_cap") or 0.0)
    volume_24h = float(entry.get("volume_24h") or entry.get("volume") or 0.0)
    liquidity_usd = float(entry.get("liquidity_usd") or entry.get("liquidity") or 0.0)
    buys_24h = entry.get("buys_24h") or 0
    sells_24h = entry.get("sells_24h") or 0

    mcap_str = f"${market_cap/1_000:.1f}K" if market_cap < 1_000_000 else f"${market_cap/1_000_000:.2f}M"
    vol_str = f"${volume_24h/1_000:.1f}K" if volume_24h < 1_000_000 else f"${volume_24h/1_000_000:.2f}M"
    liq_str = f"${liquidity_usd/1_000:.1f}K" if liquidity_usd < 1_000_000 else f"${liquidity_usd/1_000_000:.2f}M"

    holder_count = entry.get("holder_count")
    top_holder_pct = entry.get("top_holder_pct")
    holders_str = f"{holder_count}" if holder_count is not None else "n/a"
    if holder_count is not None and top_holder_pct is not None:
        if top_holder_pct > 5.0:
            holders_str += f" (⚠️ top {top_holder_pct:.1f}%)"
        else:
            holders_str += f" (top {top_holder_pct:.1f}%)"

    dev_alias = entry.get("dev_alias") or (esc(entry.get("dev_wallet", ""))[:6] + "..." if entry.get("dev_wallet") else "Unknown")
    dev_moons = entry.get("dev_moons") or 0
    dev_rugs = entry.get("dev_rugs") or 0
    dev_total = entry.get("dev_total_launches") or 1
    dev_wallet = entry.get("dev_wallet", "")

    priors = entry.get("dev_prior_tickers") or [
        h.get("ticker") for h in DEV_LAUNCH_HISTORY.get(dev_wallet, [])
        if h.get("ticker") and h.get("ticker") != "UNKNOWN" and h.get("token_address") != token_address
    ]
    unique_priors = list(dict.fromkeys(priors))

    dev_str = f"{esc(dev_alias)} (🚀{dev_moons}/💀{dev_rugs})"
    if dev_total > 1:
        dev_str += f" · ⚠️ {dev_total}x"

    dev_warn_line = ""
    if dev_total > 1:
        prior_launches = entry.get("dev_prior_launches") or [
            h for h in DEV_LAUNCH_HISTORY.get(dev_wallet, [])
            if h.get("token_address") != token_address
        ]
        dedup_map: dict[str, dict[str, Any]] = {}
        for h in prior_launches:
            t = h.get("ticker")
            if not t or t == "UNKNOWN":
                continue
            peak = float(h.get("peak_market_cap") or 0.0) or get_known_token_peak_mcap(h.get("token_address", ""))
            if t not in dedup_map or peak > float(dedup_map[t].get("peak_market_cap") or 0.0):
                dedup_map[t] = {"ticker": t, "peak_market_cap": peak}
        if dedup_map:
            prior_items = []
            for d in list(dedup_map.values())[:4]:
                p_mcap = d["peak_market_cap"]
                if p_mcap > 0:
                    prior_items.append(f"${esc(d['ticker'])} (peak {format_mcap_compact(p_mcap)})")
                else:
                    prior_items.append(f"${esc(d['ticker'])}")
            if len(dedup_map) > 4:
                prior_items.append(f"(+{len(dedup_map)-4} more)")
            dev_warn_line = f"⚠️ <b>Dev Previously Launched ({dev_total}x):</b> {', '.join(prior_items)}\n"
        elif unique_priors:
            prior_display = ", ".join(f"${esc(t)}" for t in unique_priors[:4])
            if len(unique_priors) > 4:
                prior_display += f" (+{len(unique_priors)-4} more)"
            dev_warn_line = f"⚠️ <b>Dev Previously Launched ({dev_total}x):</b> {prior_display}\n"
        else:
            dev_warn_line = f"⚠️ <b>Dev History:</b> {dev_total} launches on record\n"

    gp = entry.get("goplus") or {}
    gp_raw = gp.get("summary") or "Clean · Renounced"
    clean_gp = gp_raw.replace("🛡️", "").strip() or "Clean"

    buy_tax = float(gp.get("buy_tax") or entry.get("buy_tax") or 0.0)
    sell_tax = float(gp.get("sell_tax") or entry.get("sell_tax") or 0.0)
    if buy_tax > 0 or sell_tax > 0:
        tax_str = f"Buy {buy_tax:g}% / Sell {sell_tax:g}%"
    else:
        tax_str = "0% / 0%"

    is_stonk = (platform.lower() == "stonkfun") or is_stonkboard_token(token_address)
    plat_display = "📈 StonkFun" if is_stonk else esc(platform)
    title_badge = " <i>[📈 StonkFun]</i>" if is_stonk else ""

    created_at = entry.get("created_at") or entry.get("timestamp")
    timing_parts = []
    if created_at:
        age_sec = max(0, int(time.time() - created_at))
        if age_sec < 60:
            timing_parts.append(f"{age_sec}s ago")
        elif age_sec < 3600:
            timing_parts.append(f"{age_sec // 60}m ago")
        elif age_sec < 86400:
            timing_parts.append(f"{age_sec // 3600}h {(age_sec % 3600) // 60}m ago")
        else:
            timing_parts.append(f"{age_sec // 86400}d ago")
    now_utc = datetime.fromtimestamp(entry.get("timestamp") or time.time(), tz=timezone.utc).strftime("%H:%M UTC")
    timing_parts.append(now_utc)
    timing_str = " · ".join(timing_parts)

    header_block = (
        f"⚡ <b>Early Momentum: ${esc(ticker)}</b>{title_badge}\n"
        f"🪐 <b>Platform:</b> {esc(chain)} • {plat_display}\n\n"
        f"📋 <b>CA:</b> <i>(tap to copy)</i>\n"
        f"<pre><code>{esc(token_address)}</code></pre>\n\n"
        f"💰 <b>MCap:</b> {mcap_str}  │  💧 <b>Liq:</b> {liq_str}\n"
        f"📊 <b>Vol 24h:</b> {vol_str}  <i>({buys_24h}B / {sells_24h}S)</i>\n"
        f"👥 <b>Holders:</b> {holders_str}  │  🧑‍💻 <b>Dev:</b> {dev_str}\n"
        + dev_warn_line +
        f"🛡️ <b>Security:</b> {esc(clean_gp)}  │  💸 <b>Tax:</b> {esc(tax_str)}\n"
        f"⏱️ <b>Timing:</b> {esc(timing_str)}\n\n"
    )

    reasons = entry.get("early_momentum_reasons") or []
    signal_bullets = []
    active_boosts = int((entry.get("socials") or {}).get("active_boosts") or 0)
    if active_boosts > 0:
        signal_bullets.append(f"• ⚡ Paid DexScreener Boosts ({active_boosts}x)")
    for r in reasons:
        cleaned = _clean_signal_reason(r)
        if not cleaned or any(x in cleaned for x in ["Ticker not yet resolved", "Volume/mcap ratio", "Contract suffix", "Not from a launchpad"]):
            continue
        signal_bullets.append(f"• {esc(cleaned)}")
    if not signal_bullets:
        signal_bullets.append("• Early breakout volume surge & velocity")
    signals_block = "💡 <b>Key Signals:</b>\n" + "\n".join(signal_bullets[:2]) + "\n\n"

    # Dedicated link block (FOMO only, with StonkBoard for StonkFun)
    links = entry.get("links") or {}
    fomo_url = links.get("fomo")
    if not fomo_url:
        chain_lower = (chain or "solana").lower()
        fomo_chain = chain_lower if chain_lower in ("solana", "bnb", "base", "robinhood") else ("bnb" if chain_lower in ("bsc", "binance") else "solana")
        fomo_url = f"https://fomo.family/tokens/{fomo_chain}/{token_address}"
    stonk_url = links.get("stonkboard") or (f"https://thestonkboard.com/coin/{token_address}" if (platform.lower() == "stonkfun" or is_stonkboard_token(token_address)) else None)

    link_items = [f'<a href="{esc(fomo_url)}">🦄 FOMO.family</a>']
    if stonk_url:
        link_items.append(f'<a href="{esc(stonk_url)}">📈 StonkBoard</a>')
    research_str = " │ ".join(link_items)
    research_block = f"🔗 <b>Link:</b> {research_str}"

    soc_links = entry.get("socials") or {}
    soc_items = []
    tw_url = soc_links.get("twitter")
    tg_url = soc_links.get("telegram")
    web_url = soc_links.get("website")
    if tw_url:
        tw_meta = entry.get("twitter_meta") or {}
        tw_tag = f" ({tw_meta['created_at']})" if tw_meta.get("created_at") else ""
        soc_items.append(f'<a href="{esc(tw_url)}">🐦 X{tw_tag}</a>')
    if tg_url:
        soc_items.append(f'<a href="{esc(tg_url)}">✈️ TG</a>')
    if web_url:
        soc_items.append(f'<a href="{esc(web_url)}">🌐 Web</a>')
    socials_str = " │ ".join(soc_items) if soc_items else ""
    socials_block = f"\n🌐 <b>Socials:</b> {socials_str}" if socials_str else ""

    # Dynamically budget space for DeBot Narrative so Research and Socials NEVER get squeezed out
    fixed_len = len(header_block) + len(signals_block) + len(research_block) + len(socials_block)
    max_total = 1010
    narrative_budget = max_total - fixed_len

    debot = entry.get("debot") or {}
    origin_text = (debot.get("origin_text") or "").strip()
    narrative_type = debot.get("narrative_type")
    ref_link = f' · <a href="{esc(debot["origin_ref"])}">🔗 Source</a>' if debot.get("origin_ref") else ""

    if not origin_text:
        # Fallback 1: Project's own description / lore (from pump.fun or StonkBoard metadata)
        desc_raw = (entry.get("description") or (TOKEN_WATCHLIST.get(token_address) or {}).get("description") or "").strip()
        if desc_raw:
            origin_text = desc_raw
            narrative_type = "Creator Lore"
            ref_link = ""
        elif entry.get("early_momentum_reasons"):
            # Fallback 2: Check for AI narrative / lore observations in reasons
            for r in entry.get("early_momentum_reasons", []):
                if any(k in r.lower() for k in ["narrative", "lore", "concept", "theme"]):
                    cleaned_r = _clean_signal_reason(r)
                    if cleaned_r:
                        origin_text = cleaned_r
                        narrative_type = "AI Intel"
                        ref_link = ""
                        break

    debot_line = ""
    if origin_text and narrative_budget > 60:
        type_tag = f"<i>[{esc(narrative_type)}]</i> " if narrative_type else ""
        prefix = f"📖 <b>Narrative:</b> {type_tag}"
        suffix = f"{ref_link}\n\n"
        max_orig_len = narrative_budget - len(prefix) - len(suffix)
        if max_orig_len > 15:
            if len(origin_text) > max_orig_len:
                clean_orig = origin_text[:max_orig_len - 3].rsplit(' ', 1)[0] + "..."
            else:
                clean_orig = origin_text
            debot_line = f"{prefix}{esc(clean_orig)}{suffix}"

    text = header_block + debot_line + signals_block + research_block + socials_block
    return _fit_telegram_caption(text.strip(), max_len=1024)



def _build_telegram_reply_markup(chain: str, token_address: str, platform: Optional[str] = None) -> Optional[dict[str, Any]]:
    chain_norm = (chain or "solana").lower()
    fomo_chain = chain_norm if chain_norm in ("solana", "bnb", "base", "robinhood") else ("bnb" if chain_norm in ("bsc", "binance") else "solana")
    fomo_url = f"https://fomo.family/tokens/{fomo_chain}/{token_address}"

    buttons: list[dict[str, str]] = [
        {"text": "🦄 Open in FOMO.family", "url": fomo_url}
    ]
    if (platform or "").lower() == "stonkfun" or is_stonkboard_token(token_address):
        buttons.append({"text": "📈 StonkBoard", "url": f"https://thestonkboard.com/coin/{token_address}"})

    return {"inline_keyboard": [buttons]}


async def send_telegram_message(
    text: str, photo_url: Optional[str] = None, reply_markup: Optional[dict[str, Any]] = None
) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    chat_ids = [c.strip() for c in TELEGRAM_CHAT_ID.split(",") if c.strip()]
    if not chat_ids:
        return
    try:
        async with aiohttp.ClientSession() as session:
            for cid in chat_ids:
                sent = False
                if photo_url:
                    caption = _fit_telegram_caption(text, max_len=1024)
                    payload: dict[str, Any] = {
                        "chat_id": cid,
                        "photo": photo_url,
                        "caption": caption,
                        "parse_mode": "HTML",
                    }
                    if reply_markup:
                        payload["reply_markup"] = reply_markup
                    resp_photo = await session.post(
                        f"{TELEGRAM_API_BASE}/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=15),
                    )
                    if resp_photo.status == 200:
                        sent = True
                    else:
                        body = await resp_photo.text()
                        logger.debug(f"[telegram] sendPhoto to {cid} failed HTTP {resp_photo.status}, falling back to text: {body[:200]}")
                if not sent:
                    payload = {
                        "chat_id": cid,
                        "text": text[:4096],
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    }
                    if reply_markup:
                        payload["reply_markup"] = reply_markup
                    resp = await session.post(
                        f"{TELEGRAM_API_BASE}/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=15),
                    )
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning(f"[telegram] sendMessage to {cid} failed HTTP {resp.status}: {body[:300]}")
    except Exception as exc:
        logger.warning(f"[telegram] send failed: {exc!r}")


async def telegram_sender_worker() -> None:
    """Single consumer draining TELEGRAM_SEND_QUEUE, paced — several tokens
    could cross the opportunity threshold within the same poll cycle, and
    firing all of them as parallel unthrottled requests risks tripping
    Telegram's per-chat rate limit. Producers (see _rescore_token_and_maybe_ping)
    only ever enqueue, never call send_telegram_message directly."""
    while True:
        item = await TELEGRAM_SEND_QUEUE.get()
        text = item[0]
        photo_url = item[1] if len(item) > 1 else None
        reply_markup = item[2] if len(item) > 2 else None
        await send_telegram_message(text, photo_url, reply_markup=reply_markup)
        await asyncio.sleep(TELEGRAM_MIN_SEND_INTERVAL_SECONDS)


def _mark_opportunity_recorded(token_address: str) -> None:
    OPPORTUNITY_RECORDED_TOKENS[token_address] = time.time()
    if len(OPPORTUNITY_RECORDED_TOKENS) > OPPORTUNITY_RECORDED_MAX:
        oldest_key = next(iter(OPPORTUNITY_RECORDED_TOKENS))
        if oldest_key != token_address:
            del OPPORTUNITY_RECORDED_TOKENS[oldest_key]


def _is_safe_vetted_token(token_address: str, entry: dict[str, Any]) -> bool:
    """Verifies that a token passes all strict anti-honeypot, dev trust,
    launchpad / suffix, and under-100k market cap rules:
    1. Market cap strictly under $100k ceiling ONLY for TheStonkBoard / StonkFun coins.
    2. Launched from a recognized launchpad (pump.fun, stonkfun, etc.) and/or contract ends in 7777, pump, 4444.
    3. Honeypot / sell whitelist checks (no active freeze authority, no transfer hooks, not a honeypot chart).
    4. Dev has 0 failed/rug launches and is not blacklisted (multi-launch allowed with warning)."""
    if token_address in STONKFUN_QUOTE_MINTS:
        return False

    # Under 100k MC ceiling check - strictly for TheStonkBoard / StonkFun coins
    mcap = entry.get("market_cap") or 0.0
    is_stonk = is_stonkboard_token(token_address, entry.get("platform"), entry.get("links"))
    if is_stonk and mcap > MAX_OPPORTUNITY_MARKET_CAP_USD:
        return False
    if mcap > IMPLAUSIBLE_NEW_LAUNCH_MCAP_USD:
        return False

    # Launchpad & Contract suffix check
    qualifies_launch, _ = is_launchpad_or_target_suffix(entry.get("platform"), token_address)
    if not qualifies_launch:
        return False

    # Anti-honeypot / sell whitelist checks
    if entry.get("is_honeypot") or entry.get("sell_whitelist"):
        return False
    if entry.get("freeze_authority_active") is True:
        return False
    buys = entry.get("buys_24h") or 0
    sells = entry.get("sells_24h") or 0
    if (buys >= 4 and sells == 0) or (buys >= 15 and sells <= 1):
        return False

    dev_wallet = entry.get("dev_wallet", "")
    is_infra = (dev_wallet.lower() in KNOWN_INFRASTRUCTURE_ADDRESSES or dev_wallet.lower().startswith("stonkboard") or dev_wallet.lower().startswith("discovered:")) if dev_wallet else False
    if not is_infra and dev_wallet:
        dev_rep = DEV_REPUTATION_DATABASE.get(dev_wallet)
        dev_rugs = dev_rep.get("failed_spams", 0) if dev_rep else (entry.get("dev_rugs") or 0)
        is_bl = (dev_rep.get("is_blacklisted") if dev_rep else False) or bool(entry.get("dev_blacklisted"))
        if is_bl or dev_rugs > 0 or len(DEV_RUG_HISTORY.get(dev_wallet, [])) > 0:
            return False

    return True


def _open_mock_position(token_address: str, entry: dict[str, Any], score: int, ts: float) -> None:
    if token_address in STONKFUN_QUOTE_MINTS:
        return
    if token_address in MOCK_PORTFOLIO:
        return
    if not _is_safe_vetted_token(token_address, entry):
        return
    entry_market_cap = entry.get("market_cap") or 0.0
    record_token_peak_mcap(token_address, entry_market_cap, entry.get("dev_wallet"))
    is_stonk = is_stonkboard_token(token_address, entry.get("platform"), entry.get("links"))
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
        # Peak mcap SINCE this opportunity was called
        "peak_market_cap": entry_market_cap,
        "peak_ts": ts,
        "goplus": entry.get("goplus"),
        "is_stonkboard": is_stonk,
        "dev_total_launches": entry.get("dev_total_launches"),
        "dev_prior_tickers": entry.get("dev_prior_tickers"),
        "dev_prior_launches": entry.get("dev_prior_launches"),
    }
    if len(MOCK_PORTFOLIO) > MOCK_PORTFOLIO_MAX:
        oldest_key = next(iter(MOCK_PORTFOLIO))
        if oldest_key != token_address:
            del MOCK_PORTFOLIO[oldest_key]




async def _update_mock_portfolio_market_caps() -> None:
    """Polls latest market cap and updates peak multiples for all tracked calls."""
    now = time.time()
    for addr, pos in list(MOCK_PORTFOLIO.items()):
        last_ts = float(pos.get("last_known_ts") or 0.0)
        feed_entry = TOKEN_FEED.get(addr)
        if feed_entry and feed_entry.get("market_cap"):
            mcap = float(feed_entry["market_cap"])
            pos["last_known_market_cap"] = mcap
            pos["last_known_status"] = feed_entry.get("status")
            pos["last_known_ts"] = now
            if mcap > pos.get("peak_market_cap", 0.0):
                pos["peak_market_cap"] = mcap
                pos["peak_ts"] = now
                record_token_peak_mcap(addr, pos["peak_market_cap"], pos.get("dev_wallet"))
        elif now - last_ts > 45:
            try:
                dex = await fetch_dexscreener_info(addr)
                if dex.get("market_cap"):
                    mcap = float(dex["market_cap"])
                    pos["last_known_market_cap"] = mcap
                    pos["last_known_ts"] = now
                    if mcap > pos.get("peak_market_cap", 0.0):
                        pos["peak_market_cap"] = mcap
                        pos["peak_ts"] = now
                        record_token_peak_mcap(addr, pos["peak_market_cap"], pos.get("dev_wallet"))
                await asyncio.sleep(0.2)
            except Exception:
                pass


def _mock_position_with_pnl(token_address: str, position: dict[str, Any]) -> dict[str, Any]:
    entry_market_cap = float(position.get("entry_market_cap") or 0.0)

    feed_entry = TOKEN_FEED.get(token_address)
    if feed_entry is not None and feed_entry.get("market_cap"):
        now = time.time()
        feed_mcap = float(feed_entry["market_cap"])
        if feed_mcap > 0:
            position["last_known_market_cap"] = feed_mcap
        position["last_known_status"] = feed_entry.get("status")
        position["last_known_ts"] = now
        if feed_mcap > position.get("peak_market_cap", 0.0):
            position["peak_market_cap"] = feed_mcap
            position["peak_ts"] = now
        if feed_entry.get("goplus"):
            position["goplus"] = feed_entry.get("goplus")
        if feed_entry.get("links"):
            position["links"] = feed_entry.get("links")

    current_market_cap = float(position.get("last_known_market_cap") or entry_market_cap)
    peak_market_cap = float(position.get("peak_market_cap") or current_market_cap)
    peak_multiple = (peak_market_cap / entry_market_cap) if entry_market_cap > 0 else 1.0
    current_multiple = (current_market_cap / entry_market_cap) if entry_market_cap > 0 else 1.0
    pnl_pct = ((current_market_cap - entry_market_cap) / entry_market_cap * 100.0) if entry_market_cap > 0 else 0.0
    pnl_usd = (MOCK_BUY_SIZE_USD * current_multiple) - MOCK_BUY_SIZE_USD

    return {
        **position,
        "token_address": token_address,
        "entry_market_cap": entry_market_cap,
        "current_market_cap": current_market_cap,
        "peak_market_cap": peak_market_cap,
        "peak_multiple": peak_multiple,
        "current_multiple": current_multiple,
        "pnl_usd": pnl_usd,
        "pnl_pct": pnl_pct,
        "buy_size_usd": MOCK_BUY_SIZE_USD,
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
            await _update_mock_portfolio_market_caps()
            await broadcast_json({"kind": "mock_portfolio", "payload": _build_mock_portfolio_snapshot()})
        if RECENT_OPPORTUNITIES:
            await broadcast_json({"kind": "opportunity_history", "payload": _refresh_opportunity_history_statuses()})
        if JEV_ENABLED:
            await broadcast_json({"kind": "jev_stats", "payload": build_jev_stats()})


async def _rescore_token_and_maybe_ping(token_address: str) -> None:
    """Drop-in replacement for _rescore_token at every call site — rescoring
    is the one operation guaranteed to run every time any signal that feeds
    the opportunity score changes (dev trust, narrative, holders, bundle,
    momentum, volume...), which makes it the correct single choke point to
    hang the ping check off of rather than duplicating the check at every
    place a score-affecting field gets updated."""
    if token_address in STONKFUN_QUOTE_MINTS:
        return
    _rescore_token(token_address)
    entry = TOKEN_FEED.get(token_address)
    if not entry:
        return
    # Jev semantic evaluation — gated + once-per-candidate + budget-capped (see
    # maybe_evaluate_token_with_jev). Runs after the deterministic rescore so
    # the cost gate sees a fresh opportunity_score; on a successful judgment it
    # re-rescores internally so the ping/opportunity logic below sees the
    # Jev-adjusted score. No-ops instantly when disabled/over budget/not gated.
    if JEV_ENABLED and entry.get("status") == "WATCHING":
        await maybe_screen_early_momentum_with_jev(token_address, entry)
        await maybe_evaluate_token_with_jev(token_address, entry)
        await maybe_run_pvp_choice(token_address, entry)
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
        # Pre-opportunity security verification (GoPlus & honeypot check)
        sec = (await check_token_honeypot_and_whitelist(entry.get("chain", "solana"), token_address, entry)) or {}
        if sec.get("is_honeypot") or sec.get("sell_whitelist"):
            hp_reason = sec.get("reason") or "Sell whitelist / honeypot detected"
            entry["is_honeypot"] = True
            entry["sell_whitelist"] = True
            entry["honeypot_reason"] = hp_reason
            await _kick_out_watchlist_token(token_address, entry, time.time(), "HONEYPOT_SELL_WHITELIST", f"SKIPPED - {hp_reason.upper()}")
            return

        # Twitter quality verification:
        # User rule: if token has Twitter, it MUST be < 1 year old (<= 365d) and have 0 username changes.
        # Tokens without Twitter are allowed through. Anything else is blocked from opportunities & pings.
        tw_url = (entry.get("socials") or {}).get("twitter") or (entry.get("links") or {}).get("twitter")
        tw_ok, tw_reason, tw_meta = await verify_twitter_quality(tw_url)
        if not tw_ok:
            logger.info(f"ALERT SKIPPED: SKIPPED - {tw_reason} for {entry.get('ticker')} ({token_address})")
            return
        if tw_meta:
            entry["twitter_meta"] = tw_meta

        _mark_opportunity_recorded(token_address)
        if token_address.lower().endswith("7777"):
            if not entry.get("platform") or entry.get("platform") in ("?", "unknown"):
                entry["platform"] = "flap.sh"

        if entry.get("debot") is None:
            entry["debot"] = await fetch_debot_story(token_address)

        entry["links"] = build_token_links(entry.get("chain", "solana"), token_address, entry.get("platform"))
        is_stonk = is_stonkboard_token(token_address, entry.get("platform"), entry.get("links"))
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
            "goplus": entry.get("goplus"),
            "debot": entry.get("debot"),
            "is_stonkboard": is_stonk,
            "dev_total_launches": entry.get("dev_total_launches"),
            "dev_prior_tickers": entry.get("dev_prior_tickers"),
            "dev_prior_launches": entry.get("dev_prior_launches"),
            "timestamp": time.time(),
        }
        RECENT_OPPORTUNITIES.append(opp_alert)
        await broadcast_alert(opp_alert)
        await bump_daily_stat(opp_alert["timestamp"], "opportunities_flagged")
        _open_mock_position(token_address, entry, score, opp_alert["timestamp"])
        await persist_state()


    # Telegram opportunity ping — fires only for vetted calls that opened in MOCK_PORTFOLIO
    # (cleared single-launch, anti-rug, and honeypot filters)
    if token_address not in TELEGRAM_PINGED_TOKENS and token_address in MOCK_PORTFOLIO:
        _mark_telegram_pinged(token_address)
        if entry.get("debot") is None:
            entry["debot"] = await fetch_debot_story(token_address)
        await ensure_prior_launches_peak_mcap(entry)
        text = _format_telegram_opportunity_message(entry)
        markup = _build_telegram_reply_markup(entry.get("chain", "solana"), token_address, entry.get("platform"))
        TELEGRAM_SEND_QUEUE.put_nowait((text, entry.get("image_url"), markup))

    # Early Momentum ping — disabled by default, only opportunity calls are sent
    if TELEGRAM_SEND_EARLY_MOMENTUM:
        early_score = entry.get("early_momentum_score", 0)
        market_cap = entry.get("market_cap") or 0.0
        if (
            token_address not in TELEGRAM_EARLY_PINGED_TOKENS
            and _is_safe_vetted_token(token_address, entry)
            and early_score >= TELEGRAM_EARLY_MOMENTUM_SCORE_THRESHOLD
            and EARLY_MOMENTUM_PING_MIN_MCAP_USD <= market_cap <= EARLY_MOMENTUM_PING_MAX_MCAP_USD
        ):
            # Twitter quality verification for early momentum
            tw_url = (entry.get("socials") or {}).get("twitter") or (entry.get("links") or {}).get("twitter")
            tw_ok, tw_reason, tw_meta = await verify_twitter_quality(tw_url)
            if not tw_ok:
                logger.info(f"EARLY MOMENTUM SKIPPED: SKIPPED - {tw_reason} for {entry.get('ticker')} ({token_address})")
                return
            if tw_meta:
                entry["twitter_meta"] = tw_meta
            _mark_telegram_early_pinged(token_address)
            if entry.get("debot") is None:
                entry["debot"] = await fetch_debot_story(token_address)
            await ensure_prior_launches_peak_mcap(entry)
            early_text = _format_telegram_early_momentum_message(entry)
            markup = _build_telegram_reply_markup(entry.get("chain", "solana"), token_address, entry.get("platform"))
            TELEGRAM_SEND_QUEUE.put_nowait((early_text, entry.get("image_url"), markup))


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
    token_feed_upsert(token_address, status="SKIPPED", dev_decision="SKIPPED", opportunity_score=0, early_momentum_score=0)
    if token_address in MOCK_PORTFOLIO:
        MOCK_PORTFOLIO[token_address]["last_known_status"] = "SKIPPED"
        MOCK_PORTFOLIO[token_address]["last_known_market_cap"] = 0.0
        MOCK_PORTFOLIO[token_address]["exit_reason"] = reason
        MOCK_PORTFOLIO[token_address]["last_known_ts"] = now

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

    chain = info["chain"]
    # Quota guardrail: for Solana tokens with small mcap / low score that have already had their initial security check,
    # poll at a much slower cadence (every 5 min) so dead/dust tokens don't consume RPC quota.
    if chain == "solana" and info.get("holder_stats_initial_checked"):
        mcap = float(info.get("market_cap") or (TOKEN_FEED.get(token_address, {}).get("market_cap")) or 0.0)
        opp_score = float((TOKEN_FEED.get(token_address, {}).get("opportunity_score")) or 0.0)
        if mcap < 5000 and opp_score < 8:
            info["next_holder_poll_at"] = now + 300.0
            return

    info["next_holder_poll_at"] = now + HOLDER_STATS_POLL_INTERVAL_SECONDS
    info["holder_stats_initial_checked"] = True
    if chain == "solana":
        holder_stats = (await fetch_solana_holder_stats(token_address)) or {}
        # Fallback: only when the RPC scan returned no holders AND this is a real
        # candidate worth a Birdeye call (mcap at/above the Jev floor) — cached so
        # it doesn't re-fetch every 60s. Protects the free quota.
        if (BIRDEYE_ENABLED and not holder_stats.get("holder_count")
                and (info.get("market_cap") or (TOKEN_FEED.get(token_address, {}).get("market_cap")) or 0) >= JEV_PVP_MIN_MCAP
                and (now - info.get("birdeye_holder_checked_at", 0.0)) > BIRDEYE_RECHECK_SECONDS):
            info["birdeye_holder_checked_at"] = now
            be = await fetch_birdeye_holders(token_address)
            if be:
                holder_stats.update(be)
    elif chain in ("bnb", "robinhood"):
        holder_stats = _evm_holder_stats_from_ledger(token_address) or {}
    else:
        return

    if holder_stats.get("mint_authority_active") and info["status"] == "WATCHING":
        await _kick_out_watchlist_token(token_address, info, now, "MINTABLE", "SKIPPED - MINT AUTHORITY NOT RENOUNCED")
        return

    if holder_stats.get("freeze_authority_active") and info["status"] == "WATCHING":
        await _kick_out_watchlist_token(token_address, info, now, "FREEZE_HONEYPOT", "SKIPPED - FREEZE AUTHORITY ACTIVE (Honeypot risk)")
        return

    if (holder_stats.get("sell_whitelist") or holder_stats.get("is_honeypot")) and info["status"] == "WATCHING":
        hp_reason = holder_stats.get("honeypot_reason") or "Sell whitelist / honeypot detected"
        info["is_honeypot"] = True
        info["sell_whitelist"] = True
        info["honeypot_reason"] = hp_reason
        await _kick_out_watchlist_token(token_address, info, now, "HONEYPOT_SELL_WHITELIST", f"SKIPPED - {hp_reason.upper()}")
        return

    sec = (await check_token_honeypot_and_whitelist(chain, token_address, info)) or {}
    if (sec.get("is_honeypot") or sec.get("sell_whitelist")) and info["status"] == "WATCHING":
        hp_reason = sec.get("reason") or "Sell whitelist / honeypot detected"
        info["is_honeypot"] = True
        info["sell_whitelist"] = True
        info["honeypot_reason"] = hp_reason
        await _kick_out_watchlist_token(token_address, info, now, "HONEYPOT_SELL_WHITELIST", f"SKIPPED - {hp_reason.upper()}")
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
        # "Substantial holders" — how many wallets hold a >= $1k POSITION in
        # this coin (their token balance x current price). Your intuition:
        # buyers taking a real position signal more legitimacy than a swarm of
        # dust/sybil wallets. Computed FREE from the per-wallet balances the
        # holder scan already produced (SOLANA_LAST_HOLDER_BALANCES) x the
        # DexScreener price — no extra RPC. NOTE: this is position value in
        # THIS coin, not the wallet's total net worth (which would need a
        # per-wallet balance sweep, too expensive on the current RPC plan).
        substantial = _compute_substantial_holders(token_address, prev_entry)
        if substantial is not None:
            holder_stats.update(substantial)
        token_feed_upsert(token_address, **holder_stats)


def _compute_substantial_holders(token_address: str, entry: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Count holders whose position (balance x price) is >= $1k / $10k, from the
    per-wallet balances the last holder scan already produced. Solana only for
    now (that's where SOLANA_LAST_HOLDER_BALANCES is populated)."""
    balances = SOLANA_LAST_HOLDER_BALANCES.get(token_address)
    if not balances:
        return None
    price = entry.get("price_usd") or 0.0
    if price <= 0:
        return None
    over_1k = sum(1 for bal in balances.values() if bal * price >= 1000)
    over_10k = sum(1 for bal in balances.values() if bal * price >= 10000)
    total = len(balances) or 1
    return {
        "substantial_holders_1k": over_1k,
        "substantial_holders_10k": over_10k,
        "substantial_holders_pct": round(over_1k / total * 100, 1),
    }


BUNDLE_CHECK_INTERVAL_SECONDS = 300  # bundle detection is RPC-heavier (Solana) / needs enough tx history (EVM) than a plain stats refresh


async def _maybe_check_bundle(token_address: str, info: dict[str, Any], now: float) -> None:
    if token_address in TOKEN_BUNDLE_INFO:
        return  # already confirmed — no need to keep re-checking
    next_due = info.get("next_bundle_check_at", 0.0)
    if now < next_due:
        return

    # Quota guardrail: don't waste RPC calls bundle-checking dead/dust tokens
    mcap = float(info.get("market_cap") or (TOKEN_FEED.get(token_address, {}).get("market_cap")) or 0.0)
    opp_score = float((TOKEN_FEED.get(token_address, {}).get("opportunity_score")) or 0.0)
    if mcap < 10000 and opp_score < 10:
        info["next_bundle_check_at"] = now + 60.0  # defer until token gains traction
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

    dex_info = (await fetch_dexscreener_info(token_address)) or {}
    market_cap = dex_info.get("market_cap", 0.0)
    # Birdeye enrichment for real Solana candidates: fill price/mcap/liquidity/vol
    # that DexScreener is MISSING (not just when mcap is 0 — DexScreener often has
    # mcap but $0 liquidity, and Birdeye has it). Gated to non-dust candidates
    # (>= the Jev mcap floor OR mcap unknown) so the free tier isn't spent on the
    # flood of $2 dead coins. Globally throttled + fail-safe.
    _be_ticker_ok = info.get("ticker") and info["ticker"] != "UNKNOWN" and not ticker_is_invalid(info.get("ticker"))[0]
    # QUOTA-CONSERVING: Birdeye free tier is small. Only call it as a genuine
    # last resort — when DexScreener returned NOTHING at all (mcap 0) for a
    # real, resolved-ticker Solana coin — and cache the result so the same coin
    # is never re-fetched within the cache window. Coins DexScreener already
    # covers (even partially) never touch Birdeye.
    _dex_empty = (market_cap <= 0)
    _be_cached_at = info.get("birdeye_checked_at", 0.0)
    _be_due = (now - _be_cached_at) > BIRDEYE_RECHECK_SECONDS
    if (BIRDEYE_ENABLED and info.get("chain") == "solana"
            and _be_ticker_ok and _dex_empty and _be_due):
        info["birdeye_checked_at"] = now
        be = await fetch_birdeye_overview(token_address)
        if be:
            for k in ("market_cap", "price_usd", "liquidity_usd", "volume_24h"):
                if be.get(k) and not dex_info.get(k):
                    dex_info[k] = be[k]
            market_cap = dex_info.get("market_cap", 0.0)
    # NEVER blank a previously-known mcap: if this poll returned 0 but we had a
    # real value before, keep the last good one (free — no API call). This alone
    # fixes most "data went missing" cases (DexScreener returns 0 transiently).
    _prev = TOKEN_FEED.get(token_address) or {}
    if market_cap <= 0 and (_prev.get("market_cap") or 0) > 0:
        market_cap = _prev["market_cap"]
        dex_info["market_cap"] = market_cap
        if not dex_info.get("liquidity_usd") and _prev.get("liquidity_usd"):
            dex_info["liquidity_usd"] = _prev["liquidity_usd"]
    volume_24h = dex_info.get("volume_24h", 0.0)
    liquidity_usd = dex_info.get("liquidity_usd", 0.0)
    txns_24h = dex_info.get("txns_24h", 0)
    buys_24h = dex_info.get("buys_24h", 0)
    sells_24h = dex_info.get("sells_24h", 0)
    image_url = dex_info.get("image_url") or info.get("image_url") or (_prev.get("image_url") if _prev else None)
    if not image_url and is_stonkboard_token(token_address, info.get("platform")):
        image_url = f"https://thestonkboard.com/api/logos/{token_address}"
    if image_url:
        info["image_url"] = image_url
    price_usd = dex_info.get("price_usd", 0.0)
    if market_cap > info["peak_market_cap"]:
        info["peak_market_cap"] = market_cap
    record_token_peak_mcap(token_address, info["peak_market_cap"], info.get("dev_wallet"))
    record_big_runner_if_qualified(token_address, TOKEN_FEED.get(token_address) or info, info["peak_market_cap"])

    if token_address in MOCK_PORTFOLIO:
        mock_pos = MOCK_PORTFOLIO[token_address]
        if market_cap > 0:
            mock_pos["last_known_market_cap"] = market_cap
            if market_cap > mock_pos.get("peak_market_cap", 0.0):
                mock_pos["peak_market_cap"] = market_cap
                mock_pos["peak_ts"] = now
        mock_pos["last_known_status"] = info.get("status")
        mock_pos["last_known_ts"] = now

    # Store price on the entry BEFORE the holder-stats refresh so the
    # substantial-holders ($1k position) calc there can read it.
    token_feed_upsert(token_address, price_usd=price_usd)

    dex_socials = dex_info.get("socials")
    if dex_socials and dex_socials.get("social_count", 0) > 0:
        existing_soc = dict(info.get("socials") or {})
        for k in ("twitter", "telegram", "website"):
            if dex_socials.get(k):
                existing_soc[k] = dex_socials[k]
                existing_soc[f"has_{k}"] = True
        if dex_socials.get("active_boosts"):
            existing_soc["active_boosts"] = max(existing_soc.get("active_boosts", 0), dex_socials["active_boosts"])
        existing_soc["social_count"] = sum(1 for x in [existing_soc.get("twitter"), existing_soc.get("telegram"), existing_soc.get("website")] if x)
        info["socials"] = existing_soc
        token_feed_upsert(token_address, socials=existing_soc)
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
                jev_record_outcome(token_address, "rugged")
                jev_record_pvp_outcome(token_address, "rugged")
                jev_record_proposed_outcome(token_address, "rugged")
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
                if token_address in MOCK_PORTFOLIO:
                    mock_pos = MOCK_PORTFOLIO[token_address]
                    mock_pos["last_known_status"] = "RUGGED"
                    if market_cap > 0:
                        mock_pos["last_known_market_cap"] = market_cap
                    mock_pos["last_known_ts"] = now
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

    # Cross-platform established coin / tokenized stock / implausible new-launch
    # mcap (any platform). Catches HYPE/MSTRx/ZEC leaks whose symbol only
    # resolved after DexScreener backfill.
    est, est_reason = is_probably_established_or_stock(
        info.get("ticker"),
        market_cap,
        platform=info.get("platform"),
        token_address=token_address,
    )
    if est:
        await _kick_out_watchlist_token(
            token_address, info, now, "NOT_A_NEW_LAUNCH",
            f"SKIPPED - NOT A NEW LAUNCH ({est_reason})",
        )
        return

    # Serial rugger check (multi-launch dev is allowed with warning, but ruggers are discarded):
    dev_wallet = info.get("dev_wallet", "")
    is_infra = dev_wallet.lower() in KNOWN_INFRASTRUCTURE_ADDRESSES or dev_wallet.lower().startswith("stonkboard") or dev_wallet.lower().startswith("discovered:")
    if not is_infra and dev_wallet:
        dev = DEV_REPUTATION_DATABASE.get(dev_wallet)
        if dev:
            if (dev.get("is_blacklisted") or dev.get("failed_spams", 0) > 0 or len(DEV_RUG_HISTORY.get(dev_wallet, [])) > 0) and info["status"] == "WATCHING":
                await _kick_out_watchlist_token(
                    token_address, info, now, "SERIAL_RUGGER_DISCARDED",
                    "SKIPPED - SERIAL RUGGER / PRIOR RUGS ON RECORD",
                )
                return

    # Honeypot chart detection (0 sells or extreme buy-to-sell ratio):
    token_age = now - info["created_at"]
    is_honeypot_chart = False
    if token_age >= 45:
        if (buys_24h >= 4 and sells_24h == 0) or (buys_24h >= 12 and sells_24h <= 1):
            is_honeypot_chart = True

    if is_honeypot_chart and info["status"] == "WATCHING":
        if not is_infra and dev_wallet:
            dev = DEV_REPUTATION_DATABASE.get(dev_wallet)
            if dev:
                dev["is_blacklisted"] = True
                dev["failed_spams"] = dev.get("failed_spams", 0) + 1
            DEV_SPAM_LOG[dev_wallet].append(now)
        await _kick_out_watchlist_token(
            token_address, info, now, "HONEYPOT_CHART",
            f"SKIPPED - HONEYPOT CHART DETECTED ({buys_24h} buys / {sells_24h} sells - no sells possible)",
        )
        return

    # Free alternative to PumpPortal's paid per-trade subscription: poll
    # the token's own on-chain bonding curve account directly (decoder
    # verified empirically — see fetch_pumpfun_bonding_curve_state).
    bonding_curve_key = info.get("bonding_curve_key")
    if info["platform"] == "pump.fun" and bonding_curve_key:
        bonding_state = await fetch_pumpfun_bonding_curve_state(bonding_curve_key)
        if bonding_state:
            sol_raised = bonding_state.get("bonding_sol_raised") or 0.0
            if liquidity_usd <= 0.0 and sol_raised > 0:
                liquidity_usd = round(sol_raised * 150.0, 2)
            token_feed_upsert(
                token_address,
                bonding_sol_raised=sol_raised,
                bonding_progress_pct=bonding_state.get("bonding_progress_pct"),
                bonding_complete=bonding_state.get("bonding_complete"),
                liquidity_usd=liquidity_usd,
            )

    # Backfill a ticker that showed up as UNKNOWN at creation (StonkFun
    # never has one; some Ember events lack it too) once DexScreener
    # has indexed the pair and can tell us the real symbol.
    resolved_symbol = dex_info.get("symbol")
    if resolved_symbol and (not info["ticker"] or info["ticker"] == "UNKNOWN"):
        info["ticker"] = resolved_symbol
        token_feed_upsert(token_address, ticker=resolved_symbol)
    # Store the token NAME (DexScreener provides it) — Jev needs to know what the
    # coin actually IS to judge narrative/quality/legitimacy. Without this, name
    # was always blank and Jev flagged 'insufficient info' on every judgment.
    resolved_name = dex_info.get("name")
    if resolved_name and not (TOKEN_FEED.get(token_address, {}).get("name")):
        token_feed_upsert(token_address, name=resolved_name)

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
            if token_address in MOCK_PORTFOLIO:
                mock_pos = MOCK_PORTFOLIO[token_address]
                mock_pos["last_known_status"] = "RUGGED"
                if market_cap > 0:
                    mock_pos["last_known_market_cap"] = market_cap
                mock_pos["last_known_ts"] = now
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
            # Terminal for learning purposes: an evaluated coin that faded out
            # without graduating or rugging is a 'flat' outcome (not a winner) —
            # record it so the correlation/proposal stats aren't blind to the
            # many coins that simply never went anywhere.
            jev_record_outcome(token_address, "flat")
            jev_record_pvp_outcome(token_address, "flat")
            jev_record_proposed_outcome(token_address, "flat")
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
        if token_address in STONKFUN_QUOTE_MINTS:
            continue
        if token_address in TOKEN_WATCHLIST:
            continue
        status = position.get("last_known_status")
        if status not in ("WATCHING", "GRADUATED", "RUGGED"):
            continue
        entry_mc = position.get("entry_market_cap") or 0.0
        chain = position.get("chain")
        platform = position.get("platform")
        dev_wallet = position.get("dev_wallet")
        ticker = position.get("ticker")
        if not chain or not platform or not dev_wallet:
            continue
        is_stonk = is_stonkboard_token(token_address, platform, position.get("links"))
        if is_stonk and entry_mc > MAX_OPPORTUNITY_MARKET_CAP_USD:
            continue
        if entry_mc > IMPLAUSIBLE_NEW_LAUNCH_MCAP_USD:
            continue
        qualifies, _ = is_launchpad_or_target_suffix(platform, token_address)
        if not qualifies:
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
            goplus=position.get("goplus"),
            **dev_rep_badge_fields(dev_wallet),
        )
        restored += 1
    return restored


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_state_sync()
    _seed_big_runners()
    purge_stonkfun_quote_tokens()
    purge_established_from_jev()
    try:
        await sync_stonkboard_coins()
    except Exception as exc:
        logger.debug(f"Initial stonkboard sync error: {exc!r}")
    purge_stonkfun_quote_tokens()
    await persist_state()
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
        (jev_auto_proposer_worker, "jev-auto-proposer"),
        (jev_trajectory_worker, "jev-trajectory"),
        (jev_autotune_worker, "jev-autotune"),
        (discovery_worker, "discovery"),
        (token_profiles_worker, "token-profiles"),
        (stonkboard_sync_worker, "stonkboard-sync"),
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
        "recent_smart_money": list(RECENT_SMART_MONEY)[-50:],
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
        "jev": (feed_entry or {}).get("jev"),
        "jev_judgment_log": JEV_JUDGMENT_LOG.get(token_address),
    }


@app.get("/api/jev")
async def api_jev() -> dict:
    """Aggregate Jev decision + budget + learning-loop stats."""
    return build_jev_stats()


@app.post("/api/jev/propose")
async def api_jev_propose(request: Request) -> dict:
    """Add a candidate question to be tested live (manual proposal). Body:
    {instructions, type?('noul'|'score'), criteria?, name?}."""
    body = await request.json()
    ok, res = jev_add_proposed_question(
        instructions=body.get("instructions", ""),
        qtype=body.get("type", "noul"),
        criteria=body.get("criteria"),
        proposed_by=body.get("proposed_by", "manual"),
        name=body.get("name"),
    )
    return {"ok": ok, "id" if ok else "error": res}


@app.post("/api/jev/retire")
async def api_jev_retire(request: Request) -> dict:
    """Deactivate a proposed question by id. Body: {id}."""
    body = await request.json()
    qid = body.get("id")
    q = JEV_PROPOSED_QUESTIONS.get(qid)
    if not q:
        return {"ok": False, "error": "unknown id"}
    q["active"] = False
    q["status"] = "retired"
    return {"ok": True, "id": qid}


@app.post("/api/jev/promote")
async def api_jev_promote(request: Request) -> dict:
    """Mark a proposed question 'promoted' (kept as a winner). Body: {id}.
    Stays in the proposed set (still merged into calls) but flagged as trusted."""
    body = await request.json()
    qid = body.get("id")
    q = JEV_PROPOSED_QUESTIONS.get(qid)
    if not q:
        return {"ok": False, "error": "unknown id"}
    q["status"] = "promoted"
    return {"ok": True, "id": qid}


@app.websocket("/ws/dashboard")
async def dashboard_ws(websocket: WebSocket) -> None:
    await websocket.accept()
    CONNECTED_CLIENTS.add(websocket)
    try:
        elite_count = sum(1 for d in DEV_REPUTATION_DATABASE.values() if d.get("successful_launches", 0) >= 1 and not d.get("is_blacklisted"))
        blacklist_count = sum(1 for d in DEV_REPUTATION_DATABASE.values() if d.get("is_blacklisted"))
        filtered_dev_rep = {k: v for k, v in DEV_REPUTATION_DATABASE.items() if v.get("successful_launches", 0) >= 1 or v.get("is_blacklisted")}
        snapshot = {
            "kind": "snapshot",
            "payload": {
                "alerts": list(ALERT_HISTORY)[-40:],
                "smart_wallets": SMART_WALLETS,
                "dev_reputation": {},
                "elite_count": elite_count,
                "blacklist_count": blacklist_count,
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
                "recent_graduations": list(RECENT_GRADUATIONS)[-25:],
                "recent_rugs": list(RECENT_RUGS)[-25:],
                "mock_portfolio": _build_mock_portfolio_snapshot(),
                "recent_opportunities": _refresh_opportunity_history_statuses()[-30:],
                "recent_smart_money": list(RECENT_SMART_MONEY)[-50:],
                "jev_stats": build_jev_stats(),
                "description_feed": build_description_feed(),
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
