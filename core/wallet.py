"""Polygon wallet — signing + pUSD balance checks.

Polymarket V2 settles in pUSD (Polymarket USD), an ERC-20 on Polygon backed
1:1 by USDC. The legacy USDC.e collateral was retired with the 2026-04-28
CLOB V2 migration.

The wallet is constructed lazily so paper-mode bots can run without a key.
Calling `initialize(private_key)` derives the address. Balance lookups use
web3 against a configured RPC endpoint and the canonical pUSD contract.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any


log = logging.getLogger(__name__)


# pUSD on Polygon (Polymarket V2 collateral, 6 decimals, backed 1:1 by USDC).
PUSD_POLYGON_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
PUSD_DECIMALS = 6

# V2 exchange contract addresses (Polygon mainnet, chain_id=137).
CTF_EXCHANGE_V2 = "0xE111180000d2663C0091e4f400237545B87B996B"
NEG_RISK_CTF_EXCHANGE_V2 = "0xe2222d279d744050d28e00520010520000310F59"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
COLLATERAL_ONRAMP = "0x93070a847efEf7F70739046A929D47a521F5B8ee"
COLLATERAL_OFFRAMP = "0x2957922Eb93258b93368531d39fAcCA3B4dC5854"

# Minimal ERC-20 ABI: balanceOf + decimals.
_ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
]


@dataclass
class WalletConfig:
    rpc_url: str
    pusd_address: str = PUSD_POLYGON_ADDRESS


class Wallet:
    """Polygon wallet wrapper. Holds a private key in memory only after init."""

    def __init__(self, config: WalletConfig) -> None:
        self.config = config
        self._account: Any = None
        self._w3: Any = None
        self._initialized = False

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    @property
    def address(self) -> str:
        if not self._initialized:
            raise RuntimeError("Wallet not initialized.")
        return str(self._account.address)

    def initialize(self, private_key: str) -> None:
        try:
            from eth_account import Account  # type: ignore
            from web3 import Web3  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "web3 / eth-account not installed. Run `pip install -r requirements.txt`."
            ) from e

        if not private_key or not private_key.startswith("0x"):
            raise ValueError("PK must be a 0x-prefixed hex string.")

        self._account = Account.from_key(private_key)
        self._w3 = Web3(Web3.HTTPProvider(self.config.rpc_url))
        self._initialized = True
        log.info("Wallet initialized for address=%s", self._account.address)

    def sign_message(self, message: bytes) -> str:
        if not self._initialized:
            raise RuntimeError("Wallet not initialized.")
        from eth_account.messages import encode_defunct  # type: ignore

        signable = encode_defunct(message)
        signed = self._account.sign_message(signable)
        return signed.signature.hex()

    async def get_pusd_balance(self) -> float:
        """Return pUSD balance as a float (already divided by 10**decimals)."""
        if not self._initialized:
            raise RuntimeError("Wallet not initialized.")
        return await asyncio.to_thread(self._read_pusd_balance)

    # Backwards-compatible alias used by HealthMonitor / bankroll provider.
    # New callers should prefer get_pusd_balance().
    async def get_usdc_balance(self) -> float:
        return await self.get_pusd_balance()

    def _read_pusd_balance(self) -> float:
        contract = self._w3.eth.contract(
            address=self._w3.to_checksum_address(self.config.pusd_address),
            abi=_ERC20_ABI,
        )
        raw = contract.functions.balanceOf(self._account.address).call()
        try:
            decimals = int(contract.functions.decimals().call())
        except Exception:
            decimals = PUSD_DECIMALS
        return float(raw) / (10 ** decimals)

    async def get_matic_balance(self) -> float:
        """Return native MATIC balance for gas. Returns float in MATIC units."""
        if not self._initialized:
            raise RuntimeError("Wallet not initialized.")
        return await asyncio.to_thread(self._read_matic_balance)

    def _read_matic_balance(self) -> float:
        wei = self._w3.eth.get_balance(self._account.address)
        return float(wei) / 1e18
