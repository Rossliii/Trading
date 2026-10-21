import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol

from eth_account import Account
from polymarket import PRODUCTION, SecureClient, UserInputError
from polymarket._internal.wallet import classify_wallet_type, signature_type_for
from py_clob_client_v2.clob_types import (
    AssetType,
    BalanceAllowanceParams,
    MarketOrderArgsV2,
    OrderArgsV2,
    OrderPayload,
    OrderType,
    TradeParams,
)

from app.bot import secure_client as sc
from app.bot.auth import build_clob_client
from app.bot.order_snapshot import OrderSnapshot, normalize_snapshot
from app.bot.schemas import LimitOrder
from app.db.wallets import Wallet
from app.exceptions import WalletNotDeployedError
from app.infra.config import settings

logger = logging.getLogger(__name__)


class ExecutionClient(Protocol):
    wallet_type: str
    supports_heartbeat: bool

    async def place_limit_order(self, order: LimitOrder, post_only: bool = False) -> str: ...

    async def place_market_order(self, token_id: str, side: str, amount: float) -> str: ...

    async def cancel_order(self, order_id: str) -> Any: ...

    async def cancel_orders(self, *order_ids: str) -> None: ...

    async def cancel_all(self) -> Any: ...

    async def get_open_order_ids(self) -> set[str]: ...

    async def send_heartbeat(self, heartbeat_id: str = "") -> dict[str, Any]: ...

    def ws_auth(self) -> dict[str, str]: ...

    async def total_earnings_today(self) -> Decimal: ...

    async def market_earnings_today(self) -> dict[str, Decimal]: ...

    async def reward_percentages(self) -> dict[str, Decimal]: ...

    async def refresh_conditional_balance(self, token_id: str) -> bool: ...

    async def order_snapshot(self, order_id: str) -> OrderSnapshot: ...

    async def conditional_balance(self, token_id: str) -> Decimal: ...


class LegacyExecutionClient:
    supports_heartbeat = True

    def __init__(self, client: Any, wallet_type: str) -> None:
        self.client = client
        self.wallet_type = wallet_type

    async def order_snapshot(self, order_id: str) -> OrderSnapshot:
        def read():
            order = self.client.get_order(order_id)
            trades = []
            for tid in order.get("associate_trades") or []:
                trades.extend(self.client.get_trades(TradeParams(id=tid)))
            return normalize_snapshot(order, trades)

        return await asyncio.to_thread(read)

    async def conditional_balance(self, token_id: str) -> Decimal:
        def read():
            params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            self.client.update_balance_allowance(params)
            result = self.client.get_balance_allowance(params)
            return Decimal(str(result["balance"])) / Decimal(1_000_000)

        return await asyncio.to_thread(read)

    async def place_limit_order(self, order: LimitOrder, post_only: bool = False) -> str:
        order_args = OrderArgsV2(
            token_id=order.token_id,
            side=order.side,
            size=order.size,
            price=order.price,
        )
        result = await asyncio.to_thread(
            self.client.create_and_post_order,
            order_args,
            None,
            OrderType.GTC,
            post_only,
        )
        return result["orderID"]

    async def place_market_order(self, token_id: str, side: str, amount: float) -> str:
        order_args = MarketOrderArgsV2(token_id=token_id, side=side, amount=amount)
        result = await asyncio.to_thread(
            self.client.create_and_post_market_order, order_args, None, OrderType.FAK
        )
        return result["orderID"]

    async def cancel_order(self, order_id: str) -> Any:
        payload = OrderPayload(orderID=order_id)
        return await asyncio.to_thread(self.client.cancel_order, payload)

    async def cancel_all(self) -> Any:
        return await asyncio.to_thread(self.client.cancel_all)

    async def cancel_orders(self, *order_ids: str) -> None:
        ids = [oid for oid in order_ids if oid]
        if not ids:
            return
        try:
            result = await asyncio.to_thread(self.client.cancel_orders, ids)
        except Exception as exc:
            logger.warning("cancel_orders: batch cancel of %d ids failed: %s", len(ids), exc)
            return
        not_canceled = (result or {}).get("not_canceled") or {}
        for oid, reason in not_canceled.items():
            logger.debug("cancel_orders: id %s not canceled: %s", oid, reason)

    async def get_open_order_ids(self) -> set[str]:
        orders = await asyncio.to_thread(self.client.get_open_orders)
        return {o["id"] for o in orders if isinstance(o, dict) and o.get("id")}

    async def send_heartbeat(self, heartbeat_id: str = "") -> dict[str, Any]:
        return await asyncio.to_thread(self.client.post_heartbeat, heartbeat_id)

    def ws_auth(self) -> dict[str, str]:
        c = self.client.creds
        return {"apiKey": c.api_key, "secret": c.api_secret, "passphrase": c.api_passphrase}

    async def total_earnings_today(self) -> Decimal:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        result = await asyncio.to_thread(self.client.get_total_earnings_for_user_for_day, date_str)
        total = Decimal(0)
        for entry in result:
            try:
                earned = Decimal(str(entry["earnings"]))
            except (KeyError, TypeError, ValueError, ArithmeticError):
                logger.debug("total_earnings_today: skipping malformed entry %s", entry)
                continue
            total += earned
        return total

    async def market_earnings_today(self) -> dict[str, Decimal]:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        rows = await asyncio.to_thread(self.client.get_earnings_for_user_for_day, date_str)
        out: dict[str, Decimal] = {}
        for row in rows:
            try:
                cid = str(row["condition_id"])
                earned = Decimal(str(row["earnings"]))
            except (KeyError, TypeError, ValueError, ArithmeticError):
                logger.debug("market_earnings_today: skipping malformed row %s", row)
                continue
            if cid:
                out[cid] = out.get(cid, Decimal(0)) + earned
        return out

    async def reward_percentages(self) -> dict[str, Decimal]:
        result = await asyncio.to_thread(self.client.get_reward_percentages)
        return {cid: Decimal(str(pct)) for cid, pct in (result or {}).items()}

    async def refresh_conditional_balance(self, token_id: str) -> bool:
        try:
            await asyncio.to_thread(
                self.client.update_balance_allowance,
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id),
            )
            return True
        except Exception:
            logger.exception("balance-allowance refresh failed for token %s", token_id)
            return False


class SecureExecutionClient:
    supports_heartbeat = False

    def __init__(self, client: SecureClient) -> None:
        self.client = client
        self.wallet_type = client.wallet_type

    async def order_snapshot(self, order_id: str) -> OrderSnapshot:
        def read():
            order = self.client.get_order(order_id=order_id)
            trades = []
            for tid in order.associate_trades:
                trades.extend(
                    t.model_dump() for t in self.client.list_account_trades(id=tid).iter_items()
                )
            return normalize_snapshot(order.model_dump(), trades)

        return await asyncio.to_thread(read)

    async def conditional_balance(self, token_id: str) -> Decimal:
        result = await asyncio.to_thread(
            self.client.get_balance_allowance, asset_type="CONDITIONAL", token_id=token_id
        )
        return Decimal(result.balance) / Decimal(1_000_000)

    async def place_limit_order(self, order: LimitOrder, post_only: bool = False) -> str:
        return await sc.place_limit_order(self.client, order, post_only=post_only)

    async def place_market_order(self, token_id: str, side: str, amount: float) -> str:
        return await sc.place_market_order(self.client, token_id, side, amount)

    async def cancel_order(self, order_id: str) -> Any:
        return await sc.cancel_order(self.client, order_id)

    async def cancel_all(self) -> Any:
        return await sc.cancel_all(self.client)

    async def cancel_orders(self, *order_ids: str) -> None:
        await sc.cancel_orders(self.client, *order_ids)

    async def get_open_order_ids(self) -> set[str]:
        page_iter = await asyncio.to_thread(
            lambda: list(self.client.list_open_orders().iter_items())
        )
        return {o.id for o in page_iter if o.id}

    async def send_heartbeat(self, heartbeat_id: str = "") -> dict[str, Any]:
        return {}

    def ws_auth(self) -> dict[str, str]:
        c = self.client.credentials
        return {"apiKey": c.key, "secret": c.secret, "passphrase": c.passphrase}

    async def total_earnings_today(self) -> Decimal:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        result = await asyncio.to_thread(self.client.get_total_earnings_for_user_for_day, date=date)
        return sum((Decimal(str(e.earnings)) for e in result), Decimal(0))

    async def market_earnings_today(self) -> dict[str, Decimal]:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        paginator = await asyncio.to_thread(self.client.list_user_earnings_for_day, date=date)
        out: dict[str, Decimal] = {}
        for r in paginator.iter_items():
            cid = str(r.condition_id)
            if cid:
                out[cid] = out.get(cid, Decimal(0)) + Decimal(str(r.earnings))
        return out

    async def reward_percentages(self) -> dict[str, Decimal]:
        result = await asyncio.to_thread(self.client.get_reward_percentages)
        return {str(cid): Decimal(str(pct)) for cid, pct in result.items()}

    async def refresh_conditional_balance(self, token_id: str) -> bool:
        try:
            await asyncio.to_thread(
                self.client.get_balance_allowance,
                asset_type="CONDITIONAL",
                token_id=token_id,
            )
            return True
        except Exception:
            logger.exception("secure balance refresh failed for token %s", token_id)
            return False


def detect_wallet_type(private_key: str, proxy_address: str) -> str:
    """Offline CREATE2-based wallet type detection. Pure; no network calls."""
    signer = Account.from_key(private_key).address
    return classify_wallet_type(
        signer=signer, wallet=proxy_address, config=PRODUCTION.wallet_derivation
    )


def build_execution_client(wallet: Wallet) -> ExecutionClient:
    """Select and construct the right ExecutionClient for this wallet's type.

    DEPOSIT_WALLET → SecureExecutionClient (polymarket-client).
    All others → LegacyExecutionClient with the correct per-type signature_type.
    force_legacy_execution=True → always Legacy (rollback switch).
    Undeployed deposit wallet → raises WalletNotDeployedError.
    """
    if settings.force_legacy_execution:
        logger.info("force_legacy_execution: using legacy adapter for %s", wallet.proxy_address)
        return LegacyExecutionClient(
            build_clob_client(wallet.private_key, wallet.proxy_address, signature_type=2),
            "GNOSIS_SAFE",
        )

    try:
        wtype = detect_wallet_type(wallet.private_key, wallet.proxy_address)
    except Exception:
        logger.warning(
            "wallet-type classify failed for %s; defaulting to legacy GNOSIS_SAFE",
            wallet.proxy_address,
        )
        wtype = "GNOSIS_SAFE"

    if wtype == "DEPOSIT_WALLET":
        try:
            secure = sc.build_secure_client(wallet.private_key, wallet.proxy_address)
        except UserInputError as exc:
            raise WalletNotDeployedError(str(exc)) from exc
        return SecureExecutionClient(secure)

    sig_type = signature_type_for(wtype)
    return LegacyExecutionClient(
        build_clob_client(wallet.private_key, wallet.proxy_address, signature_type=sig_type),
        wtype,
    )
