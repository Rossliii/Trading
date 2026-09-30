"""Interactive terminal client for the existing WebSocket backend."""
import argparse
import getpass
import json
import threading
from pathlib import Path

from pydantic import ValidationError
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

from app.api.messages import client_message_adapter
from app.farm.schemas import FarmConfig, FarmFilters

PROJECT_ROOT = Path(__file__).resolve().parents[1]

HELP = """
Commands:
  wallets             List wallets and balances
  add-wallet          Add a wallet (private key input is hidden)
  remove-wallet ID    Remove a wallet record
  blacklist           List blocked markets
  block URL           Block a market
  unblock ID          Remove a condition ID from the blacklist
  clear-blacklist     Clear the blacklist
  start               Read Config, ask for missing values, confirm live trading
  stop                Request farm shutdown
  status              Show latest received farm events
  help                Show commands
  quit                Disconnect (backend begins farm cleanup)
"""


def redact(value):
    if isinstance(value, dict):
        return {
            k: ("[hidden]" if k in {"private_key", "license_key"} else redact(v))
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [redact(v) for v in value]
    return value


def validated_message(payload):
    return client_message_adapter.validate_python(payload).model_dump(mode="json")


def read_config():
    # Prefer the user's existing extensionless file; also support Config.json.
    path = next(
        (p for p in (PROJECT_ROOT / "Config", PROJECT_ROOT / "Config.json") if p.is_file()),
        None,
    )
    if path is None:
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{path.name}: invalid JSON at line {exc.lineno}, column {exc.colno}."
        ) from None
    except (OSError, UnicodeError):
        raise ValueError(f"Cannot read {path.name} as UTF-8.") from None
    if not isinstance(data, dict):
        raise ValueError("Config must contain a JSON object.")
    allowed = {"license_key", *FarmConfig.model_fields}
    unknown = set(data) - allowed
    if unknown:
        raise ValueError("Config contains unrecognized top-level fields.")
    if data.get("license_key") is not None and not isinstance(data["license_key"], str):
        raise ValueError("Config license_key must be a string.")
    filters = data.get("filters")
    if filters is not None:
        if not isinstance(filters, dict):
            raise ValueError("Config filters must be a JSON object.")
        if set(filters) - FarmFilters.model_fields.keys():
            raise ValueError("Config filters contains unrecognized fields.")
    return data


def ask_fields(model, supplied=None):
    values = {
        key: value for key, value in (supplied or {}).items()
        if key in model.model_fields and key != "filters" and value is not None and value != ""
    }
    for name, field in model.model_fields.items():
        if not field.is_required() or name == "filters" or name in values:
            continue
        choices = getattr(field.annotation, "__args__", ())
        hint = " / ".join(str(v) for v in choices if isinstance(v, str))
        label = f"{name} ({hint})" if hint else name
        values[name] = input(f"{label}: ").strip()
    return values


def farm_message():
    supplied = read_config()
    print("Reading Config. Only missing required settings will be prompted.")
    values = ask_fields(FarmConfig, supplied)
    values["filters"] = ask_fields(FarmFilters, supplied.get("filters"))
    config = FarmConfig.model_validate(values)
    payload = {"type": "farm_create", **config.model_dump(mode="json")}
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    print("This starts LIVE trading using the first wallet in backend sort order.")
    return payload if input("Type START to submit: ").strip() == "START" else None


def command_message(line):
    command, _, argument = line.strip().partition(" ")
    argument = argument.strip()
    simple = {
        "wallets": "wallet_list", "blacklist": "blacklist_list", "stop": "farm_cancel",
    }
    if command in simple and not argument:
        return {"type": simple[command]}
    if command == "add-wallet" and not argument:
        print("The existing backend stores the private key in Supabase without encryption.")
        return {
            "type": "wallet_register",
            "proxy_address": input("Polymarket proxy address: ").strip(),
            "private_key": getpass.getpass("Wallet private key (hidden): ").strip(),
        }
    if command == "remove-wallet" and argument:
        return {"type": "wallet_remove", "wallet_id": argument}
    if command == "block" and argument:
        return {"type": "blacklist_add", "market_url": argument}
    if command == "unblock" and argument:
        return {"type": "blacklist_remove", "condition_id": argument}
    if command == "clear-blacklist" and not argument:
        return {"type": "blacklist_clear"}
    if command == "start" and not argument:
        return farm_message()
    raise ValueError("Unknown command or missing argument. Type help.")


def receive_events(socket, stopped, latest, lock):
    try:
        for raw in socket:
            event = json.loads(raw)
            with lock:
                latest[event.get("type", "event")] = redact(event)
            print("\n" + json.dumps(redact(event), ensure_ascii=False), flush=True)
    except (ConnectionClosed, OSError, ValueError):
        print("\nConnection ended. Press Enter to return to the terminal.", flush=True)
    finally:
        stopped.set()


def run(url):
    supplied = read_config()
    license_key = (supplied.get("license_key") or "").strip()
    if not license_key:
        license_key = getpass.getpass("License key (hidden): ").strip()
    if not license_key:
        print("License key is required.")
        return 1
    with connect(url, open_timeout=10, close_timeout=10) as socket:
        socket.send(json.dumps({"type": "auth", "license_key": license_key}))
        reply = json.loads(socket.recv(timeout=30))
        if reply.get("type") != "auth_ok":
            print("Authentication failed:", reply.get("reason", "unexpected response"))
            return 1
        print("Connected. No trading has been started.")
        print(HELP)
        stopped = threading.Event()
        latest = {}
        lock = threading.Lock()
        reader = threading.Thread(
            target=receive_events, args=(socket, stopped, latest, lock), daemon=True
        )
        reader.start()
        try:
            while not stopped.is_set():
                line = input("bot> ").strip()
                if stopped.is_set() or line == "quit":
                    break
                if not line:
                    continue
                if line == "help":
                    print(HELP)
                    continue
                if line == "status":
                    with lock:
                        snapshot = dict(latest)
                    print(json.dumps(snapshot, indent=2, ensure_ascii=False))
                    continue
                try:
                    payload = command_message(line)
                    if payload is not None:
                        socket.send(json.dumps(validated_message(payload)))
                except ValidationError as exc:
                    # Never print Pydantic input values, which may include private keys.
                    for error in exc.errors(include_input=False, include_url=False):
                        print("Invalid", ".".join(map(str, error["loc"])), error["msg"])
                except ValueError as exc:
                    print(str(exc))
        except (EOFError, KeyboardInterrupt):
            print("\nDisconnecting; backend will begin farm cleanup.")
        finally:
            socket.close()
            reader.join(timeout=2)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="ws://127.0.0.1:8000/ws")
    args = parser.parse_args()
    try:
        return run(args.url)
    except ValueError as exc:
        print(str(exc))
        return 1
    except (OSError, ConnectionClosed, TimeoutError):
        print("Could not communicate with the backend. Check that server is running.")
        return 1
    except (KeyboardInterrupt, EOFError):
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

