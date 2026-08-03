"""
Bridges Coinbase's public Exchange WebSocket feed into Kafka.

Subscribes to the `level2_batch` (order book) and `matches` (executed
trades) channels for one or more products and republishes each message onto
Kafka, partitioned by product_id so that all messages for a given product
traverse the same partition in the order they were received -- essential
for correctly reconstructing an order book downstream.

No API key needed: these are public market-data channels.

Run inside the spark-iceberg container, e.g.:
docker compose exec spark-iceberg python /home/jobs/kafka_producer.py --product-ids BTC-USD ETH-USD --duration-seconds 3600

Ctrl+C (or a SIGTERM from `docker compose stop`) shuts down cleanly.
"""
import argparse
import json
import logging
import signal
import threading
import time
from typing import Optional

import websocket
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("coinbase-producer")

WS_URL = "wss://ws-feed.exchange.coinbase.com"

ORDERBOOK_TYPES = {"snapshot", "l2update"}
TRADE_TYPES = {"match", "last_match"}


class CoinbaseBridge:
    """Owns the WebSocket connection, its Kafka producer, and per-product
    sequence tracking used to detect dropped messages."""

    def __init__(
        self,
        product_ids: list[str],
        bootstrap_servers: str,
        orderbook_topic: str,
        trades_topic: str,
        max_reconnect_backoff: float = 30.0,
    ) -> None:
        self.product_ids = product_ids
        self.orderbook_topic = orderbook_topic
        self.trades_topic = trades_topic
        self.max_reconnect_backoff = max_reconnect_backoff

        self._stop = False
        self._last_sequence: dict[str, int] = {}
        self._messages_sent = 0
        self._ws_app: Optional[websocket.WebSocketApp] = None
        self._last_disconnect_reason: Optional[str] = None

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=5,
            max_in_flight_requests_per_connection=1,
            linger_ms=20,
        )

    # -- lifecycle -----------------------------------------------------

    def run_forever(self, duration_seconds: Optional[float] = None) -> None:
        deadline = time.monotonic() + duration_seconds if duration_seconds else None
        backoff = 1.0

        while not self._stop:
            if deadline and time.monotonic() >= deadline:
                log.info("Duration elapsed, stopping.")
                break

            self._last_disconnect_reason = None
            session_start = time.monotonic()
            try:
                self._connect_and_stream(deadline)
            except Exception as exc:  # noqa: BLE001 -- top-level reconnect loop
                self._last_disconnect_reason = str(exc)

            if self._stop:
                break

            # A session that lasted a while before dropping wasn't a
            # persistent problem -- reset the backoff instead of letting it
            # ratchet up forever from one bad patch of network.
            if time.monotonic() - session_start > 60:
                backoff = 1.0

            log.warning(
                "WebSocket session ended (%s); reconnecting in %.1fs",
                self._last_disconnect_reason or "unknown reason", backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, self.max_reconnect_backoff)

        self.producer.flush()
        self.producer.close()
        log.info("Shut down cleanly. Total messages forwarded: %d", self._messages_sent)

    def stop(self, *_args) -> None:
        log.info("Stop signal received, closing after current message...")
        self._stop = True
        if self._ws_app:
            self._ws_app.close()

    # -- one WebSocket session ------------------------------------------

    def _connect_and_stream(self, deadline: Optional[float]) -> None:
        # A reconnect intentionally resets sequence tracking: Coinbase sends
        # a fresh `snapshot` on (re)subscribe, so a new session always
        # starts from a known-good order-book state rather than trying to
        # patch a possibly-stale one.
        self._last_sequence.clear()

        app = websocket.WebSocketApp(
            WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._ws_app = app

        # Enforce --duration-seconds by closing the socket from a timer
        # thread; run_forever() below blocks until the connection ends.
        timer = None
        if deadline is not None:
            remaining = max(deadline - time.monotonic(), 0)
            timer = threading.Timer(remaining, app.close)
            timer.daemon = True
            timer.start()

        try:
            # ping_interval/ping_timeout are the actual fix here: this
            # library only sends background keepalive pings through
            # WebSocketApp.run_forever(), never through the low-level
            # create_connection()/recv() client. Sending a Ping every 20s
            # (well under Coinbase's own heartbeat cadence) both keeps
            # NATs/proxies from treating the connection as idle and detects
            # a truly dead connection faster than waiting on a bare socket
            # timeout.
            app.run_forever(ping_interval=20, ping_timeout=10)
        finally:
            if timer:
                timer.cancel()

    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": self.product_ids,
            "channels": ["level2_batch", "matches", "heartbeat"],
        }))
        log.info("Subscribed to level2_batch/matches/heartbeat for %s", self.product_ids)

    def _on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        self._handle_message(json.loads(message))

    def _on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        self._last_disconnect_reason = str(error)

    def _on_close(self, ws: websocket.WebSocketApp, close_status_code, close_msg) -> None:
        if not self._last_disconnect_reason:
            self._last_disconnect_reason = close_msg or f"close code {close_status_code}"

    # -- per-message routing ---------------------------------------------

    def _handle_message(self, msg: dict) -> None:
        msg_type = msg.get("type")
        product_id = msg.get("product_id")

        if msg_type == "subscriptions":
            log.info("Subscription confirmed: %s", msg.get("channels"))
            return
        if msg_type == "error":
            log.error("Coinbase feed error: %s", msg)
            return
        if msg_type == "heartbeat":
            self._check_sequence_gap(product_id, msg.get("sequence"))
            return

        if msg_type in ORDERBOOK_TYPES:
            self._check_sequence_gap(product_id, msg.get("sequence"))
            self._send(self.orderbook_topic, product_id, msg)
        elif msg_type in TRADE_TYPES:
            self._send(self.trades_topic, product_id, msg)

    def _check_sequence_gap(self, product_id: Optional[str], sequence: Optional[int]) -> None:
        if product_id is None or sequence is None:
            return
        last = self._last_sequence.get(product_id)
        if last is not None and sequence > last + 1:
            log.warning(
                "Sequence gap for %s: expected %d, got %d (%d messages possibly dropped)",
                product_id, last + 1, sequence, sequence - last - 1,
            )
        self._last_sequence[product_id] = sequence

    def _send(self, topic: str, key: Optional[str], value: dict) -> None:
        future = self.producer.send(topic, key=key or "unknown", value=value)
        future.add_errback(
            lambda exc: log.error("Send to '%s' failed permanently: %s", topic, exc)
        )
        self._messages_sent += 1
        if self._messages_sent % 500 == 0:
            log.info("Forwarded %d messages so far...", self._messages_sent)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--product-ids", nargs="+", default=["ETH-USD"],
                         help="Start with one lower-liquidity pair before scaling to BTC-USD")
    parser.add_argument("--orderbook-topic", default="orderbook_updates")
    parser.add_argument("--trades-topic", default="trades")
    parser.add_argument("--duration-seconds", type=float, default=None,
                         help="Optional auto-stop for test runs; omit to stream indefinitely")
    args = parser.parse_args()

    bridge = CoinbaseBridge(
        product_ids=args.product_ids,
        bootstrap_servers=args.bootstrap_servers,
        orderbook_topic=args.orderbook_topic,
        trades_topic=args.trades_topic,
    )
    signal.signal(signal.SIGINT, bridge.stop)
    signal.signal(signal.SIGTERM, bridge.stop)

    log.info(
        "Starting Coinbase -> Kafka bridge for %s (orderbook -> '%s', trades -> '%s')",
        args.product_ids, args.orderbook_topic, args.trades_topic,
    )
    bridge.run_forever(duration_seconds=args.duration_seconds)


if __name__ == "__main__":
    main()