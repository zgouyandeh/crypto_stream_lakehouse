"""
Live dashboard over the crypto lakehouse's gold/silver Iceberg tables.

Reads directly via PyIceberg against the Iceberg REST catalog + MinIO -- no
Spark, Thrift, Trino, DuckDB, or Postgres. Coarser candle durations (1h, 2h)
are built by resampling the 1-minute gold bars in pandas.

Run locally (pair with the .streamlit/config.toml dark theme alongside this
file for the full look):
    pip install -r requirements.txt
    streamlit run dashboard.py
"""
import argparse
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from pyiceberg.catalog import load_catalog

st.set_page_config(page_title="Crypto Lakehouse", layout="wide", page_icon="📈")

PRODUCTS = ["BTC-USD", "ETH-USD"]
DURATIONS = {"1 Minute": ("1min", 1), "1 Hour": ("1h", 60), "2 Hours": ("2h", 120)}

UP_COLOR = "#0ECB81"
DOWN_COLOR = "#F6465D"
BG_COLOR = "#0B0E11"
GRID_COLOR = "#1E2329"

CUSTOM_CSS = """
<style>
    .block-container { padding-top: 1.2rem; padding-bottom: 1rem; }
    div[data-testid="stMetricValue"] { font-size: 1.4rem; }
    div[data-testid="stDataFrame"] { border: 1px solid #1E2329; }
    .header-card {
        border: 1px solid #2a2e35;
        border-radius: 8px;
        padding: 1rem 1.2rem 0.5rem 1.2rem;
        margin-bottom: 0.8rem;
        background-color: #0e1217;
    }
</style>
"""


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog-uri", default="http://localhost:8181")
    parser.add_argument("--s3-endpoint", default="http://localhost:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin123")
    args, _ = parser.parse_known_args()
    return args


@st.cache_resource
def get_catalog(catalog_uri: str, s3_endpoint: str, s3_access_key: str, s3_secret_key: str):
    return load_catalog(
        "demo",
        **{
            "uri": catalog_uri,
            "type": "rest",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def scan_table(catalog, table_name: str, row_filter: str = "true") -> pd.DataFrame:
    return catalog.load_table(table_name).scan(row_filter=row_filter).to_pandas()


def resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    df = df.set_index("bucket").sort_index()
    agg = df.resample(rule).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "trade_count": "sum",
        }
    )
    vwap_numerator = (df["vwap"] * df["volume"]).resample(rule).sum()
    agg["vwap"] = vwap_numerator / agg["volume"].replace(0, pd.NA)
    return agg.dropna(subset=["open"]).reset_index()


def order_book_tables(catalog, product_id: str, depth: int):
    levels = scan_table(
        catalog,
        "silver.stg_orderbook_levels",
        row_filter=f"product_id = '{product_id}' AND size > 0",
    )
    if levels.empty:
        return pd.DataFrame(), pd.DataFrame()
    bids = (
        levels[levels["side"] == "buy"]
        .sort_values("price", ascending=False)
        .head(depth)[["price", "size"]]
    )
    asks = (
        levels[levels["side"] == "sell"]
        .sort_values("price", ascending=True)
        .head(depth)[["price", "size"]]
    )
    return bids.reset_index(drop=True), asks.reset_index(drop=True)


def price_volume_chart(ohlcv: pd.DataFrame, product_id: str) -> go.Figure:
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.72, 0.28], vertical_spacing=0.03,
    )
    fig.add_trace(
        go.Candlestick(
            x=ohlcv["bucket"],
            open=ohlcv["open"],
            high=ohlcv["high"],
            low=ohlcv["low"],
            close=ohlcv["close"],
            name=product_id,
            increasing_line_color=UP_COLOR,
            decreasing_line_color=DOWN_COLOR,
        ),
        row=1, col=1,
    )
    bar_colors = [
        UP_COLOR if c >= o else DOWN_COLOR
        for o, c in zip(ohlcv["open"], ohlcv["close"])
    ]
    fig.add_trace(
        go.Bar(x=ohlcv["bucket"], y=ohlcv["volume"],
               marker_color=bar_colors, name="Volume"),
        row=2, col=1,
    )

    fig.update_layout(
        template="plotly_dark", height=440, showlegend=False,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_rangeslider_visible=False,
        paper_bgcolor=BG_COLOR, plot_bgcolor=BG_COLOR,
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=GRID_COLOR)
    return fig


def style_ladder(df: pd.DataFrame, color: str):
    return (
        df.style.set_properties(**{"color": color})
        .format({"price": "${:,.2f}", "size": "{:.6f}"})
    )


def style_trades(df: pd.DataFrame):
    def color_side(val):
        return f"color: {UP_COLOR if val == 'buy' else DOWN_COLOR}; font-weight: 600"
    return df.style.map(color_side, subset=["side"]).format(
        {"price": "${:,.2f}", "size": "{:.6f}"}
    )


def main() -> None:
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    args = get_args()
    catalog = get_catalog(
        args.catalog_uri, args.s3_endpoint,
        args.s3_access_key, args.s3_secret_key,
    )

    # ----- Sidebar -----
    st.sidebar.header("Controls")
    product_id = st.sidebar.selectbox("Product", PRODUCTS)
    duration_label = st.sidebar.radio("Candle duration", list(DURATIONS.keys()))
    bars_to_show = st.sidebar.slider("Bars to show", 20, 300, 100)
    order_book_depth = st.sidebar.slider("Order book depth", 5, 25, 10)

    rule, minutes_per_bar = DURATIONS[duration_label]
    lookback = timedelta(minutes=minutes_per_bar * bars_to_show * 1.2)
    cutoff = (datetime.now(timezone.utc) - lookback).isoformat()
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    # ----- Load data with spinner -----
    with st.spinner("Fetching fresh data from Iceberg…"):
        try:
            raw_ohlcv = scan_table(
                catalog, "gold.gold_ohlcv_1min",
                row_filter=f"product_id = '{product_id}' AND bucket >= '{cutoff}'",
            )
            raw_24h = scan_table(
                catalog, "gold.gold_ohlcv_1min",
                row_filter=f"product_id = '{product_id}' AND bucket >= '{cutoff_24h}'",
            )
        except Exception as exc:
            st.error(f"Could not read gold.gold_ohlcv_1min: {exc}")
            raw_ohlcv, raw_24h = pd.DataFrame(), pd.DataFrame()

    ohlcv = (
        resample_ohlcv(raw_ohlcv, rule).tail(bars_to_show)
        if not raw_ohlcv.empty
        else pd.DataFrame()
    )

    # ----- Ticker header KPI row -----
    st.markdown('<div class="header-card">', unsafe_allow_html=True)
    col1, col2, col3, col4, col5 = st.columns([1.5, 1.8, 1, 1, 1])

    with col1:
        st.markdown(f"### {product_id}")

    if not raw_24h.empty:
        last_price = raw_24h.sort_values("bucket")["close"].iloc[-1]
        first_price_24h = raw_24h.sort_values("bucket")["close"].iloc[0]
        change_24h = (
            (last_price - first_price_24h) / first_price_24h * 100
            if first_price_24h
            else 0.0
        )
        col2.metric(
            "Price",
            f"${last_price:,.2f}",
            delta=f"{change_24h:.2f}%",
            delta_color="normal",
        )
        col3.metric("24h High", f"${raw_24h['high'].max():,.2f}")
        col4.metric("24h Low", f"${raw_24h['low'].min():,.2f}")
        col5.metric("24h Volume", f"{raw_24h['volume'].sum():,.2f}")
    else:
        for col in [col2, col3, col4, col5]:
            col.metric("—", "—")

    st.markdown("</div>", unsafe_allow_html=True)

    # ----- Main chart + order book -----
    chart_col, book_col = st.columns([2, 1])

    with chart_col:
        if ohlcv.empty:
            st.info("No OHLCV data yet — has dbt run since trades started flowing?")
        else:
            st.plotly_chart(price_volume_chart(ohlcv, product_id), use_container_width=True)

    with book_col:
        try:
            bids, asks = order_book_tables(catalog, product_id, order_book_depth)
            if bids.empty or asks.empty:
                st.info("No order book data yet.")
            else:
                # Show spread as a simple caption (no depth chart)
                spread = asks["price"].iloc[0] - bids["price"].iloc[0]
                st.caption(f"Spread: ${spread:,.2f}")

                bid_col, ask_col = st.columns(2)
                with bid_col:
                    st.caption("Bids")
                    st.dataframe(
                        style_ladder(bids, UP_COLOR),
                        use_container_width=True,
                        hide_index=True,
                        height=280,
                    )
                with ask_col:
                    st.caption("Asks")
                    st.dataframe(
                        style_ladder(asks, DOWN_COLOR),
                        use_container_width=True,
                        hide_index=True,
                        height=280,
                    )
        except Exception as exc:
            st.warning(f"Order book unavailable: {exc}")

    # ----- Recent trades tape -----
    st.subheader("Recent trades")
    try:
        recent_trades = scan_table(
            catalog, "silver.stg_trades",
            row_filter=f"product_id = '{product_id}' AND time >= '{cutoff}'",
        )
        if recent_trades.empty:
            st.info("No recent trades in the selected window.")
        else:
            recent_trades = recent_trades.sort_values("time", ascending=False).head(30)
            st.dataframe(
                style_trades(recent_trades[["time", "side", "price", "size", "trade_id"]]),
                use_container_width=True,
                hide_index=True,
                height=280,
            )
    except Exception as exc:
        st.warning(f"Trades unavailable: {exc}")

    # ----- Footer -----
    st.sidebar.caption(
        f"Last refresh: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    st.caption("Click Refresh (top‑right menu, or press R) to pull the latest data.")


if __name__ == "__main__":
    main()