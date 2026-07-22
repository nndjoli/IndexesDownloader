#!/usr/bin/env python3
"""Download daily and month-end histories for eight European indices.

Version 11 builds each month-end table at Gross/Price pair level:
1. inner join both daily histories on common dates;
2. retain the last common observation of each completed calendar month;
3. relabel that observation with the calendar month-end date;
4. outer join the four pair tables once to create the final EOM dataset.

No index level is smoothed, replaced, or rescaled after source extraction.
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

VERSION = "11.0.0"
OUT = Path("index_histories")
EOM_OUT = OUT / "eom"
TODAY = pd.Timestamp.today().normalize()
LAST_COMPLETED_MONTH_END = (TODAY.to_period("M") - 1).to_timestamp("M")
FT_SOURCE = os.getenv("FT_SOURCE", "42085953e1cc8d0a")
DIVIDEND_ROUNDING_TOLERANCE = 1e-5

STOXX_PAGE = "https://stoxx.com/index/{symbol}/?factsheet=true"
STOXX_RECENT = (
    "https://www.stoxx.com/document/Indices/Current/HistoricalData/"
    "h_3m{symbol}.txt"
)
FT_URLS = (
    "https://markets.ft.com/research/webservices/securities/v1/"
    "historical-series-quotes",
    "https://markets.ft.markitdigital.com/research/webservices/securities/v1/"
    "historical-series-quotes",
)


class DownloadError(RuntimeError):
    """Raised when an index history cannot be built safely."""


@dataclass(frozen=True)
class IndexSpec:
    source: str
    aliases: tuple[str, ...]
    name: str
    isin: str
    symbol: str
    source_page: str


SPECS: dict[str, IndexSpec] = {
    "STOXX_600_Gross_Return": IndexSpec(
        "STOXX", ("SXXGR",), "STOXX® Europe 600 (Gross Return EUR)",
        "CH0102635015", "SXXGR",
        "https://stoxx.com/index/sxxgr/?factsheet=true",
    ),
    "STOXX_600_Price_Return": IndexSpec(
        "STOXX", ("SXXP",), "STOXX® Europe 600 (Price Return EUR)",
        "EU0009658202", "SXXP",
        "https://stoxx.com/index/sxxp/?factsheet=true",
    ),
    "EURO_STOXX_50_Gross_Return": IndexSpec(
        "STOXX", ("SX5GT",), "EURO STOXX 50® (Gross Return EUR)",
        "CH0102173264", "SX5GT",
        "https://stoxx.com/index/sx5gt/?factsheet=true",
    ),
    "EURO_STOXX_50_Price_Return": IndexSpec(
        "STOXX", ("SX5E",), "EURO STOXX 50® (Price Return EUR)",
        "EU0009658145", "SX5E",
        "https://stoxx.com/index/sx5e/?factsheet=true",
    ),
    "CAC_All_Tradable_Gross_Return": IndexSpec(
        "FT", ("CACTR:PAR",), "CAC All-Tradable Gross Return",
        "QS0011131891", "CACTR",
        "https://markets.ft.com/data/indices/tearsheet/summary?s=CACTR%3APAR",
    ),
    "CAC_All_Tradable_Price_Return": IndexSpec(
        "FT", ("CACT:PAR", "SBFA:PAR", "IDXSBFA:PAR"),
        "CAC All-Tradable", "FR0003999499", "CACT",
        "https://markets.ft.com/data/indices/tearsheet/summary?s=CACT%3APAR",
    ),
    "CAC_40_Gross_Return": IndexSpec(
        "FT", ("PX1GR:PAR",), "CAC 40 Gross Return",
        "QS0011131834", "PX1GR",
        "https://markets.ft.com/data/indices/tearsheet/summary?s=PX1GR%3APAR",
    ),
    "CAC_40_Price_Return": IndexSpec(
        "FT", ("CAC:PAR", "PX1:PAR"), "CAC 40",
        "FR0003500008", "CAC",
        "https://markets.ft.com/data/indices/tearsheet/summary?s=CAC%3APAR",
    ),
}

RETURN_PAIRS: dict[str, tuple[str, str]] = {
    "STOXX Europe 600": (
        "STOXX_600_Gross_Return", "STOXX_600_Price_Return"
    ),
    "EURO STOXX 50": (
        "EURO_STOXX_50_Gross_Return", "EURO_STOXX_50_Price_Return"
    ),
    "CAC All-Tradable": (
        "CAC_All_Tradable_Gross_Return", "CAC_All_Tradable_Price_Return"
    ),
    "CAC 40": ("CAC_40_Gross_Return", "CAC_40_Price_Return"),
}


async def get_text(
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, object] | None = None,
) -> str:
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        return await response.text()


def clean(df: pd.DataFrame, label: str) -> pd.DataFrame:
    if df.empty or not {"Date", "Close"}.issubset(df.columns):
        raise DownloadError(f"Aucune donnée exploitable pour {label}")

    result = df[["Date", "Close"]].copy()
    result["Date"] = pd.to_datetime(
        result["Date"], errors="coerce", utc=True
    ).dt.tz_localize(None).dt.normalize()
    result["Close"] = pd.to_numeric(result["Close"], errors="coerce")
    result = result[result["Date"].between(pd.Timestamp("1950-01-01"), TODAY)]
    result = (
        result.dropna(subset=["Date", "Close"])
        .drop_duplicates("Date", keep="last")
        .sort_values("Date")
        .reset_index(drop=True)
    )
    if result.empty:
        raise DownloadError(f"Historique inutilisable pour {label}")
    return result


def parse_stoxx_chart(html: str, symbol: str) -> pd.DataFrame:
    text = BeautifulSoup(html, "html.parser").get_text("\n")
    pairs = re.findall(
        r"(?m)^\s*(\d{12,13})\s+(-?\d+(?:[.,]\d+)?(?:[eE][+-]?\d+)?)\s*$",
        text,
    )
    if not pairs:
        raise DownloadError(f"Aucun point STOXX détecté pour {symbol}")

    groups: list[list[tuple[str, str]]] = []
    current: list[tuple[str, str]] = []
    previous: int | None = None
    for timestamp, value in pairs:
        current_timestamp = int(timestamp)
        if previous is not None and current_timestamp <= previous:
            if current:
                groups.append(current)
            current = []
        current.append((timestamp, value))
        previous = current_timestamp
    if current:
        groups.append(current)

    candidates: list[pd.DataFrame] = []
    for group in groups:
        frame = pd.DataFrame(group, columns=["Timestamp", "Close"])
        frame["Date"] = pd.to_datetime(
            pd.to_numeric(frame["Timestamp"], errors="coerce"),
            unit="ms", utc=True, errors="coerce",
        )
        frame["Close"] = pd.to_numeric(
            frame["Close"].str.replace(",", ".", regex=False),
            errors="coerce",
        )
        try:
            frame = clean(frame, symbol)
        except DownloadError:
            continue
        gaps = frame["Date"].diff().dt.days.dropna()
        regularity = gaps.between(1, 7).mean() if not gaps.empty else 0.0
        if len(frame) >= 250 and regularity >= 0.90:
            candidates.append(frame)

    if not candidates:
        raise DownloadError(f"Aucune série STOXX cohérente pour {symbol}")
    return max(candidates, key=len)


def parse_stoxx_recent(text: str, symbol: str) -> pd.DataFrame:
    rows = re.findall(
        rf"(\d{{2}}\.\d{{2}}\.\d{{4}});{re.escape(symbol)};"
        rf"(-?\d+(?:[.,]\d+)?);",
        text,
        flags=re.IGNORECASE,
    )
    if not rows:
        raise DownloadError(f"Fichier STOXX récent vide pour {symbol}")
    frame = pd.DataFrame(rows, columns=["Date", "Close"])
    frame["Date"] = pd.to_datetime(
        frame["Date"], format="%d.%m.%Y", errors="coerce"
    )
    frame["Close"] = pd.to_numeric(
        frame["Close"].str.replace(",", ".", regex=False),
        errors="coerce",
    )
    return clean(frame, symbol)


def rescale_stoxx(
    chart: pd.DataFrame, recent: pd.DataFrame, symbol: str
) -> pd.DataFrame:
    overlap = chart.merge(
        recent, on="Date", how="inner", suffixes=("_chart", "_actual")
    )
    if len(overlap) < 10:
        raise DownloadError(
            f"Seulement {len(overlap)} dates communes pour remettre "
            f"{symbol} à l’échelle"
        )
    ratios = (overlap["Close_actual"] / overlap["Close_chart"]).replace(
        [float("inf"), float("-inf")], pd.NA
    ).dropna()
    if ratios.empty:
        raise DownloadError(f"Facteur d’échelle impossible pour {symbol}")
    scale = float(ratios.median())
    dispersion = float((ratios / scale - 1).abs().median())
    if dispersion > 0.005:
        raise DownloadError(
            f"Échelle STOXX instable pour {symbol}: {dispersion:.4%}"
        )
    chart = chart.copy()
    chart["Close"] *= scale
    return clean(pd.concat([chart, recent], ignore_index=True), symbol)


async def fetch_stoxx(
    session: aiohttp.ClientSession, symbol: str
) -> pd.DataFrame:
    html, recent_text = await asyncio.gather(
        get_text(session, STOXX_PAGE.format(symbol=symbol.lower())),
        get_text(session, STOXX_RECENT.format(symbol=symbol.lower())),
    )
    return rescale_stoxx(
        parse_stoxx_chart(html, symbol),
        parse_stoxx_recent(recent_text, symbol),
        symbol,
    )


def parse_ft(text: str, symbol: str) -> pd.DataFrame:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise DownloadError(f"Réponse FT non JSON pour {symbol}") from exc
    items = payload.get("data", {}).get("items") or []
    quotes = (
        items[0].get("historicalSeries", {}).get("historicalQuoteData")
        if items else None
    ) or []
    if not quotes:
        return pd.DataFrame(columns=["Date", "Close"])
    return pd.DataFrame(quotes).rename(
        columns={"date": "Date", "close": "Close"}
    )


async def fetch_ft(
    session: aiohttp.ClientSession, symbol: str
) -> pd.DataFrame:
    params: dict[str, object] = {
        "symbols": symbol,
        "intervalType": "day",
        "dayCount": 73_050,
        "source": FT_SOURCE,
    }
    errors: list[str] = []
    for url in FT_URLS:
        try:
            frame = parse_ft(await get_text(session, url, params), symbol)
            if not frame.empty:
                return clean(frame, symbol)
        except Exception as exc:
            errors.append(str(exc))
    raise DownloadError(" | ".join(errors) or f"Aucune donnée FT pour {symbol}")


async def fetch_one(
    session: aiohttp.ClientSession, key: str, spec: IndexSpec
) -> tuple[str, pd.DataFrame]:
    errors: list[str] = []
    for symbol in spec.aliases:
        try:
            frame = (
                await fetch_stoxx(session, symbol)
                if spec.source == "STOXX"
                else await fetch_ft(session, symbol)
            )
            return key, frame
        except Exception as exc:
            errors.append(f"{symbol}: {exc}")
    raise DownloadError(" ; ".join(errors))


def validate_pair_dividends(
    pair: pd.DataFrame,
    gross_key: str,
    price_key: str,
    pair_name: str,
    observation_dates: pd.Series,
) -> None:
    gross = pd.to_numeric(pair[gross_key], errors="coerce")
    price = pd.to_numeric(pair[price_key], errors="coerce")
    if gross.isna().any() or price.isna().any():
        raise DownloadError(f"Valeur manquante dans la paire {pair_name}")
    if (gross <= 0).any() or (price <= 0).any():
        raise DownloadError(f"Niveau nul ou négatif dans la paire {pair_name}")

    dividend = (gross / gross.shift()) / (price / price.shift()) - 1.0
    material = dividend < -DIVIDEND_ROUNDING_TOLERANCE
    if material.any():
        position = int(material.to_numpy().nonzero()[0][0])
        raise DownloadError(
            f"Dividende implicite négatif pour {pair_name} "
            f"au mois {pair.at[position, 'Date']:%Y-%m}: "
            f"{dividend.iloc[position]:.10%}; date commune "
            f"{pd.Timestamp(observation_dates.iloc[position]):%Y-%m-%d}."
        )

    rounding = (dividend < 0) & ~material
    if rounding.any():
        print(
            f"  {pair_name}: {int(rounding.sum())} résidu(s) d’arrondi "
            f"négatif(s), minimum {float(dividend[rounding].min()):.10%}; "
            "niveaux source inchangés."
        )


def pair_to_eom(
    gross_df: pd.DataFrame,
    price_df: pd.DataFrame,
    gross_key: str,
    price_key: str,
    pair_name: str,
) -> pd.DataFrame:
    daily = gross_df.rename(columns={"Close": gross_key}).merge(
        price_df.rename(columns={"Close": price_key}),
        on="Date", how="inner", validate="one_to_one",
    )
    if daily.empty:
        raise DownloadError(f"Aucune date commune pour {pair_name}")

    daily = daily.sort_values("Date").reset_index(drop=True)
    daily["Month"] = daily["Date"].dt.to_period("M")
    daily["Month_End"] = daily["Month"].dt.to_timestamp("M")
    daily = daily[daily["Month_End"] <= LAST_COMPLETED_MONTH_END]
    if daily.empty:
        raise DownloadError(f"Aucun mois terminé pour {pair_name}")

    monthly = (
        daily.groupby("Month", as_index=False, sort=True)
        .tail(1).sort_values("Date").reset_index(drop=True)
    )
    observation_dates = monthly["Date"].copy()
    monthly["Date"] = monthly["Month_End"]
    monthly = monthly[["Date", gross_key, price_key]].reset_index(drop=True)
    validate_pair_dividends(
        monthly, gross_key, price_key, pair_name, observation_dates
    )
    print(
        f"  {pair_name}: {len(monthly):,} mois; "
        f"{observation_dates.iloc[-1]:%Y-%m-%d} assignée à "
        f"{monthly['Date'].iloc[-1]:%Y-%m-%d}"
    )
    return monthly


def build_pair_tables(
    histories: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    for pair_name, (gross_key, price_key) in RETURN_PAIRS.items():
        missing = [
            key for key in (gross_key, price_key) if key not in histories
        ]
        if missing:
            raise DownloadError(
                f"Paire incomplète pour {pair_name}: {', '.join(missing)}"
            )
        tables[pair_name] = pair_to_eom(
            histories[gross_key], histories[price_key],
            gross_key, price_key, pair_name,
        )
    return tables


def merge_pair_tables(
    pair_tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    for pair_name in RETURN_PAIRS:
        pair = pair_tables[pair_name]
        merged = pair if merged is None else merged.merge(
            pair, on="Date", how="outer", validate="one_to_one"
        )
    if merged is None:
        raise DownloadError("Aucune table EOM à consolider")
    return (
        merged.sort_values("Date").reset_index(drop=True)
        .reindex(columns=["Date", *SPECS])
    )


def outer_join(histories: dict[str, pd.DataFrame]) -> pd.DataFrame:
    series = [
        histories[key].drop_duplicates("Date", keep="last")
        .set_index("Date")["Close"].rename(key)
        for key in SPECS
    ]
    return (
        pd.concat(series, axis=1, join="outer")
        .sort_index().reset_index()
        .reindex(columns=["Date", *SPECS])
    )


def write_csv(
    path: Path, frame: pd.DataFrame, metadata: list[list[str]]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        csv.writer(file, lineterminator="\n").writerows(metadata)
    frame.to_csv(
        path, mode="a", index=False, encoding="utf-8",
        date_format="%Y-%m-%d", lineterminator="\n",
    )


def individual_metadata(spec: IndexSpec) -> list[list[str]]:
    return [
        ["Nom", spec.name], ["ISIN", spec.isin],
        ["Symbole", spec.symbol], ["Source", spec.source_page],
    ]


def consolidated_metadata() -> list[list[str]]:
    return [
        ["Nom", *[spec.name for spec in SPECS.values()]],
        ["ISIN", *[spec.isin for spec in SPECS.values()]],
        ["Symbole", *[spec.symbol for spec in SPECS.values()]],
        ["Source", *[spec.source_page for spec in SPECS.values()]],
    ]


def validate_histories(histories: dict[str, pd.DataFrame]) -> None:
    missing = [key for key in SPECS if key not in histories]
    if missing:
        raise DownloadError(f"Séries manquantes: {', '.join(missing)}")
    empty = [key for key in SPECS if histories[key].empty]
    if empty:
        raise DownloadError(f"Séries vides: {', '.join(empty)}")


def save(histories: dict[str, pd.DataFrame]) -> tuple[Path, Path]:
    validate_histories(histories)
    OUT.mkdir(parents=True, exist_ok=True)
    EOM_OUT.mkdir(parents=True, exist_ok=True)

    for key, spec in SPECS.items():
        write_csv(OUT / f"{key}.csv", histories[key], individual_metadata(spec))

    print("\nConstruction EOM par dates communes :")
    pair_tables = build_pair_tables(histories)
    eom_merged = merge_pair_tables(pair_tables)

    for pair_name, (gross_key, price_key) in RETURN_PAIRS.items():
        pair = pair_tables[pair_name]
        for key in (gross_key, price_key):
            individual = pair[["Date", key]].rename(columns={key: "Close"})
            write_csv(
                EOM_OUT / f"{key}_EOM.csv",
                individual,
                individual_metadata(SPECS[key]),
            )

    daily_merged = outer_join(histories)
    daily_path = OUT / "All_Indices_Close.csv"
    eom_path = EOM_OUT / "All_Indices_Close_EOM.csv"
    write_csv(daily_path, daily_merged, consolidated_metadata())
    write_csv(eom_path, eom_merged, consolidated_metadata())
    return daily_path, eom_path


async def main() -> None:
    print(f"Indexes.py — version {VERSION}")
    timeout = aiohttp.ClientTimeout(total=180, connect=30, sock_read=120)
    connector = aiohttp.TCPConnector(limit=16, ttl_dns_cache=300)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0 Safari/537.36"
        ),
        "Accept": "application/json,text/html,text/plain,*/*",
        "Referer": "https://markets.ft.com/data/",
    }
    async with aiohttp.ClientSession(
        timeout=timeout, connector=connector, headers=headers
    ) as session:
        results = await asyncio.gather(
            *[fetch_one(session, key, spec) for key, spec in SPECS.items()],
            return_exceptions=True,
        )

    histories: dict[str, pd.DataFrame] = {}
    failures: list[str] = []
    for key, result in zip(SPECS, results):
        if isinstance(result, Exception):
            failures.append(f"{key}: {result}")
            continue
        name, frame = result
        histories[name] = frame
        print(
            f"{name}: {len(frame):,} lignes, "
            f"{frame['Date'].min().date()} -> {frame['Date'].max().date()}"
        )

    if failures:
        raise DownloadError("Échecs de téléchargement:\n- " + "\n- ".join(failures))

    daily_path, eom_path = save(histories)
    print(f"\nCSV quotidien consolidé : {daily_path.resolve()}")
    print(f"CSV EOM consolidé       : {eom_path.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
