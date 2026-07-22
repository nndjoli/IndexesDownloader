#!/usr/bin/env python3
"""Download daily and month-end histories for eight European indices.

Version 12 uses curl_cffi for browser-like TLS/HTTP requests and treats
source outages as data-quality events rather than fatal program errors.

For each Gross Return / Price Return pair, the monthly table is built by:
1. inner joining the two daily histories on common dates;
2. retaining the last common observation of each completed calendar month;
3. relabelling that observation with the calendar month-end date;
4. outer joining the available pair tables into the consolidated EOM file.

All expected CSV files are written on every run. When a current download fails,
the previously published individual daily CSV is reused when available;
otherwise the relevant columns and individual files are left empty. Download,
pair-construction, and dividend-validation diagnostics are exported separately.
No index level is corrected, smoothed, or rescaled after source extraction.
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

VERSION = "12.0.0"
OUT = Path("index_histories")
EOM_OUT = OUT / "eom"
TODAY = pd.Timestamp.today().normalize()
LAST_COMPLETED_MONTH_END = (TODAY.to_period("M") - 1).to_timestamp("M")
FT_SOURCE = os.getenv("FT_SOURCE", "42085953e1cc8d0a")
DIVIDEND_ROUNDING_TOLERANCE = 1e-5
HTTP_ATTEMPTS = 3
HTTP_TIMEOUT_SECONDS = 180

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
    """Raised when an individual history or pair cannot be built safely."""


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


def empty_history() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": pd.Series(dtype="datetime64[ns]"),
            "Close": pd.Series(dtype="float64"),
        }
    )


def empty_consolidated() -> pd.DataFrame:
    frame = pd.DataFrame({"Date": pd.Series(dtype="datetime64[ns]")})
    for key in SPECS:
        frame[key] = pd.Series(dtype="float64")
    return frame[["Date", *SPECS]]


async def get_text(
    session: AsyncSession,
    url: str,
    params: dict[str, object] | None = None,
) -> str:
    """Fetch text with browser impersonation and bounded retries."""
    errors: list[str] = []
    for attempt in range(1, HTTP_ATTEMPTS + 1):
        try:
            response = await session.get(
                url,
                params=params,
                impersonate="chrome",
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.text
        except Exception as exc:
            errors.append(f"tentative {attempt}: {exc}")
            if attempt < HTTP_ATTEMPTS:
                await asyncio.sleep(1.25 * (2 ** (attempt - 1)))
    raise DownloadError(f"Échec HTTP pour {url}: {' | '.join(errors)}")


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
    points = re.findall(
        r"(?m)^\s*(\d{12,13})\s+(-?\d+(?:[.,]\d+)?(?:[eE][+-]?\d+)?)\s*$",
        text,
    )
    if not points:
        raise DownloadError(f"Aucun point STOXX détecté pour {symbol}")
    groups: list[list[tuple[str, str]]] = []
    current: list[tuple[str, str]] = []
    previous: int | None = None
    for timestamp, value in points:
        numeric_timestamp = int(timestamp)
        if previous is not None and numeric_timestamp <= previous:
            if current:
                groups.append(current)
            current = []
        current.append((timestamp, value))
        previous = numeric_timestamp
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


async def fetch_stoxx(session: AsyncSession, symbol: str) -> pd.DataFrame:
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
        return empty_history()
    return pd.DataFrame(quotes).rename(
        columns={"date": "Date", "close": "Close"}
    )


async def fetch_ft(session: AsyncSession, symbol: str) -> pd.DataFrame:
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
    session: AsyncSession, key: str, spec: IndexSpec
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
) -> list[dict[str, Any]]:
    gross = pd.to_numeric(pair[gross_key], errors="coerce")
    price = pd.to_numeric(pair[price_key], errors="coerce")
    if gross.isna().any() or price.isna().any():
        raise DownloadError(f"Valeur manquante dans la paire {pair_name}")
    if (gross <= 0).any() or (price <= 0).any():
        raise DownloadError(f"Niveau nul ou négatif dans la paire {pair_name}")
    dividend = (gross / gross.shift()) / (price / price.shift()) - 1.0
    warnings: list[dict[str, Any]] = []
    for position in dividend[dividend < 0].index.tolist():
        value = float(dividend.loc[position])
        severity = (
            "material"
            if value < -DIVIDEND_ROUNDING_TOLERANCE
            else "rounding"
        )
        warnings.append(
            {
                "Pair": pair_name,
                "Month_End": pair.at[position, "Date"],
                "Observation_Date": pd.Timestamp(observation_dates.loc[position]),
                "Dividend_Yield": value,
                "Tolerance": DIVIDEND_ROUNDING_TOLERANCE,
                "Severity": severity,
            }
        )
    if warnings:
        material_count = sum(row["Severity"] == "material" for row in warnings)
        rounding_count = len(warnings) - material_count
        minimum = min(float(row["Dividend_Yield"]) for row in warnings)
        print(
            f"  Avertissement {pair_name}: {len(warnings)} dividende(s) "
            f"implicite(s) négatif(s), dont {material_count} matériel(s) et "
            f"{rounding_count} résidu(s) d’arrondi; minimum {minimum:.10%}. "
            "Les niveaux source restent inchangés."
        )
    return warnings


def pair_to_eom(
    gross_df: pd.DataFrame,
    price_df: pd.DataFrame,
    gross_key: str,
    price_key: str,
    pair_name: str,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
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
    warnings = validate_pair_dividends(
        monthly, gross_key, price_key, pair_name, observation_dates
    )
    print(
        f"  {pair_name}: {len(monthly):,} mois; "
        f"{observation_dates.iloc[-1]:%Y-%m-%d} assignée à "
        f"{monthly['Date'].iloc[-1]:%Y-%m-%d}"
    )
    return monthly, warnings


def load_previous_history(key: str) -> pd.DataFrame | None:
    path = OUT / f"{key}.csv"
    if not path.exists():
        return None
    try:
        return clean(pd.read_csv(path, skiprows=4), f"historique précédent {key}")
    except Exception as exc:
        print(f"  Historique précédent inutilisable pour {key}: {exc}")
        return None


def load_previous_eom_pair(
    gross_key: str, price_key: str
) -> pd.DataFrame | None:
    gross_path = EOM_OUT / f"{gross_key}_EOM.csv"
    price_path = EOM_OUT / f"{price_key}_EOM.csv"
    if not gross_path.exists() or not price_path.exists():
        return None
    try:
        gross = clean(pd.read_csv(gross_path, skiprows=4), gross_key)
        price = clean(pd.read_csv(price_path, skiprows=4), price_key)
        pair = gross.rename(columns={"Close": gross_key}).merge(
            price.rename(columns={"Close": price_key}),
            on="Date", how="inner", validate="one_to_one",
        )
        if pair.empty:
            return None
        return pair[["Date", gross_key, price_key]].sort_values("Date")
    except Exception as exc:
        print(
            f"  Paire EOM précédente inutilisable pour "
            f"{gross_key}/{price_key}: {exc}"
        )
        return None


def build_pair_tables(
    histories: dict[str, pd.DataFrame],
) -> tuple[
    dict[str, pd.DataFrame],
    list[dict[str, str]],
    list[dict[str, Any]],
]:
    tables: dict[str, pd.DataFrame] = {}
    failures: list[dict[str, str]] = []
    dividend_warnings: list[dict[str, Any]] = []
    for pair_name, (gross_key, price_key) in RETURN_PAIRS.items():
        missing = [
            key for key in (gross_key, price_key)
            if key not in histories or histories[key].empty
        ]
        if missing:
            error = f"Paire incomplète: {', '.join(missing)}"
            previous_pair = load_previous_eom_pair(gross_key, price_key)
            if previous_pair is not None:
                tables[pair_name] = previous_pair
                status = "fallback_previous_eom"
            else:
                status = "failed_empty"
            failures.append({"Pair": pair_name, "Status": status, "Error": error})
            continue
        try:
            table, warnings = pair_to_eom(
                histories[gross_key], histories[price_key],
                gross_key, price_key, pair_name,
            )
            tables[pair_name] = table
            dividend_warnings.extend(warnings)
        except Exception as exc:
            error = str(exc)
            previous_pair = load_previous_eom_pair(gross_key, price_key)
            if previous_pair is not None:
                tables[pair_name] = previous_pair
                status = "fallback_previous_eom"
            else:
                status = "failed_empty"
            failures.append({"Pair": pair_name, "Status": status, "Error": error})
            print(f"  Échec EOM {pair_name}: {error}; statut={status}")
    return tables, failures, dividend_warnings


def merge_pair_tables(
    pair_tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    for pair_name in RETURN_PAIRS:
        pair = pair_tables.get(pair_name)
        if pair is None or pair.empty:
            continue
        merged = pair.copy() if merged is None else merged.merge(
            pair, on="Date", how="outer", validate="one_to_one"
        )
    if merged is None:
        return empty_consolidated()
    return (
        merged.sort_values("Date").reset_index(drop=True)
        .reindex(columns=["Date", *SPECS])
    )


def outer_join(histories: dict[str, pd.DataFrame]) -> pd.DataFrame:
    series: list[pd.Series] = []
    for key in SPECS:
        frame = histories.get(key)
        if frame is None or frame.empty:
            continue
        series.append(
            frame.drop_duplicates("Date", keep="last")
            .set_index("Date")["Close"].rename(key)
        )
    if not series:
        return empty_consolidated()
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


def save(
    histories: dict[str, pd.DataFrame],
    download_status: list[dict[str, Any]],
) -> tuple[Path, Path]:
    OUT.mkdir(parents=True, exist_ok=True)
    EOM_OUT.mkdir(parents=True, exist_ok=True)
    for key, spec in SPECS.items():
        write_csv(
            OUT / f"{key}.csv",
            histories.get(key, empty_history()),
            individual_metadata(spec),
        )
    print("\nConstruction EOM par dates communes :")
    pair_tables, pair_failures, dividend_warnings = build_pair_tables(histories)
    eom_merged = merge_pair_tables(pair_tables)
    eom_individual: dict[str, pd.DataFrame] = {
        key: empty_history() for key in SPECS
    }
    for pair_name, (gross_key, price_key) in RETURN_PAIRS.items():
        pair = pair_tables.get(pair_name)
        if pair is None or pair.empty:
            continue
        eom_individual[gross_key] = pair[["Date", gross_key]].rename(
            columns={gross_key: "Close"}
        )
        eom_individual[price_key] = pair[["Date", price_key]].rename(
            columns={price_key: "Close"}
        )
    for key, spec in SPECS.items():
        write_csv(
            EOM_OUT / f"{key}_EOM.csv",
            eom_individual[key],
            individual_metadata(spec),
        )
    daily_merged = outer_join(histories)
    daily_path = OUT / "All_Indices_Close.csv"
    eom_path = EOM_OUT / "All_Indices_Close_EOM.csv"
    write_csv(daily_path, daily_merged, consolidated_metadata())
    write_csv(eom_path, eom_merged, consolidated_metadata())
    status_columns = [
        "Run_UTC", "Version", "Series", "Status", "Rows",
        "Start_Date", "End_Date", "Error",
    ]
    pd.DataFrame(download_status, columns=status_columns).to_csv(
        OUT / "Download_Status.csv", index=False, encoding="utf-8-sig"
    )
    pd.DataFrame(
        pair_failures, columns=["Pair", "Status", "Error"]
    ).to_csv(
        EOM_OUT / "Pair_Status.csv", index=False, encoding="utf-8-sig"
    )
    warning_columns = [
        "Pair", "Month_End", "Observation_Date", "Dividend_Yield",
        "Tolerance", "Severity",
    ]
    pd.DataFrame(
        dividend_warnings, columns=warning_columns
    ).to_csv(
        EOM_OUT / "Dividend_Validation_Warnings.csv",
        index=False, encoding="utf-8-sig", date_format="%Y-%m-%d",
    )
    summary = {
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "version": VERSION,
        "downloaded": sum(row["Status"] == "downloaded" for row in download_status),
        "fallback_previous": sum(
            row["Status"] == "fallback_previous" for row in download_status
        ),
        "failed_empty": sum(
            row["Status"] == "failed_empty" for row in download_status
        ),
        "available_daily_series": sorted(histories),
        "available_eom_pairs": sorted(pair_tables),
        "pair_events": pair_failures,
        "negative_dividend_warnings": len(dividend_warnings),
    }
    with (OUT / "Run_Summary.json").open("w", encoding="utf-8") as file:
        json.dump(summary, file, ensure_ascii=False, indent=2, default=str)
        file.write("\n")
    return daily_path, eom_path


async def main() -> None:
    print(f"Indexes.py — version {VERSION}")
    headers = {
        "Accept": "application/json,text/html,text/plain,*/*",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://markets.ft.com/data/",
    }
    async with AsyncSession(headers=headers, max_clients=16) as session:
        results = await asyncio.gather(
            *[fetch_one(session, key, spec) for key, spec in SPECS.items()],
            return_exceptions=True,
        )
    histories: dict[str, pd.DataFrame] = {}
    status_rows: list[dict[str, Any]] = []
    run_utc = datetime.now(timezone.utc).isoformat()
    for key, result in zip(SPECS, results):
        error = ""
        if isinstance(result, Exception):
            error = str(result)
            previous = load_previous_history(key)
            if previous is not None and not previous.empty:
                frame = previous
                status = "fallback_previous"
                histories[key] = frame
                print(
                    f"{key}: téléchargement échoué; historique précédent "
                    f"réutilisé ({len(frame):,} lignes)."
                )
            else:
                frame = empty_history()
                status = "failed_empty"
                print(f"{key}: téléchargement échoué; fichier vide produit.")
        else:
            name, frame = result
            status = "downloaded"
            histories[name] = frame
            print(
                f"{name}: {len(frame):,} lignes, "
                f"{frame['Date'].min().date()} -> {frame['Date'].max().date()}"
            )
        status_rows.append(
            {
                "Run_UTC": run_utc,
                "Version": VERSION,
                "Series": key,
                "Status": status,
                "Rows": len(frame),
                "Start_Date": (
                    frame["Date"].min().date().isoformat()
                    if not frame.empty else ""
                ),
                "End_Date": (
                    frame["Date"].max().date().isoformat()
                    if not frame.empty else ""
                ),
                "Error": error,
            }
        )
    daily_path, eom_path = save(histories, status_rows)
    print(f"\nCSV quotidien consolidé : {daily_path.resolve()}")
    print(f"CSV EOM consolidé       : {eom_path.resolve()}")
    degraded = [row for row in status_rows if row["Status"] != "downloaded"]
    if degraded:
        print("\nExécution dégradée mais sorties produites :")
        for row in degraded:
            print(f"- {row['Series']}: {row['Status']} — {row['Error']}")


if __name__ == "__main__":
    asyncio.run(main())
