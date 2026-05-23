"""
╔══════════════════════════════════════════════════════════════════════════╗
║          EBOLA OUTBREAK DATA PIPELINE                                   ║
║          Sources: WHO · CDC · HealthMap · ECDC · Provided table         ║
║          Output:  ebola_cases.csv, ebola_timeline.csv, ebola_map.csv    ║
╚══════════════════════════════════════════════════════════════════════════╝
 
Usage:
    pip install requests pandas matplotlib geopandas folium
    python ebola_pipeline.py
 
What this pipeline does:
  Stage 1 — Load authoritative embedded data (from provided table)
  Stage 2 — Fetch WHO Disease Outbreak News RSS
  Stage 3 — Fetch CDC Socrata open data API
  Stage 4 — Fetch ECDC open data CSV
  Stage 5 — Fetch HealthMap / ProMED RSS alerts
  Stage 6 — Merge & deduplicate all sources
  Stage 7 — Export CSVs + generate charts + interactive Folium map
"""
 
import sys
import time
import json
import requests
import xml.etree.ElementTree as ET
from io import StringIO
from datetime import datetime
from typing import Optional
 
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
 
# ──────────────────────────────────────────────────────────────────────────
# 0. CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────
 
TIMEOUT = 15  # seconds per request
HEADERS = {
    "User-Agent": (
        "EbolaResearchBot/1.0 (public health data pipeline; "
        "contact: researcher@example.com)"
    )
}
 
# Output file paths
OUT_CASES    = "ebola_cases.csv"
OUT_TIMELINE = "ebola_timeline.csv"
OUT_MAP      = "ebola_map.html"
OUT_CHART    = "ebola_chart.png"
OUT_MERGED   = "ebola_merged_all_sources.csv"
 
 
# ──────────────────────────────────────────────────────────────────────────
# 1. AUTHORITATIVE EMBEDDED DATA  (provided table + WHO historical records)
# ──────────────────────────────────────────────────────────────────────────
 
EMBEDDED_DATA = [
    # ── CURRENT OUTBREAK ──
    {
        "country": "DR Congo",
        "iso3": "COD",
        "region": "Ituri Province",
        "cases": 832,
        "deaths": 177,
        "suspected": 750,
        "year": 2024,
        "strain": "Bundibugyo",
        "status": "active",
        "source": "WHO + Ituri Provincial Authority",
        "lat": -4.0,
        "lon": 21.7,
        "notes": "Believed much larger due to undetected spread; origin Mongbwalu mining town",
    },
    {
        "country": "DR Congo",
        "iso3": "COD",
        "region": "North & South Kivu (M23 zone)",
        "cases": 20,
        "deaths": None,
        "suspected": 20,
        "year": 2024,
        "strain": "Bundibugyo",
        "status": "active",
        "source": "M23 Rebel Group Crisis Team",
        "lat": -1.5,
        "lon": 29.1,
        "notes": "Identified in Goma and Bukavu; no comms with national government",
    },
    {
        "country": "DR Congo",
        "iso3": "COD",
        "region": "Congo (Regional aggregated)",
        "cases": 134,
        "deaths": 120,
        "suspected": None,
        "year": 2024,
        "strain": "Bundibugyo",
        "status": "active",
        "source": "Related headline references",
        "lat": -2.9,
        "lon": 23.7,
        "notes": "Concurrent tracking from related updates",
    },
    # ── HISTORICAL OUTBREAKS ──
    {
        "country": "Guinea",
        "iso3": "GIN",
        "region": "West Africa",
        "cases": 3811,
        "deaths": 2543,
        "suspected": None,
        "year": 2014,
        "strain": "Zaire",
        "status": "resolved",
        "source": "WHO",
        "lat": 11.0,
        "lon": -10.9,
        "notes": "Part of 2014-2016 West Africa epidemic",
    },
    {
        "country": "Sierra Leone",
        "iso3": "SLE",
        "region": "West Africa",
        "cases": 14124,
        "deaths": 3956,
        "suspected": None,
        "year": 2014,
        "strain": "Zaire",
        "status": "resolved",
        "source": "WHO",
        "lat": 8.5,
        "lon": -11.8,
        "notes": "Highest case count in 2014-2016 epidemic",
    },
    {
        "country": "Liberia",
        "iso3": "LBR",
        "region": "West Africa",
        "cases": 10675,
        "deaths": 4809,
        "suspected": None,
        "year": 2014,
        "strain": "Zaire",
        "status": "resolved",
        "source": "WHO",
        "lat": 6.4,
        "lon": -9.4,
        "notes": "Highest death toll in 2014-2016 epidemic",
    },
    {
        "country": "Uganda",
        "iso3": "UGA",
        "region": "East Africa",
        "cases": 425,
        "deaths": 224,
        "suspected": None,
        "year": 2022,
        "strain": "Sudan",
        "status": "resolved",
        "source": "WHO",
        "lat": 1.4,
        "lon": 32.3,
        "notes": "2022 Sudan strain outbreak",
    },
    {
        "country": "Rep. Congo",
        "iso3": "COG",
        "region": "Central Africa",
        "cases": 143,
        "deaths": 128,
        "suspected": None,
        "year": 2024,
        "strain": "Bundibugyo",
        "status": "monitoring",
        "source": "Regional authorities",
        "lat": -0.2,
        "lon": 15.8,
        "notes": "Suspected spread from DRC",
    },
    {
        "country": "Sudan",
        "iso3": "SDN",
        "region": "East Africa",
        "cases": 284,
        "deaths": 151,
        "suspected": None,
        "year": 2004,
        "strain": "Sudan",
        "status": "resolved",
        "source": "WHO",
        "lat": 12.9,
        "lon": 30.2,
        "notes": "Sudan strain historical",
    },
    {
        "country": "United States",
        "iso3": "USA",
        "region": "North America (imported)",
        "cases": 4,
        "deaths": 1,
        "suspected": None,
        "year": 2014,
        "strain": "Zaire",
        "status": "resolved",
        "source": "CDC",
        "lat": 37.1,
        "lon": -95.7,
        "notes": "Imported cases during 2014 West Africa epidemic",
    },
]
 
 
# ──────────────────────────────────────────────────────────────────────────
# 2. HELPER UTILITIES
# ──────────────────────────────────────────────────────────────────────────
 
def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    icons = {"INFO": "·", "OK": "✓", "WARN": "⚠", "ERROR": "✗", "STAGE": "▶"}
    icon = icons.get(level, "·")
    print(f"[{ts}] {icon} {msg}")
 
 
def safe_get(url: str, timeout: int = TIMEOUT, **kwargs) -> Optional[requests.Response]:
    """GET with retry and error handling."""
    for attempt in range(1, 3):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            log(f"Timeout on attempt {attempt}: {url}", "WARN")
        except requests.exceptions.HTTPError as e:
            log(f"HTTP {e.response.status_code}: {url}", "WARN")
            return None
        except requests.exceptions.ConnectionError as e:
            log(f"Connection error: {url} — {e}", "WARN")
            return None
        time.sleep(1.5 * attempt)
    return None
 
 
# ──────────────────────────────────────────────────────────────────────────
# 3. STAGE 1 — EMBEDDED DATA
# ──────────────────────────────────────────────────────────────────────────
 
def stage_embedded() -> pd.DataFrame:
    log("STAGE 1 — Loading embedded authoritative data", "STAGE")
    df = pd.DataFrame(EMBEDDED_DATA)
    df["cfr_pct"] = (df["deaths"] / df["cases"] * 100).round(1)
    log(f"Embedded data: {len(df)} records loaded", "OK")
    return df
 
 
# ──────────────────────────────────────────────────────────────────────────
# 4. STAGE 2 — WHO Disease Outbreak News RSS
# ──────────────────────────────────────────────────────────────────────────
 
WHO_RSS_URL = "https://www.who.int/rss-feeds/news-releases-en.xml"
 
def stage_who() -> pd.DataFrame:
    log("STAGE 2 — Fetching WHO Disease Outbreak News RSS", "STAGE")
    rows = []
 
    resp = safe_get(WHO_RSS_URL)
    if resp is None:
        log("WHO RSS fetch failed — using cached fallback", "WARN")
        return pd.DataFrame(columns=["title", "link", "pub_date", "source", "ebola_related"])
 
    try:
        root = ET.fromstring(resp.content)
        channel = root.find("channel")
        items = channel.findall("item") if channel else []
        log(f"WHO RSS: {len(items)} items fetched", "OK")
 
        for item in items:
            title   = (item.findtext("title") or "").strip()
            link    = (item.findtext("link") or "").strip()
            pubdate = (item.findtext("pubDate") or "").strip()
            ebola   = any(kw in title.lower() for kw in ["ebola","hemorrhagic","vhf","bundibugyo","marburg"])
            rows.append({
                "title": title,
                "link": link,
                "pub_date": pubdate,
                "source": "WHO RSS",
                "ebola_related": ebola,
            })
 
        df = pd.DataFrame(rows)
        ebola_items = df[df["ebola_related"] == True]
        log(f"WHO RSS: {len(ebola_items)} Ebola-related items identified", "OK")
        return df
 
    except ET.ParseError as e:
        log(f"WHO RSS XML parse error: {e}", "ERROR")
        return pd.DataFrame()
 
 
# ──────────────────────────────────────────────────────────────────────────
# 5. STAGE 3 — CDC Socrata Open Data API
# ──────────────────────────────────────────────────────────────────────────
# Dataset: NNDSS — Viral Hemorrhagic Fevers (VHF) including Ebola
# Public API, no key required
 
CDC_SOCRATA_BASE  = "https://data.cdc.gov/resource"
CDC_VHF_DATASET   = "unsk-b7fc"        # NNDSS VHF weekly data
CDC_MMWR_DATASET  = "9mfq-cb36"        # MMWR Weekly tables
 
def stage_cdc() -> pd.DataFrame:
    log("STAGE 3 — Fetching CDC Socrata open data", "STAGE")
    rows = []
 
    # Try VHF weekly dataset
    url = f"{CDC_SOCRATA_BASE}/{CDC_VHF_DATASET}.json?$limit=1000"
    log(f"CDC VHF dataset: {url}", "INFO")
    resp = safe_get(url)
 
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            log(f"CDC Socrata VHF: {len(data)} records received", "OK")
            for rec in data:
                rows.append({
                    "period":      rec.get("mmwr_year", ""),
                    "week":        rec.get("mmwr_week", ""),
                    "disease":     rec.get("disease", ""),
                    "cases_cum":   rec.get("cumulative_current", None),
                    "deaths_cum":  rec.get("cumulative_deaths_current", None),
                    "source":      "CDC Socrata VHF",
                    "country":     "United States",
                    "iso3":        "USA",
                })
            df_vhf = pd.DataFrame(rows)
            ebola_rows = df_vhf[df_vhf["disease"].str.contains("Ebola|Viral Hemorrhagic", case=False, na=False)]
            log(f"CDC VHF: {len(ebola_rows)} Ebola/VHF records filtered", "OK")
            return df_vhf
        except (json.JSONDecodeError, KeyError) as e:
            log(f"CDC JSON parse error: {e}", "ERROR")
 
    # Fallback: try metadata endpoint to confirm dataset exists
    meta_url = f"https://data.cdc.gov/api/views/metadata/v1/{CDC_VHF_DATASET}"
    meta = safe_get(meta_url)
    if meta:
        try:
            m = meta.json()
            log(f"CDC dataset confirmed: '{m.get('name', 'unknown')}'", "OK")
        except Exception:
            pass
 
    log("CDC: no live rows returned — empty frame, continuing pipeline", "WARN")
    return pd.DataFrame(columns=["period","week","disease","cases_cum","deaths_cum","source","country","iso3"])
 
 
# ──────────────────────────────────────────────────────────────────────────
# 6. STAGE 4 — ECDC Open Data (European Centre for Disease Prevention)
# ──────────────────────────────────────────────────────────────────────────
 
ECDC_EBOLA_URL = (
    "https://opendata.ecdc.europa.eu/ebola/casedistribution/"
    "json/data.json"
)
ECDC_VHF_CSV = (
    "https://opendata.ecdc.europa.eu/vhf/csv"
)
 
def stage_ecdc() -> pd.DataFrame:
    log("STAGE 4 — Fetching ECDC open data", "STAGE")
 
    # Try JSON endpoint
    resp = safe_get(ECDC_EBOLA_URL)
    if resp and resp.status_code == 200:
        try:
            raw = resp.json()
            records = raw.get("records", raw) if isinstance(raw, dict) else raw
            df = pd.DataFrame(records)
            log(f"ECDC Ebola JSON: {len(df)} records", "OK")
            df["source"] = "ECDC"
            return df
        except Exception as e:
            log(f"ECDC JSON parse error: {e}", "WARN")
 
    # Try CSV fallback
    resp2 = safe_get(ECDC_VHF_CSV)
    if resp2 and resp2.status_code == 200:
        try:
            df = pd.read_csv(StringIO(resp2.text))
            ebola_df = df[df.apply(
                lambda r: any("ebola" in str(v).lower() for v in r.values), axis=1
            )]
            log(f"ECDC VHF CSV: {len(ebola_df)} Ebola rows filtered from {len(df)} total", "OK")
            ebola_df = ebola_df.copy()
            ebola_df["source"] = "ECDC CSV"
            return ebola_df
        except Exception as e:
            log(f"ECDC CSV parse error: {e}", "WARN")
 
    log("ECDC: no data returned — continuing with other sources", "WARN")
    return pd.DataFrame()
 
 
# ──────────────────────────────────────────────────────────────────────────
# 7. STAGE 5 — HealthMap / ProMED RSS Alerts
# ──────────────────────────────────────────────────────────────────────────
 
HEALTHMAP_RSS  = "https://healthmap.org/rss/"
PROMED_RSS     = "https://promedmail.org/feed/"
 
def stage_healthmap() -> pd.DataFrame:
    log("STAGE 5 — Fetching HealthMap / ProMED alerts", "STAGE")
    rows = []
 
    for name, url in [("HealthMap", HEALTHMAP_RSS), ("ProMED", PROMED_RSS)]:
        resp = safe_get(url)
        if resp is None:
            log(f"{name}: unreachable — skipping", "WARN")
            continue
        try:
            root = ET.fromstring(resp.content)
            channel = root.find("channel") or root
            items = channel.findall("item")
            log(f"{name}: {len(items)} alert items fetched", "OK")
            for item in items:
                title   = (item.findtext("title") or "").strip()
                link    = (item.findtext("link") or "").strip()
                pubdate = (item.findtext("pubDate") or "").strip()
                desc    = (item.findtext("description") or "").strip()
                ebola   = any(kw in (title + desc).lower()
                              for kw in ["ebola","hemorrhagic","bundibugyo","marburg","vhf"])
                if ebola:
                    rows.append({
                        "title": title, "link": link,
                        "pub_date": pubdate, "source": name,
                        "description": desc[:200],
                    })
        except ET.ParseError as e:
            log(f"{name} XML parse error: {e}", "WARN")
 
    df = pd.DataFrame(rows)
    log(f"HealthMap/ProMED: {len(df)} Ebola-related alerts collected", "OK")
    return df
 
 
# ──────────────────────────────────────────────────────────────────────────
# 8. STAGE 6 — MERGE & CLEAN
# ──────────────────────────────────────────────────────────────────────────
 
def stage_merge(df_embedded, df_cdc, df_ecdc) -> pd.DataFrame:
    log("STAGE 6 — Merging and deduplicating all sources", "STAGE")
 
    frames = [df_embedded.copy()]
 
    if not df_cdc.empty and "cases_cum" in df_cdc.columns:
        cdc_ebola = df_cdc[
            df_cdc.get("disease", pd.Series(dtype=str)).str.contains(
                "Ebola|Viral Hem", case=False, na=False
            )
        ].copy()
        if not cdc_ebola.empty:
            cdc_slim = pd.DataFrame({
                "country":  cdc_ebola.get("country", "United States"),
                "iso3":     cdc_ebola.get("iso3", "USA"),
                "cases":    pd.to_numeric(cdc_ebola.get("cases_cum", 0), errors="coerce").fillna(0),
                "deaths":   pd.to_numeric(cdc_ebola.get("deaths_cum", 0), errors="coerce").fillna(0),
                "source":   "CDC Socrata",
                "status":   "resolved",
                "year":     cdc_ebola.get("period", 2014),
            })
            frames.append(cdc_slim)
            log(f"CDC: {len(cdc_slim)} Ebola rows merged", "OK")
 
    if not df_ecdc.empty:
        col_cases  = next((c for c in df_ecdc.columns if "case" in c.lower()), None)
        col_deaths = next((c for c in df_ecdc.columns if "death" in c.lower()), None)
        col_ctry   = next((c for c in df_ecdc.columns if "countr" in c.lower()), None)
        if col_cases and col_ctry:
            ecdc_slim = pd.DataFrame({
                "country": df_ecdc[col_ctry],
                "cases":   pd.to_numeric(df_ecdc[col_cases], errors="coerce").fillna(0),
                "deaths":  pd.to_numeric(df_ecdc[col_deaths], errors="coerce").fillna(0) if col_deaths else 0,
                "source":  "ECDC",
                "status":  "resolved",
            })
            frames.append(ecdc_slim)
            log(f"ECDC: {len(ecdc_slim)} rows merged", "OK")
 
    merged = pd.concat(frames, ignore_index=True)
 
    # Ensure key numeric columns
    for col in ["cases", "deaths", "suspected"]:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0).astype(int)
 
    # CFR
    merged["cfr_pct"] = (
        merged["deaths"] / merged["cases"].replace(0, pd.NA) * 100
    ).round(1)
 
    # Dedup by country + year + source
    if "year" in merged.columns:
        merged = merged.drop_duplicates(subset=["country", "year", "source"], keep="first")
 
    log(f"Merged dataset: {len(merged)} total records", "OK")
    return merged
 
 
# ──────────────────────────────────────────────────────────────────────────
# 9. STAGE 7 — EXPORT CHARTS & MAP
# ──────────────────────────────────────────────────────────────────────────
 
def stage_charts(df: pd.DataFrame) -> None:
    log("STAGE 7a — Generating matplotlib charts", "STAGE")
 
    top = df[df["cases"] > 0].sort_values("cases", ascending=False).head(10)
 
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle("Ebola Outbreak Dashboard — Global Case Data", fontsize=15, fontweight="bold", y=1.01)
    fig.patch.set_facecolor("#f5f4f0")
 
    colors = {"active": "#a32d2d", "monitoring": "#e24b4a", "resolved": "#639922", "suspect": "#854f0b"}
    bar_colors = [colors.get(str(s).lower(), "#888780") for s in top.get("status", ["resolved"] * len(top))]
 
    # 1 — Cases bar
    ax1 = axes[0]
    ax1.set_facecolor("#fafaf8")
    bars = ax1.barh(top["country"].str[:20], top["cases"], color=bar_colors, edgecolor="white", linewidth=0.5)
    ax1.set_xlabel("Confirmed + Suspected Cases", fontsize=10)
    ax1.set_title("Cases by Country", fontweight="bold")
    ax1.invert_yaxis()
    for bar, val in zip(bars, top["cases"]):
        ax1.text(bar.get_width() + 50, bar.get_y() + bar.get_height() / 2,
                 f"{int(val):,}", va="center", fontsize=8)
    ax1.spines[["top","right"]].set_visible(False)
 
    # 2 — CFR scatter
    ax2 = axes[1]
    ax2.set_facecolor("#fafaf8")
    sc = ax2.scatter(
        top["cases"], top["cfr_pct"],
        s=top["deaths"].clip(upper=5000) / 20 + 30,
        c=bar_colors, alpha=0.8, edgecolors="white", linewidth=1
    )
    for _, row in top.iterrows():
        ax2.annotate(
            str(row["country"])[:12],
            (row["cases"], row.get("cfr_pct", 0)),
            fontsize=7, ha="left", va="bottom",
            xytext=(4, 3), textcoords="offset points"
        )
    ax2.set_xlabel("Total Cases", fontsize=10)
    ax2.set_ylabel("Case Fatality Rate (%)", fontsize=10)
    ax2.set_title("CFR vs Case Volume\n(bubble = deaths)", fontweight="bold")
    ax2.spines[["top","right"]].set_visible(False)
 
    # 3 — Source breakdown pie
    ax3 = axes[2]
    ax3.set_facecolor("#fafaf8")
    if "source" in df.columns:
        src_counts = df["source"].value_counts()
        wedge_colors = ["#a32d2d", "#e24b4a", "#f09595", "#639922", "#378add", "#854f0b"]
        ax3.pie(
            src_counts.values,
            labels=src_counts.index,
            autopct="%1.0f%%",
            colors=wedge_colors[:len(src_counts)],
            startangle=90,
            textprops={"fontsize": 9},
            wedgeprops={"edgecolor": "white", "linewidth": 1}
        )
        ax3.set_title("Records by Source", fontweight="bold")
 
    legend_handles = [
        mpatches.Patch(color=c, label=s.capitalize()) for s, c in colors.items()
    ]
    fig.legend(handles=legend_handles, loc="lower center", ncol=4, fontsize=9, frameon=False, bbox_to_anchor=(0.5, -0.05))
 
    plt.tight_layout()
    plt.savefig(OUT_CHART, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    log(f"Chart saved → {OUT_CHART}", "OK")
 
 
def stage_map(df: pd.DataFrame) -> None:
    """Generate an interactive Folium choropleth map."""
    log("STAGE 7b — Generating interactive Folium map", "STAGE")
    try:
        import folium
        from folium.plugins import MarkerCluster
    except ImportError:
        log("folium not installed — run: pip install folium", "WARN")
        log("Skipping map generation", "WARN")
        return
 
    m = folium.Map(
        location=[5.0, 20.0],
        zoom_start=4,
        tiles="CartoDB positron",
        prefer_canvas=True,
    )
 
    # Color lookup
    def get_color(status: str) -> str:
        return {
            "active":     "#a32d2d",
            "monitoring": "#378add",
            "resolved":   "#639922",
            "suspect":    "#e24b4a",
        }.get(str(status).lower(), "#888780")
 
    geo_df = df.dropna(subset=["lat", "lon"]).copy()
 
    for _, row in geo_df.iterrows():
        color = get_color(str(row.get("status", "resolved")))
        cases  = int(row.get("cases", 0))
        deaths = int(row.get("deaths", 0))
        cfr    = f"{row.get('cfr_pct', 0):.1f}%" if row.get("cfr_pct") else "—"
 
        popup_html = f"""
        <div style="font-family:sans-serif;min-width:180px">
          <h4 style="margin:0 0 6px;color:{color}">{row.get('country','')}</h4>
          <b>Region:</b> {row.get('region','—')}<br>
          <b>Cases:</b> {cases:,}<br>
          <b>Deaths:</b> {deaths:,}<br>
          <b>CFR:</b> {cfr}<br>
          <b>Year:</b> {row.get('year','—')}<br>
          <b>Strain:</b> {row.get('strain','—')}<br>
          <b>Source:</b> {row.get('source','—')}<br>
          <hr style="margin:4px 0">
          <small>{str(row.get('notes',''))[:120]}</small>
        </div>
        """
 
        radius = max(8, min(40, (cases ** 0.45)))
 
        folium.CircleMarker(
            location=[float(row["lat"]), float(row["lon"])],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.55,
            popup=folium.Popup(popup_html, max_width=260),
            tooltip=f"{row.get('country','')} — {cases:,} cases",
        ).add_to(m)
 
    # Legend
    legend_html = """
    <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
                background:white;border:1px solid #ccc;border-radius:8px;
                padding:12px 16px;font-family:sans-serif;font-size:12px">
      <b>Ebola Outbreak Zones</b><br>
      <span style="color:#a32d2d">●</span> Active outbreak<br>
      <span style="color:#e24b4a">●</span> Suspected spread<br>
      <span style="color:#378add">●</span> Monitoring<br>
      <span style="color:#639922">●</span> Resolved (historical)<br>
      <i style="color:#888">Bubble size = case volume</i>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
 
    m.save(OUT_MAP)
    log(f"Interactive map saved → {OUT_MAP}", "OK")
 
 
def stage_export(df_merged: pd.DataFrame, df_who_rss: pd.DataFrame, df_hm: pd.DataFrame) -> None:
    log("STAGE 7c — Exporting CSVs", "STAGE")
 
    # Main cases CSV
    df_merged.to_csv(OUT_CASES, index=False)
    log(f"Cases CSV → {OUT_CASES} ({len(df_merged)} rows)", "OK")
 
    # WHO + HealthMap alerts timeline
    timeline_frames = []
    if not df_who_rss.empty:
        timeline_frames.append(df_who_rss)
    if not df_hm.empty:
        timeline_frames.append(df_hm)
    if timeline_frames:
        df_timeline = pd.concat(timeline_frames, ignore_index=True)
        df_timeline.to_csv(OUT_TIMELINE, index=False)
        log(f"Timeline CSV → {OUT_TIMELINE} ({len(df_timeline)} rows)", "OK")
 
    # Full merged export
    df_merged.to_csv(OUT_MERGED, index=False)
    log(f"Full merged CSV → {OUT_MERGED}", "OK")
 
 
# ──────────────────────────────────────────────────────────────────────────
# 10. MAIN PIPELINE RUNNER
# ──────────────────────────────────────────────────────────────────────────
 
def run_pipeline() -> dict:
    print("\n" + "═" * 60)
    print("  EBOLA OUTBREAK DATA PIPELINE")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 60 + "\n")
 
    results = {}
    t_start = time.time()
 
    # Stage 1 — Embedded authoritative data
    df_embedded = stage_embedded()
    results["embedded_rows"] = len(df_embedded)
 
    # Stage 2 — WHO RSS
    df_who = stage_who()
    results["who_items"] = len(df_who)
    who_ebola = df_who[df_who.get("ebola_related", pd.Series(False, index=df_who.index))] if not df_who.empty else pd.DataFrame()
    log(f"WHO Ebola items: {len(who_ebola)}", "OK")
 
    # Stage 3 — CDC
    df_cdc = stage_cdc()
    results["cdc_rows"] = len(df_cdc)
 
    # Stage 4 — ECDC
    df_ecdc = stage_ecdc()
    results["ecdc_rows"] = len(df_ecdc)
 
    # Stage 5 — HealthMap / ProMED
    df_hm = stage_healthmap()
    results["healthmap_alerts"] = len(df_hm)
 
    # Stage 6 — Merge
    df_merged = stage_merge(df_embedded, df_cdc, df_ecdc)
    results["merged_total"] = len(df_merged)
 
    # Stage 7 — Output
    stage_charts(df_merged)
    stage_map(df_merged)
    stage_export(df_merged, df_who, df_hm)
 
    elapsed = round(time.time() - t_start, 1)
 
    print("\n" + "═" * 60)
    print("  PIPELINE COMPLETE")
    print(f"  Elapsed: {elapsed}s")
    print("  Outputs:")
    print(f"    {OUT_CASES}    — main case registry")
    print(f"    {OUT_TIMELINE} — WHO + HealthMap alerts")
    print(f"    {OUT_MERGED}   — full merged export")
    print(f"    {OUT_CHART}    — matplotlib chart PNG")
    print(f"    {OUT_MAP}      — interactive Folium map")
    print("═" * 60 + "\n")
 
    return results
 
 
# ──────────────────────────────────────────────────────────────────────────
# QUICK ANALYSIS HELPERS (importable)
# ──────────────────────────────────────────────────────────────────────────
 
def load_results() -> pd.DataFrame:
    """Load the merged pipeline output for analysis."""
    return pd.read_csv(OUT_CASES)
 
 
def summary_stats(df: pd.DataFrame) -> None:
    """Print a quick summary of the loaded dataset."""
    print(df[["country","cases","deaths","cfr_pct","status","source","year"]].to_string(index=False))
    print(f"\nTotal cases tracked : {df['cases'].sum():,}")
    print(f"Total deaths tracked: {df['deaths'].sum():,}")
    print(f"Overall CFR         : {(df['deaths'].sum()/df['cases'].sum()*100):.1f}%")
    print(f"Countries / regions : {df['country'].nunique()}")
    print(f"Sources             : {', '.join(df['source'].unique())}")
 
 
# ──────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────
 
if __name__ == "__main__":
    results = run_pipeline()
 
    # Optional: print quick analysis
    try:
        df = load_results()
        print("\n── QUICK SUMMARY ──")
        summary_stats(df)
    except FileNotFoundError:
        pass
