"""
County ZIP Code Mapper
======================
For a list of counties, generates:
  - An interactive HTML map of ZIP code areas
  - A CSV with ZIP, city, county, and notes

Rules:
  - A ZIP is included if any part of it overlaps a target county
    (per Census ZCTA-to-County relationship file).
  - City = USPS/postal city name (from pgeocode/GeoNames).
  - County in CSV = county where that postal city is located
    (may differ from the target county if the ZIP straddles a boundary).
  - If the postal county is not in the target list, a note is added:
    "ZIP overlaps <target county>"
  - If the ZIP has no Census shapefile polygon (PO Box / unique ZIP),
    it is included in the CSV only with note "PO Box only".
  - Map polygons are colored by the target county with the largest
    geographic overlap, so the map always reflects your target region.

Dependencies:
    pip install requests folium geopandas shapely pandas pgeocode

Data sources (all free, no API key required):
  - Census TIGER/Line ZCTA shapefile (polygons)
  - Census ZCTA-to-County relationship file (overlap + area)
  - pgeocode / GeoNames (postal city and county names)
  - Census Geocoder API (county name -> FIPS)
"""

import sys
import time
import zipfile
from io import BytesIO, StringIO
from pathlib import Path

import folium
import geopandas as gpd
import pandas as pd
import requests

# ─────────────────────────────────────────────
#  ★  CONFIGURE YOUR COUNTIES HERE  ★
#  Format: list of (county_name, state_abbr) tuples.
#  Omit "County" from the name.
# ─────────────────────────────────────────────
TARGET_COUNTIES = [
    ("Scott",       "IA"),
    ("Rock Island", "IL"),
    ("Muscatine",   "IA"),
    ("Clinton","IA"),
    ("Mercer","IL"),
    ("Henry","IL"),
]

OUTPUT_MAP_HTML = "QC_zip_code_map.html"
OUTPUT_ZIP_CSV  = "QC_zip_codes_by_county.csv"

CACHE_DIR = Path(".zip_mapper_cache")
CACHE_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────
#  State abbreviation -> 2-digit FIPS
# ─────────────────────────────────────────────
STATE_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09",
    "DE":"10","FL":"12","GA":"13","HI":"15","ID":"16","IL":"17","IN":"18",
    "IA":"19","KS":"20","KY":"21","LA":"22","ME":"23","MD":"24","MA":"25",
    "MI":"26","MN":"27","MS":"28","MO":"29","MT":"30","NE":"31","NV":"32",
    "NH":"33","NJ":"34","NM":"35","NY":"36","NC":"37","ND":"38","OH":"39",
    "OK":"40","OR":"41","PA":"42","RI":"44","SC":"45","SD":"46","TN":"47",
    "TX":"48","UT":"49","VT":"50","VA":"51","WA":"53","WV":"54","WI":"55",
    "WY":"56","DC":"11","PR":"72",
}

# ─────────────────────────────────────────────
#  Step 1 — Resolve county names -> FIPS codes
# ─────────────────────────────────────────────

def get_county_fips(county_name: str, state_abbr: str) -> str | None:
    """Return 5-digit FIPS for a county via the Census API."""
    state_fips = STATE_FIPS.get(state_abbr.upper())
    if not state_fips:
        print(f"  ✗ Unknown state: {state_abbr}")
        return None

    # Primary: geocoder API
    try:
        r = requests.get(
            "https://geocoding.geo.census.gov/geocoder/geographies/address",
            params={"street": "1 Main St", "city": county_name, "state": state_abbr,
                    "benchmark": "Public_AR_Current", "vintage": "Current_Current",
                    "layers": "Counties", "format": "json"},
            timeout=15,
        )
        matches = (r.json().get("result", {})
                           .get("addressMatches", [{}])[0]
                           .get("geographies", {})
                           .get("Counties", []))
        if matches:
            return matches[0]["GEOID"]
    except Exception:
        pass

    # Fallback: Census county list
    try:
        r = requests.get(
            f"https://api.census.gov/data/2020/dec/pl"
            f"?get=NAME&for=county:*&in=state:{state_fips}",
            timeout=15,
        )
        for row in r.json()[1:]:
            if county_name.lower() in row[0].lower():
                return state_fips + row[-1]
    except Exception as e:
        print(f"  ✗ FIPS lookup failed: {e}")

    return None


# ─────────────────────────────────────────────
#  Step 2 — Census ZCTA-to-County crosswalk
#  Gives us every ZIP that overlaps each county,
#  plus the land area of that overlap for tiebreaking.
# ─────────────────────────────────────────────

def fetch_crosswalk() -> pd.DataFrame:
    cache = CACHE_DIR / "zip_county_crosswalk.csv"
    if cache.exists():
        df = pd.read_csv(cache, dtype=str)
        if "area_land" in df.columns:
            print("  ✔ Using cached crosswalk.")
            return df
        cache.unlink()  # stale — missing area_land

    print("  Downloading ZIP→County crosswalk from Census …")
    url = ("https://www2.census.gov/geo/docs/maps-data/data/rel2020/"
           "zcta520/tab20_zcta520_county20_natl.txt")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text), sep="|", dtype=str,
                     usecols=["GEOID_ZCTA5_20", "GEOID_COUNTY_20", "AREALAND_PART"])
    df.columns = ["zip", "county_fips", "area_land"]
    df.to_csv(cache, index=False)
    print(f"  ✔ Downloaded {len(df):,} rows.")
    return df


# ─────────────────────────────────────────────
#  Step 3 — pgeocode postal lookup
#  For each ZIP returns: city, county_name, state_code
# ─────────────────────────────────────────────

def postal_lookup(zip_codes: list[str]) -> pd.DataFrame:
    try:
        import pgeocode
    except ImportError:
        print("  ✗ pgeocode not installed. Run: pip install pgeocode")
        sys.exit(1)

    print(f"  Looking up {len(zip_codes)} ZIP codes in postal database …")
    nomi = pgeocode.Nominatim("us")
    result = nomi.query_postal_code(zip_codes)

    return pd.DataFrame({
        "zip":         zip_codes,
        "city":        result["place_name"].fillna("").values,
        "county_name": result["county_name"].fillna("").values,
        "state_code":  result["state_code"].fillna("").values,
    })


# ─────────────────────────────────────────────
#  Step 4 — ZCTA shapefile
# ─────────────────────────────────────────────

def fetch_shapefile() -> gpd.GeoDataFrame:
    cache_shp = CACHE_DIR / "zcta" / "tl_2023_us_zcta520.shp"
    if cache_shp.exists():
        print("  ✔ Using cached ZCTA shapefile.")
        return gpd.read_file(cache_shp)

    print("  Downloading ZCTA shapefile (~170 MB) …")
    url = ("https://www2.census.gov/geo/tiger/TIGER2023/ZCTA520/"
           "tl_2023_us_zcta520.zip")
    r = requests.get(url, timeout=300, stream=True)
    r.raise_for_status()
    total = int(r.headers.get("content-length", 0))
    buf = BytesIO()
    done = 0
    for chunk in r.iter_content(1 << 20):
        buf.write(chunk)
        done += len(chunk)
        if total:
            print(f"    {done/total*100:.0f}%", end="\r", flush=True)
    print()
    out = CACHE_DIR / "zcta"
    out.mkdir(exist_ok=True)
    with zipfile.ZipFile(buf) as z:
        z.extractall(out)
    print("  ✔ Extracted.")
    return gpd.read_file(cache_shp)


# ─────────────────────────────────────────────
#  Step 5 — Build the ZIP table
# ─────────────────────────────────────────────

def build_zip_table(target_counties: list[tuple[str, str]]) -> pd.DataFrame:
    """
    Returns a DataFrame with columns:
      zip, city, postal_county, postal_state, target_county, note

    target_county = the target county with the largest area overlap
                    (used for map coloring)
    postal_county = county where the postal city is located
                    (used for CSV display)
    note          = explanation if postal county != target county,
                    or 'PO Box only' if no shapefile polygon exists
    """

    # --- Resolve target county FIPS ---
    print("\n[1/4] Resolving county FIPS codes …")
    # fips -> "Name County, ST"
    fips_to_label: dict[str, str] = {}
    # (county_name_lower, state_upper) -> fips  (for postal matching)
    name_state_to_fips: dict[tuple, str] = {}

    for cname, state in target_counties:
        label = f"{cname} County, {state.upper()}"
        print(f"  {label} …", end=" ")
        fips = get_county_fips(cname, state)
        if fips:
            fips_to_label[fips] = label
            name_state_to_fips[(cname.lower(), state.upper())] = fips
            print(f"FIPS {fips}")
        else:
            print("NOT FOUND")
        time.sleep(0.3)

    if not fips_to_label:
        print("No counties resolved. Check TARGET_COUNTIES.")
        sys.exit(1)

    target_fips = set(fips_to_label.keys())

    # --- Crosswalk: find all ZIPs touching target counties ---
    print("\n[2/4] Loading crosswalk …")
    xwalk = fetch_crosswalk()
    xwalk["area_land"] = pd.to_numeric(xwalk["area_land"], errors="coerce").fillna(0)

    # For each ZIP that touches any target county, find the target county
    # with the LARGEST area overlap -> this is the map color / primary association
    target_rows = xwalk[xwalk["county_fips"].isin(target_fips)].copy()
    best = (target_rows.sort_values("area_land", ascending=False)
                       .drop_duplicates(subset="zip", keep="first")
                       [["zip", "county_fips"]])
    best["target_county"] = best["county_fips"].map(fips_to_label)
    candidate_zips = best["zip"].dropna().unique().tolist()
    print(f"  {len(candidate_zips)} ZIPs overlap the target counties.")

    # --- Postal lookup (full pgeocode database for target states) ---
    # PO Box ZIPs have zero land area and no crosswalk entry at all.
    # The only way to find them is to query pgeocode's full database
    # and filter by state + county name. We do NOT rely on the crosswalk
    # for this step — we query pgeocode for every ZIP it knows about in
    # the target states by reading its cached SQLite database directly.
    target_state_abbrs = {label.split(",")[1].strip().upper()
                          for label in fips_to_label.values()}
    print(f"\n[3/4] Loading full postal database for states: {target_state_abbrs} …")
    import pgeocode as _pgeocode
    nomi = _pgeocode.Nominatim("us")
    # Access the underlying dataframe that pgeocode caches locally
    all_postal_df = nomi._data.copy()
    all_postal_df = all_postal_df.rename(columns={
        "postal_code": "zip",
        "place_name":  "city",
        "county_name": "county_name",
        "state_code":  "state_code",
    })
    # Keep only ZIPs in target states
    state_postal = all_postal_df[
        all_postal_df["state_code"].str.upper().isin(target_state_abbrs)
    ].copy()
    state_postal["zip"] = state_postal["zip"].astype(str).str.zfill(5)
    # Union with crosswalk candidates (other states if multi-state target)
    all_zip_set = set(str(z).zfill(5) for z in candidate_zips) | set(state_postal["zip"])
    # Build postal lookup from pgeocode data directly (no extra API call)
    postal = state_postal[state_postal["zip"].isin(all_zip_set)][
        ["zip", "city", "county_name", "state_code"]
    ].drop_duplicates("zip").copy()
    # For ZIPs in candidate_zips from other states, fall back to API lookup
    other_zips = [z for z in candidate_zips
                  if str(z).zfill(5) not in set(state_postal["zip"])]
    if other_zips:
        print(f"  Looking up {len(other_zips)} ZIPs from other states …")
        extra = postal_lookup(other_zips)
        postal = pd.concat([postal, extra], ignore_index=True).drop_duplicates("zip")
    postal["city"] = postal["city"].fillna("").where(postal["city"].fillna("") != "",
                                                      other="Unknown")
    postal["county_name"] = postal["county_name"].fillna("")
    postal["state_code"]  = postal["state_code"].fillna("")
    print(f"  {len(postal)} ZIPs loaded from postal database.")

    # Build "County, ST" label from postal county_name + state_code
    def make_postal_county(row) -> str:
        c = str(row["county_name"]).strip()
        s = str(row["state_code"]).strip().upper()
        if c and s:
            return f"{c} County, {s}"
        return ""

    postal["postal_county"] = postal.apply(make_postal_county, axis=1)
    postal["city"] = postal["city"].where(postal["city"] != "", other="Unknown")

    # Find PO Box ZIPs: not in geo candidates, but whose postal county
    # matches a target county by name. Assign them target_county directly.
    target_labels = set(fips_to_label.values())
    po_box_rows = postal[
        (~postal["zip"].isin(candidate_zips)) &
        (postal["postal_county"].isin(target_labels))
    ].copy()
    po_box_rows["target_county"] = po_box_rows["postal_county"]

    # Extend best with PO Box entries (no area overlap row needed)
    if not po_box_rows.empty:
        print(f"  Found {len(po_box_rows)} PO Box ZIP(s) via postal county match.")
        po_best = po_box_rows[["zip", "target_county"]].drop_duplicates("zip")
        best = pd.concat([best[["zip", "target_county"]], po_best], ignore_index=True)
        best = best.drop_duplicates("zip", keep="first")

    # --- Merge everything ---
    df = best[["zip", "target_county"]].merge(postal, on="zip", how="left")

    # Build note:
    # - If postal county IS a target county -> no note needed
    # - If postal county is NOT a target county -> explain inclusion
    # - PO Box / no polygon -> added later after shapefile check
    def make_note(row) -> str:
        pc = row["postal_county"]
        tc = row["target_county"]
        # Is the postal county one of our targets?
        if pc in fips_to_label.values():
            return ""
        # Postal county is outside our list
        if pc:
            return f"ZIP overlaps {tc}"
        # pgeocode returned nothing (PO Box or unknown)
        return f"ZIP overlaps {tc}"

    df["note"] = df.apply(make_note, axis=1)

    # Clean up columns
    df = df[["zip", "city", "postal_county", "postal_state",
             "target_county", "note"]].copy() if "postal_state" in df.columns else \
         df.assign(postal_state=df["state_code"] if "state_code" in df.columns else "")[
             ["zip", "city", "postal_county", "target_county", "note"]]

    # Ensure we have the state col
    df = df.merge(postal[["zip", "state_code"]], on="zip", how="left")

    return df, candidate_zips


# ─────────────────────────────────────────────
#  Step 6 — Build GeoDataFrame + flag PO Box ZIPs
# ─────────────────────────────────────────────

def build_geo(df: pd.DataFrame) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Load shapefile, merge with ZIP table.
    Returns (geo_gdf, updated_df) where updated_df has PO Box notes added.
    """
    print("\n[4/4] Loading ZCTA shapefile …")
    shp = fetch_shapefile()
    zcta_col = "ZCTA5CE20" if "ZCTA5CE20" in shp.columns else shp.columns[0]
    shp = shp.rename(columns={zcta_col: "zip"})
    shp = shp.to_crs(epsg=4326)

    target_zips = set(df["zip"].unique())
    geo = shp[shp["zip"].isin(target_zips)].copy()
    geo = geo.merge(df[["zip", "city", "postal_county", "target_county", "note"]],
                    on="zip", how="left")

    # Flag ZIPs with no polygon
    mapped = set(geo["zip"].unique())
    po_mask = ~df["zip"].isin(mapped)
    df.loc[po_mask, "note"] = "PO Box only"

    n_po = po_mask.sum()
    if n_po:
        print(f"  ℹ {n_po} PO Box-only ZIP(s) included in CSV but not map:")
        for _, row in df[po_mask].iterrows():
            print(f"    {row['zip']} ({row['city']}, {row['postal_county']})")

    return geo, df


# ─────────────────────────────────────────────
#  Step 7 — Build Folium map
# ─────────────────────────────────────────────

COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
]


def build_map(geo: gpd.GeoDataFrame) -> folium.Map:
    if geo.empty:
        raise ValueError("No geometries — nothing to map.")

    centroid = geo.geometry.unary_union.centroid
    m = folium.Map(location=[centroid.y, centroid.x],
                   zoom_start=9, tiles="CartoDB positron")

    counties = sorted(geo["target_county"].dropna().unique())
    color_map = {c: COLORS[i % len(COLORS)] for i, c in enumerate(counties)}

    for county, color in color_map.items():
        layer = geo[geo["target_county"] == county]
        folium.GeoJson(
            layer.__geo_interface__,
            name=county,
            style_function=lambda f, c=color: {
                "fillColor": c, "color": "white",
                "weight": 1.2, "fillOpacity": 0.55,
            },
            highlight_function=lambda f: {"weight": 2.5, "fillOpacity": 0.8},
            tooltip=folium.GeoJsonTooltip(
                fields=["zip", "city", "postal_county", "note"],
                aliases=["ZIP:", "City:", "County:", "Note:"],
            ),
        ).add_to(m)

    # Legend
    legend = ('<div style="position:fixed;bottom:30px;left:30px;z-index:1000;'
              'background:white;padding:12px 18px;border-radius:8px;'
              'box-shadow:0 2px 8px rgba(0,0,0,.25);font-family:sans-serif;'
              'font-size:13px;line-height:1.8"><b>Target Counties</b><br>')
    for county, color in sorted(color_map.items()):
        legend += (f'<span style="display:inline-block;width:14px;height:14px;'
                   f'background:{color};margin-right:6px;border-radius:3px;'
                   f'vertical-align:middle"></span>{county}<br>')
    legend += "</div>"
    m.get_root().html.add_child(folium.Element(legend))
    folium.LayerControl().add_to(m)
    return m


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  County ZIP Code Mapper")
    print("=" * 55)
    print(f"\nTarget counties: {TARGET_COUNTIES}\n")

    df, candidate_zips = build_zip_table(TARGET_COUNTIES)
    geo, df = build_geo(df)

    # CSV: zip | city | county | note
    # "county" = postal county (where the city is), or target county if unknown
    def csv_county(row):
        return row["postal_county"] if row["postal_county"] else row["target_county"]

    csv = df.copy()
    csv["county"] = csv.apply(csv_county, axis=1)
    csv = (csv[["county", "zip", "city", "note"]]
           .sort_values(["county", "zip"])
           .reset_index(drop=True))
    csv.to_csv(OUTPUT_ZIP_CSV, index=False)
    print(f"\n✔ CSV saved  → {OUTPUT_ZIP_CSV}  ({len(csv)} ZIPs)")

    # Map
    print("  Building map …")
    m = build_map(geo)
    m.save(OUTPUT_MAP_HTML)
    print(f"✔ Map saved  → {OUTPUT_MAP_HTML}")

    # Console summary
    print("\n── Summary " + "─" * 44)
    for county in sorted(csv["county"].unique()):
        rows = csv[csv["county"] == county].sort_values("zip")
        print(f"\n  {county} ({len(rows)} ZIPs):")
        print(f"  {'ZIP':<8} {'City':<26} {'Note'}")
        print(f"  {'─'*7} {'─'*26} {'─'*30}")
        for _, row in rows.iterrows():
            print(f"  {row['zip']:<8} {row['city']:<26} {row['note']}")
    print("\n" + "=" * 55)


if __name__ == "__main__":
    main()