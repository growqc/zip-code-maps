"""
County ZIP Code Mapper
======================
Fetches all ZIP codes for a specified set of counties and generates
an interactive choropleth map of those ZIP code areas.

Dependencies:
    pip install requests folium geopandas shapely pandas pgeocode

Data Sources (all free, no API key required):
  - US Census Bureau TIGER/Line ZIP Code Tabulation Areas (ZCTA) shapefiles
  - Census ZCTA→County relationship file  (to discover which ZIPs touch each county)
  - pgeocode library (GeoNames/USPS postal database) for city names + postal county
  - Census Geocoder API (county FIPS lookup)
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
#  County name should match the official Census name (omit "County").
# ─────────────────────────────────────────────
TARGET_COUNTIES = [
    ("Scott",       "IA"),
    ("Rock Island", "IL"),
    ("Muscatine",   "IA"),
    ("Clinton", "IA"),
    ("Mercer", "IL"),
    ("Henry", "IL"),
]



# Output files
OUTPUT_MAP_HTML = "zip_code_map.html"
OUTPUT_ZIP_CSV  = "zip_codes_by_county.csv"

# Cache directory (avoids re-downloading large files)
CACHE_DIR = Path(".zip_mapper_cache")
CACHE_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────
#  Step 1 — Resolve county names → FIPS codes
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


def get_county_fips(county_name: str, state_abbr: str) -> str | None:
    """Return 5-digit FIPS for a county via the Census Geocoder API."""
    state_fips = STATE_FIPS.get(state_abbr.upper())
    if not state_fips:
        print(f"  ✗ Unknown state abbreviation: {state_abbr}")
        return None

    url = "https://geocoding.geo.census.gov/geocoder/geographies/address"
    params = {
        "street": "1 Main St",
        "city": county_name,
        "state": state_abbr,
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "layers": "Counties",
        "format": "json",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        counties = (data.get("result", {})
                        .get("addressMatches", [{}])[0]
                        .get("geographies", {})
                        .get("Counties", []))
        if counties:
            return counties[0]["GEOID"]
    except Exception:
        pass

    # Fallback: search Census county list directly
    list_url = (
        f"https://api.census.gov/data/2020/dec/pl"
        f"?get=NAME&for=county:*&in=state:{state_fips}"
    )
    try:
        r = requests.get(list_url, timeout=15)
        rows = r.json()[1:]
        for row in rows:
            if county_name.lower() in row[0].lower():
                return state_fips + row[-1]
    except Exception as e:
        print(f"  ✗ FIPS lookup failed for {county_name}, {state_abbr}: {e}")

    return None


# ─────────────────────────────────────────────
#  Step 2 — ZIP → County crosswalk
#  (used only to discover candidate ZIPs that touch each county;
#   final county assignment comes from the postal city in Step 3)
# ─────────────────────────────────────────────

def fetch_zip_county_crosswalk() -> pd.DataFrame:
    """Census ZCTA-to-County relationship file (2020)."""
    cache_file = CACHE_DIR / "zip_county_crosswalk.csv"
    if cache_file.exists():
        cached = pd.read_csv(cache_file, dtype=str)
        if "area_land" in cached.columns:
            print("  ✔ Using cached ZIP→County crosswalk.")
            return cached
        else:
            print("  ℹ Cached crosswalk missing area_land — re-downloading …")
            cache_file.unlink()

    print("  Downloading ZIP→County crosswalk from Census …")
    url = (
        "https://www2.census.gov/geo/docs/maps-data/data/rel2020/"
        "zcta520/tab20_zcta520_county20_natl.txt"
    )
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        df = pd.read_csv(StringIO(r.text), sep="|", dtype=str,
                         usecols=["GEOID_ZCTA5_20", "GEOID_COUNTY_20",
                                  "AREALAND_PART"])
        df.columns = ["zip", "county_fips", "area_land"]
        df.to_csv(cache_file, index=False)
        print(f"  ✔ Downloaded {len(df):,} ZIP→County relationships.")
        return df
    except Exception as e:
        print(f"  ✗ Crosswalk download failed: {e}")
        return pd.DataFrame(columns=["zip", "county_fips"])


# ─────────────────────────────────────────────
#  Step 3 — ZIP → Postal city + county (pgeocode / GeoNames / USPS)
# ─────────────────────────────────────────────

def fetch_zip_postal_info(zip_codes: list[str]) -> pd.DataFrame:
    """
    Return the USPS postal city name AND the 5-digit county FIPS of that
    city for each ZIP, using pgeocode (GeoNames/USPS postal database).

    pgeocode's 'county_code' field is the FIPS of the county where the
    post-office city sits — so county assignment follows the postal city,
    not a geographic area overlap.  A border ZIP like 52726 will be placed
    in whichever county its post-office city belongs to.

    Install: pip install pgeocode
    Downloads ~5 MB on first use; fully cached after that.
    """
    try:
        import pgeocode
    except ImportError:
        print("  ✗ 'pgeocode' not installed. Run: pip install pgeocode")
        return pd.DataFrame({
            "zip": zip_codes,
            "city": ["Unknown"] * len(zip_codes),
            "postal_county_fips": [""] * len(zip_codes),
        })

    print(f"  Looking up postal city + county for {len(zip_codes)} ZIP codes …")
    nomi = pgeocode.Nominatim("us")
    result = nomi.query_postal_code(zip_codes)   # single vectorised batch call

    # Diagnostic: print all columns and a sample row to see actual field names
    print(f"  pgeocode columns: {list(result.columns)}")
    sample_row = result.iloc[0].to_dict()
    print("  pgeocode sample row:", sample_row)

    # pgeocode state_code is a 2-letter abbreviation (e.g. 'IA'), not a number.
    # county_code is a bare 3-digit number (e.g. 139.0 for Muscatine).
    # Combine using STATE_FIPS lookup to build the full 5-digit Census FIPS.
    def build_fips(state_abbr, county_val) -> str:
        if state_abbr != state_abbr or county_val != county_val:  # NaN check
            return ""
        try:
            state_part  = STATE_FIPS.get(str(state_abbr).strip().upper(), "")
            if not state_part:
                return ""
            county_part = str(int(float(county_val))).zfill(3)
            return state_part + county_part
        except (ValueError, TypeError):
            return ""

    fips_list = [
        build_fips(s, c)
        for s, c in zip(result["state_code"].values, result["county_code"].values)
    ]

    df = pd.DataFrame({
        "zip":                zip_codes,
        "city":               result["place_name"].fillna("Unknown").values,
        "postal_county_fips": fips_list,
        "postal_county_name": result["county_name"].fillna("").values,
        "postal_state_code":  result["state_code"].fillna("").values,
    })
    print("  Sample built FIPS:", [x for x in fips_list if x][:5])
    print("  ✔ Postal city/county lookup complete.")
    return df


# ─────────────────────────────────────────────
#  Step 4 — Download ZCTA shapefile
# ─────────────────────────────────────────────

def fetch_zcta_shapefile() -> gpd.GeoDataFrame:
    """National ZCTA shapefile from Census TIGER/Line (~170 MB, cached)."""
    cache_shp = CACHE_DIR / "zcta" / "tl_2023_us_zcta520.shp"
    if cache_shp.exists():
        print("  ✔ Using cached ZCTA shapefile.")
        return gpd.read_file(cache_shp)

    print("  Downloading ZCTA shapefile from Census TIGER/Line (~170 MB) …")
    url = (
        "https://www2.census.gov/geo/tiger/TIGER2023/ZCTA520/"
        "tl_2023_us_zcta520.zip"
    )
    try:
        r = requests.get(url, timeout=300, stream=True)
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        buf = BytesIO()
        for chunk in r.iter_content(chunk_size=1 << 20):
            buf.write(chunk)
            downloaded += len(chunk)
            if total:
                print(f"    {downloaded/total*100:.0f}%", end="\r", flush=True)
        print()

        out_dir = CACHE_DIR / "zcta"
        out_dir.mkdir(exist_ok=True)
        with zipfile.ZipFile(buf) as z:
            z.extractall(out_dir)

        print("  ✔ ZCTA shapefile extracted.")
        return gpd.read_file(cache_shp)
    except Exception as e:
        print(f"  ✗ ZCTA shapefile download failed: {e}")
        sys.exit(1)


# ─────────────────────────────────────────────
#  Step 5 — Build ZIP GeoDataFrame
# ─────────────────────────────────────────────

def build_zip_geodataframe(
    counties: list[tuple[str, str]]
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """
    County assignment logic:
      1. Use the Census crosswalk to find all ZIPs that touch any target county.
      2. Look up each candidate ZIP's postal city + that city's county FIPS
         via pgeocode (USPS / GeoNames).
      3. Keep only ZIPs whose postal-city county is one of the target counties.
         This means a border ZIP is placed in whatever county its post office
         city belongs to — not split or duplicated.
      4. ZIPs whose postal city falls outside all target counties are dropped
         (they only graze a target county geographically; they don't belong to it
         postally).

    Returns:
      - GeoDataFrame of ZCTA polygons (zip, city, county_label)
      - Flat DataFrame of the same rows (for CSV export)
    """
    print("\n[1/5] Resolving county FIPS codes …")
    county_fips_map: dict[str, str] = {}   # fips → "County, ST" label
    for county_name, state_abbr in counties:
        label = f"{county_name} County, {state_abbr.upper()}"
        print(f"  Looking up {label} …")
        fips = get_county_fips(county_name, state_abbr)
        if fips:
            county_fips_map[fips] = label
            print(f"  ✔ {label} → FIPS {fips}")
        else:
            print(f"  ✗ Could not find FIPS for {label}")
        time.sleep(0.3)

    if not county_fips_map:
        print("No valid counties found. Check your TARGET_COUNTIES list.")
        sys.exit(1)

    print("\n[2/5] Fetching ZIP→County crosswalk …")
    crosswalk = fetch_zip_county_crosswalk()
    target_fips = set(county_fips_map.keys())

    # TWO candidate sources so no ZIP is missed:
    # A) geographic: ZIPs whose area touches a target county
    # B) postal: all ZIPs in the target states (catches ZIPs whose
    #    postal city is in a target county but polygon doesn't touch it)
    geo_candidates = set(
        crosswalk[crosswalk["county_fips"].isin(target_fips)]["zip"]
        .dropna().unique()
    )
    target_state_fips = {f[:2] for f in target_fips}
    state_candidates = set(
        crosswalk[crosswalk["county_fips"].str[:2].isin(target_state_fips)]["zip"]
        .dropna().unique()
    )
    all_candidates = sorted(str(z) for z in (geo_candidates | state_candidates))
    print(f"  {len(geo_candidates)} ZIPs touch target counties geographically.")
    print(f"  {len(state_candidates)} ZIPs are in the target states.")
    print(f"  {len(all_candidates)} unique candidate ZIPs to evaluate.")

    print("\n[3/5] Fetching postal city + county for candidate ZIPs …")
    postal = fetch_zip_postal_info(all_candidates)

    # Primary: ZIPs whose postal city county is a target county
    postal["county_label"] = postal["postal_county_fips"].map(county_fips_map)
    matched = postal[postal["county_label"].notna()].copy()

    # Fallback: geographic candidates not matched by postal city
    # assigned by largest land-area overlap with a target county
    unmatched_geo = postal[
        postal["county_label"].isna() & postal["zip"].isin(geo_candidates)
    ].copy()
    if not unmatched_geo.empty:
        crosswalk["area_land"] = pd.to_numeric(
            crosswalk["area_land"], errors="coerce").fillna(0)
        target_rows = crosswalk[crosswalk["county_fips"].isin(target_fips)].copy()
        best = (target_rows.sort_values("area_land", ascending=False)
                           .drop_duplicates(subset="zip", keep="first")
                           [["zip", "county_fips"]])
        fallback = unmatched_geo.merge(best, on="zip", how="inner")
        fallback["county_label"] = fallback["county_fips"].map(county_fips_map)
        fallback = fallback.drop(columns=["county_fips"])
        if not fallback.empty:
            print(f"  ℹ {len(fallback)} ZIP(s) assigned by geographic fallback:")
            for _, row in fallback.iterrows():
                print(f"    {row['zip']} ({row['city']}) -> {row['county_label']}")
        matched = pd.concat([matched, fallback], ignore_index=True)

    # PO Box fallback: ZIPs still unmatched after both postal-FIPS and
    # geographic fallback. PO Box ZIPs have NaN lat/lon/county_code in
    # pgeocode but DO have county_name. Match by county name + state.
    still_unmatched = postal[
        ~postal["zip"].isin(matched["zip"])
    ].copy()
    if not still_unmatched.empty:
        # Build a lookup: (county_name_lower, state_abbr) -> county_label
        name_state_map = {}
        for fips, label in county_fips_map.items():
            # label is e.g. "Scott County, IA"
            parts = label.split(",")
            cname = parts[0].replace("County", "").strip().lower()
            state = parts[1].strip().upper() if len(parts) > 1 else ""
            name_state_map[(cname, state)] = label

        def match_by_name(row):
            cname = str(row["postal_county_name"]).strip().lower()
            state = str(row["postal_state_code"]).strip().upper()
            return name_state_map.get((cname, state), None)

        still_unmatched["county_label"] = still_unmatched.apply(
            match_by_name, axis=1
        )
        po_matched = still_unmatched[still_unmatched["county_label"].notna()].copy()
        if not po_matched.empty:
            print(f"  ℹ {len(po_matched)} PO Box ZIP(s) matched by county name:")
            for _, row in po_matched.iterrows():
                print(f"    {row['zip']} ({row['city']}) -> {row['county_label']}")
            matched = pd.concat([matched, po_matched], ignore_index=True)

    filtered = matched.drop_duplicates(subset="zip").reset_index(drop=True)
    print(f"  {len(filtered)} ZIPs assigned in total.")

    print("\n  ZIP codes assigned per county:")
    for county in sorted(filtered["county_label"].unique()):
        n = len(filtered[filtered["county_label"] == county])
        print(f"    {county}: {n} ZIPs")

    print("\n[4/5] (skipped — county already assigned from postal data)")

    print("\n[5/5] Loading ZCTA geometries …")
    zcta_gdf = fetch_zcta_shapefile()

    target_zips = set(filtered["zip"].unique())
    zcta_col = "ZCTA5CE20" if "ZCTA5CE20" in zcta_gdf.columns else zcta_gdf.columns[0]
    geo = zcta_gdf[zcta_gdf[zcta_col].isin(target_zips)].copy()
    geo = geo.rename(columns={zcta_col: "zip"})
    geo = geo.merge(filtered[["zip", "city", "county_label"]], on="zip", how="left")
    geo = geo.to_crs(epsg=4326)

    # Identify PO Box-only ZIPs: in filtered but not in the shapefile.
    # They have no polygon so they appear in the CSV but not the map.
    mapped_zips = set(geo["zip"].unique())
    po_box_only = filtered[~filtered["zip"].isin(mapped_zips)].copy()
    po_box_only = po_box_only.assign(note="PO Box only - no map polygon")
    if not po_box_only.empty:
        print(f"  ℹ {len(po_box_only)} PO Box-only ZIP(s) included in CSV but not map:")
        for _, row in po_box_only.iterrows():
            print(f"    {row['zip']} ({row['city']}, {row['county_label']})")

    return geo, filtered, po_box_only


# ─────────────────────────────────────────────
#  Step 6 — Build interactive Folium map
# ─────────────────────────────────────────────

COUNTY_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
]


def build_map(geo: gpd.GeoDataFrame) -> folium.Map:
    """Create an interactive Folium map with ZIP, City, and County tooltips."""
    if geo.empty:
        raise ValueError(
            "No ZCTA geometries found. This usually means no ZIPs matched — "
            "check the diagnostic output above for county_code format issues."
        )
    union = geo.geometry.unary_union
    centroid = union.centroid
    if centroid.is_empty:
        # Fallback: use bounds midpoint
        b = union.bounds
        centroid_y = (b[1] + b[3]) / 2
        centroid_x = (b[0] + b[2]) / 2
    else:
        centroid_y, centroid_x = centroid.y, centroid.x
    m = folium.Map(
        location=[centroid_y, centroid_x],
        zoom_start=9,
        tiles="CartoDB positron",
    )

    counties = geo["county_label"].unique()
    color_map = {c: COUNTY_COLORS[i % len(COUNTY_COLORS)]
                 for i, c in enumerate(sorted(counties))}

    for county, color in color_map.items():
        county_geo = geo[geo["county_label"] == county]
        folium.GeoJson(
            county_geo.__geo_interface__,
            name=county,
            style_function=lambda feat, c=color: {
                "fillColor": c,
                "color": "white",
                "weight": 1.2,
                "fillOpacity": 0.55,
            },
            highlight_function=lambda feat: {
                "weight": 2.5,
                "fillOpacity": 0.8,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=["zip", "city", "county_label"],
                aliases=["ZIP Code:", "City:", "County:"],
                localize=True,
            ),
        ).add_to(m)

    # Legend
    legend_html = """
    <div style="
        position: fixed; bottom: 30px; left: 30px; z-index: 1000;
        background: white; padding: 12px 18px; border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,.25); font-family: sans-serif;
        font-size: 13px; line-height: 1.8;
    ">
    <b>Counties</b><br>
    """
    for county, color in sorted(color_map.items()):
        legend_html += (
            f'<span style="display:inline-block;width:14px;height:14px;'
            f'background:{color};margin-right:6px;border-radius:3px;'
            f'vertical-align:middle;"></span>{county}<br>'
        )
    legend_html += "</div>"
    m.get_root().html.add_child(folium.Element(legend_html))

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

    geo, filtered, po_box_only = build_zip_geodataframe(TARGET_COUNTIES)

    # ── Save CSV  (county | zip | city | note)
    # Regular ZIPs have an empty note; PO Box-only ZIPs are flagged.
    csv_df = (
        filtered[["county_label", "zip", "city"]]
        .rename(columns={"county_label": "county"})
        .assign(note="")
    )
    if not po_box_only.empty:
        po_csv = (
            po_box_only[["county_label", "zip", "city", "note"]]
            .rename(columns={"county_label": "county"})
        )
        csv_df = pd.concat([csv_df, po_csv], ignore_index=True)
    csv_df = csv_df.sort_values(["county", "zip"]).reset_index(drop=True)
    csv_df.to_csv(OUTPUT_ZIP_CSV, index=False)
    print(f"\n✔ ZIP list saved → {OUTPUT_ZIP_CSV}")

    # ── Save Map
    print("  Building interactive map …")
    m = build_map(geo)
    m.save(OUTPUT_MAP_HTML)
    print(f"✔ Map saved        → {OUTPUT_MAP_HTML}")

    # ── Print summary
    print("\n── Summary ─────────────────────────────────────")
    po_zips = set(po_box_only["zip"]) if not po_box_only.empty else set()
    all_rows = pd.concat([filtered, po_box_only], ignore_index=True)
    for county in sorted(all_rows["county_label"].unique()):
        rows = all_rows[all_rows["county_label"] == county].sort_values("zip")
        print(f"\n  {county} ({len(rows)} ZIPs):")
        print(f"  {'ZIP':<8} {'City':<25} {'Note'}")
        print(f"  {'─'*7} {'─'*25} {'─'*18}")
        for _, row in rows.iterrows():
            note = "PO Box only" if row["zip"] in po_zips else ""
            print(f"  {row['zip']:<8} {row['city']:<25} {note}")
    print("\n" + "=" * 55)


if __name__ == "__main__":
    main()
