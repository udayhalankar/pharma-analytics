import math
import os
from datetime import datetime, timezone
from io import StringIO
from hashlib import sha256
from pathlib import Path
from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

try:
    import boto3
except ImportError:
    boto3 = None

st.set_page_config(page_title="Pharma Analytics (Excel, No DB)", layout="wide")

DATA_DIR = Path("data")
UPLOADS_DIR = DATA_DIR / "uploads"
NORMALIZED_DIR = DATA_DIR / "normalized"
UPLOAD_LOG_PATH = DATA_DIR / "upload_log.csv"
MASTER_DATA_PATH = DATA_DIR / "master_data.csv"
S3_DEFAULT_PREFIX = "pharma-analytics"

REQUIRED_COLUMNS = {
    "territory",
    "cnf",
    "stockist",
    "chemist",
    "doctor",
    "medical_rep",
    "product",
    "units",
    "date",
}

COLUMN_ALIASES = {
    "medical rep": "medical_rep",
    "medicalrep": "medical_rep",
    "mr": "medical_rep",
    "rep": "medical_rep",
    "product_name": "product",
    "item_name": "product",
    "invdate": "date",
    "inv_date": "date",
    "invoice_date": "date",
    "sale_date": "date",
    "transaction_date": "date",
    "area": "territory",
    "city": "cnf",
    "invno": "invoice_no",
    "qty": "units",
    "quantity": "units",
    "netamt": "net_amount",
    "net_amt": "net_amount",
    "rate": "rate",
    "cp": "cp",
    "nsr": "nsr",
    "free": "free",
    "showbatch": "show_batch",
    "tcp": "tcp",
}

UPLOAD_LOG_COLUMNS = [
    "import_key",
    "file_hash",
    "original_name",
    "sheet_name",
    "raw_path",
    "normalized_path",
    "row_count",
    "imported_at_utc",
]


def get_s3_settings() -> dict[str, str]:
    def secret_or_env(key: str, default: str = "") -> str:
        try:
            if hasattr(st, "secrets"):
                value = st.secrets[key]
                if value is not None:
                    return str(value)
        except Exception:
            pass
        return os.getenv(key, default)

    settings: dict[str, str] = {}
    settings["bucket"] = secret_or_env("S3_BUCKET", "")
    settings["prefix"] = secret_or_env("S3_PREFIX", S3_DEFAULT_PREFIX)
    settings["region"] = secret_or_env("AWS_DEFAULT_REGION", "")
    settings["access_key"] = secret_or_env("AWS_ACCESS_KEY_ID", "")
    settings["secret_key"] = secret_or_env("AWS_SECRET_ACCESS_KEY", "")
    return settings


def s3_is_available() -> bool:
    settings = get_s3_settings()
    return boto3 is not None and bool(settings["bucket"])


def get_storage_backend() -> str:
    options = ["local"]
    if s3_is_available():
        options.append("s3")

    default_index = 0
    if "storage_backend" not in st.session_state:
        st.session_state["storage_backend"] = options[default_index]

    return st.radio(
        "Storage location",
        options=options,
        horizontal=True,
        format_func=lambda value: "Local folder" if value == "local" else "Amazon S3",
        key="storage_backend",
    )


def to_snake(name: str) -> str:
    return (
        str(name)
        .strip()
        .lower()
        .replace("-", "_")
        .replace("/", "_")
        .replace(" ", "_")
    )


def ensure_storage_dirs() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    UPLOADS_DIR.mkdir(exist_ok=True)
    NORMALIZED_DIR.mkdir(exist_ok=True)


def get_s3_client():
    settings = get_s3_settings()
    kwargs = {}
    if settings["region"]:
        kwargs["region_name"] = settings["region"]
    if settings["access_key"] and settings["secret_key"]:
        kwargs["aws_access_key_id"] = settings["access_key"]
        kwargs["aws_secret_access_key"] = settings["secret_key"]
    return boto3.client("s3", **kwargs)


def get_storage_paths(storage_backend: str) -> dict[str, object]:
    if storage_backend == "local":
        return {
            "uploads_dir": UPLOADS_DIR,
            "normalized_dir": NORMALIZED_DIR,
            "upload_log": UPLOAD_LOG_PATH,
            "master_data": MASTER_DATA_PATH,
        }

    settings = get_s3_settings()
    prefix = settings["prefix"].strip("/").rstrip("/")
    return {
        "bucket": settings["bucket"],
        "uploads_dir": f"{prefix}/uploads",
        "normalized_dir": f"{prefix}/normalized",
        "upload_log": f"{prefix}/upload_log.csv",
        "master_data": f"{prefix}/master_data.csv",
    }


def storage_exists(storage_backend: str, target) -> bool:
    if storage_backend == "local":
        return Path(target).exists()

    client = get_s3_client()
    paths = get_storage_paths(storage_backend)
    try:
        client.head_object(Bucket=paths["bucket"], Key=str(target))
        return True
    except Exception:
        return False


def storage_write_bytes(storage_backend: str, target, content: bytes) -> None:
    if storage_backend == "local":
        Path(target).write_bytes(content)
        return

    client = get_s3_client()
    paths = get_storage_paths(storage_backend)
    client.put_object(Bucket=paths["bucket"], Key=str(target), Body=content)


def storage_write_text(storage_backend: str, target, content: str) -> None:
    storage_write_bytes(storage_backend, target, content.encode("utf-8"))


def storage_read_text(storage_backend: str, target) -> str:
    if storage_backend == "local":
        return Path(target).read_text(encoding="utf-8")

    client = get_s3_client()
    paths = get_storage_paths(storage_backend)
    response = client.get_object(Bucket=paths["bucket"], Key=str(target))
    return response["Body"].read().decode("utf-8")


def sanitize_filename(name: str) -> str:
    clean = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in name.strip())
    return clean or "upload.xlsx"


def compute_sha256(content: bytes) -> str:
    return sha256(content).hexdigest()


def load_upload_log() -> pd.DataFrame:
    storage_backend = st.session_state.get("storage_backend", "local")
    if storage_backend == "local":
        ensure_storage_dirs()
    paths = get_storage_paths(storage_backend)
    if not storage_exists(storage_backend, paths["upload_log"]):
        return pd.DataFrame(columns=UPLOAD_LOG_COLUMNS)
    log_df = pd.read_csv(StringIO(storage_read_text(storage_backend, paths["upload_log"])))
    for col in UPLOAD_LOG_COLUMNS:
        if col not in log_df.columns:
            log_df[col] = pd.NA
    return log_df[UPLOAD_LOG_COLUMNS]


def save_upload_log(log_df: pd.DataFrame) -> None:
    storage_backend = st.session_state.get("storage_backend", "local")
    if storage_backend == "local":
        ensure_storage_dirs()
    paths = get_storage_paths(storage_backend)
    storage_write_text(storage_backend, paths["upload_log"], log_df.to_csv(index=False))


def rebuild_master_data() -> pd.DataFrame:
    storage_backend = st.session_state.get("storage_backend", "local")
    log_df = load_upload_log()
    frames = []

    for _, row in log_df.iterrows():
        normalized_path = row.get("normalized_path")
        if pd.isna(normalized_path):
            continue
        if not storage_exists(storage_backend, str(normalized_path)):
            continue
        frame = pd.read_csv(StringIO(storage_read_text(storage_backend, str(normalized_path))))
        frame["source_import_key"] = row["import_key"]
        frame["source_file"] = row["original_name"]
        frame["source_sheet"] = row["sheet_name"]
        frames.append(frame)

    if frames:
        master_df = pd.concat(frames, ignore_index=True)
    else:
        master_df = pd.DataFrame()

    paths = get_storage_paths(storage_backend)
    storage_write_text(storage_backend, paths["master_data"], master_df.to_csv(index=False))
    return master_df


def load_persisted_dataset() -> pd.DataFrame:
    storage_backend = st.session_state.get("storage_backend", "local")
    if storage_backend == "local":
        ensure_storage_dirs()
    paths = get_storage_paths(storage_backend)
    if storage_exists(storage_backend, paths["master_data"]):
        return pd.read_csv(StringIO(storage_read_text(storage_backend, paths["master_data"])))
    return rebuild_master_data()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    col_map: Dict[str, str] = {}
    seen = set()
    for c in df.columns:
        norm = to_snake(c)
        norm = COLUMN_ALIASES.get(norm.replace("_", " "), norm)
        if norm in COLUMN_ALIASES:
            norm = COLUMN_ALIASES[norm]
        if norm in seen:
            i = 2
            candidate = f"{norm}_{i}"
            while candidate in seen:
                i += 1
                candidate = f"{norm}_{i}"
            norm = candidate
        seen.add(norm)
        col_map[c] = norm
    return df.rename(columns=col_map)


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    if "date" not in df.columns:
        return df
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()]
    return df


def load_excel_file(uploaded_file) -> Optional[pd.ExcelFile]:
    filename = str(getattr(uploaded_file, "name", "")).lower()
    if filename.endswith(".xlsx"):
        engine = "openpyxl"
    elif filename.endswith(".xls"):
        engine = "xlrd"
    else:
        st.error("Unsupported file type. Please upload .xlsx or .xls.")
        return None

    try:
        return pd.ExcelFile(uploaded_file, engine=engine)
    except ImportError:
        if engine == "xlrd":
            st.error(
                "Missing dependency for .xls files: install xlrd >= 2.0.1 "
                "(example: pip install xlrd>=2.0.1), or upload as .xlsx."
            )
        else:
            st.error(
                "Missing dependency for .xlsx files: install openpyxl "
                "(example: pip install openpyxl)."
            )
        return None
    except Exception as exc:
        st.error(f"Could not read Excel file: {exc}")
        return None


def prepare_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df_raw)

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    df = parse_dates(df)
    if df.empty:
        return df

    df["units"] = try_float(df, "units").fillna(0)
    return df


def import_uploaded_sheet(uploaded_file, sheet_name: str, df_raw: pd.DataFrame) -> tuple[bool, str]:
    storage_backend = st.session_state.get("storage_backend", "local")
    if storage_backend == "local":
        ensure_storage_dirs()
    file_bytes = uploaded_file.getvalue()
    file_hash = compute_sha256(file_bytes)
    import_key = compute_sha256(file_bytes + f"|{sheet_name}".encode("utf-8"))

    log_df = load_upload_log()
    if not log_df.empty and import_key in log_df["import_key"].astype(str).tolist():
        return False, f"Duplicate upload skipped. `{uploaded_file.name}` sheet `{sheet_name}` was already imported."

    prepared = prepare_dataframe(df_raw)
    if prepared.empty:
        return False, "The selected sheet has no valid rows after date parsing."

    raw_name = f"{file_hash[:12]}_{sanitize_filename(uploaded_file.name)}"
    paths = get_storage_paths(storage_backend)
    if storage_backend == "local":
        raw_path = Path(paths["uploads_dir"]) / raw_name
    else:
        raw_path = f"{paths['uploads_dir']}/{raw_name}"
    if not storage_exists(storage_backend, raw_path):
        storage_write_bytes(storage_backend, raw_path, file_bytes)

    if storage_backend == "local":
        normalized_path = Path(paths["normalized_dir"]) / f"{import_key}.csv"
    else:
        normalized_path = f"{paths['normalized_dir']}/{import_key}.csv"
    storage_write_text(storage_backend, normalized_path, prepared.to_csv(index=False))

    log_row = pd.DataFrame(
        [
            {
                "import_key": import_key,
                "file_hash": file_hash,
                "original_name": uploaded_file.name,
                "sheet_name": sheet_name,
                "raw_path": str(raw_path),
                "normalized_path": str(normalized_path),
                "row_count": len(prepared),
                "imported_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        ]
    )
    updated_log = pd.concat([log_df, log_row], ignore_index=True)
    save_upload_log(updated_log)
    rebuild_master_data()
    return True, f"Imported `{uploaded_file.name}` sheet `{sheet_name}` with {len(prepared):,} rows."


def enrich_time(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.to_period("M").astype(str)
    q = df["date"].dt.quarter
    df["quarter"] = df["date"].dt.year.astype(str) + "-Q" + q.astype(str)
    half = np.where(q <= 2, "H1", "H2")
    df["half_year"] = df["date"].dt.year.astype(str) + "-" + half
    return df


def try_float(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def top_pair_mapping(df: pd.DataFrame, key_col: str, value_col: str) -> pd.DataFrame:
    pairs = (
        df[[key_col, value_col]]
        .dropna()
        .value_counts()
        .reset_index(name="pair_count")
        .sort_values("pair_count", ascending=False)
    )
    return pairs.drop_duplicates(subset=[key_col]).drop(columns=["pair_count"])


def infer_doctor_attribution(
    filtered_df: pd.DataFrame,
    proximity_km_threshold: float,
) -> pd.DataFrame:
    sales = filtered_df.dropna(subset=["product", "units"]).copy()
    sales["units"] = try_float(sales, "units")
    sales = sales[sales["units"] > 0]

    if sales.empty:
        return pd.DataFrame(columns=["doctor", "medical_rep", "product", "inferred_units"])

    # Prefer direct attribution from medical_rep when available.
    direct_rep = sales[sales["medical_rep"].notna()].copy()
    direct_rep = direct_rep[["doctor", "medical_rep", "product", "units"]]
    direct_rep = direct_rep.rename(columns={"units": "inferred_units"})

    unresolved = sales[sales["medical_rep"].isna()].copy()

    if unresolved.empty:
        return direct_rep

    doctor_pool = filtered_df.dropna(subset=["doctor", "territory"])[
        ["doctor", "territory", "medical_rep", "doctor_lat", "doctor_lon"]
    ].copy() if set(["doctor_lat", "doctor_lon"]).issubset(filtered_df.columns) else filtered_df.dropna(subset=["doctor", "territory"])[
        ["doctor", "territory", "medical_rep"]
    ].copy()

    if doctor_pool.empty:
        return direct_rep

    chemist_geo_cols = set(["chemist_lat", "chemist_lon"]).issubset(unresolved.columns)
    doctor_geo_cols = set(["doctor_lat", "doctor_lon"]).issubset(doctor_pool.columns)

    inferred_rows = []
    for _, sale in unresolved.iterrows():
        candidates = doctor_pool[doctor_pool["territory"] == sale["territory"]].copy()
        if candidates.empty:
            continue

        if chemist_geo_cols and doctor_geo_cols and pd.notna(sale.get("chemist_lat")) and pd.notna(sale.get("chemist_lon")):
            candidates = candidates.dropna(subset=["doctor_lat", "doctor_lon"])
            if not candidates.empty:
                candidates["distance_km"] = candidates.apply(
                    lambda r: haversine_km(
                        float(sale["chemist_lat"]),
                        float(sale["chemist_lon"]),
                        float(r["doctor_lat"]),
                        float(r["doctor_lon"]),
                    ),
                    axis=1,
                )
                candidates = candidates[candidates["distance_km"] <= proximity_km_threshold]

        if candidates.empty:
            continue

        weight = 1.0 / len(candidates)
        for _, c in candidates.iterrows():
            inferred_rows.append(
                {
                    "doctor": c["doctor"],
                    "medical_rep": c.get("medical_rep"),
                    "product": sale["product"],
                    "inferred_units": float(sale["units"]) * weight,
                }
            )

    inferred = pd.DataFrame(inferred_rows)

    combined = pd.concat([direct_rep, inferred], ignore_index=True)
    if combined.empty:
        return combined

    # Fill missing medical rep using most common doctor->rep mapping.
    mapping = top_pair_mapping(filtered_df, "doctor", "medical_rep")
    combined = combined.merge(mapping, on="doctor", how="left", suffixes=("", "_mapped"))
    combined["medical_rep"] = combined["medical_rep"].fillna(combined["medical_rep_mapped"])
    combined = combined.drop(columns=["medical_rep_mapped"])

    return combined


def build_pivot(
    df: pd.DataFrame,
    group_cols: Iterable[str],
    value_col: str = "units",
    value_name: str = "units",
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[*group_cols, value_name])
    agg = (
        df.groupby(list(group_cols), dropna=False)[value_col]
        .sum()
        .reset_index()
        .sort_values(value_col, ascending=False)
    )
    return agg.rename(columns={value_col: value_name})


def build_monthly_matrix_report(df: pd.DataFrame, group_cols: Iterable[str]) -> pd.DataFrame:
    display_cols = ["stockist", "cnf", "territory", "medical_rep", "chemist", "product"]
    base_cols = [c for c in display_cols if c in set(group_cols)]
    rename_map = {
        "stockist": "Stockist",
        "cnf": "City",
        "territory": "Area",
        "medical_rep": "Medical Rep",
        "product": "Product",
        "chemist": "Chemist",
        "total": "Total",
    }
    working = df.dropna(subset=["date"]).copy()
    if working.empty:
        empty_cols = [rename_map.get(c, c) for c in display_cols] + ["Total"]
        return pd.DataFrame(columns=empty_cols)

    working["month_dt"] = working["date"].dt.to_period("M").dt.to_timestamp()
    working["month_label"] = working["month_dt"].dt.strftime("%B'%y")

    report = (
        working.pivot_table(
            index=base_cols,
            columns="month_label",
            values="units",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )

    month_order = (
        working[["month_label", "month_dt"]]
        .drop_duplicates()
        .sort_values("month_dt")["month_label"]
        .tolist()
    )

    for col in display_cols:
        if col not in report.columns:
            report[col] = "All"

    ordered_cols = [*display_cols, *month_order]
    report = report.reindex(columns=ordered_cols, fill_value=0)
    report["total"] = report[month_order].sum(axis=1) if month_order else 0

    report = report.rename(columns=rename_map)
    return report


def add_sidebar_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filters")

    min_date = df["date"].min().date()
    max_date = df["date"].max().date()
    date_range = st.sidebar.date_input(
        "Date range",
        (min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start, end = date_range
        filtered = df[(df["date"].dt.date >= start) & (df["date"].dt.date <= end)].copy()
    else:
        filtered = df.copy()

    for col in ["territory", "cnf", "stockist", "chemist", "doctor", "medical_rep", "product"]:
        values = sorted([v for v in filtered[col].dropna().unique().tolist() if str(v).strip()])
        selected = st.sidebar.multiselect(f"{col.replace('_', ' ').title()}", values)
        if selected:
            filtered = filtered[filtered[col].isin(selected)]

    return filtered


def draw_time_trend(df: pd.DataFrame, title: str, entity_col: str) -> None:
    trend = build_pivot(df, ["month", "product"], value_col="units", value_name="units")
    st.subheader(title)
    if trend.empty:
        st.info("No data for current filters.")
        return
    fig = px.line(trend, x="month", y="units", color="product", markers=True)
    fig.update_layout(height=360, xaxis_title="Month", yaxis_title="Units")
    st.plotly_chart(fig, use_container_width=True)


def sort_time_labels(labels: Iterable[str], time_grain: str) -> list[str]:
    raw = [str(v) for v in labels if pd.notna(v)]
    if time_grain == "year":
        return sorted(raw, key=lambda x: int(x))
    if time_grain == "month":
        return sorted(raw, key=lambda x: pd.Period(x, freq="M"))
    if time_grain == "quarter":
        return sorted(raw, key=lambda x: pd.Period(x, freq="Q"))
    if time_grain == "half_year":
        return sorted(
            raw,
            key=lambda x: (int(x.split("-")[0]), 1 if x.endswith("H1") else 2),
        )
    return sorted(raw)


def render_upload_manager() -> None:
    st.subheader("Upload Manager")
    storage_backend = get_storage_backend()
    if storage_backend == "local":
        st.caption("Uploads, import log, and merged data are stored on the local machine.")
    else:
        settings = get_s3_settings()
        st.caption(f"Uploads, import log, and merged data are stored in S3 bucket `{settings['bucket']}`.")

    uploaded = st.file_uploader("Upload Excel file (.xlsx / .xls)", type=["xlsx", "xls"])

    if uploaded is not None:
        xl = load_excel_file(uploaded)
        if xl is not None:
            sheet = st.selectbox("Select sheet to import", xl.sheet_names)
            preview = xl.parse(sheet)
            st.caption(f"Preview rows in selected sheet: {len(preview):,}")
            if st.button("Import selected sheet", type="primary"):
                success, message = import_uploaded_sheet(uploaded, sheet, preview)
                if success:
                    st.success(message)
                    st.rerun()
                else:
                    st.warning(message)

    log_df = load_upload_log()
    st.subheader("Imported Files")
    if log_df.empty:
        st.info("No files imported yet.")
        return

    display_df = log_df.rename(
        columns={
            "original_name": "File",
            "sheet_name": "Sheet",
            "row_count": "Rows",
            "imported_at_utc": "Imported At (UTC)",
        }
    )[["File", "Sheet", "Rows", "Imported At (UTC)"]]
    st.dataframe(display_df.sort_values("Imported At (UTC)", ascending=False), use_container_width=True, hide_index=True)


def main() -> None:
    st.title("Pharma Analytics App (Excel Upload, No Database)")
    st.caption(
        "Upload Excel, keep it locally as flat files, and analyze the combined dataset without a database."
    )
    render_upload_manager()

    df = load_persisted_dataset()
    if df.empty:
        st.info("Import at least one sheet to start analytics.")
        return

    df = normalize_columns(df)
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    missing_core = [c for c in ["date", "product", "units"] if c not in df.columns]
    if missing_core:
        st.error(f"Missing required columns in persisted data: {', '.join(missing_core)}")
        return

    df = parse_dates(df)
    if df.empty:
        st.error("No valid date rows found in saved uploads. Please check your date column.")
        return

    df["units"] = try_float(df, "units").fillna(0)
    df = enrich_time(df)

    filtered = add_sidebar_filters(df)
    if filtered.empty:
        st.warning("No rows match current filters.")
        return

    st.subheader("KPI Overview")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Units", f"{filtered['units'].sum():,.0f}")
    c2.metric("Territories", int(filtered["territory"].nunique(dropna=True)))
    c3.metric("Products", int(filtered["product"].nunique(dropna=True)))
    c4.metric("Chemists", int(filtered["chemist"].nunique(dropna=True)))

    time_grain = st.radio("Time granularity", ["month", "quarter", "half_year", "year"], horizontal=True)

    st.subheader("Units Sold by Chemist (By Product)")
    chemist_table = build_pivot(filtered.dropna(subset=["chemist", "product"]), ["chemist", "product"]) 
    st.dataframe(chemist_table, use_container_width=True, hide_index=True)
    if not chemist_table.empty:
        chart = chemist_table.head(30)
        fig = px.bar(chart, x="chemist", y="units", color="product", title="Top Chemist-Product Sales")
        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Units Sold by Stockist (By Product)")
    stockist_table = build_pivot(filtered.dropna(subset=["stockist", "product"]), ["stockist", "product"])
    st.dataframe(stockist_table, use_container_width=True, hide_index=True)
    if not stockist_table.empty:
        chart = stockist_table.head(30)
        fig = px.bar(chart, x="stockist", y="units", color="product", title="Top Stockist-Product Sales")
        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True)

    proximity_threshold = st.slider("Doctor-Chemist proximity threshold (km)", min_value=1, max_value=100, value=15)
    inferred = infer_doctor_attribution(filtered, proximity_threshold)

    st.subheader("Units Sold by Medical Rep (By Product, Inferred)")
    mr_table = build_pivot(inferred.dropna(subset=["medical_rep", "product"]), ["medical_rep", "product"], value_col="inferred_units")
    st.dataframe(mr_table, use_container_width=True, hide_index=True)
    if not mr_table.empty:
        chart = mr_table.head(30)
        fig = px.bar(chart, x="medical_rep", y="units", color="product", title="Medical Rep Product Units (Inferred)")
        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Product Sales by Geographic Region and Time")
    geo_col = st.selectbox("Geographic level", ["territory", "cnf", "stockist", "chemist"])
    region_time = build_pivot(
        filtered.dropna(subset=[geo_col, "product"]),
        [geo_col, "product", time_grain],
    )
    st.dataframe(region_time, use_container_width=True, hide_index=True)
    if not region_time.empty:
        geo_values = sorted([v for v in region_time[geo_col].dropna().unique().tolist() if str(v).strip()])
        selected_geo = st.selectbox(
            f"{geo_col.replace('_', ' ').title()} for chart",
            options=["All"] + geo_values,
        )

        if selected_geo == "All":
            chart = (
                region_time.groupby([time_grain, "product"], dropna=False)["units"]
                .sum()
                .reset_index()
            )
            chart_title = f"Product Units by {time_grain.replace('_', ' ').title()} (All {geo_col.title()})"
        else:
            chart = region_time[region_time[geo_col] == selected_geo].copy()
            chart_title = (
                f"Product Units for {geo_col.replace('_', ' ').title()}: {selected_geo} "
                f"by {time_grain.replace('_', ' ').title()}"
            )

        period_order = sort_time_labels(chart[time_grain].dropna().unique().tolist(), time_grain)
        fig = px.bar(
            chart,
            x=time_grain,
            y="units",
            color="product",
            category_orders={time_grain: period_order},
            title=chart_title,
        )
        fig.update_layout(height=460, xaxis_title=time_grain.replace("_", " ").title(), yaxis_title="Units")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Export Current Filtered Data")
    csv = filtered.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download filtered rows as CSV",
        data=csv,
        file_name="pharma_filtered_data.csv",
        mime="text/csv",
    )

    st.subheader("Monthly Report")
    group_by_options = {
        "Stockist": "stockist",
        "City": "cnf",
        "Area": "territory",
        "Medical Rep": "medical_rep",
        "Product": "product",
        "Chemist": "chemist",
    }
    selected_group_labels = st.multiselect(
        "Group by option",
        options=list(group_by_options.keys()),
        default=["City", "Area", "Medical Rep", "Product", "Chemist"],
    )
    selected_group_cols = [group_by_options[label] for label in selected_group_labels]
    if not selected_group_cols:
        st.warning("Select at least one group by option for monthly report.")
        return

    monthly_report = build_monthly_matrix_report(filtered, selected_group_cols)
    st.dataframe(monthly_report, use_container_width=True, hide_index=True)
    report_csv = monthly_report.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download monthly report as CSV",
        data=report_csv,
        file_name="pharma_monthly_report.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
