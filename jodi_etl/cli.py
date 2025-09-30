#!/usr/bin/env python3
import argparse
import os
import sys
import time
import zipfile
from typing import List

import requests

# Official JODI Oil monthly CSV bundles (world)
WORLD_PRIMARY_ZIP_URL = "https://www.jodidata.org/_resources/files/downloads/oil-data/world_primary_csv.zip?iid=163"
WORLD_SECONDARY_ZIP_URL = "https://www.jodidata.org/_resources/files/downloads/oil-data/world_secondary_csv.zip?iid=163"

# Default output directory inside this package
DEFAULT_OUTDIR = os.path.join(os.path.dirname(__file__), "data")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def download_file(url: str, dest: str, chunk: int = 1024 * 1024) -> None:
    r = requests.get(url, stream=True, timeout=300)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for part in r.iter_content(chunk_size=chunk):
            if part:
                f.write(part)


def extract_csvs_from_zip(zip_path: str, out_dir: str) -> List[str]:
    """Extract all CSV files from the zip into out_dir, preserving filenames and contents."""
    saved: List[str] = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            base = os.path.basename(name)
            target = os.path.join(out_dir, base)
            # Extract by reading and writing (avoids creating nested dirs)
            with zf.open(name) as src, open(target, "wb") as dst:
                dst.write(src.read())
            saved.append(target)
    return saved


def split_csv_by_country(csv_path: str, out_dir: str, fmt: str = "csv") -> List[str]:
    import pandas as pd

    df = pd.read_csv(csv_path, dtype=str)
    out_paths: List[str] = []

    # Normalise values
    df["REF_AREA"] = df["REF_AREA"].astype(str).str.strip().str.upper()

    for country, group in df.groupby("REF_AREA", sort=True):
        if not country:
            continue
        country_dir = os.path.join(out_dir, country)
        ensure_dir(country_dir)
        base = os.path.splitext(os.path.basename(csv_path))[0]
        file_stem = f"{base}_{country}"
        if fmt == "parquet":
            dest_path = os.path.join(country_dir, f"{file_stem}.parquet")
            group.to_parquet(dest_path, index=False)
        else:
            dest_path = os.path.join(country_dir, f"{file_stem}.csv")
            group.to_csv(dest_path, index=False)
        out_paths.append(dest_path)

    return out_paths


def cmd_fetch(args: argparse.Namespace) -> None:
    start = time.time()
    out_dir = args.outdir
    ensure_dir(out_dir)

    # Download zips
    primary_zip = os.path.join(out_dir, "world_primary_csv.zip")
    secondary_zip = os.path.join(out_dir, "world_secondary_csv.zip")

    if not args.quiet:
        print("Downloading primary ZIP…")
    download_file(args.primary_url, primary_zip)

    if not args.quiet:
        print("Downloading secondary ZIP…")
    download_file(args.secondary_url, secondary_zip)

    # Extract CSVs unchanged
    if not args.quiet:
        print("Extracting CSVs…")
    primary_csvs = extract_csvs_from_zip(primary_zip, out_dir)
    secondary_csvs = extract_csvs_from_zip(secondary_zip, out_dir)

    # Split by country
    split_format = args.split_format
    split_out = os.path.join(out_dir, "split")
    ensure_dir(split_out)

    if not args.quiet:
        print(f"Splitting CSVs by country into {split_format.upper()} format…")

    for csv_file in primary_csvs + secondary_csvs:
        basename = os.path.basename(csv_file)
        section_dir = os.path.join(split_out, "primary" if "Primary" in basename else "secondary")
        ensure_dir(section_dir)
        split_csv_by_country(csv_file, section_dir, fmt=split_format)

        # Remove original bulky CSV after successful split
        try:
            os.remove(csv_file)
            if not args.quiet:
                print(f"Deleted extracted CSV: {csv_file}")
        except FileNotFoundError:
            pass
        except OSError as exc:
            if not args.quiet:
                print(f"Warning: could not delete {csv_file}: {exc}")

    # Remove temporary ZIP archives to conserve disk space
    for zip_path in (primary_zip, secondary_zip):
        try:
            os.remove(zip_path)
            if not args.quiet:
                print(f"Deleted temporary archive: {zip_path}")
        except FileNotFoundError:
            pass
        except OSError as exc:
            if not args.quiet:
                print(f"Warning: could not delete {zip_path}: {exc}")

    if not args.quiet:
        for p in primary_csvs:
            print(f"Saved: {p}")
        for s in secondary_csvs:
            print(f"Saved: {s}")

    elapsed = time.time() - start
    if not args.quiet:
        print(f"Done in {elapsed:.1f}s")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Fetch JODI Oil CSVs (primary and secondary) and save unchanged")
    sub = p.add_subparsers(dest="cmd", required=True)

    f = sub.add_parser("fetch", help="Download JODI CSV bundles, extract them, and split by country")
    f.add_argument("--outdir", default=DEFAULT_OUTDIR, help="Output directory for ZIPs and CSVs")
    f.add_argument("--primary-url", default=WORLD_PRIMARY_ZIP_URL)
    f.add_argument("--secondary-url", default=WORLD_SECONDARY_ZIP_URL)
    f.add_argument("--split-format", choices=["csv", "parquet"], default="csv", help="File format for split output")
    f.add_argument("--quiet", action="store_true")
    f.set_defaults(func=cmd_fetch)

    args = p.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
