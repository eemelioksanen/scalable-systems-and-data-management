import argparse
import datetime as dt
import hashlib
import json
import shutil
import subprocess
from pathlib import Path


RECORD = "https://zenodo.org/records/6382482"


def checksum(path, algorithm):
    digest = hashlib.new(algorithm)
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("day", type=dt.date.fromisoformat)
    parser.add_argument("--data-dir", type=Path, default=Path("data/daily"))
    args = parser.parse_args()
    if not dt.date(2021, 11, 8) <= args.day <= dt.date(2021, 11, 14):
        parser.error("day must be within 2021-11-08 through 2021-11-14")
    args.data_dir.mkdir(parents=True, exist_ok=True)
    metadata = json.loads(subprocess.check_output([
        "curl", "--fail", "--silent", "--show-error", "--retry", "3",
        "--max-time", "60", "https://zenodo.org/api/records/6382482",
    ]))
    name = f"debs2022-gc-trading-day-{args.day:%d-%m-%y}.csv"
    entry = next(item for item in metadata["files"] if item["key"] == name)
    destination = args.data_dir / name
    partial = destination.with_suffix(".csv.partial")
    if not destination.exists():
        remaining = entry["size"] - (partial.stat().st_size if partial.exists() else 0)
        if shutil.disk_usage(args.data_dir).free < remaining + 20 * 1024**3:
            raise SystemExit("Need space for the remaining download plus 20 GiB of analysis headroom")
        if remaining > 0:
            subprocess.run([
                "curl", "--fail", "--location", "--show-error", "--retry", "5",
                "--retry-delay", "5", "--continue-at", "-", "--output", str(partial),
                f"{RECORD}/files/{name}?download=1",
            ], check=True)
        candidate = partial
    else:
        candidate = destination
    expected = entry["checksum"]
    algorithm, value = expected.split(":", 1)
    if candidate.stat().st_size != entry["size"] or checksum(candidate, algorithm) != value:
        raise SystemExit(f"Size or checksum mismatch: {candidate}; retained for inspection")
    sha256 = checksum(candidate, "sha256")
    if candidate == partial:
        partial.replace(destination)
    provenance = {
        "source_url": RECORD,
        "download_url": f"{RECORD}/files/{name}?download=1",
        "doi": "10.5281/zenodo.6382482",
        "license": "CC-BY-NC-SA-4.0",
        "attribution": "Frischbier, Tahir, Doblander, Hormann, Mayer, Jacobsen (2022), DEBS 2022 Grand Challenge Data Set: Trading Data",
        "day": str(args.day), "file": name, "size_bytes": entry["size"],
        "published_checksum": expected, "sha256": sha256,
        "verified_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "sampling": "complete daily file",
        "published_week_bytes": sum(item["size"] for item in metadata["files"]),
    }
    manifest = destination.with_suffix(".json")
    manifest.write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps(provenance, indent=2), flush=True)


if __name__ == "__main__":
    main()
