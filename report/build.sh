#!/usr/bin/env bash
set -euo pipefail
report_dir=$(cd "$(dirname "$0")" && pwd)
repo_dir=$(dirname "$report_dir")
engine=${REPORT_ENGINE:-podman}
image=${REPORT_IMAGE:-localhost/issue3-report:texlive}
mkdir -p "$report_dir/build" "$repo_dir/output/pdf"
"$engine" build -t "$image" "$report_dir"
"$engine" image inspect "$image" > "$report_dir/build/image.json"
git -C "$repo_dir" rev-parse HEAD > "$report_dir/build/source-revision.txt"
git -C "$repo_dir" status --short > "$report_dir/build/source-status.txt"
"$engine" run --rm --network none --cap-drop ALL --security-opt no-new-privileges \
  --cpus 2 --memory 2g -v "$report_dir:/work:ro" -v "$report_dir/build:/out" \
  "$image" sh -ec '
    latexmk -pdf -interaction=nonstopmode -halt-on-error -file-line-error -outdir=/out main.tex
    pdfinfo /out/main.pdf > /out/pdfinfo.txt
    { pdflatex --version; latexmk -v; dpkg-query -W "texlive*" latexmk; } > /out/toolchain.txt
    rm -f /out/page-*.png
    pdftoppm -scale-to 1600 -png /out/main.pdf /out/page
  '
cp "$report_dir/build/main.pdf" "$repo_dir/output/pdf/trading-trends-report.pdf"
printf 'PDF: %s\n' "$repo_dir/output/pdf/trading-trends-report.pdf"
