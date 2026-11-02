#!/usr/bin/env python3
"""Export only the report sources, with main.tex at the archive root."""
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

report = Path(__file__).resolve().parent
output = report / "build" / "overleaf.zip"
output.parent.mkdir(exist_ok=True)
with ZipFile(output, "w", ZIP_DEFLATED) as archive:
    for pattern in ("*.tex", "*.bib", "sections/*.tex", "figures/**/*"):
        for path in sorted(report.glob(pattern)):
            if path.is_file():
                archive.write(path, path.relative_to(report))
print(output)
