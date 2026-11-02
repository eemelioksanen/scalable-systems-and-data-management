# ACM report skeleton

This is a working skeleton, not the final report. The team must replace every **Pending** marker with supported prose, verify authorship and select final evaluation results before submission. The source is `main.tex`, with body and individual reflections in `sections/`, and BibTeX sources in `references.bib`.

The [project brief](../CS_E4780_Course_Project_2026.pdf), §4, requires 4–5 pages in the ACM proceedings template. The [rubric](../Evaluation_Criteria_26.pdf), page 2, requires individual reflections and a strong focus on dataset analysis. We use the real `acmart` class with `sigconf,nonacm`: two-column proceedings layout without pretending this course paper is an ACM publication. No custom class, altered margins or smaller fonts are used. See [ACM's template](https://www.acm.org/publications/proceedings-template) and the [class maintainer's source and guide](https://github.com/borisveytsman/acmart).

## Build and inspect

From the repository root, with a running Podman VM:

```bash
bash report/build.sh
```

Docker is also supported with `REPORT_ENGINE=docker bash report/build.sh`. The image includes TeX Live, the distribution's `acmart`/ACM bibliography style, latexmk, BibTeX and Poppler. No host JDK or LaTeX installation is needed. The first build downloads packages; subsequent builds reuse the image layers. Compilation is offline, capped at two CPUs and 2 GiB, in a disposable container. It mounts only report sources and the build directory, with no service ports or shared volumes.

The result is `output/pdf/trading-trends-report.pdf`. `report/build/` retains `main.log`, `main.bbl`, page PNGs, `pdfinfo.txt`, `toolchain.txt`, `image.json`, source revision and dirty status. Open **each** `page-*.png` and check text, citations, tables, figure labels, margins and page breaks. Check `main.log` for undefined citations/references and overfull boxes; `pdfinfo.txt` records page count. Build success alone is not layout review. Rebuild and re-inspect after changes.

The build reuses a digest-pinned, Debian-based Python image already used by this repository; compilation does not need Python. The large `texlive-fonts-extra` package supplies the required Libertine, Inconsolata and newtx fonts; the smaller recommended-fonts package alone falls back to the wrong fonts. Debian package repositories are not snapshot-pinned, so clean builds at different dates can resolve different TeX packages. Preserve the emitted image identity and package versions with report-build evidence; exact binary reproducibility is not claimed. `REPORT_IMAGE` can name a separate task image. The build container is removed automatically; keep or explicitly remove only the task's image, never prune unrelated containers, caches or volumes.

For an existing local TeX Live installation with these packages, the underlying build is:

```bash
cd report
latexmk -pdf -interaction=nonstopmode -halt-on-error -file-line-error -outdir=build main.tex
```

The container script also renders all pages and copies the PDF to the output path. A local-only build must do those inspection steps separately. The generated PDF, ZIP, TeX intermediates and image metadata are ignored by Git; source and small review notes belong in the PR.

## Final page allocation

Budget all content, including references and reflections, inside **5 pages** until the teacher explicitly grants an exception. The initial skeleton can be shorter; empty figure slots reserve writing space, not results. This is an editorial allocation, not a forced page layout.

| Content | Pages in the final five-page version |
|---|---:|
| Title, abstract and introduction | 0.45 |
| Dataset analysis, with one compact evidence figure/table | 0.80 |
| Architecture and design reasoning | 0.65 |
| Implementation and tools | 0.45 |
| Measured evaluation, figures and limitations | 1.50 |
| Visualization and conclusion | 0.35 |
| References | 0.25 |
| Individual reflections for the confirmed roster | 0.55 |
| **Total** | **5.00** |

A four-page edit can reduce background and implementation prose, but must preserve dataset analysis, measured evaluation and each person's three answers. If a fourth member joins, rebalance before writing. Do not solve overflow by changing ACM fonts or margins. Final issue #14 owns finished prose, final plots, confirmed author names and the submission check.

## Team and evidence

[team.md](team.md) records A/B/C and optional D responsibilities, an explicitly unconfirmed roster and each person's three reflection prompts. The existing [decision log](../docs/decisions.md) owns design reasoning. Link D-numbers rather than duplicating it. Personal implementation claims need actual PR/output links.

[experiments/](experiments/README.md) defines provenance and measurement fields and indexes the merged November validation. Do not rerun completed benchmarks to fill this skeleton. Evidence citations in the PDF pin a repository snapshot and state runtime revisions and run identities. Read that index before reusing results; no final performance claim is implied.

## Overleaf synchronization

Status on 2 November 2026: **no team Overleaf project or project URL has been confirmed**. Nothing was created or published for this task. GitHub report sources remain canonical. Choose one editor per sync interval and agree who synchronizes. Keep contribution notes and labeled Overleaf history because sync authorship may bundle several people's changes into one account.

The simplest workflow works without a paid sync feature:

1. Run `python3 report/export-overleaf.py`. It creates `report/build/overleaf.zip` containing only LaTeX, BibTeX and any `figures/` files, with `main.tex` at the root. It excludes datasets, validation JSON, build outputs and the rest of the code repository.
2. A team member uploads that ZIP to the team's chosen Overleaf project, sets **main.tex** as main document, uses **pdfLaTeX**, and records the project URL and selected TeX Live version in the team record. Choose a TeX Live version matching the local build where available; otherwise record and compare both renders.
3. Before editing, label the Overleaf version with the GitHub source SHA. During that interval, keep local report edits paused. Before importing local changes, download a source backup and resolve comments/tracked changes.
4. Download the edited **source** ZIP from Overleaf. Unpack into a temporary directory, compare with `report/`, and copy only intentional `.tex`, `.bib` and figure changes back. Preserve member attribution in the contribution notes and source history. Never replace the whole repository or overwrite generated evidence from a ZIP.
5. Build locally, inspect every page, review the diff and open a normal GitHub PR. After it merges, upload the new canonical source ZIP to Overleaf and label its revision. Repeat with a clean handoff. No automated bidirectional sync is claimed.

Optional [Git integration](https://docs.overleaf.com/integrations-and-add-ons/git-integration-and-github-synchronization/git-integration) and [GitHub sync](https://docs.overleaf.com/integrations-and-add-ons/git-integration-and-github-synchronization/github-synchronization) require eligible premium access, which is unconfirmed and untested here. Direct sync imports a whole repository, cannot attach an existing project to an existing repository, and has size limits. Keep the report-only ZIP workflow unless the team deliberately adopts another route. Neither integration supports Overleaf branches, and sync can lose tracked comments; check the linked current docs before enabling it.

## Sources and licenses

The brief and rubric are the course PDFs already in this repository. The dataset citation follows the brief's reference, with DOI [10.5281/zenodo.6382482](https://doi.org/10.5281/zenodo.6382482). Dataset material is CC BY-NC-SA 4.0 and is not distributed by this skeleton. Retain attribution, license and adaptation notices on figures derived from it. This task adds original report sources and uses the packaged ACM class/style rather than copying a sample paper or vendoring upstream files. It does not grant a new license to existing repository content. If class/style files are later vendored for Overleaf, preserve their upstream license and version notices.
