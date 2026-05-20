# OfficeScrap rewritten version

Default behavior is manifest-only. Matched files are pointed to in:

- `manifest.jsonl`
- `manifest.csv`
- `summary.json`
- `officescrap.sqlite3`

Use `--copy-matches` only when you want physical evidence copies under `output/evidence/`.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python3 main.py --data /path/to/data --config config.yaml --output ./out
```

With evidence copies:

```bash
python3 main.py --data /path/to/data --config config.yaml --output ./out --copy-matches
```

## Notes

- The output directory is automatically excluded from scanning.
- Logs avoid absolute paths where possible.
- Archive support includes zip, 7z, tar, tgz, tar.gz, tbz/tbz2, tar.bz2, txz/tar.xz, gz, bz2, and xz.
- OOXML files are scanned directly as ZIP/XML containers, including comments, headers, footers, slide notes, relationship hyperlinks, shared strings, metadata, custom XML, and embedded objects when extractable.
