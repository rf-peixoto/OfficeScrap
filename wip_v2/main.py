#!/usr/bin/env python3
"""
OfficeScrap rewritten scanner.

Key changes:
- Streaming search for text-like files.
- Aho-Corasick keyword matching instead of one regex per keyword.
- Manifest-first evidence model; file copying is opt-in with --copy-matches.
- SQLite persistence for files and per-file matches.
- Structured terminal progress.
- Safe archive support for zip, 7z, tar, tar.gz/tgz, tar.bz2/tbz2, tar.xz/txz, gz, bz2, xz.
- Deeper OOXML extraction for docx, pptx, xlsx: XML parts, comments, notes, headers,
  footers, shared strings, metadata, rel hyperlinks, and embedded files.
- Output directory is excluded automatically, even when placed under --data.
- Logs avoid absolute paths; paths are shown relative to --data when possible,
  otherwise relative to the current working directory.
"""

from __future__ import annotations

import argparse
import bz2
import csv
import gzip
import hashlib
import html
import json
import logging
import lzma
import mimetypes
import os
import queue
import re
import shutil
import signal
import sqlite3
import sys
import tarfile
import tempfile
import threading
import time
import zipfile
from dataclasses import dataclass, field
from email import policy
from email.parser import BytesParser
from pathlib import Path, PurePosixPath
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple
from xml.etree import ElementTree as ET

try:
    import ahocorasick  # type: ignore
except ImportError:  # pragma: no cover
    ahocorasick = None

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    yaml = None

try:
    import py7zr  # type: ignore
except ImportError:  # pragma: no cover
    py7zr = None

try:
    import PyPDF2  # type: ignore
except ImportError:  # pragma: no cover
    PyPDF2 = None

try:
    import openpyxl  # type: ignore
except ImportError:  # pragma: no cover
    openpyxl = None

try:
    import xlrd  # type: ignore
except ImportError:  # pragma: no cover
    xlrd = None

try:
    import extract_msg  # type: ignore
except ImportError:  # pragma: no cover
    extract_msg = None

try:
    from odf import text as odf_text, teletype  # type: ignore
    from odf.opendocument import load as load_odf  # type: ignore
except ImportError:  # pragma: no cover
    odf_text = None
    teletype = None
    load_odf = None


TEXT_EXTENSIONS = {
    "", ".txt", ".csv", ".tsv", ".json", ".jsonl", ".sql", ".xml", ".html", ".htm",
    ".log", ".conf", ".cfg", ".ini", ".toml", ".yaml", ".yml", ".md", ".rst",
    ".py", ".js", ".ts", ".java", ".go", ".rs", ".c", ".cpp", ".h", ".hpp",
    ".cs", ".php", ".rb", ".pl", ".sh", ".bash", ".ps1", ".bat", ".cmd",
}
OOXML_EXTENSIONS = {".docx", ".docm", ".dotx", ".dotm", ".pptx", ".pptm", ".potx", ".potm", ".xlsx", ".xlsm", ".xltx", ".xltm"}
ARCHIVE_EXTENSIONS = {".zip", ".7z", ".tar", ".tgz", ".tbz", ".tbz2", ".txz", ".gz", ".bz2", ".xz"}
CHUNK_SIZE = 1024 * 1024
OOXML_XML_LIMIT_BYTES = 128 * 1024 * 1024
MAX_FILENAME_LEN = 180


@dataclass(frozen=True)
class EvidenceRef:
    source_path: Path
    rel_source_path: str
    internal_path: str = ""
    display_path: str = ""
    size: int = 0
    mtime: float = 0.0
    file_type: str = ""


@dataclass
class MatchResult:
    evidence: EvidenceRef
    groups_to_keywords: Dict[str, Set[str]] = field(default_factory=dict)
    keyword_counts: Dict[str, int] = field(default_factory=dict)
    bytes_read: int = 0
    copied_to: str = ""
    error: str = ""

    @property
    def matched(self) -> bool:
        return bool(self.groups_to_keywords)


class Progress:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started = time.time()
        self.files_seen = 0
        self.files_scanned = 0
        self.files_matched = 0
        self.files_failed = 0
        self.bytes_read = 0
        self.matches = 0
        self.archives_opened = 0
        self.archive_members = 0
        self.running = True

    def add_seen(self, n: int = 1) -> None:
        with self.lock:
            self.files_seen += n

    def add_scanned(self, bytes_read: int, matched: bool, match_count: int) -> None:
        with self.lock:
            self.files_scanned += 1
            self.bytes_read += max(0, int(bytes_read))
            if matched:
                self.files_matched += 1
            self.matches += int(match_count)

    def add_failed(self) -> None:
        with self.lock:
            self.files_failed += 1

    def add_archive(self) -> None:
        with self.lock:
            self.archives_opened += 1

    def add_archive_member(self, n: int = 1) -> None:
        with self.lock:
            self.archive_members += n

    def snapshot(self) -> dict:
        with self.lock:
            elapsed = max(0.001, time.time() - self.started)
            mb = self.bytes_read / (1024 * 1024)
            return {
                "seen": self.files_seen,
                "scanned": self.files_scanned,
                "matched": self.files_matched,
                "failed": self.files_failed,
                "read_mb": mb,
                "speed_mb_s": mb / elapsed,
                "matches": self.matches,
                "archives": self.archives_opened,
                "archive_members": self.archive_members,
            }


def print_progress(progress: Progress, interval: float) -> None:
    while progress.running:
        time.sleep(interval)
        snap = progress.snapshot()
        print(
            f"[STATS] seen={snap['seen']} scanned={snap['scanned']} matched={snap['matched']} "
            f"failed={snap['failed']} read={snap['read_mb']:.2f}MB speed={snap['speed_mb_s']:.2f}MB/s "
            f"matches={snap['matches']} archives={snap['archives']} members={snap['archive_members']}",
            flush=True,
        )


def clean_log_path(path: Path, data_root: Path, cwd: Path) -> str:
    try:
        return path.resolve().relative_to(data_root.resolve()).as_posix()
    except Exception:
        pass
    try:
        return path.resolve().relative_to(cwd.resolve()).as_posix()
    except Exception:
        return path.name


def sanitize_component(value: str) -> str:
    value = value.replace("\\", "/")
    value = re.sub(r"[^A-Za-z0-9._@+=,()\[\]{} -]+", "_", value)
    value = value.strip(" .") or "unnamed"
    return value[:MAX_FILENAME_LEN]


def safe_join(base: Path, member_name: str) -> Optional[Path]:
    member = PurePosixPath(member_name.replace("\\", "/"))
    if member.is_absolute() or ".." in member.parts:
        return None
    candidate = (base / Path(*member.parts)).resolve()
    try:
        candidate.relative_to(base.resolve())
    except Exception:
        return None
    return candidate


def is_under(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False


def load_config(path: Path) -> Dict[str, List[str]]:
    if yaml is None:
        raise RuntimeError("Missing dependency: pyyaml. Install with: pip install -r requirements.txt")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    groups = data.get("Keyword_Groups")
    if not isinstance(groups, dict):
        raise ValueError("config.yaml must contain a dictionary named Keyword_Groups")
    out: Dict[str, List[str]] = {}
    for group, keywords in groups.items():
        if not isinstance(keywords, list):
            raise ValueError(f"Keyword_Groups.{group} must be a list")
        cleaned = [str(k) for k in keywords if str(k)]
        if cleaned:
            out[str(group)] = cleaned
    if not out:
        raise ValueError("No keywords found in config")
    return out


class AhoMatcher:
    def __init__(self, keyword_groups: Dict[str, List[str]], case_sensitive: bool = False) -> None:
        if ahocorasick is None:
            raise RuntimeError("Missing dependency: pyahocorasick. Install with: pip install -r requirements.txt")
        self.case_sensitive = case_sensitive
        self.keyword_to_groups: Dict[str, Set[str]] = {}
        self.display_keyword: Dict[str, str] = {}
        self.max_len = 1
        automaton = ahocorasick.Automaton()
        for group, keywords in keyword_groups.items():
            for keyword in keywords:
                norm = self.norm(keyword)
                if not norm:
                    continue
                self.keyword_to_groups.setdefault(norm, set()).add(group)
                self.display_keyword.setdefault(norm, keyword)
                self.max_len = max(self.max_len, len(norm))
        for norm in self.keyword_to_groups:
            automaton.add_word(norm, norm)
        automaton.make_automaton()
        self.automaton = automaton

    def norm(self, text: str) -> str:
        return text if self.case_sensitive else text.casefold()

    def scan_text_units(self, units: Iterable[str]) -> Tuple[Dict[str, Set[str]], Dict[str, int], int]:
        groups: Dict[str, Set[str]] = {}
        counts: Dict[str, int] = {}
        tail = ""
        bytes_read = 0
        overlap = max(0, self.max_len - 1)
        for unit in units:
            if not unit:
                continue
            bytes_read += len(unit.encode("utf-8", errors="ignore"))
            norm_unit = self.norm(unit)
            combined = tail + norm_unit
            tail_len = len(tail)
            for end_idx, norm_kw in self.automaton.iter(combined):
                start_idx = end_idx - len(norm_kw) + 1
                if start_idx < tail_len:
                    continue
                display = self.display_keyword[norm_kw]
                counts[display] = counts.get(display, 0) + 1
                for group in self.keyword_to_groups[norm_kw]:
                    groups.setdefault(group, set()).add(display)
            tail = combined[-overlap:] if overlap else ""
        return groups, counts, bytes_read


def iter_text_file(path: Path, chunk_size: int = CHUNK_SIZE) -> Iterator[str]:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk


def strip_html(value: str) -> str:
    value = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", value)
    value = re.sub(r"(?s)<[^>]+>", " ", value)
    return html.unescape(value)


def iter_xml_text_bytes(data: bytes) -> Iterator[str]:
    try:
        root = ET.fromstring(data)
    except Exception:
        text = data.decode("utf-8", errors="ignore")
        yield strip_html(text)
        return
    for elem in root.iter():
        if elem.text and elem.text.strip():
            yield elem.text
            yield "\n"
        for key, value in elem.attrib.items():
            if value and key.lower().endswith(("target", "tooltip", "title", "descr", "name")):
                yield value
                yield "\n"
        if elem.tail and elem.tail.strip():
            yield elem.tail
            yield "\n"


def iter_ooxml(path: Path, temp_root: Path, recurse_callback) -> Iterator[str]:
    # OOXML files are ZIP containers. Instead of relying only on python-docx/python-pptx/openpyxl,
    # scan meaningful XML and relationship parts directly. This catches comments, headers, footers,
    # slide notes, metadata, hidden sheets/sharedStrings, hyperlinks, and many custom XML fields.
    try:
        with zipfile.ZipFile(path) as zf:
            for info in zf.infolist():
                name = info.filename
                if info.is_dir() or name.startswith("__MACOSX/"):
                    continue
                lower = name.lower()
                if info.file_size > OOXML_XML_LIMIT_BYTES:
                    logging.warning("Skipping large OOXML part %s inside %s", name, path.name)
                    continue
                if lower.endswith((".xml", ".rels", ".vml")):
                    try:
                        yield f"\n[OOXML_PART:{name}]\n"
                        yield from iter_xml_text_bytes(zf.read(info))
                    except Exception as e:
                        logging.warning("Failed reading OOXML XML part %s in %s: %s", name, path.name, e)
                elif any(marker in lower for marker in ("/embeddings/", "/media/", "/oleobjects/")):
                    # Extract embedded files safely and let the normal scanner inspect them.
                    member_path = safe_join(temp_root, name)
                    if member_path is None:
                        continue
                    member_path.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        with zf.open(info) as src, member_path.open("wb") as dst:
                            shutil.copyfileobj(src, dst, CHUNK_SIZE)
                        yield f"\n[EMBEDDED_FILE:{name}]\n"
                        yield from recurse_callback(member_path, name)
                    except Exception as e:
                        logging.warning("Failed extracting OOXML embedded part %s in %s: %s", name, path.name, e)
    except zipfile.BadZipFile:
        return


def iter_pdf(path: Path) -> Iterator[str]:
    if PyPDF2 is None:
        logging.warning("PyPDF2 is not installed; skipping PDF text extraction for %s", path.name)
        return
    try:
        with path.open("rb") as f:
            reader = PyPDF2.PdfReader(f)
            if reader.metadata:
                yield str(reader.metadata)
                yield "\n"
            for i, page in enumerate(reader.pages):
                text = page.extract_text() or ""
                if text:
                    yield f"\n[PDF_PAGE:{i + 1}]\n"
                    yield text
    except Exception as e:
        logging.warning("PDF extraction failed for %s: %s", path.name, e)


def iter_xls(path: Path) -> Iterator[str]:
    if xlrd is None:
        logging.warning("xlrd is not installed; skipping legacy XLS extraction for %s", path.name)
        return
    try:
        book = xlrd.open_workbook(str(path), on_demand=True)
        for sheet_name in book.sheet_names():
            sheet = book.sheet_by_name(sheet_name)
            yield f"\n[XLS_SHEET:{sheet_name}]\n"
            for row_idx in range(sheet.nrows):
                yield "\t".join(str(c.value) for c in sheet.row(row_idx))
                yield "\n"
    except Exception as e:
        logging.warning("XLS extraction failed for %s: %s", path.name, e)


def iter_odf(path: Path) -> Iterator[str]:
    if load_odf is None or odf_text is None or teletype is None:
        logging.warning("odfpy is not installed; skipping ODF extraction for %s", path.name)
        return
    try:
        doc = load_odf(str(path))
        for paragraph in doc.getElementsByType(odf_text.P):
            value = teletype.extractText(paragraph)
            if value:
                yield value
                yield "\n"
    except Exception as e:
        logging.warning("ODF extraction failed for %s: %s", path.name, e)


def iter_eml(path: Path, temp_root: Path, recurse_callback) -> Iterator[str]:
    try:
        with path.open("rb") as f:
            msg = BytesParser(policy=policy.default).parse(f)
        for header in ("from", "to", "cc", "bcc", "subject", "date", "message-id"):
            value = msg.get(header)
            if value:
                yield f"{header}: {value}\n"
        for part in msg.walk():
            ctype = part.get_content_type()
            filename = part.get_filename()
            if ctype == "text/plain":
                yield str(part.get_content())
                yield "\n"
            elif ctype == "text/html":
                yield strip_html(str(part.get_content()))
                yield "\n"
            elif filename:
                safe_name = sanitize_component(filename)
                dst = temp_root / safe_name
                try:
                    payload = part.get_payload(decode=True)
                    if payload:
                        dst.write_bytes(payload)
                        yield f"\n[EML_ATTACHMENT:{filename}]\n"
                        yield from recurse_callback(dst, filename)
                except Exception as e:
                    logging.warning("Failed extracting EML attachment %s in %s: %s", filename, path.name, e)
    except Exception as e:
        logging.warning("EML extraction failed for %s: %s", path.name, e)


def iter_msg(path: Path) -> Iterator[str]:
    if extract_msg is None:
        logging.warning("extract-msg is not installed; skipping MSG extraction for %s", path.name)
        return
    try:
        msg = extract_msg.Message(str(path))
        for value in (msg.sender, msg.to, msg.cc, msg.date, msg.subject, msg.body):
            if value:
                yield str(value)
                yield "\n"
    except Exception as e:
        logging.warning("MSG extraction failed for %s: %s", path.name, e)


class ResultStore:
    def __init__(self, db_path: Path) -> None:
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                data_root TEXT NOT NULL,
                output_root TEXT NOT NULL,
                config_path TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                source_path TEXT NOT NULL,
                internal_path TEXT NOT NULL DEFAULT '',
                display_path TEXT NOT NULL,
                file_type TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime REAL NOT NULL,
                bytes_read INTEGER NOT NULL DEFAULT 0,
                matched INTEGER NOT NULL DEFAULT 0,
                copied_to TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                UNIQUE(run_id, source_path, internal_path)
            );
            CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                file_id INTEGER NOT NULL,
                keyword_group TEXT NOT NULL,
                keyword TEXT NOT NULL,
                count INTEGER NOT NULL,
                FOREIGN KEY(file_id) REFERENCES files(id)
            );
            CREATE INDEX IF NOT EXISTS idx_files_run ON files(run_id);
            CREATE INDEX IF NOT EXISTS idx_matches_run ON matches(run_id);
            CREATE INDEX IF NOT EXISTS idx_matches_keyword ON matches(keyword);
            """
        )
        self.conn.commit()

    def create_run(self, data_root: Path, output_root: Path, config_path: Path) -> int:
        with self.lock:
            cur = self.conn.execute(
                "INSERT INTO runs(started_at, data_root, output_root, config_path) VALUES(datetime('now'), ?, ?, ?)",
                (str(data_root), str(output_root), str(config_path)),
            )
            self.conn.commit()
            return int(cur.lastrowid)

    def save_result(self, run_id: int, result: MatchResult) -> None:
        ev = result.evidence
        with self.lock:
            cur = self.conn.execute(
                """
                INSERT OR REPLACE INTO files(
                    run_id, source_path, internal_path, display_path, file_type, size, mtime,
                    bytes_read, matched, copied_to, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    ev.rel_source_path,
                    ev.internal_path,
                    ev.display_path,
                    ev.file_type,
                    ev.size,
                    ev.mtime,
                    result.bytes_read,
                    1 if result.matched else 0,
                    result.copied_to,
                    result.error,
                ),
            )
            file_id = int(cur.lastrowid)
            for group, keywords in result.groups_to_keywords.items():
                for keyword in sorted(keywords):
                    self.conn.execute(
                        "INSERT INTO matches(run_id, file_id, keyword_group, keyword, count) VALUES (?, ?, ?, ?, ?)",
                        (run_id, file_id, group, keyword, int(result.keyword_counts.get(keyword, 1))),
                    )
            self.conn.commit()

    def close(self) -> None:
        with self.lock:
            self.conn.commit()
            self.conn.close()


class ManifestWriter:
    def __init__(self, output_dir: Path) -> None:
        self.lock = threading.Lock()
        self.jsonl_path = output_dir / "manifest.jsonl"
        self.csv_path = output_dir / "manifest.csv"
        self.jsonl = self.jsonl_path.open("w", encoding="utf-8")
        self.csv = self.csv_path.open("w", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(
            self.csv,
            fieldnames=["display_path", "source_path", "internal_path", "file_type", "size", "groups", "keywords", "copied_to"],
        )
        self.writer.writeheader()

    def write(self, result: MatchResult) -> None:
        if not result.matched:
            return
        ev = result.evidence
        keywords = sorted({kw for kws in result.groups_to_keywords.values() for kw in kws})
        record = {
            "display_path": ev.display_path,
            "source_path": ev.rel_source_path,
            "internal_path": ev.internal_path,
            "file_type": ev.file_type,
            "size": ev.size,
            "groups": {group: sorted(values) for group, values in result.groups_to_keywords.items()},
            "keyword_counts": result.keyword_counts,
            "copied_to": result.copied_to,
        }
        with self.lock:
            self.jsonl.write(json.dumps(record, ensure_ascii=False) + "\n")
            self.jsonl.flush()
            self.writer.writerow(
                {
                    "display_path": ev.display_path,
                    "source_path": ev.rel_source_path,
                    "internal_path": ev.internal_path,
                    "file_type": ev.file_type,
                    "size": ev.size,
                    "groups": ";".join(sorted(result.groups_to_keywords)),
                    "keywords": ";".join(keywords),
                    "copied_to": result.copied_to,
                }
            )
            self.csv.flush()

    def close(self) -> None:
        with self.lock:
            self.jsonl.close()
            self.csv.close()


class Scanner:
    def __init__(
        self,
        data_root: Path,
        output_dir: Path,
        matcher: AhoMatcher,
        store: ResultStore,
        manifest: ManifestWriter,
        run_id: int,
        copy_matches: bool,
        max_archive_depth: int,
        progress: Progress,
        cwd: Path,
    ) -> None:
        self.data_root = data_root.resolve()
        self.output_dir = output_dir.resolve()
        self.matcher = matcher
        self.store = store
        self.manifest = manifest
        self.run_id = run_id
        self.copy_matches = copy_matches
        self.max_archive_depth = max_archive_depth
        self.progress = progress
        self.cwd = cwd.resolve()
        self.seen_archives: Set[Tuple[str, str]] = set()

    def rel(self, path: Path) -> str:
        return clean_log_path(path, self.data_root, self.cwd)

    def evidence_for(self, source: Path, internal_path: str = "") -> EvidenceRef:
        try:
            st = source.stat()
            size = st.st_size
            mtime = st.st_mtime
        except Exception:
            size = 0
            mtime = 0.0
        rel_source = self.rel(source)
        display = rel_source if not internal_path else f"{rel_source}::{internal_path}"
        file_type = Path(internal_path).suffix.lower() if internal_path else source.suffix.lower()
        return EvidenceRef(source, rel_source, internal_path, display, size, mtime, file_type)

    def copy_evidence(self, ev: EvidenceRef) -> str:
        if not self.copy_matches:
            return ""
        root = self.output_dir / "evidence"
        source_parts = [sanitize_component(p) for p in Path(ev.rel_source_path).parts]
        if ev.internal_path:
            digest = hashlib.sha256(ev.display_path.encode("utf-8", errors="ignore")).hexdigest()[:12]
            dest = root / Path(*source_parts) / f"container_{digest}{ev.source_path.suffix}"
        else:
            dest = root / Path(*source_parts)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            return self.rel(dest)
        try:
            shutil.copy2(ev.source_path, dest)
            return self.rel(dest)
        except Exception as e:
            logging.warning("Failed copying evidence %s: %s", ev.display_path, e)
            return ""

    def scan_path(self, path: Path, internal_path: str = "", depth: int = 0) -> Optional[MatchResult]:
        ev = self.evidence_for(path, internal_path)
        result = MatchResult(ev)
        ext = ev.file_type.lower()
        temp_dir_obj = None
        try:
            if depth <= self.max_archive_depth and self.is_archive(path, ext) and not internal_path:
                self.scan_archive(path, depth)
                # The archive container itself can also contain text-like names or comments, but we do not count it as scanned text.
                return None

            temp_dir_obj = tempfile.TemporaryDirectory(prefix="officescrap_embedded_")
            temp_root = Path(temp_dir_obj.name)
            units = self.iter_units(path, ext, temp_root, depth, internal_path)
            groups, counts, bytes_read = self.matcher.scan_text_units(units)
            result.groups_to_keywords = groups
            result.keyword_counts = counts
            result.bytes_read = bytes_read
            if result.matched:
                result.copied_to = self.copy_evidence(ev)
                self.manifest.write(result)
            self.store.save_result(self.run_id, result)
            self.progress.add_scanned(bytes_read, result.matched, sum(counts.values()))
            return result
        except Exception as e:
            result.error = str(e)
            self.store.save_result(self.run_id, result)
            self.progress.add_failed()
            logging.warning("Failed processing %s: %s", ev.display_path, e)
            return result
        finally:
            if temp_dir_obj is not None:
                temp_dir_obj.cleanup()

    def iter_units(self, path: Path, ext: str, temp_root: Path, depth: int, internal_prefix: str) -> Iterator[str]:
        if ext in TEXT_EXTENSIONS:
            yield from iter_text_file(path)
        elif ext in OOXML_EXTENSIONS:
            def cb(member: Path, member_name: str) -> Iterator[str]:
                nested_internal = f"{internal_prefix}/{member_name}" if internal_prefix else member_name
                nested = self.scan_embedded_to_units(member, nested_internal, depth + 1)
                yield from nested
            yield from iter_ooxml(path, temp_root, cb)
        elif ext == ".pdf":
            yield from iter_pdf(path)
        elif ext == ".xls":
            yield from iter_xls(path)
        elif ext in {".odt", ".ods", ".odp"}:
            yield from iter_odf(path)
        elif ext == ".eml":
            def cb(member: Path, member_name: str) -> Iterator[str]:
                nested_internal = f"{internal_prefix}/{member_name}" if internal_prefix else member_name
                yield from self.scan_embedded_to_units(member, nested_internal, depth + 1)
            yield from iter_eml(path, temp_root, cb)
        elif ext == ".msg":
            yield from iter_msg(path)
        else:
            # Last-resort text decode for unknown small/medium files. This helps extensionless dumps.
            if self.looks_textual(path):
                yield from iter_text_file(path)

    def scan_embedded_to_units(self, member: Path, internal_path: str, depth: int) -> Iterator[str]:
        if depth > self.max_archive_depth:
            return
        ext = member.suffix.lower()
        if self.is_archive(member, ext):
            self.scan_archive(member, depth, outer_internal=internal_path)
            return
        res = self.scan_path(member, internal_path=internal_path, depth=depth)
        if res and res.matched:
            yield f"\n[EMBEDDED_MATCH:{internal_path}]\n"
            for group, keywords in res.groups_to_keywords.items():
                yield group + " " + " ".join(sorted(keywords)) + "\n"

    def looks_textual(self, path: Path) -> bool:
        try:
            with path.open("rb") as f:
                sample = f.read(4096)
            if not sample:
                return False
            return b"\x00" not in sample and sum(1 for b in sample if b < 9 or (13 < b < 32)) < len(sample) * 0.05
        except Exception:
            return False

    def is_archive(self, path: Path, ext: str) -> bool:
        name = path.name.lower()
        return ext in ARCHIVE_EXTENSIONS or name.endswith((".tar.gz", ".tar.bz2", ".tar.xz"))

    def scan_archive(self, path: Path, depth: int, outer_internal: str = "") -> None:
        if depth >= self.max_archive_depth:
            return
        key = (str(path.resolve()), outer_internal)
        if key in self.seen_archives:
            return
        self.seen_archives.add(key)
        self.progress.add_archive()
        logging.info("Opening archive %s", self.rel(path))
        with tempfile.TemporaryDirectory(prefix="officescrap_archive_") as tmp:
            tmp_root = Path(tmp)
            members = self.extract_archive_safe(path, tmp_root)
            for member_path, member_name in members:
                self.progress.add_archive_member()
                internal = f"{outer_internal}/{member_name}" if outer_internal else member_name
                self.scan_path(member_path, internal_path=internal, depth=depth + 1)

    def extract_archive_safe(self, path: Path, dest: Path) -> List[Tuple[Path, str]]:
        lower = path.name.lower()
        out: List[Tuple[Path, str]] = []
        try:
            if zipfile.is_zipfile(path):
                with zipfile.ZipFile(path) as zf:
                    for info in zf.infolist():
                        if info.is_dir():
                            continue
                        target = safe_join(dest, info.filename)
                        if target is None:
                            logging.warning("Blocked unsafe ZIP member %s in %s", info.filename, path.name)
                            continue
                        target.parent.mkdir(parents=True, exist_ok=True)
                        with zf.open(info) as src, target.open("wb") as dst:
                            shutil.copyfileobj(src, dst, CHUNK_SIZE)
                        out.append((target, info.filename))
            elif tarfile.is_tarfile(path):
                with tarfile.open(path) as tf:
                    for member in tf.getmembers():
                        if not member.isfile():
                            continue
                        target = safe_join(dest, member.name)
                        if target is None:
                            logging.warning("Blocked unsafe TAR member %s in %s", member.name, path.name)
                            continue
                        src = tf.extractfile(member)
                        if src is None:
                            continue
                        target.parent.mkdir(parents=True, exist_ok=True)
                        with src, target.open("wb") as dst:
                            shutil.copyfileobj(src, dst, CHUNK_SIZE)
                        out.append((target, member.name))
            elif lower.endswith(".7z"):
                if py7zr is None:
                    logging.warning("py7zr is not installed; skipping 7z archive %s", path.name)
                    return out
                with py7zr.SevenZipFile(path, mode="r") as archive:
                    names = archive.getnames()
                    archive.extractall(path=dest)
                for name in names:
                    target = safe_join(dest, name)
                    if target and target.is_file():
                        out.append((target, name))
            elif lower.endswith(".gz") and not lower.endswith((".tar.gz", ".tgz")):
                name = Path(path.stem).name or "decompressed"
                target = dest / name
                with gzip.open(path, "rb") as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst, CHUNK_SIZE)
                out.append((target, name))
            elif lower.endswith(".bz2") and not lower.endswith((".tar.bz2", ".tbz", ".tbz2")):
                name = Path(path.stem).name or "decompressed"
                target = dest / name
                with bz2.open(path, "rb") as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst, CHUNK_SIZE)
                out.append((target, name))
            elif lower.endswith(".xz") and not lower.endswith((".tar.xz", ".txz")):
                name = Path(path.stem).name or "decompressed"
                target = dest / name
                with lzma.open(path, "rb") as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst, CHUNK_SIZE)
                out.append((target, name))
        except Exception as e:
            logging.warning("Archive extraction failed for %s: %s", path.name, e)
        return out


def setup_logging(output_dir: Path, verbose: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "officescrap.log"
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root.addHandler(file_handler)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO if verbose else logging.WARNING)
    console.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    root.addHandler(console)


def iter_input_files(data_root: Path, output_dir: Path) -> Iterator[Path]:
    for dirpath, dirnames, filenames in os.walk(data_root):
        current = Path(dirpath)
        # Exclude output dir from traversal to avoid self-scanning copied evidence, logs, DB, manifests, stats.
        dirnames[:] = [d for d in dirnames if not is_under(current / d, output_dir)]
        for name in filenames:
            p = current / name
            if is_under(p, output_dir):
                continue
            yield p


def build_summary(db_path: Path, run_id: int, output_dir: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        summary = {
            "run_id": run_id,
            "files": {},
            "groups": {},
            "keywords": {},
            "matched_files": [],
        }
        cur = conn.execute(
            "SELECT COUNT(*), SUM(matched), SUM(bytes_read), SUM(CASE WHEN error != '' THEN 1 ELSE 0 END) FROM files WHERE run_id=?",
            (run_id,),
        )
        total, matched, bytes_read, failed = cur.fetchone()
        summary["files"] = {
            "scanned": total or 0,
            "matched": matched or 0,
            "failed": failed or 0,
            "bytes_read": bytes_read or 0,
            "read_gb": round((bytes_read or 0) / (1024 ** 3), 4),
        }
        for group, count in conn.execute(
            "SELECT keyword_group, COUNT(DISTINCT file_id) FROM matches WHERE run_id=? GROUP BY keyword_group ORDER BY keyword_group",
            (run_id,),
        ):
            summary["groups"][group] = count
        for keyword, count in conn.execute(
            "SELECT keyword, SUM(count) FROM matches WHERE run_id=? GROUP BY keyword ORDER BY keyword",
            (run_id,),
        ):
            summary["keywords"][keyword] = count
        for row in conn.execute(
            """
            SELECT f.display_path, f.copied_to, GROUP_CONCAT(DISTINCT m.keyword_group), GROUP_CONCAT(DISTINCT m.keyword)
            FROM files f JOIN matches m ON f.id=m.file_id
            WHERE f.run_id=?
            GROUP BY f.id
            ORDER BY f.display_path
            """,
            (run_id,),
        ):
            summary["matched_files"].append({
                "display_path": row[0],
                "copied_to": row[1],
                "groups": row[2].split(",") if row[2] else [],
                "keywords": row[3].split(",") if row[3] else [],
            })
        with (output_dir / "summary.json").open("w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
    finally:
        conn.close()


def worker(q: "queue.Queue[Optional[Path]]", scanner: Scanner) -> None:
    while True:
        path = q.get()
        try:
            if path is None:
                return
            scanner.scan_path(path)
        finally:
            q.task_done()


def main() -> int:
    parser = argparse.ArgumentParser(description="OfficeScrap - streaming keyword scanner with SQLite evidence manifest.")
    parser.add_argument("--data", required=True, help="Directory to scan")
    parser.add_argument("--config", required=True, help="YAML file containing Keyword_Groups")
    parser.add_argument("--output", required=True, help="Output directory for DB, manifests, logs, optional evidence copies")
    parser.add_argument("--threads", type=int, default=max(2, min(8, (os.cpu_count() or 4))), help="Worker threads")
    parser.add_argument("--copy-matches", action="store_true", help="Copy matched source files/containers into output/evidence. Default is manifest only.")
    parser.add_argument("--case-sensitive", action="store_true", help="Use case-sensitive keyword matching")
    parser.add_argument("--max-archive-depth", type=int, default=3, help="Maximum nested archive depth")
    parser.add_argument("--progress-interval", type=float, default=5.0, help="Seconds between progress lines")
    parser.add_argument("--verbose", action="store_true", help="Print INFO logs to console")
    args = parser.parse_args()

    data_root = Path(args.data).expanduser().resolve()
    output_dir = Path(args.output).expanduser().resolve()
    config_path = Path(args.config).expanduser().resolve()
    cwd = Path.cwd().resolve()

    if not data_root.is_dir():
        print(f"ERROR: --data is not a directory: {data_root}", file=sys.stderr)
        return 2
    output_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(output_dir, args.verbose)

    keyword_groups = load_config(config_path)
    matcher = AhoMatcher(keyword_groups, case_sensitive=args.case_sensitive)
    db_path = output_dir / "officescrap.sqlite3"
    store = ResultStore(db_path)
    manifest = ManifestWriter(output_dir)
    run_id = store.create_run(data_root, output_dir, config_path)
    progress = Progress()
    scanner = Scanner(
        data_root=data_root,
        output_dir=output_dir,
        matcher=matcher,
        store=store,
        manifest=manifest,
        run_id=run_id,
        copy_matches=args.copy_matches,
        max_archive_depth=args.max_archive_depth,
        progress=progress,
        cwd=cwd,
    )

    stop = threading.Event()

    def handle_signal(signum, frame):
        stop.set()
        progress.running = False
        logging.warning("Received signal %s; stopping after queued work", signum)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    progress_thread = threading.Thread(target=print_progress, args=(progress, args.progress_interval), daemon=True)
    progress_thread.start()

    q: "queue.Queue[Optional[Path]]" = queue.Queue(maxsize=args.threads * 4)
    workers = [threading.Thread(target=worker, args=(q, scanner), daemon=True) for _ in range(args.threads)]
    for t in workers:
        t.start()

    try:
        for path in iter_input_files(data_root, output_dir):
            if stop.is_set():
                break
            progress.add_seen()
            q.put(path)
    finally:
        for _ in workers:
            q.put(None)
        q.join()
        progress.running = False
        for t in workers:
            t.join(timeout=2)
        manifest.close()
        build_summary(db_path, run_id, output_dir)
        store.close()
        snap = progress.snapshot()
        print(
            f"[DONE] run_id={run_id} scanned={snap['scanned']} matched={snap['matched']} failed={snap['failed']} "
            f"read={snap['read_mb']:.2f}MB speed={snap['speed_mb_s']:.2f}MB/s matches={snap['matches']}",
            flush=True,
        )
        print(f"[OUTPUT] database={db_path}")
        print(f"[OUTPUT] manifest_jsonl={output_dir / 'manifest.jsonl'}")
        print(f"[OUTPUT] manifest_csv={output_dir / 'manifest.csv'}")
        print(f"[OUTPUT] summary={output_dir / 'summary.json'}")
        if args.copy_matches:
            print(f"[OUTPUT] copied_evidence={output_dir / 'evidence'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
