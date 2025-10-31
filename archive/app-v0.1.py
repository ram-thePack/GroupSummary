#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Weekly summaries for Indie Parents Pack groups (privacy-safe).

Features:
- Discovers ALL groups whose name contains 'Indie Parents Pack' (1–6, future too)
- No UserName/Phone usage anywhere
- Robust IST timezone (tzdata fallback, or fixed +05:30)
- Debug logs (SQL, chunking, LLM calls, word counts)
- Chunked LLM calls + final synthesis
- 250–300 word group summaries (hard-clamped <=300)
- Optional rollup (also clamped) across all Indie groups

Run:
  python app.py --week-start 2025-08-04 --dry-run --debug
"""

import os
import re
import json
import argparse
import datetime as dt
import logging
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional, Set

# --- .env loader (Windows-friendly) ---
from dotenv import load_dotenv
load_dotenv()

# --- Robust IST timezone (tzdata fallback, then fixed +05:30) ---
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from datetime import timezone, timedelta

def get_ist_tz():
    try:
        return ZoneInfo("Asia/Kolkata")
    except ZoneInfoNotFoundError:
        try:
            import tzdata  # ensure package present
            return ZoneInfo("Asia/Kolkata")
        except Exception:
            # Final fallback: fixed +05:30 (IST has no DST)
            return timezone(timedelta(hours=5, minutes=30))

TZ_IST = get_ist_tz()

import pymysql
from openai import OpenAI

# ===== Logging =====
def setup_logger(debug: bool) -> logging.Logger:
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"run_{ts}.log"

    level = logging.DEBUG if debug else logging.INFO
    fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("indie_summary")
    logger.setLevel(level)
    logger.handlers.clear()

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(fmt, datefmt))
    logger.addHandler(ch)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(fmt, datefmt))
    logger.addHandler(fh)

    logger.debug(f"Logger initialized. Writing to {log_path}")
    return logger

log = None  # set in __main__

# ===== Summarization budgets =====
MAX_CHARS_PER_CHUNK = 5000       # safe payload per chunk call
CHUNK_MAX_TOKENS    = 450        # LLM budget per chunk
FINAL_MAX_TOKENS    = 700        # LLM budget for final 250–300 words
TARGET_WORDS_LOW    = 250
TARGET_WORDS_HIGH   = 300

def word_count(s: str) -> int:
    return len(re.findall(r"\S+", s or ""))

def clamp_words(text: str, max_words: int = TARGET_WORDS_HIGH) -> str:
    words = re.findall(r"\S+", text or "")
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]) + "…"

# ===== URL extraction (robust, dedup) =====
URL_REGEX = re.compile(r"""https?://[^\s<>'")\]}]+""", re.IGNORECASE)

def extract_urls(*texts: Optional[str]) -> Set[str]:
    urls: Set[str] = set()
    for t in texts:
        if not t:
            continue
        for m in URL_REGEX.findall(t):
            urls.add(m.rstrip('.,);!?"\''))
    return urls

# ===== Date windowing (IST) =====
def monday_ist(d: dt.date) -> dt.datetime:
    base = dt.datetime(d.year, d.month, d.day, 0, 0, tzinfo=TZ_IST)
    return base - dt.timedelta(days=base.weekday())

def last_week_bounds_ist(now_ist: dt.datetime) -> Tuple[dt.datetime, dt.datetime]:
    this_monday = monday_ist(now_ist.date())
    last_monday = this_monday - dt.timedelta(days=7)
    return last_monday, this_monday  # [inclusive, exclusive)

def compute_bounds(week_start_str: Optional[str]) -> Tuple[dt.datetime, dt.datetime, dt.date]:
    if week_start_str:
        try:
            d = dt.datetime.strptime(week_start_str, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit("--week-start must be YYYY-MM-DD")
        week_start_ist = monday_ist(d)
        week_end_ist = week_start_ist + dt.timedelta(days=7)
    else:
        now_ist = dt.datetime.now(TZ_IST)
        week_start_ist, week_end_ist = last_week_bounds_ist(now_ist)

    if log:
        log.info(f"IST week window: {week_start_ist} → {week_end_ist} (exclusive)")
    return week_start_ist, week_end_ist, week_start_ist.date()

# ===== DB client =====
def build_db_cfg() -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": True,
    }
    if os.getenv("DB_SSL_CA"):
        cfg["ssl"] = {"ca": os.getenv("DB_SSL_CA")}
    if log:
        redacted_host = (cfg["host"] or "").split(".")[0] + "... " if cfg.get("host") else "(missing)"
        log.debug(f"DB config: host={redacted_host} db={cfg.get('database')} user={cfg.get('user')}")
    return cfg

def connect_db():
    try:
        conn = pymysql.connect(**build_db_cfg())
        if log:
            log.info("MySQL connection established.")
        return conn
    except Exception as e:
        if log:
            log.exception("Failed to connect to MySQL.")
        raise

# ===== Group discovery =====
def discover_indie_group_names(cur) -> List[str]:
    """
    Discover ALL distinct WhatsApp group names containing 'Indie Parents Pack'.
    Returns exact DB strings (emoji-safe), sorted: base first, then 2,3,4,...
    """
    sql = """
        SELECT DISTINCT GroupName AS name
        FROM WhatsAppExport
        WHERE GroupName LIKE %s
    """
    param = ("%Indie Parents Pack%",)

    if log:
        try:
            q = cur.mogrify(sql, param)
            log.debug(f"[discover] SQL → {q.decode('utf-8','ignore')}")
        except Exception:
            log.debug(f"[discover] SQL → {sql.strip()} | params={param}")

    cur.execute(sql, param)
    names = [r["name"] for r in cur.fetchall()]
    if log:
        log.info(f"[discover] Found {len(names)} Indie groups: {names}")

    def order_key(name: str):
        m = re.search(r'(\d+)\s*$', name)
        return (0 if not m else int(m.group(1)))

    names.sort(key=order_key)
    return names

# ===== DB helpers (NO NAMES OR PHONES) =====
def fetch_group_rows(cur, group_name: str, dt_from: dt.datetime, dt_to: dt.datetime) -> List[Dict[str, Any]]:
    """
    Privacy-safe: do NOT select UserName or Phone. Exact match on discovered name.
    """
    sql = """
        SELECT
          Message AS msg,
          CreatedDate,
          COALESCE(NULLIF(TRIM(Links),''), NULL) AS link
        FROM WhatsAppExport
        WHERE GroupName = %s
          AND CreatedDate >= %s
          AND CreatedDate <  %s
        ORDER BY CreatedDate ASC
    """
    params = (group_name, dt_from.replace(tzinfo=None), dt_to.replace(tzinfo=None))

    if log:
        try:
            q = cur.mogrify(sql, params)
            log.debug(f"SQL → {q.decode('utf-8', 'ignore')}")
        except Exception:
            log.debug(f"SQL → {sql.strip()} | params={params}")

    cur.execute(sql, params)
    rows = cur.fetchall()
    if log:
        log.info(f"Fetched {len(rows)} rows from MySQL for '{group_name}'.")
    return rows

def upsert_summary(cur, *, scope: str, canonical: str, group_name: Optional[str],
                   week_start: dt.date, message_count: int, participants: int, links: int,
                   summary: str, highlights: List[str], keywords: List[str]) -> None:
    if log:
        log.debug(
            f"Upserting summary: scope={scope} group={group_name or '(rollup)'} "
            f"week_start={week_start} msgs={message_count} links={links} "
            f"hl_count={len(highlights)} kw_count={len(keywords)}"
        )
    cur.execute("""
        INSERT INTO WeeklySummaries
          (scope, canonical, group_name, week_start, message_count, participants, links, keywords, summary, highlights)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          message_count=VALUES(message_count),
          participants=VALUES(participants),
          links=VALUES(links),
          keywords=VALUES(keywords),
          summary=VALUES(summary),
          highlights=VALUES(highlights)
    """, (
        scope, canonical, group_name, week_start, message_count, participants, links,
        json.dumps(keywords, ensure_ascii=False),
        summary,
        json.dumps(highlights, ensure_ascii=False),
    ))

# ===== Groq (OpenAI-compatible) =====
def groq_client() -> OpenAI:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        if log:
            log.error("GROQ_API_KEY missing.")
        raise SystemExit("Missing GROQ_API_KEY. Set it in .env or your environment.")
    if log:
        log.debug("Groq client initialized (OpenAI-compatible).")
    return OpenAI(base_url="https://api.groq.com/openai/v1", api_key=api_key)

GROQ_MODEL = "llama-3.1-8b-instant"

# ===== Chunking & LLM wrappers =====
def chunk_lines(lines: List[str], max_chars: int = MAX_CHARS_PER_CHUNK) -> List[str]:
    """Split long weekly logs into safe-sized chunks by character count."""
    chunks, buf, size = [], [], 0
    for ln in lines:
        ln = ln if len(ln) <= 1000 else (ln[:1000] + " …")
        if size + len(ln) + 1 > max_chars and buf:
            chunks.append("\n".join(buf)); buf, size = [], 0
        buf.append(ln); size += len(ln) + 1
    if buf: chunks.append("\n".join(buf))
    if log:
        log.info(f"Chunked {len(lines)} lines into {len(chunks)} chunk(s).")
        for i, c in enumerate(chunks, 1):
            log.debug(f"Chunk {i}: {len(c)} chars")
    return chunks

def llm_json(system_hint: str, user_text: str, max_tokens: int) -> Dict[str, Any]:
    client = groq_client()
    if log:
        log.info(f"LLM request → model={GROQ_MODEL} payload_chars={len(user_text)} max_tokens={max_tokens}")
    try:
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            temperature=0.2,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_hint},
                {"role": "user", "content": user_text}
            ],
        )
    except Exception:
        if log:
            log.exception("LLM API call failed.")
        return {}

    content = (resp.choices[0].message.content or "").strip()
    if log:
        log.info(f"LLM response received. chars={len(content)} preview={(content[:300]).replace(chr(10),' ')}")
    try:
        return json.loads(content)
    except Exception:
        if log: log.warning("LLM JSON parse failed; returning empty dict.")
        return {}

# --- Prompts ---
CHUNK_PROMPT = """Summarize these WhatsApp messages into compact JSON.
Return:
{
  "points": ["short bullet", ...],   // 5–10 bullets, no filler
  "links": ["https://...", ...],     // deduped
  "keywords": ["topic", ...]         // up to 10
}
Keep it factual. No prose paragraphs. DO NOT include names."""

FINAL_GROUP_PROMPT = f"""Combine these chunk bullets into one weekly group summary.
Return JSON:
{{
  "summary": "250–300 words narrative summary, concise, factual",
  "highlights": ["top 5 bullets"],
  "keywords": ["top topics (<=10)"]
}}
Target {TARGET_WORDS_LOW}–{TARGET_WORDS_HIGH} words for 'summary'. No names."""

ROLLUP_PROMPT = f"""Create a combined weekly digest from multiple group summaries.
Return JSON:
{{
  "summary": "250–300 words executive summary",
  "highlights": ["cross-group top 5"],
  "keywords": ["cross-group topics (<=10)"]
}}"""

def summarize_chunk(text: str) -> Dict[str, Any]:
    return llm_json(CHUNK_PROMPT, text, max_tokens=CHUNK_MAX_TOKENS) or {"points": [], "links": [], "keywords": []}

def summarize_group_lines(lines: List[str]) -> Dict[str, Any]:
    # 1) Chunk raw lines
    chunks = chunk_lines(lines)

    # 2) Summarize each chunk to bullets
    merged_points, merged_links, merged_keywords = [], set(), set()
    for i, ch in enumerate(chunks, 1):
        if log: log.debug(f"Summarizing chunk {i}/{len(chunks)}")
        data = summarize_chunk(ch)
        merged_points.extend([p for p in data.get("points", []) if isinstance(p, str)])
        for l in data.get("links", []):
            if isinstance(l, str): merged_links.add(l)
        for k in data.get("keywords", []):
            if isinstance(k, str): merged_keywords.add(k)

    # 3) Final 250–300 word summary from chunk bullets
    synthesis_text = "Chunk bullet points:\n" + "\n".join(f"- {p}" for p in merged_points[:120])
    if merged_links:
        synthesis_text += "\n\nLinks:\n" + "\n".join(sorted(list(merged_links))[:30])

    data = llm_json(FINAL_GROUP_PROMPT, synthesis_text, max_tokens=FINAL_MAX_TOKENS) or {"summary":"", "highlights": [], "keywords": []}

    # 4) Hard cap at 300 words
    data["summary"] = clamp_words(data.get("summary", ""), TARGET_WORDS_HIGH)
    if log: log.info(f"Final group summary words={word_count(data['summary'])}")
    if "keywords" not in data or not isinstance(data["keywords"], list):
        data["keywords"] = list(sorted(merged_keywords))[:10]
    return data

# ===== Message shaping (NO NAMES) =====
def rows_to_lines(rows: List[Dict[str, Any]]) -> Tuple[List[str], int]:
    """
    Returns:
      lines        -> ["YYYY-MM-DD HH:MM:SS: message ...url", ...]  (no names)
      unique_links -> count of unique URLs across Message + Links
    """
    lines: List[str] = []
    unique_urls: Set[str] = set()

    for r in rows:
        msg = (r["msg"] or "").strip()
        link_field = (r["link"] or "").strip()
        urls = extract_urls(msg, link_field)
        unique_urls |= urls

        url_suffix = f" {' '.join(list(urls)[:3])}" if urls else ""
        lines.append(f"{r['CreatedDate']}: {msg}{url_suffix}")

    if log:
        log.debug(f"Prepared {len(lines)} lines; unique_links={len(unique_urls)}")
    return lines, len(unique_urls)

# ===== Main =====
def run(week_start_str: Optional[str], dry_run: bool) -> None:
    if log:
        log.info(f"Starting run. dry_run={dry_run} canonical=Indie Parents Pack")

    dt_from, dt_to, week_start_date = compute_bounds(week_start_str)

    conn = connect_db()
    rollup_items: List[Dict[str, Any]] = []

    try:
        with conn.cursor() as cur:
            # Discover exact Indie groups
            target_groups = discover_indie_group_names(cur)
            if not target_groups:
                if log:
                    log.warning("[discover] No Indie groups found via LIKE. Aborting run.")
                return

            # Per-group
            for gname in target_groups:
                rows = fetch_group_rows(cur, gname, dt_from, dt_to)
                if not rows:
                    if log:
                        log.info(f"No messages for '{gname}' in the window.")
                    upsert_summary(cur,
                        scope="group",
                        canonical="Indie Parents Pack",
                        group_name=gname,
                        week_start=week_start_date,
                        message_count=0,
                        participants=0,          # privacy
                        links=0,
                        summary="(No messages this week.)",
                        highlights=[],
                        keywords=[]
                    )
                    continue

                lines, link_count = rows_to_lines(rows)
                sg = summarize_group_lines(lines)

                upsert_summary(cur,
                    scope="group",
                    canonical="Indie Parents Pack",
                    group_name=gname,
                    week_start=week_start_date,
                    message_count=len(rows),
                    participants=0,          # privacy: always 0
                    links=link_count,
                    summary=sg.get("summary", ""),
                    highlights=sg.get("highlights", []),
                    keywords=sg.get("keywords", [])
                )

                rollup_items.append({
                    "group_name": gname,
                    "summary": sg.get("summary",""),
                    "highlights": sg.get("highlights", []),
                    "keywords": sg.get("keywords", []),
                    "msg_count": len(rows),
                    "links": link_count
                })

            # Rollup across all Indie groups (optional but useful)
            if rollup_items:
                rollup_text = []
                for it in rollup_items:
                    rollup_text.append(
                        f"[{it['group_name']}]\n{it['summary']}\nHighlights: {', '.join(it['highlights'])}"
                    )
                rollup_blob = "\n\n".join(rollup_text)
                sr = llm_json(ROLLUP_PROMPT, rollup_blob, max_tokens=FINAL_MAX_TOKENS) or {"summary":"", "highlights": [], "keywords": []}
                sr["summary"] = clamp_words(sr.get("summary",""), TARGET_WORDS_HIGH)

                upsert_summary(cur,
                    scope="rollup",
                    canonical="Indie Parents Pack",
                    group_name=None,
                    week_start=week_start_date,
                    message_count=sum(x["msg_count"] for x in rollup_items),
                    participants=0,
                    links=sum(x["links"] for x in rollup_items),
                    summary=sr.get("summary",""),
                    highlights=sr.get("highlights", []),
                    keywords=sorted({k for x in rollup_items for k in x.get("keywords", [])})
                )

        if dry_run:
            conn.rollback()
            if log: log.info("[DRY RUN] Completed without committing.")
            print("[DRY RUN] Completed without committing.")
        else:
            if log: log.info("Commit successful. Run finished.")
            print("Done.")
    except Exception:
        if log:
            log.exception("Run failed with an exception.")
        raise
    finally:
        conn.close()
        if log:
            log.info("MySQL connection closed.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Weekly Indie summaries (IST, privacy-safe).")
    ap.add_argument("--week-start", help="YYYY-MM-DD (any date inside the target week; Monday recommended). If omitted, uses last week.")
    ap.add_argument("--dry-run", action="store_true", help="Run without committing DB changes.")
    ap.add_argument("--debug", action="store_true", help="Verbose logging → console + logs/run_*.log")
    args = ap.parse_args()

    log = setup_logger(args.debug)
    run(args.week_start, args.dry_run)
