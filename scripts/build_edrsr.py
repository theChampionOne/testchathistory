#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import re
import sqlite3
import threading
import time
import zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (compatible; EDRSR-public-research/1.0)"
TL = threading.local()
ARTICLE_RE = re.compile(r"(?<!\d)204\s*[-‑–—]?\s*1(?!\d)|незакон\w*\s+перетин\w*\s+державн\w*\s+кордон", re.I)
UKR_CITIZEN_RE = re.compile(r"громадян(?:ин|ка|ина|ки|ином|кою)?\s+україн", re.I)
OUTBOUND_RE = re.compile(r"з\s+україн\w*\s+(?:до|в)|у\s+напрямк\w+\s+(?:державн\w+\s+)?кордон|залишит\w+\s+територ\w+\s+україн|виїхати\s+за\s+меж", re.I)

COUNTRIES = {
    "Молдова": re.compile(r"молдов", re.I),
    "Румыния": re.compile(r"румун", re.I),
    "Польша": re.compile(r"польщ|польськ", re.I),
    "Словакия": re.compile(r"словач|словацьк", re.I),
    "Венгрия": re.compile(r"угорщ|угорськ|венгр", re.I),
    "Беларусь": re.compile(r"білорус|беларус", re.I),
}
DETECTION = {
    "БПЛА/дрон": re.compile(r"бпла|безпілот|дрон", re.I),
    "тепловизор/ИК": re.compile(r"тепловіз|інфрачервон|ік[- ]?камер", re.I),
    "фотоловушка/камера": re.compile(r"фотопаст|фотолов|камера відеоспостереж", re.I),
    "служебная собака": re.compile(r"службов\w+\s+собак|кінолог", re.I),
    "контрольный пост": re.compile(r"контрольн\w+\s+(?:пост|пункт)|\bкпп\b|\bкпр\b", re.I),
    "оперативная информация": re.compile(r"оперативн\w+\s+(?:інформац|дан)", re.I),
    "наряд/патруль/группа реагирования": re.compile(r"прикордонн\w+\s+наряд|патрул|груп\w+\s+реагув", re.I),
    "реадмиссия/иностранная служба": re.compile(r"реадмісі|прикордонн\w+\s+страж|передан\w+\s+україн", re.I),
}
MOVEMENT = {
    "вплавь/река": re.compile(r"вплав|переплив|річк|водн\w+\s+перешкод|тис[ау]|дністер", re.I),
    "пешком": re.compile(r"пішки|пішим\s+ходом|рухав\w+\s+піш", re.I),
    "автомобиль": re.compile(r"автомобіл|транспортн\w+\s+засоб", re.I),
    "поезд": re.compile(r"поїзд|залізнич", re.I),
}

@dataclass
class Meta:
    doc_id: int
    source_year: int
    court_code: str = ""
    court_name: str = ""
    region_code: str = ""
    region_name: str = ""
    category_code: str = ""
    category_name: str = ""
    judgment_code: str = ""
    judgment_name: str = ""
    justice_kind: str = ""
    cause_num: str = ""
    adjudication_date: str = ""
    receipt_date: str = ""
    date_publ: str = ""
    judge: str = ""
    source_url: str = ""
    status: str = ""


def args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--archives", nargs="+", required=True)
    p.add_argument("--date-from", required=True)
    p.add_argument("--date-to", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--workers", type=int, default=12)
    p.add_argument("--limit", type=int, default=0)
    return p.parse_args()


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def member(z: zipfile.ZipFile, basename: str) -> str:
    b = basename.lower()
    hits = [n for n in z.namelist() if n.replace("\\", "/").lower().split("/")[-1] == b]
    if not hits:
        raise RuntimeError(f"{basename} absent in {z.filename}; members={z.namelist()[:30]}")
    return min(hits, key=len)


def rows(z: zipfile.ZipFile, filename: str) -> Iterable[dict[str, str]]:
    with z.open(member(z, filename)) as raw:
        txt = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace", newline="")
        reader = csv.DictReader(txt, delimiter="\t", quotechar='"')
        for r in reader:
            yield {norm(str(k)): norm(v or "") for k, v in r.items() if k is not None}


def pick(r: dict[str, str], *names: str) -> str:
    low = {k.lower(): v for k, v in r.items()}
    for n in names:
        if n.lower() in low:
            return low[n.lower()]
    return ""


def dictionary(z: zipfile.ZipFile, filename: str, key_names: tuple[str, ...]) -> dict[str, dict[str, str]]:
    out = {}
    for r in rows(z, filename):
        key = pick(r, *key_names)
        if key:
            out[key] = r
    return out


def parse_date(s: str) -> date | None:
    s = norm(s)
    for raw in (s[:10], s):
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw[:10], fmt).date()
            except ValueError:
                pass
    return None


def dict_name(r: dict[str, str]) -> str:
    return pick(r, "name", "category_name", "court_name", "region_name", "judgment_name", "justice_kind_name", "title")


def scan(path: Path, d1: date, d2: date) -> tuple[list[Meta], list[dict[str, str]], dict[str, object]]:
    year_m = re.search(r"20\d{2}", path.name)
    year = int(year_m.group()) if year_m else 0
    with zipfile.ZipFile(path) as z:
        cats = dictionary(z, "cause_categories.csv", ("category_code", "code"))
        courts = dictionary(z, "courts.csv", ("court_code", "code"))
        regions = dictionary(z, "regions.csv", ("region_code", "code"))
        judgments = dictionary(z, "judgment_forms.csv", ("judgment_code", "code"))
        matched = [r for r in cats.values() if ARTICLE_RE.search(dict_name(r))]
        codes = {pick(r, "category_code", "code") for r in matched}
        print(f"{path.name}: matched category codes={len(codes)}", flush=True)
        for r in matched:
            print("  ", pick(r, "category_code", "code"), dict_name(r), flush=True)
        selected: list[Meta] = []
        total = 0
        headers: list[str] = []
        for r in rows(z, "documents.csv"):
            total += 1
            if not headers:
                headers = list(r)
                print(f"documents headers: {headers}", flush=True)
            if total % 500000 == 0:
                print(f"{path.name}: scanned={total:,} selected={len(selected):,}", flush=True)
            dt = parse_date(pick(r, "adjudication_date", "decision_date", "date"))
            cat_code = pick(r, "category_code", "cause_category_code")
            if not dt or not d1 <= dt <= d2 or cat_code not in codes:
                continue
            status = pick(r, "status")
            if status == "0":
                continue
            doc_raw = pick(r, "doc_id", "document_id", "id")
            try:
                doc_id = int(doc_raw)
            except Exception:
                continue
            court_code = pick(r, "court_code")
            court = courts.get(court_code, {})
            region_code = pick(court, "region_code")
            region = regions.get(region_code, {})
            judgment_code = pick(r, "judgment_code")
            selected.append(Meta(
                doc_id=doc_id, source_year=year,
                court_code=court_code, court_name=dict_name(court),
                region_code=region_code, region_name=dict_name(region),
                category_code=cat_code, category_name=dict_name(cats.get(cat_code, {})),
                judgment_code=judgment_code, judgment_name=dict_name(judgments.get(judgment_code, {})),
                justice_kind=pick(r, "justice_kind"), cause_num=pick(r, "cause_num", "case_number"),
                adjudication_date=pick(r, "adjudication_date", "decision_date", "date"),
                receipt_date=pick(r, "receipt_date"), date_publ=pick(r, "date_publ", "publication_date"),
                judge=pick(r, "judge"), source_url=pick(r, "doc_url", "url"), status=status,
            ))
        return selected, matched, {"year": year, "total_documents": total, "document_headers": headers}


def sess() -> requests.Session:
    s = getattr(TL, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": UA, "Accept-Language": "uk,en;q=0.8"})
        a = requests.adapters.HTTPAdapter(pool_connections=24, pool_maxsize=24)
        s.mount("https://", a); s.mount("http://", a)
        TL.s = s
    return s


def urls(m: Meta) -> list[str]:
    out = [m.source_url, f"https://reyestr.court.gov.ua/Review/{m.doc_id}", f"https://opendatabot.ua/court/{m.doc_id}", f"https://youcontrol.com.ua/catalog/court-document/{m.doc_id}/"]
    if m.source_url.startswith("http://"):
        out.insert(1, "https://" + m.source_url[7:])
    return list(dict.fromkeys(x for x in out if x))


def html_text(content: bytes) -> str:
    soup = BeautifulSoup(content.decode("utf-8", errors="replace"), "lxml")
    for t in soup(["script", "style", "svg", "noscript"]): t.decompose()
    text = soup.get_text("\n")
    text = re.sub(r"[\t\r\f\v]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def classify(text: str) -> dict[str, object]:
    country_scores = {k: len(v.findall(text)) for k, v in COUNTRIES.items()}
    country, n = max(country_scores.items(), key=lambda x: x[1])
    if n == 0: country = "Не определено"
    detections = [k for k, v in DETECTION.items() if v.search(text)]
    movement = [k for k, v in MOVEMENT.items() if v.search(text)]
    part = "ч.2" if re.search(r"ч\.?\s*2\s*(?:ст\.?|статт)\s*204", text, re.I) else ("ч.1" if re.search(r"ч\.?\s*1\s*(?:ст\.?|статт)\s*204", text, re.I) else "не определена")
    tail = text[-8000:].lower()
    if re.search(r"провадження[^.]{0,300}закрит", tail): outcome = "закрыто"
    elif re.search(r"повернут\w+[^.]{0,150}матеріал", tail): outcome = "материалы возвращены"
    elif re.search(r"визнат\w+[^.]{0,350}винн|накласт\w+[^.]{0,200}стягнен", tail): outcome = "виновен/взыскание"
    else: outcome = "не определено"
    fines = sorted(set(re.findall(r"(?:штраф\w*[^\d]{0,80})((?:\d[\s.,]?){3,8})\s*(?:грн|грив)", tail, re.I)))
    return {
        "country": country,
        "detection": "; ".join(detections) or "Не определено",
        "movement": "; ".join(movement) or "Не определено",
        "article_part": part, "outcome": outcome,
        "mentions_ukrainian_citizen": int(bool(UKR_CITIZEN_RE.search(text))),
        "outbound_markers": int(bool(OUTBOUND_RE.search(text))),
        "likely_target": int(bool(UKR_CITIZEN_RE.search(text) and (OUTBOUND_RE.search(text) or country != "Не определено"))),
        "fine_raw": "; ".join(fines),
    }


def fetch(m: Meta) -> dict[str, object]:
    error = ""
    last_code = None
    for url in urls(m):
        for attempt in range(3):
            try:
                r = sess().get(url, timeout=35, allow_redirects=True)
                last_code = r.status_code
                if r.status_code == 200 and len(r.content) > 500:
                    text = html_text(r.content)
                    if len(text) > 500 and ARTICLE_RE.search(text):
                        return {**asdict(m), "fetch_status": "ok", "http_status": r.status_code, "final_url": r.url,
                                "text_sha256": hashlib.sha256(text.encode()).hexdigest(), "text_length": len(text),
                                **classify(text), "text": text}
                if r.status_code in (403, 429, 500, 502, 503, 504):
                    time.sleep(1.5 ** attempt); continue
                break
            except Exception as e:
                error = f"{type(e).__name__}: {e}"; time.sleep(1.5 ** attempt)
    return {**asdict(m), "fetch_status": error or f"http_{last_code}", "http_status": last_code, "final_url": "",
            "text_sha256": "", "text_length": 0, "country": "Не определено", "detection": "Не определено",
            "movement": "Не определено", "article_part": "не определена", "outcome": "не определено",
            "mentions_ukrainian_citizen": 0, "outbound_markers": 0, "likely_target": 0, "fine_raw": "", "text": ""}


def init_db(p: Path) -> sqlite3.Connection:
    c = sqlite3.connect(p)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript("""
    CREATE TABLE cases(doc_id INTEGER PRIMARY KEY, source_year INTEGER, court_code TEXT, court_name TEXT,
      region_code TEXT, region_name TEXT, category_code TEXT, category_name TEXT, judgment_code TEXT,
      judgment_name TEXT, justice_kind TEXT, cause_num TEXT, adjudication_date TEXT, receipt_date TEXT,
      date_publ TEXT, judge TEXT, source_url TEXT, status TEXT, fetch_status TEXT, http_status INTEGER,
      final_url TEXT, text_sha256 TEXT, text_length INTEGER, country TEXT, detection TEXT, movement TEXT,
      article_part TEXT, outcome TEXT, mentions_ukrainian_citizen INTEGER, outbound_markers INTEGER,
      likely_target INTEGER, fine_raw TEXT, text TEXT);
    CREATE VIRTUAL TABLE cases_fts USING fts5(cause_num,court_name,region_name,category_name,text,content='cases',content_rowid='doc_id',tokenize='unicode61');
    CREATE INDEX idx_date ON cases(adjudication_date); CREATE INDEX idx_target ON cases(likely_target);
    CREATE INDEX idx_country ON cases(country); CREATE INDEX idx_region ON cases(region_name);
    """)
    return c


def insert(c: sqlite3.Connection, r: dict[str, object]) -> None:
    cols = [x[1] for x in c.execute("PRAGMA table_info(cases)")]
    vals = [r.get(k) for k in cols]
    c.execute(f"INSERT OR REPLACE INTO cases({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})", vals)
    if r.get("text"):
        c.execute("INSERT OR REPLACE INTO cases_fts(rowid,cause_num,court_name,region_name,category_name,text) VALUES(?,?,?,?,?,?)",
                  (r["doc_id"],r["cause_num"],r["court_name"],r["region_name"],r["category_name"],r["text"]))


def main() -> int:
    a = args(); out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    d1, d2 = date.fromisoformat(a.date_from), date.fromisoformat(a.date_to)
    all_meta: dict[int, Meta] = {}; categories = []; archive_reports = []
    for p in map(Path, a.archives):
        found, cats, rep = scan(p, d1, d2); archive_reports.append(rep); categories.extend(cats)
        for m in found: all_meta[m.doc_id] = m
    metas = sorted(all_meta.values(), key=lambda x:(x.adjudication_date,x.doc_id))
    if a.limit: metas = metas[:a.limit]
    print(f"UNIQUE CANDIDATES={len(metas):,}", flush=True)
    with (out/"metadata_candidates.csv").open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(asdict(metas[0]).keys()) if metas else ["doc_id"]); w.writeheader(); w.writerows(asdict(x) for x in metas)
    (out/"matched_categories.json").write_text(json.dumps(categories,ensure_ascii=False,indent=2),encoding="utf-8")
    (out/"archive_report.json").write_text(json.dumps(archive_reports,ensure_ascii=False,indent=2),encoding="utf-8")

    c=init_db(out/"edrsr_204_1.sqlite")
    csvf=(out/"cases.csv").open("w",encoding="utf-8-sig",newline=""); cw=None
    jgz=gzip.open(out/"cases_fulltext.jsonl.gz","wt",encoding="utf-8",compresslevel=6)
    stats=Counter(); by_country=Counter(); by_outcome=Counter(); errors=[]
    try:
        with ThreadPoolExecutor(max_workers=a.workers) as ex:
            futs={ex.submit(fetch,m):m for m in metas}
            for i,fut in enumerate(as_completed(futs),1):
                r=fut.result(); insert(c,r)
                public={k:v for k,v in r.items() if k!="text"}
                if cw is None: cw=csv.DictWriter(csvf,fieldnames=list(public)); cw.writeheader()
                cw.writerow(public); jgz.write(json.dumps(r,ensure_ascii=False)+"\n")
                stats["processed"]+=1
                if r["fetch_status"]=="ok": stats["fetched_ok"]+=1
                else: stats["fetch_failed"]+=1; errors.append(public)
                stats["likely_target"]+=int(r["likely_target"]); stats["ukrainian_citizen"]+=int(r["mentions_ukrainian_citizen"])
                by_country[str(r["country"])]+=1; by_outcome[str(r["outcome"])]+=1
                if i%100==0 or i==len(metas): c.commit(); print(f"FETCH {i:,}/{len(metas):,} ok={stats['fetched_ok']:,} fail={stats['fetch_failed']:,} target={stats['likely_target']:,}",flush=True)
    finally:
        c.commit(); c.close(); csvf.close(); jgz.close()
    with (out/"fetch_errors.csv").open("w",encoding="utf-8-sig",newline="") as f:
        fields=list(errors[0]) if errors else ["doc_id","fetch_status"]; w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(errors)
    summary={"period":[a.date_from,a.date_to],"metadata_candidates":len(metas),**stats,"countries":by_country,"outcomes":by_outcome,"archives":archive_reports}
    (out/"summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2,default=dict),encoding="utf-8")
    with sqlite3.connect(out/"edrsr_204_1.sqlite") as db: db.execute("PRAGMA wal_checkpoint(TRUNCATE)"); db.execute("VACUUM")
    with (out/"edrsr_204_1.sqlite").open("rb") as src,gzip.open(out/"edrsr_204_1.sqlite.gz","wb",compresslevel=6) as dst:
        while b:=src.read(1024*1024): dst.write(b)
    sums=[]
    for p in sorted(out.iterdir()):
        if p.is_file() and p.name!="SHA256SUMS.txt": sums.append(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}")
    (out/"SHA256SUMS.txt").write_text("\n".join(sums)+"\n",encoding="utf-8")
    print(json.dumps(summary,ensure_ascii=False,indent=2,default=dict),flush=True)
    return 0

if __name__=="__main__": raise SystemExit(main())
