import re
import difflib
import os
import glob
import pandas as pd
import csv
import json
from datetime import datetime

def normalize_text(text):
    if not text:
        return ""
    # Remove non-alphanumeric characters and lowercase
    return re.sub(r'[^a-zA-Z0-9]', '', str(text)).lower()

def title_similarity(a, b):
    if not a or not b: return 0
    # Quick length check
    if abs(len(a) - len(b)) > max(len(a), len(b)) * 0.2:
        return 0
    return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()

class Record:
    def __init__(self, source_file, original_text, pmid=None, doi=None, title=None, authors=None, year=None, extra_data=None):
        self.source_file = source_file
        self.original_text = original_text
        self.pmid = str(pmid).strip() if pmid and str(pmid).strip().lower() != 'nan' else None
        
        # Normalize DOI
        self.doi = str(doi).lower().strip() if doi and str(doi).strip().lower() != 'nan' else None
        if self.doi:
            # Remove "http://doi.org/" or "https://doi.org/" or "doi:"
            self.doi = re.sub(r'https?://(dx\.)?doi\.org/', '', self.doi)
            self.doi = re.sub(r'^doi:\s*', '', self.doi)
            self.doi = self.doi.split(' ')[0] # Handle cases like "10.1001/jama.201.1 [doi]"
            
        self.title = str(title).strip() if title and str(title).strip().lower() != 'nan' else ""
        self.normalized_title = normalize_text(self.title)
        
        if isinstance(authors, list):
            self.authors = [str(a) for a in authors]
        elif authors and str(authors).lower() != 'nan':
            self.authors = [str(authors)]
        else:
            self.authors = []
            
        self.year = str(year).strip() if year and str(year).strip().lower() != 'nan' else None
        self.extra_data = extra_data or {}

    def is_duplicate_of(self, other):
        """Check if this record is a duplicate. Returns (is_dup, method, confidence)."""
        # 1. DOI Match (Strongest)
        if self.doi and other.doi and self.doi == other.doi:
            return True, "DOI", 1.0
        
        # 2. PMID Match
        if self.pmid and other.pmid and self.pmid == other.pmid:
            return True, "PMID", 1.0

        # 3. Exact Normalized Title Match (if title is long enough)
        if self.normalized_title and other.normalized_title and len(self.normalized_title) > 30: 
            if self.normalized_title == other.normalized_title:
                return True, "ExactTitle", 0.99

        # 4. Title Similarity (95%+)
        if self.title and other.title and abs(len(self.title) - len(other.title)) < 40: 
            sim = title_similarity(self.title, other.title)
            if sim >= 0.95:
                return True, "TitleSimilarity", sim
            # Relaxed match if year also matches
            if sim >= 0.85 and self.year and other.year and self.year == other.year:
                return True, "TitleYear", sim
        
        return False, None, 0.0

def parse_pubmed(filename):
    records = []
    try:
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []
    
    blocks = re.split(r'\n(?=PMID- )', content)
    for block in blocks:
        if not block.strip(): continue
        
        pmid = re.search(r'^PMID- (.*)', block, re.M)
        doi = re.search(r'^LID - (.*) \[doi\]', block, re.M) or \
              re.search(r'^AID - (.*) \[doi\]', block, re.M) or \
              re.search(r'^SO  - .*?doi: (.*?)\.', block, re.M)
        title = re.search(r'^TI  - (.*?)(?=\n[A-Z]{2,4} - |\n\n|$)', block, re.S | re.M)
        year = re.search(r'^DP  - (\d{4})', block, re.M)
        authors = re.findall(r'^FAU - (.*)', block, re.M)
        
        t_str = ""
        if title:
            t_str = " ".join(line.strip() for line in title.group(1).split('\n'))

        records.append(Record(
            source_file=filename,
            original_text=block,
            pmid=pmid.group(1).strip() if pmid else None,
            doi=doi.group(1).strip() if doi else None,
            title=t_str,
            authors=authors,
            year=year.group(1).strip() if year else None
        ))
    return records

def parse_bib(filename):
    records = []
    try:
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []
    
    entries = re.findall(r'@\w+\s*\{.*?\n\}', content, re.S)
    for entry in entries:
        title_match = re.search(r'title\s*=\s*[\{"](.*?)[}\"],', entry, re.S | re.I) or \
                      re.search(r'title\s*=\s*\{(.*)\}', entry, re.S | re.I)
        doi_match = re.search(r'doi\s*=\s*[\{"](.*?)[}\"]', entry, re.S | re.I)
        year_match = re.search(r'year\s*=\s*[\{"]?(\d{4})[\"\}]?', entry, re.S | re.I)
        author_match = re.search(r'author\s*=\s*[\{"](.*?)[}\"]', entry, re.S | re.I)
        
        t_str = ""
        if title_match:
            t_str = " ".join(line.strip() for line in title_match.group(1).split('\n'))
            t_str = re.sub(r'[\{\}]', '', t_str)

        records.append(Record(
            source_file=filename,
            original_text=entry,
            doi=doi_match.group(1).strip() if doi_match else None,
            title=t_str,
            authors=author_match.group(1).split(' and ') if author_match else [],
            year=year_match.group(1).strip() if year_match else None
        ))
    return records

def parse_ris(filename):
    records = []
    try:
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []
    
    entries = re.split(r'\nER\s+-', content)
    for entry in entries:
        if not entry.strip(): continue
        
        title_match = re.search(r'^(?:TI|T1)\s+-\s+(.*)', entry, re.M | re.I)
        doi_match = re.search(r'^DO\s+-\s+(.*)', entry, re.M | re.I)
        year_match = re.search(r'^(?:PY|Y1)\s+-\s+(\d{4})', entry, re.M | re.I)
        authors = re.findall(r'^AU\s+-\s+(.*)', entry, re.M | re.I)
        
        t_str = title_match.group(1).strip() if title_match else ""

        records.append(Record(
            source_file=filename,
            original_text=entry + "\nER  -",
            doi=doi_match.group(1).strip() if doi_match else None,
            title=t_str,
            authors=authors,
            year=year_match.group(1).strip() if year_match else None
        ))
    return records

def parse_csv(filename):
    records = []
    try:
        # Detect delimiter
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            sample = f.readline() + f.readline()
            dialect = csv.Sniffer().sniff(sample)
            f.seek(0)
            df = pd.read_csv(f, sep=dialect.delimiter)
    except Exception as e:
        try:
            df = pd.read_csv(filename, encoding='latin1')
        except:
            print(f"Error reading CSV {filename}: {e}")
            return []

    # Map headers
    cols = df.columns
    title_col = next((c for c in cols if any(x in c.lower() for x in ['title', 'ti', 'document name'])), None)
    doi_col = next((c for c in cols if any(x in c.lower() for x in ['doi', 'do', 'digital object identifier'])), None)
    pmid_col = next((c for c in cols if any(x in c.lower() for x in ['pmid', 'pubmed id', 'pm'])), None)
    author_col = next((c for c in cols if any(x in c.lower() for x in ['author', 'au', 'contributor'])), None)
    year_col = next((c for c in cols if any(x in c.lower() for x in ['year', 'py', 'publication date'])), None)

    for _, row in df.iterrows():
        title = row[title_col] if title_col else ""
        doi = row[doi_col] if doi_col else None
        pmid = row[pmid_col] if pmid_col else None
        authors = row[author_col] if author_col else ""
        year = row[year_col] if year_col else ""
        
        # Original text for CSV is the JSON of the row
        original_text = row.to_json()

        records.append(Record(
            source_file=filename,
            original_text=original_text,
            pmid=pmid,
            doi=doi,
            title=title,
            authors=str(authors).split(';') if authors else [],
            year=year,
            extra_data=row.to_dict()
        ))
    return records

def detect_and_parse(filename):
    ext = os.path.splitext(filename)[1].lower()
    
    with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
        head = f.read(2048)
    
    if 'PMID-' in head or ext == '.nbib':
        return parse_pubmed(filename), "PubMed"
    elif '@' in head and '{' in head:
        return parse_bib(filename), "BibTeX"
    elif 'TY  -' in head or 'ER  -' in head or ext == '.ris':
        return parse_ris(filename), "RIS"
    elif ext == '.csv':
        return parse_csv(filename), "CSV"
    elif 'PT ' in head and 'AU ' in head: # WoS Tab delimited
        return parse_csv(filename), "WoS-Tab"
    else:
        # Fallback to extension
        if ext == '.ris': return parse_ris(filename), "RIS"
        if ext == '.bib': return parse_bib(filename), "BibTeX"
        if ext == '.csv': return parse_csv(filename), "CSV"
        if ext == '.txt':
            # Could be anything, try RIS then PubMed
            if 'TY  -' in head: return parse_ris(filename), "RIS"
            if 'PMID-' in head: return parse_pubmed(filename), "PubMed"
    
    return [], None

def process_file(records, label, master_seen_dois, master_seen_titles, master_unique_list, audit_log):
    print(f"Deduplicating {label}...")
    local_unique = []
    skipped = 0
    flagged_for_review = []
    
    for r in records:
        # Check against master DOI index first
        if r.doi and r.doi in master_seen_dois:
            skipped += 1
            audit_log.append({
                "action": "removed", "method": "DOI_index", "confidence": 1.0,
                "source_file": r.source_file, "title": r.title[:100], "doi": r.doi
            })
            continue
        if r.normalized_title and r.normalized_title in master_seen_titles and len(r.normalized_title) > 30:
            skipped += 1
            audit_log.append({
                "action": "removed", "method": "ExactTitle_index", "confidence": 0.99,
                "source_file": r.source_file, "title": r.title[:100]
            })
            continue
            
        is_dup = False
        dup_method = None
        dup_confidence = 0.0
        matched_record = None
        for u in master_unique_list:
            result = r.is_duplicate_of(u)
            if result[0]:
                is_dup = True
                dup_method = result[1]
                dup_confidence = result[2]
                matched_record = u
                break
        
        if is_dup:
            # Conservative retention: if confidence is in uncertain range, keep both and flag
            if dup_method == "TitleYear" and dup_confidence < 0.92:
                # Uncertain match — retain both, flag for human review
                local_unique.append(r)
                master_unique_list.append(r)
                if r.doi: master_seen_dois.add(r.doi)
                if r.normalized_title: master_seen_titles.add(r.normalized_title)
                flagged_for_review.append({
                    "record_title": r.title[:100],
                    "matched_title": matched_record.title[:100] if matched_record else "",
                    "method": dup_method, "confidence": round(dup_confidence, 4),
                    "reason": "Low-confidence match retained for human review"
                })
                audit_log.append({
                    "action": "flagged_retained", "method": dup_method, "confidence": round(dup_confidence, 4),
                    "source_file": r.source_file, "title": r.title[:100],
                    "matched_title": matched_record.title[:100] if matched_record else ""
                })
            else:
                skipped += 1
                audit_log.append({
                    "action": "removed", "method": dup_method, "confidence": round(dup_confidence, 4),
                    "source_file": r.source_file, "title": r.title[:100],
                    "matched_title": matched_record.title[:100] if matched_record else ""
                })
            continue
        
        local_unique.append(r)
        master_unique_list.append(r)
        if r.doi: master_seen_dois.add(r.doi)
        if r.normalized_title: master_seen_titles.add(r.normalized_title)
        audit_log.append({
            "action": "kept", "method": "unique", "confidence": 1.0,
            "source_file": r.source_file, "title": r.title[:100]
        })
            
    print(f"  - Kept {len(local_unique)} records, removed {skipped} duplicates, flagged {len(flagged_for_review)} for review.")
    return local_unique, flagged_for_review

def save_records(records, original_filename, format_label):
    if not records:
        print(f"No records to save for {original_filename}")
        return

    name, ext = os.path.splitext(original_filename)
    out_name = f"{name}_deduplicated{ext}"
    
    if format_label == "CSV" or format_label == "WoS-Tab":
        # Reconstruct DataFrame from extra_data
        data = [r.extra_data for r in records]
        df = pd.DataFrame(data)
        df.to_csv(out_name, index=False)
    elif format_label == "PubMed":
        with open(out_name, 'w', encoding='utf-8') as f:
            f.write("\n\n".join(r.original_text.strip() for r in records))
    elif format_label == "BibTeX":
        with open(out_name, 'w', encoding='utf-8') as f:
            f.write("\n\n".join(r.original_text.strip() for r in records))
    elif format_label == "RIS":
        with open(out_name, 'w', encoding='utf-8') as f:
            # Ensure each record has ER - if missing
            text = ""
            for r in records:
                t = r.original_text.strip()
                if not t.endswith("ER  -"):
                    t += "\nER  -"
                text += t + "\n\n"
            f.write(text)
    else:
        # Default fallback
        with open(out_name, 'w', encoding='utf-8') as f:
            f.write("\n\n".join(str(r.original_text).strip() for r in records))
    
    print(f"Saved to {out_name}")

def main():
    # Find all potential files
    extensions = ['*.txt', '*.bib', '*.ris', '*.csv', '*.nbib', '*.ciw', '*.enw']
    files = []
    for ext in extensions:
        files.extend(glob.glob(ext))
    
    # Exclude script and deduplicated files
    files = [f for f in files if '_deduplicated' not in f and f not in ['deduplicate_files.py', 'count_records.py', 'verify_clean.py']]
    
    if not files:
        print("No input files found in the current directory.")
        print(f"Supported extensions: {', '.join(extensions)}")
        return

    print(f"Found {len(files)} files to process: {', '.join(files)}")

    master_seen_dois = set()
    master_seen_titles = set()
    master_unique_list = []
    audit_log = []  # Structured audit log for all decisions
    all_flagged = []  # Records flagged for human review

    all_processed = []
    file_record_counts = {}  # Track original counts per file

    # Process in a stable order (alphabetical) or maybe prioritized?
    # Usually PubMed/Cochrane are higher quality.
    files.sort()

    for f in files:
        records, format_label = detect_and_parse(f)
        if not format_label:
            print(f"Could not detect format for {f}, skipping.")
            continue
        
        file_record_counts[f] = len(records)
        print(f"Detected format: {format_label} for {f} ({len(records)} records)")
        deduped, flagged = process_file(records, f, master_seen_dois, master_seen_titles, master_unique_list, audit_log)
        all_flagged.extend(flagged)
        all_processed.append((deduped, f, format_label))

    # Summary statistics
    total_input = sum(file_record_counts.values())
    total_output = sum(len(d) for d, _, _ in all_processed)
    total_removed = len([e for e in audit_log if e["action"] == "removed"])
    total_flagged = len([e for e in audit_log if e["action"] == "flagged_retained"])
    
    # Count by method
    method_counts = {}
    for entry in audit_log:
        if entry["action"] == "removed":
            m = entry["method"]
            method_counts[m] = method_counts.get(m, 0) + 1

    print("\n" + "="*50)
    print("DEDUPLICATION SUMMARY")
    print("="*50)
    print(f"Total input records:    {total_input}")
    print(f"Total unique records:   {total_output}")
    print(f"Total duplicates:       {total_removed}")
    print(f"Flagged for review:     {total_flagged}")
    print(f"\nDuplicates by method:")
    for method, count in sorted(method_counts.items(), key=lambda x: -x[1]):
        print(f"  {method}: {count}")
    print(f"\nPer-file breakdown:")
    for deduped, f, _ in all_processed:
        orig = file_record_counts.get(f, '?')
        print(f"  {f}: {orig} input → {len(deduped)} kept")

    # Save results
    print("\nSaving files...")
    for deduped, f, format_label in all_processed:
        save_records(deduped, f, format_label)

    # Save structured audit log
    audit_output = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total_input": total_input,
            "total_unique": total_output,
            "total_duplicates_removed": total_removed,
            "total_flagged_for_review": total_flagged,
            "duplicates_by_method": method_counts,
            "files_processed": file_record_counts
        },
        "flagged_for_human_review": all_flagged,
        "decisions": audit_log
    }
    with open("dedup_audit_log.json", "w", encoding="utf-8") as f:
        json.dump(audit_output, f, indent=2, ensure_ascii=False)
    print(f"\nAudit log saved to dedup_audit_log.json")

    print("\nAll tasks completed.")

if __name__ == "__main__":
    main()
