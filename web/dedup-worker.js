// dedup-worker.js
// Runs entirely in a background thread — never blocks the UI.
// Communicates with the main thread via postMessage.

/* ─── Helpers ────────────────────────────────────── */
function normalizeText(text) {
  if (!text) return "";
  return text.replace(/[^a-zA-Z0-9]/g, "").toLowerCase();
}

function normalizeDoi(doi) {
  if (!doi) return null;
  let d = doi.toLowerCase().trim();
  d = d.replace(/https?:\/\/(dx\.)?doi\.org\//i, "");
  d = d.replace(/^doi:\s*/i, "");
  d = d.split(" ")[0];
  return d || null;
}

function lcsRatio(s1, s2) {
  const a = s1.slice(0, 300), b = s2.slice(0, 300);
  const ma = a.length, mb = b.length;
  if (!ma || !mb) return 0;
  const dp = Array.from({ length: ma + 1 }, () => new Int16Array(mb + 1));
  for (let i = 1; i <= ma; i++)
    for (let j = 1; j <= mb; j++)
      dp[i][j] = a[i-1] === b[j-1] ? dp[i-1][j-1] + 1 : Math.max(dp[i-1][j], dp[i][j-1]);
  return (2 * dp[ma][mb]) / (ma + mb);
}

function titleSimilarity(a, b) {
  if (!a || !b) return 0;
  if (Math.abs(a.length - b.length) > Math.max(a.length, b.length) * 0.2) return 0;
  return lcsRatio(a.toLowerCase(), b.toLowerCase());
}

/* ─── Record ─────────────────────────────────────── */
class Record {
  constructor({ sourceFile, originalText, pmid, doi, title, authors, year, extraData, format }) {
    this.sourceFile      = sourceFile;
    this.originalText    = originalText;
    this.pmid            = pmid && String(pmid).trim() && String(pmid).toLowerCase() !== "nan" ? String(pmid).trim() : null;
    this.doi             = normalizeDoi(doi && String(doi).toLowerCase() !== "nan" ? doi : null);
    this.title           = title && String(title).toLowerCase() !== "nan" ? String(title).trim() : "";
    this.normalizedTitle = normalizeText(this.title);
    this.authors         = Array.isArray(authors) ? authors.map(String) : (authors ? [String(authors)] : []);
    this.year            = year && String(year).trim() && String(year).toLowerCase() !== "nan" ? String(year).trim() : null;
    this.extraData       = extraData || {};
    this.format          = format || "Unknown";
  }

  isDuplicateOf(other, fuzzyT, yearT) {
    if (this.doi && other.doi && this.doi === other.doi)
      return { dup: true, method: "DOI", conf: 1.0 };
    if (this.pmid && other.pmid && this.pmid === other.pmid)
      return { dup: true, method: "PMID", conf: 1.0 };
    if (this.normalizedTitle && other.normalizedTitle && this.normalizedTitle.length > 30)
      if (this.normalizedTitle === other.normalizedTitle)
        return { dup: true, method: "ExactTitle", conf: 0.99 };
    if (this.title && other.title && Math.abs(this.title.length - other.title.length) < 40) {
      const sim = titleSimilarity(this.title, other.title);
      if (sim >= fuzzyT) return { dup: true, method: "TitleSimilarity", conf: sim };
      if (sim >= yearT && this.year && other.year && this.year === other.year)
        return { dup: true, method: "TitleYear", conf: sim };
    }
    return { dup: false, method: null, conf: 0 };
  }
}

/* ─── Parsers ─────────────────────────────────────── */
function parsePubMed(content, filename) {
  const records = [];
  const blocks = content.split(/\n(?=PMID- )/);
  for (const block of blocks) {
    if (!block.trim()) continue;
    const pmidM  = block.match(/^PMID- (.*)/m);
    const doiM   = block.match(/^LID - (.*) \[doi\]/m) || block.match(/^AID - (.*) \[doi\]/m);
    const titleM = block.match(/^TI  - ([\s\S]*?)(?=\n[A-Z]{2,4} - |\n\n|$)/m);
    const yearM  = block.match(/^DP  - (\d{4})/m);
    const authors = [...block.matchAll(/^FAU - (.*)/gm)].map(m => m[1]);
    let title = "";
    if (titleM) title = titleM[1].split("\n").map(l => l.trim()).join(" ");
    records.push(new Record({
      sourceFile: filename, originalText: block, format: "PubMed",
      pmid: pmidM?.[1]?.trim(), doi: doiM?.[1]?.trim(), title, authors, year: yearM?.[1]?.trim()
    }));
  }
  return records;
}

function parseBib(content, filename) {
  const records = [];
  const entries = content.match(/@\w+\s*\{[\s\S]*?\n\}/g) || [];
  for (const entry of entries) {
    const titleM  = entry.match(/title\s*=\s*[\{"]([\s\S]*?)["}\],]/i);
    const doiM    = entry.match(/doi\s*=\s*[\{"](.*?)["}\],]/i);
    const yearM   = entry.match(/year\s*=\s*[\{"]?(\d{4})/i);
    const authorM = entry.match(/author\s*=\s*[\{"]([\s\S]*?)["}\],]/i);
    let title = titleM ? titleM[1].replace(/[\{\}]/g, "").trim() : "";
    const authors = authorM ? authorM[1].split(/ and /i).map(a => a.trim()) : [];
    records.push(new Record({
      sourceFile: filename, originalText: entry, format: "BibTeX",
      doi: doiM?.[1]?.trim(), title, authors, year: yearM?.[1]?.trim()
    }));
  }
  return records;
}

function parseRis(content, filename) {
  const records = [];
  const entries = content.split(/\nER\s+-/);
  for (const entry of entries) {
    if (!entry.trim()) continue;
    const titleM  = entry.match(/^(?:TI|T1)\s+-\s+(.*)/m);
    const doiM    = entry.match(/^DO\s+-\s+(.*)/m);
    const yearM   = entry.match(/^(?:PY|Y1)\s+-\s+(\d{4})/m);
    const authors = [...entry.matchAll(/^AU\s+-\s+(.*)/gm)].map(m => m[1].trim());
    records.push(new Record({
      sourceFile: filename, originalText: entry + "\nER  -", format: "RIS",
      doi: doiM?.[1]?.trim(), title: titleM?.[1]?.trim() || "", authors, year: yearM?.[1]?.trim()
    }));
  }
  return records;
}

function parseCsv(content, filename) {
  const records = [];
  try {
    const lines = content.split(/\r?\n/);
    if (lines.length < 2) return records;
    const firstLine = lines[0];
    const delim = (firstLine.match(/,/g) || []).length > (firstLine.match(/\t/g) || []).length ? "," : "\t";
    const parseLine = (line) => {
      const result = []; let current = "", inQuote = false;
      for (let i = 0; i < line.length; i++) {
        const c = line[i];
        if (c === '"' && !inQuote) { inQuote = true; continue; }
        if (c === '"' && inQuote) { if (line[i+1] === '"') { current += '"'; i++; } else { inQuote = false; } continue; }
        if (c === delim && !inQuote) { result.push(current); current = ""; continue; }
        current += c;
      }
      result.push(current); return result;
    };
    const headers = parseLine(lines[0]).map(h => h.toLowerCase().trim());
    const col = (kws) => headers.findIndex(h => kws.some(k => h.includes(k)));
    const titleIdx  = col(["title", "ti", "document name"]);
    const doiIdx    = col(["doi", "do", "digital object"]);
    const pmidIdx   = col(["pmid", "pubmed id", "pm"]);
    const authorIdx = col(["author", "au", "contributor"]);
    const yearIdx   = col(["year", "py", "publication date"]);
    for (let i = 1; i < lines.length; i++) {
      if (!lines[i].trim()) continue;
      const cols = parseLine(lines[i]);
      const rowObj = {};
      headers.forEach((h, j) => { rowObj[h] = cols[j] ?? ""; });
      records.push(new Record({
        sourceFile: filename, originalText: JSON.stringify(rowObj), format: "CSV",
        doi:    doiIdx    >= 0 ? cols[doiIdx]    : null,
        pmid:   pmidIdx   >= 0 ? cols[pmidIdx]   : null,
        title:  titleIdx  >= 0 ? cols[titleIdx]  : "",
        authors: authorIdx >= 0 && cols[authorIdx] ? cols[authorIdx].split(";").map(a => a.trim()) : [],
        year:   yearIdx   >= 0 ? cols[yearIdx]   : null,
        extraData: rowObj
      }));
    }
  } catch(e) { /* ignore */ }
  return records;
}

function detectAndParse(content, filename) {
  const ext  = filename.slice(filename.lastIndexOf(".")).toLowerCase();
  const head = content.slice(0, 2048);
  if (head.includes("PMID-") || ext === ".nbib")    return { records: parsePubMed(content, filename), label: "PubMed" };
  if (head.includes("@") && head.includes("{"))      return { records: parseBib(content, filename),    label: "BibTeX" };
  if (head.includes("TY  -") || head.includes("ER  -") || ext === ".ris")
                                                     return { records: parseRis(content, filename),    label: "RIS" };
  if (ext === ".csv")                                return { records: parseCsv(content, filename),    label: "CSV" };
  if (head.includes("PT ") && head.includes("AU ")) return { records: parseCsv(content, filename),    label: "WoS-Tab" };
  if (ext === ".ris")   return { records: parseRis(content, filename),    label: "RIS" };
  if (ext === ".bib")   return { records: parseBib(content, filename),    label: "BibTeX" };
  if (ext === ".csv" || ext === ".ciw" || ext === ".enw")
                        return { records: parseCsv(content, filename),    label: "CSV" };
  if (ext === ".txt") {
    if (head.includes("TY  -")) return { records: parseRis(content, filename),    label: "RIS" };
    if (head.includes("PMID-")) return { records: parsePubMed(content, filename), label: "PubMed" };
  }
  return { records: [], label: null };
}

/* ─── Core dedup ─────────────────────────────────── */
// Yield control periodically so the worker doesn't hog the thread either.
// (Useful for very large datasets)
const YIELD_EVERY = 500; // records

function processFile(records, masterSeenDois, masterSeenTitles, masterUniqueList, auditLog, fT, yT, fileName, fileIdx, totalFiles) {
  const localUnique = [];
  const flagged = [];
  let skipped = 0;

  for (let ri = 0; ri < records.length; ri++) {
    const r = records[ri];

    // Periodic progress ping so main thread can update progress bar
    if (ri % YIELD_EVERY === 0 && ri > 0) {
      self.postMessage({
        type: "progress",
        msg: `  Processing ${fileName}: ${ri}/${records.length} records…`,
        progress: ((fileIdx + ri / records.length) / totalFiles) * 100
      });
    }

    // Fast index checks
    if (r.doi && masterSeenDois.has(r.doi)) {
      skipped++;
      auditLog.push({ action: "removed", method: "DOI_index", confidence: 1.0, sourceFile: r.sourceFile, title: r.title.slice(0, 100) });
      continue;
    }
    if (r.normalizedTitle && r.normalizedTitle.length > 30 && masterSeenTitles.has(r.normalizedTitle)) {
      skipped++;
      auditLog.push({ action: "removed", method: "ExactTitle_index", confidence: 0.99, sourceFile: r.sourceFile, title: r.title.slice(0, 100) });
      continue;
    }

    // Full comparison
    let isDup = false, matchedRecord = null, method = null, conf = 0;
    for (const u of masterUniqueList) {
      const res = r.isDuplicateOf(u, fT, yT);
      if (res.dup) { isDup = true; matchedRecord = u; method = res.method; conf = res.conf; break; }
    }

    if (isDup) {
      if (method === "TitleYear" && conf < 0.92) {
        localUnique.push(r);
        masterUniqueList.push(r);
        if (r.doi) masterSeenDois.add(r.doi);
        if (r.normalizedTitle) masterSeenTitles.add(r.normalizedTitle);
        flagged.push({ recordTitle: r.title.slice(0,100), matchedTitle: matchedRecord?.title?.slice(0,100) ?? "", method, confidence: +conf.toFixed(4), reason: "Low-confidence match retained for review" });
        auditLog.push({ action: "flagged_retained", method, confidence: +conf.toFixed(4), sourceFile: r.sourceFile, title: r.title.slice(0,100), matchedTitle: matchedRecord?.title?.slice(0,100) ?? "" });
      } else {
        skipped++;
        auditLog.push({ action: "removed", method, confidence: +conf.toFixed(4), sourceFile: r.sourceFile, title: r.title.slice(0,100), matchedTitle: matchedRecord?.title?.slice(0,100) ?? "" });
      }
      continue;
    }

    localUnique.push(r);
    masterUniqueList.push(r);
    if (r.doi) masterSeenDois.add(r.doi);
    if (r.normalizedTitle) masterSeenTitles.add(r.normalizedTitle);
    auditLog.push({ action: "kept", method: "unique", confidence: 1.0, sourceFile: r.sourceFile, title: r.title.slice(0,100) });
  }

  return { localUnique, flagged, skipped };
}

/* ─── Main message handler ───────────────────────── */
self.onmessage = function(e) {
  const { fileData, fuzzyThreshold, yearThreshold } = e.data;

  const auditLog = [];
  const masterSeenDois   = new Set();
  const masterSeenTitles = new Set();
  const masterUniqueList = [];
  const allFlagged = [];
  const fileResults = [];
  const deduplicatedRecords = [];
  const fileRecordCounts = {};

  const totalFiles = fileData.length;

  for (let fi = 0; fi < fileData.length; fi++) {
    const { name, content } = fileData[fi];
    const { records, label } = detectAndParse(content, name);

    if (!label) {
      self.postMessage({ type: "log", msg: `⚠ Could not detect format: ${name}`, level: "warn" });
      continue;
    }

    fileRecordCounts[name] = records.length;
    self.postMessage({
      type: "log",
      msg: `📄 ${name} — ${records.length} records (${label})`,
      level: "",
      progress: (fi / totalFiles) * 100
    });

    const { localUnique, flagged, skipped } = processFile(
      records, masterSeenDois, masterSeenTitles, masterUniqueList, auditLog,
      fuzzyThreshold, yearThreshold, name, fi, totalFiles
    );

    allFlagged.push(...flagged);
    fileResults.push({ name, format: label, total: records.length, unique: localUnique.length, removed: skipped, flagged: flagged.length });

    // Serialize records for transfer (strip class methods — plain objects only)
    deduplicatedRecords.push({
      name, format: label,
      records: localUnique.map(r => ({
        sourceFile:      r.sourceFile,
        originalText:    r.originalText,
        pmid:            r.pmid,
        doi:             r.doi,
        title:           r.title,
        normalizedTitle: r.normalizedTitle,
        authors:         r.authors,
        year:            r.year,
        extraData:       r.extraData,
        format:          r.format
      }))
    });

    self.postMessage({
      type: "log",
      msg: `  ✓ Kept ${localUnique.length}, removed ${skipped}, flagged ${flagged.length}`,
      level: "success",
      progress: ((fi + 1) / totalFiles) * 100
    });
  }

  // Build summary
  const totalInput   = Object.values(fileRecordCounts).reduce((a, b) => a + b, 0);
  const totalUnique  = fileResults.reduce((a, r) => a + r.unique, 0);
  const totalRemoved = auditLog.filter(e => e.action === "removed").length;
  const totalFlagged = auditLog.filter(e => e.action === "flagged_retained").length;
  const methodCounts = {};
  auditLog.filter(e => e.action === "removed").forEach(e => {
    methodCounts[e.method] = (methodCounts[e.method] || 0) + 1;
  });

  // Send everything back to main thread
  self.postMessage({
    type: "done",
    auditLog,
    deduplicatedRecords,
    results: {
      totalInput, totalUnique, totalRemoved, totalFlagged,
      methodCounts, flagged: allFlagged, fileResults
    }
  });
};
