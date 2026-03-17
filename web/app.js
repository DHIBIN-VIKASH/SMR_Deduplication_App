// app.js – SMR Deduplication Agent – Main Application
// Runs the hierarchical deduplication algorithm in-browser (Web Workers friendly)
// with Firebase persistence and a rich dashboard UI.

import {
  auth, db, provider, firebaseReady,
  signInWithPopup, signOut, onAuthStateChanged,
  collection, addDoc, getDocs, query, orderBy, limit, where as fbWhere, serverTimestamp
} from "./firebase-config.js";

/* ═══════════════════════════════════════════════════
   ❶  STATE
═══════════════════════════════════════════════════ */
const state = {
  files: [],                // File objects queued for processing
  auditLog: [],             // Full decision log from last run
  results: null,            // Summary object from last run
  deduplicatedRecords: [],  // [{name, format, records:[]}] — for download
  user: null,               // Firebase user (or null)
  history: [],              // Sessions loaded from Firestore
  auditPage: 1,
  auditFilter: "all",
  auditSearch: "",
  dashboardStats: { sessions: 0, input: 0, unique: 0, removed: 0 }
};

// Fuzzy thresholds (user-configurable)
let fuzzyThreshold = 0.95;
let yearThreshold  = 0.85;

/* ═══════════════════════════════════════════════════
   ❷  DOM REFS
═══════════════════════════════════════════════════ */
const $ = id => document.getElementById(id);

const tabs = {
  nav: {
    dashboard: $("nav-dashboard"),
    upload:    $("nav-upload"),
    results:   $("nav-results"),
    audit:     $("nav-audit"),
    history:   $("nav-history"),
  },
  panel: {
    dashboard: $("tab-dashboard"),
    upload:    $("tab-upload"),
    results:   $("tab-results"),
    audit:     $("tab-audit"),
    history:   $("tab-history"),
  }
};

/* ═══════════════════════════════════════════════════
   ❸  TAB NAVIGATION
═══════════════════════════════════════════════════ */
function switchTab(name) {
  Object.entries(tabs.nav).forEach(([k, el]) => el.classList.toggle("active", k === name));
  Object.entries(tabs.panel).forEach(([k, el]) => el.classList.toggle("active", k === name));
  $("topbar-title").textContent = {
    dashboard: "Dashboard",
    upload:    "Upload & Run",
    results:   "Results",
    audit:     "Audit Log",
    history:   "Session History"
  }[name] ?? name;
}

document.querySelectorAll(".nav-item").forEach(btn => {
  btn.addEventListener("click", () => switchTab(btn.dataset.tab));
});

$("menu-toggle").addEventListener("click", () => {
  document.querySelector(".sidebar").classList.toggle("open");
});

/* ═══════════════════════════════════════════════════
   ❹  FILE UPLOAD
═══════════════════════════════════════════════════ */
const zone      = $("upload-zone");
const fileInput = $("file-input");
const fileQueue = $("file-queue");
const btnRun    = $("btn-run");

zone.addEventListener("click", () => fileInput.click());
zone.addEventListener("keydown", e => e.key === "Enter" && fileInput.click());

zone.addEventListener("dragover", e => { e.preventDefault(); zone.classList.add("drag-over"); });
zone.addEventListener("dragleave", () => zone.classList.remove("drag-over"));
zone.addEventListener("drop", e => {
  e.preventDefault();
  zone.classList.remove("drag-over");
  addFiles([...e.dataTransfer.files]);
});

fileInput.addEventListener("change", () => addFiles([...fileInput.files]));

document.querySelector(".upload-browse").addEventListener("click", e => {
  e.stopPropagation();
  fileInput.click();
});

function addFiles(newFiles) {
  const allowed = [".txt", ".bib", ".ris", ".csv", ".nbib", ".ciw", ".enw"];
  newFiles.forEach(f => {
    const ext = f.name.slice(f.name.lastIndexOf(".")).toLowerCase();
    if (!allowed.includes(ext)) {
      toast(`⚠️ Skipped ${f.name} — unsupported format.`, "warn");
      return;
    }
    if (state.files.find(x => x.name === f.name)) {
      toast(`Already queued: ${f.name}`, "warn");
      return;
    }
    state.files.push(f);
  });
  renderFileQueue();
  btnRun.disabled = state.files.length === 0;
}

function renderFileQueue() {
  fileQueue.innerHTML = "";
  state.files.forEach((f, i) => {
    const ext = f.name.slice(f.name.lastIndexOf(".") + 1).toLowerCase();
    const div = document.createElement("div");
    div.className = "file-item";
    div.innerHTML = `
      <div class="file-icon ext-${ext}" aria-label="${ext}">${ext.toUpperCase()}</div>
      <div class="file-info">
        <div class="file-name" title="${f.name}">${f.name}</div>
        <div class="file-size">${formatBytes(f.size)}</div>
      </div>
      <button class="file-remove" data-index="${i}" aria-label="Remove ${f.name}" title="Remove">
        <svg width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/>
        </svg>
      </button>`;
    fileQueue.appendChild(div);
  });

  fileQueue.querySelectorAll(".file-remove").forEach(btn => {
    btn.addEventListener("click", () => {
      state.files.splice(+btn.dataset.index, 1);
      renderFileQueue();
      btnRun.disabled = state.files.length === 0;
    });
  });
}

/* ═══════════════════════════════════════════════════
   ❺  DEDUPLICATION ENGINE (JavaScript port)
═══════════════════════════════════════════════════ */
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

function titleSimilarity(a, b) {
  if (!a || !b) return 0;
  if (Math.abs(a.length - b.length) > Math.max(a.length, b.length) * 0.2) return 0;
  // SequenceMatcher approximation using LCS ratio
  return lcsRatio(a.toLowerCase(), b.toLowerCase());
}

function lcsRatio(s1, s2) {
  const m = s1.length, n = s2.length;
  if (!m || !n) return 0;
  // Use character-level difflib-style ratio approximation
  // For performance, cap length at 300 chars
  const a = s1.slice(0, 300), b = s2.slice(0, 300);
  const ma = a.length, mb = b.length;
  const dp = Array.from({ length: ma + 1 }, () => new Int16Array(mb + 1));
  for (let i = 1; i <= ma; i++) {
    for (let j = 1; j <= mb; j++) {
      dp[i][j] = a[i-1] === b[j-1] ? dp[i-1][j-1] + 1 : Math.max(dp[i-1][j], dp[i][j-1]);
    }
  }
  return (2 * dp[ma][mb]) / (ma + mb);
}

class Record {
  constructor({ sourceFile, originalText, pmid, doi, title, authors, year, extraData, format }) {
    this.sourceFile    = sourceFile;
    this.originalText  = originalText;
    this.pmid          = pmid && String(pmid).trim() && String(pmid).toLowerCase() !== "nan"
                          ? String(pmid).trim() : null;
    this.doi           = normalizeDoi(doi && String(doi).toLowerCase() !== "nan" ? doi : null);
    this.title         = title && String(title).toLowerCase() !== "nan" ? String(title).trim() : "";
    this.normalizedTitle = normalizeText(this.title);
    this.authors       = Array.isArray(authors) ? authors.map(String) : (authors ? [String(authors)] : []);
    this.year          = year && String(year).trim() && String(year).toLowerCase() !== "nan"
                          ? String(year).trim() : null;
    this.extraData     = extraData || {};
    this.format        = format || "Unknown";
  }

  isDuplicateOf(other, fuzzyT, yearT) {
    // 1. DOI
    if (this.doi && other.doi && this.doi === other.doi) return { dup: true, method: "DOI", conf: 1.0 };
    // 2. PMID
    if (this.pmid && other.pmid && this.pmid === other.pmid) return { dup: true, method: "PMID", conf: 1.0 };
    // 3. Exact normalized title
    if (this.normalizedTitle && other.normalizedTitle && this.normalizedTitle.length > 30)
      if (this.normalizedTitle === other.normalizedTitle)
        return { dup: true, method: "ExactTitle", conf: 0.99 };
    // 4. Fuzzy
    if (this.title && other.title && Math.abs(this.title.length - other.title.length) < 40) {
      const sim = titleSimilarity(this.title, other.title);
      if (sim >= fuzzyT) return { dup: true, method: "TitleSimilarity", conf: sim };
      if (sim >= yearT && this.year && other.year && this.year === other.year)
        return { dup: true, method: "TitleYear", conf: sim };
    }
    return { dup: false, method: null, conf: 0 };
  }
}

/* ─── Parsers ────────────────────────────────────── */
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
      pmid: pmidM?.[1]?.trim(),
      doi:  doiM?.[1]?.trim(),
      title,
      authors,
      year: yearM?.[1]?.trim()
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
      doi: doiM?.[1]?.trim(),
      title,
      authors,
      year: yearM?.[1]?.trim()
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
      doi: doiM?.[1]?.trim(),
      title: titleM?.[1]?.trim() || "",
      authors,
      year: yearM?.[1]?.trim()
    }));
  }
  return records;
}

function parseCsv(content, filename) {
  const records = [];
  try {
    const lines = content.split(/\r?\n/);
    if (lines.length < 2) return records;

    // Detect delimiter
    const firstLine = lines[0];
    const delim = (firstLine.match(/,/g) || []).length > (firstLine.match(/\t/g) || []).length ? "," : "\t";

    const parseLine = (line) => {
      const result = [];
      let current = "", inQuote = false;
      for (let i = 0; i < line.length; i++) {
        const c = line[i];
        if (c === '"' && !inQuote) { inQuote = true; continue; }
        if (c === '"' && inQuote) {
          if (line[i+1] === '"') { current += '"'; i++; }
          else { inQuote = false; }
          continue;
        }
        if (c === delim && !inQuote) { result.push(current); current = ""; continue; }
        current += c;
      }
      result.push(current);
      return result;
    };

    const headers = parseLine(lines[0]).map(h => h.toLowerCase().trim());
    const col = (keywords) => headers.findIndex(h => keywords.some(k => h.includes(k)));

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
  } catch(e) {
    console.error("CSV parse error:", e);
  }
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
  // Fallback by extension
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

/* ─── Core dedup logic ───────────────────────────── */
function processFile(records, masterSeenDois, masterSeenTitles, masterUniqueList, auditLog, fT, yT) {
  const localUnique = [];
  const flagged = [];
  let skipped = 0;

  for (const r of records) {
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
      // Conservative retention for uncertain matches
      if (method === "TitleYear" && conf < 0.92) {
        localUnique.push(r);
        masterUniqueList.push(r);
        if (r.doi) masterSeenDois.add(r.doi);
        if (r.normalizedTitle) masterSeenTitles.add(r.normalizedTitle);
        flagged.push({ recordTitle: r.title.slice(0, 100), matchedTitle: matchedRecord?.title?.slice(0, 100) ?? "", method, confidence: +conf.toFixed(4), reason: "Low-confidence match retained for review" });
        auditLog.push({ action: "flagged_retained", method, confidence: +conf.toFixed(4), sourceFile: r.sourceFile, title: r.title.slice(0, 100), matchedTitle: matchedRecord?.title?.slice(0, 100) ?? "" });
      } else {
        skipped++;
        auditLog.push({ action: "removed", method, confidence: +conf.toFixed(4), sourceFile: r.sourceFile, title: r.title.slice(0, 100), matchedTitle: matchedRecord?.title?.slice(0, 100) ?? "" });
      }
      continue;
    }

    localUnique.push(r);
    masterUniqueList.push(r);
    if (r.doi) masterSeenDois.add(r.doi);
    if (r.normalizedTitle) masterSeenTitles.add(r.normalizedTitle);
    auditLog.push({ action: "kept", method: "unique", confidence: 1.0, sourceFile: r.sourceFile, title: r.title.slice(0, 100) });
  }

  return { localUnique, flagged, skipped };
}

/* ═══════════════════════════════════════════════════
   ❻  RUN DEDUPLICATION
═══════════════════════════════════════════════════ */
btnRun.addEventListener("click", runDeduplication);

async function runDeduplication() {
  if (state.files.length === 0) return;

  const sessionName = $("session-name").value.trim() || `Session – ${new Date().toLocaleString()}`;
  fuzzyThreshold = (+$("fuzzy-threshold").value || 95) / 100;
  yearThreshold  = (+$("year-threshold").value  || 85) / 100;

  setStatus("running", "Processing…");
  btnRun.disabled = true;
  state.deduplicatedRecords = []; // reset for new run

  const progressPanel = $("progress-panel");
  const progressBar   = $("progress-bar");
  const progressLog   = $("progress-log");
  progressPanel.hidden = false;
  progressLog.innerHTML = "";

  const log = (msg, type = "") => {
    const now = new Date().toLocaleTimeString();
    const line = document.createElement("div");
    line.className = "log-line";
    line.innerHTML = `<span class="log-time">${now}</span><span class="log-msg ${type}">${escapeHtml(msg)}</span>`;
    progressLog.appendChild(line);
    progressLog.scrollTop = progressLog.scrollHeight;
  };

  log("Reading files…");

  // ① Read files on main thread (FileReader is not available in Workers)
  let fileData;
  try {
    fileData = await Promise.all(
      state.files.map(f => readFileAsync(f).then(content => ({ name: f.name, content })))
    );
  } catch(e) {
    log("❌ Error reading files: " + e.message, "error");
    setStatus("error", "Error"); btnRun.disabled = false; return;
  }

  log(`Sending ${fileData.length} file(s) to background worker…`);
  progressBar.style.width = "5%";

  // ② Send file content as Transferable ArrayBuffers — ZERO-COPY.
  //    Regular postMessage() with strings triggers a structured clone
  //    (a synchronous main-thread copy) that causes the UI freeze.
  //    Transferables hand ownership to the Worker instantly with no copy.
  const encoder = new TextEncoder();
  const transferBuffers = [];
  const transferableFileData = fileData.map(({ name, content }) => {
    const buf = encoder.encode(content).buffer;
    transferBuffers.push(buf);
    return { name, buf };
  });

  const worker = new Worker("dedup-worker.js");
  worker.postMessage(
    { fileData: transferableFileData, fuzzyThreshold, yearThreshold },
    transferBuffers   // ownership transferred — no blocking copy
  );

  worker.onmessage = async (e) => {
    const msg = e.data;

    if (msg.type === "log" || msg.type === "progress") {
      if (msg.msg) log(msg.msg, msg.level || "");
      if (msg.progress != null)
        progressBar.style.width = Math.max(5, Math.min(98, msg.progress)).toFixed(1) + "%";
    }

    if (msg.type === "done") {
      worker.terminate();

      state.auditLog            = msg.auditLog;
      state.deduplicatedRecords = msg.deduplicatedRecords;
      state.results             = { sessionName, timestamp: new Date().toISOString(), ...msg.results };

      progressBar.style.width = "100%";
      const r = state.results;
      log(`✅ Done! ${r.totalInput} in → ${r.totalUnique} unique, ${r.totalRemoved} removed, ${r.totalFlagged} flagged.`, "success");

      setStatus("ready", "Complete");
      btnRun.disabled = false;
      toast("✅ Deduplication complete!", "success");

      updateDashboard();
      renderResults();
      renderAuditTable();
      $("btn-export-audit").disabled = false;

      await saveSession(state.results);
      setTimeout(() => switchTab("results"), 800);
    }
  };

  worker.onerror = (e) => {
    worker.terminate();
    log("❌ Worker error: " + e.message, "error");
    setStatus("error", "Error");
    btnRun.disabled = false;
    toast("❌ Worker error — check console.", "error");
  };
}


/* ═══════════════════════════════════════════════════
   ❼  RENDER RESULTS TAB
═══════════════════════════════════════════════════ */
/* ─── Reconstruct file content from records ─────── */
function reconstructContent(records, format) {
  if (format === "PubMed") {
    return records.map(r => r.originalText.trim()).join("\n\n");
  }
  if (format === "BibTeX") {
    return records.map(r => r.originalText.trim()).join("\n\n");
  }
  if (format === "RIS") {
    return records.map(r => {
      let t = r.originalText.trim();
      if (!t.endsWith("ER  -")) t += "\nER  -";
      return t;
    }).join("\n\n");
  }
  if (format === "CSV" || format === "WoS-Tab") {
    if (!records.length) return "";
    const headers = Object.keys(records[0].extraData);
    const escape = v => `"${String(v ?? "").replace(/"/g, '""')}"`;
    const rows = records.map(r => headers.map(h => escape(r.extraData[h])).join(","));
    return [headers.map(escape).join(","), ...rows].join("\n");
  }
  return records.map(r => r.originalText).join("\n\n");
}

/* ─── Build a merged RIS from all unique records ── */
function buildMergedRis(deduplicatedRecords) {
  const lines = [];
  for (const { name, format, records } of deduplicatedRecords) {
    const dbLabel = name.replace(/\.[^.]+$/, ""); // strip extension
    for (const r of records) {
      // Start with original text
      let entry = r.originalText.trim();

      // Convert non-RIS formats to a minimal RIS block
      if (format === "PubMed") {
        entry = convertPubMedToRis(r, dbLabel);
      } else if (format === "BibTeX") {
        entry = convertBibToRis(r, dbLabel);
      } else if (format === "CSV" || format === "WoS-Tab") {
        entry = convertCsvToRis(r, dbLabel);
      } else {
        // RIS — inject DB source tag before ER
        entry = entry.replace(/(ER\s+-\s*$)/m, `DB  - ${dbLabel}\n$1`);
        if (!entry.includes("DB  -")) entry += `\nDB  - ${dbLabel}\nER  -`;
        if (!entry.endsWith("ER  -")) entry += "\nER  -";
      }
      lines.push(entry);
    }
  }
  return lines.join("\n\n");
}

function convertPubMedToRis(r, db) {
  const parts = ["TY  - JOUR"];
  if (r.title)   parts.push(`TI  - ${r.title}`);
  if (r.doi)     parts.push(`DO  - ${r.doi}`);
  if (r.pmid)    parts.push(`AN  - ${r.pmid}`);
  if (r.year)    parts.push(`PY  - ${r.year}`);
  r.authors.forEach(a => parts.push(`AU  - ${a}`));
  parts.push(`DB  - ${db}`);
  parts.push("ER  -");
  return parts.join("\n");
}

function convertBibToRis(r, db) {
  const parts = ["TY  - JOUR"];
  if (r.title)   parts.push(`TI  - ${r.title}`);
  if (r.doi)     parts.push(`DO  - ${r.doi}`);
  if (r.year)    parts.push(`PY  - ${r.year}`);
  r.authors.forEach(a => parts.push(`AU  - ${a}`));
  parts.push(`DB  - ${db}`);
  parts.push("ER  -");
  return parts.join("\n");
}

function convertCsvToRis(r, db) {
  const parts = ["TY  - JOUR"];
  if (r.title)   parts.push(`TI  - ${r.title}`);
  if (r.doi)     parts.push(`DO  - ${r.doi}`);
  if (r.pmid)    parts.push(`AN  - ${r.pmid}`);
  if (r.year)    parts.push(`PY  - ${r.year}`);
  r.authors.forEach(a => parts.push(`AU  - ${a}`));
  parts.push(`DB  - ${db}`);
  parts.push("ER  -");
  return parts.join("\n");
}

/* ─── Build merged CSV ───────────────────────────── */
function buildMergedCsv(deduplicatedRecords) {
  const allRows = [];
  for (const { name, records } of deduplicatedRecords) {
    const db = name.replace(/\.[^.]+$/, "");
    for (const r of records) {
      allRows.push({
        Title:   r.title || "",
        Authors: r.authors.join("; "),
        Year:    r.year || "",
        DOI:     r.doi || "",
        PMID:    r.pmid || "",
        Source:  db
      });
    }
  }
  if (!allRows.length) return "";
  const headers = Object.keys(allRows[0]);
  const escape = v => `"${String(v ?? "").replace(/"/g, '""')}"`;
  return [headers.join(","), ...allRows.map(row => headers.map(h => escape(row[h])).join(","))].join("\n");
}

function renderResults() {
  const r = state.results;
  if (!r) return;

  const container = $("results-content");
  const rate = r.totalInput > 0 ? ((r.totalRemoved / r.totalInput) * 100).toFixed(1) : "0.0";

  container.innerHTML = `
    <div class="results-summary">
      <div class="result-mini"><span class="rval">${r.totalInput.toLocaleString()}</span><span class="rlabel">Total Input</span></div>
      <div class="result-mini"><span class="rval">${r.totalUnique.toLocaleString()}</span><span class="rlabel">Unique Kept</span></div>
      <div class="result-mini"><span class="rval">${r.totalRemoved.toLocaleString()}</span><span class="rlabel">Removed</span></div>
      <div class="result-mini"><span class="rval">${r.totalFlagged.toLocaleString()}</span><span class="rlabel">Flagged</span></div>
      <div class="result-mini"><span class="rval">${rate}%</span><span class="rlabel">Dedup Rate</span></div>
    </div>

    <!-- ★ PRIMARY: Merged download panel ★ -->
    <div class="glass-card merged-download-card">
      <div class="merged-header">
        <div>
          <h3 style="margin-bottom:4px">⬇️ Download Merged File</h3>
          <p style="margin:0">All <strong>${r.totalUnique.toLocaleString()} unique records</strong> from all databases combined into one file — ready to import into Rayyan, Covidence, or Endnote for screening.</p>
        </div>
      </div>
      <div class="merged-tip">
        <svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
        <span>Each record includes a <code>DB</code> field with its source database name so you can still trace origins after screening.</span>
      </div>
      <div class="merged-actions">
        <button class="btn-download-merged ris" id="btn-dl-merged-ris">
          <svg width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"/></svg>
          Download as RIS
          <span class="fmt-badge">Recommended · works with Rayyan &amp; Covidence</span>
        </button>
        <button class="btn-download-merged csv" id="btn-dl-merged-csv">
          <svg width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"/></svg>
          Download as CSV
          <span class="fmt-badge">Excel / Sheets compatible</span>
        </button>
      </div>
    </div>

    <!-- Per-file breakdown -->
    <div class="glass-card" style="margin-bottom:16px">
      <h3>Per-File Breakdown</h3>
      <div class="file-results-grid" id="file-results-grid"></div>
    </div>

    ${r.flagged.length > 0 ? `
    <div class="glass-card flagged-panel">
      <h3>⚠️ Flagged for Human Review (${r.flagged.length})</h3>
      <div>${r.flagged.map(f => `
        <div class="flagged-row">
          <div class="flagged-title">${escapeHtml(f.recordTitle)}</div>
          <div class="flagged-matched">Matched: ${escapeHtml(f.matchedTitle)}</div>
          <div class="flagged-meta">
            <span class="conf-badge ${confClass(f.confidence)}">${(f.confidence * 100).toFixed(1)}%</span>
            <span style="color:var(--text-muted);font-size:0.7rem;">${f.method}</span>
          </div>
        </div>`).join("")}
      </div>
    </div>` : ""}
  `;

  // Wire merged download buttons
  $("btn-dl-merged-ris").addEventListener("click", () => {
    const content = buildMergedRis(state.deduplicatedRecords);
    downloadFile(content, "text/plain", `${r.sessionName.replace(/[^a-z0-9]/gi,"_")}_deduplicated_merged.ris`);
    toast("📥 Merged RIS downloaded — import into your screener!", "success");
  });

  $("btn-dl-merged-csv").addEventListener("click", () => {
    const content = buildMergedCsv(state.deduplicatedRecords);
    downloadFile(content, "text/csv", `${r.sessionName.replace(/[^a-z0-9]/gi,"_")}_deduplicated_merged.csv`);
    toast("📥 Merged CSV downloaded!", "success");
  });

  // Render per-file cards
  const grid = $("file-results-grid");
  r.fileResults.forEach((fr, idx) => {
    const pct = fr.total > 0 ? (fr.unique / fr.total) * 100 : 0;
    const card = document.createElement("div");
    card.className = "file-result-card";
    card.innerHTML = `
      <div class="frc-header">
        <div class="frc-name" title="${escapeHtml(fr.name)}">${escapeHtml(fr.name)}</div>
        <span class="frc-format">${fr.format}</span>
      </div>
      <div class="frc-bar-label">
        <span>Kept</span><span>${fr.unique} / ${fr.total}</span>
      </div>
      <div class="frc-bar-track"><div class="frc-bar-fill" style="width:0%" data-pct="${pct.toFixed(1)}"></div></div>
      <div class="frc-stats">
        <span class="frc-stat"><strong>${fr.total}</strong> in</span>
        <span class="frc-stat"><strong>${fr.unique}</strong> kept</span>
        <span class="frc-stat"><strong>${fr.removed}</strong> removed</span>
        ${fr.flagged > 0 ? `<span class="frc-stat"><strong>${fr.flagged}</strong> flagged</span>` : ""}
      </div>
      <button class="btn-dl-file" data-idx="${idx}" title="Download deduplicated ${fr.name}">
        <svg width="13" height="13" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"/></svg>
        Download
      </button>`;
    grid.appendChild(card);
  });

  // Wire per-file download buttons
  document.querySelectorAll(".btn-dl-file").forEach(btn => {
    btn.addEventListener("click", () => {
      const idx = +btn.dataset.idx;
      const { name, format, records } = state.deduplicatedRecords[idx];
      const content = reconstructContent(records, format);
      const outName = name.replace(/(\.[^.]+)$/, "_deduplicated$1");
      const mime = (format === "CSV" || format === "WoS-Tab") ? "text/csv" : "text/plain";
      downloadFile(content, mime, outName);
      toast(`📥 ${outName} downloaded!`, "success");
    });
  });

  // Animate bars
  requestAnimationFrame(() => {
    document.querySelectorAll(".frc-bar-fill").forEach(el => {
      el.style.width = el.dataset.pct + "%";
    });
  });
}

/* ═══════════════════════════════════════════════════
   ❽  AUDIT TABLE
═══════════════════════════════════════════════════ */
const PAGE_SIZE = 50;

function getFilteredAudit() {
  let rows = state.auditLog;
  if (state.auditFilter !== "all") {
    if (state.auditFilter === "flagged") rows = rows.filter(r => r.action === "flagged_retained");
    else rows = rows.filter(r => r.action === state.auditFilter);
  }
  if (state.auditSearch) {
    const q = state.auditSearch.toLowerCase();
    rows = rows.filter(r =>
      r.title?.toLowerCase().includes(q) ||
      r.method?.toLowerCase().includes(q) ||
      r.sourceFile?.toLowerCase().includes(q)
    );
  }
  return rows;
}

function renderAuditTable() {
  const rows = getFilteredAudit();
  const totalPages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
  state.auditPage = Math.min(state.auditPage, totalPages);
  const start = (state.auditPage - 1) * PAGE_SIZE;
  const pageRows = rows.slice(start, start + PAGE_SIZE);

  const tbody = $("audit-tbody");
  if (pageRows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="6" class="empty-cell">No entries match your filter.</td></tr>`;
  } else {
    tbody.innerHTML = pageRows.map((r, i) => `
      <tr>
        <td style="color:var(--text-faint);font-family:'JetBrains Mono',monospace;font-size:0.72rem">${start + i + 1}</td>
        <td><span class="action-badge ${actionClass(r.action)}">${actionLabel(r.action)}</span></td>
        <td><span class="method-pill">${r.method || "—"}</span></td>
        <td>
          <div class="conf-bar">
            <span class="conf-num">${(r.confidence * 100).toFixed(0)}%</span>
            <div class="mini-bar"><div class="mini-bar-fill" style="width:${r.confidence * 100}%"></div></div>
          </div>
        </td>
        <td style="max-width:320px;color:var(--text-primary);font-size:0.78rem;line-height:1.4">${escapeHtml(r.title || "—")}</td>
        <td style="color:var(--text-faint);font-size:0.72rem;font-family:'JetBrains Mono',monospace;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escapeHtml(r.sourceFile || "")}">${escapeHtml(shortPath(r.sourceFile || ""))}</td>
      </tr>`).join("");
  }

  // Pagination
  const pg = $("audit-pagination");
  pg.innerHTML = "";
  for (let p = 1; p <= totalPages; p++) {
    const btn = document.createElement("button");
    btn.className = `page-btn${p === state.auditPage ? " active" : ""}`;
    btn.textContent = p;
    btn.addEventListener("click", () => { state.auditPage = p; renderAuditTable(); });
    pg.appendChild(btn);
  }
}

// Audit filters
document.querySelectorAll(".filter-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".filter-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    state.auditFilter = btn.dataset.filter;
    state.auditPage = 1;
    renderAuditTable();
  });
});

$("audit-search").addEventListener("input", e => {
  state.auditSearch = e.target.value;
  state.auditPage = 1;
  renderAuditTable();
});

$("btn-export-audit").addEventListener("click", () => {
  if (!state.results) return;
  const payload = {
    timestamp: state.results.timestamp,
    sessionName: state.results.sessionName,
    summary: {
      totalInput: state.results.totalInput,
      totalUnique: state.results.totalUnique,
      totalRemoved: state.results.totalRemoved,
      totalFlagged: state.results.totalFlagged,
      duplicatesByMethod: state.results.methodCounts
    },
    flaggedForHumanReview: state.results.flagged,
    decisions: state.auditLog
  };
  downloadJson(payload, `dedup_audit_${new Date().toISOString().slice(0,10)}.json`);
  toast("📥 Audit log exported!", "success");
});

/* ═══════════════════════════════════════════════════
   ❾  DASHBOARD UPDATE
═══════════════════════════════════════════════════ */
function updateDashboard() {
  const r = state.results;
  if (!r) return;

  // Accumulate
  state.dashboardStats.sessions += 1;
  state.dashboardStats.input   += r.totalInput;
  state.dashboardStats.unique  += r.totalUnique;
  state.dashboardStats.removed += r.totalRemoved;

  animateCounter($("dash-sessions"), state.dashboardStats.sessions);
  animateCounter($("dash-input"),    state.dashboardStats.input);
  animateCounter($("dash-unique"),   state.dashboardStats.unique);
  animateCounter($("dash-removed"),  state.dashboardStats.removed);

  updateDonut(r.totalInput, r.totalRemoved, r.totalFlagged);
  updateMethodBars(r.methodCounts);
  addRecentSession(r);
}

function updateDonut(total, removed, flagged) {
  const CIRC = 339.3;
  const pct = total > 0 ? removed / total : 0;
  const pctF = total > 0 ? flagged / total : 0;

  const segR = $("donut-seg-removed");
  const segF = $("donut-seg-flagged");

  const dash = (p) => `${(CIRC * p).toFixed(1)} ${CIRC}`;
  segR.style.strokeDasharray = dash(pct);
  segR.style.strokeDashoffset = "0";

  // Offset flagged segment after removed
  const offset = -(CIRC * pct);
  segF.style.strokeDasharray = dash(pctF);
  segF.style.strokeDashoffset = offset;

  $("donut-pct").textContent = total > 0 ? `${(pct * 100).toFixed(0)}%` : "—";
}

function updateMethodBars(methodCounts) {
  const container = $("method-bars");
  const entries = Object.entries(methodCounts).sort((a, b) => b[1] - a[1]);
  if (!entries.length) { container.innerHTML = `<div class="empty-state-small">No duplicates found.</div>`; return; }

  const max = entries[0][1];
  container.innerHTML = entries.map(([m, c]) => `
    <div class="method-bar-row">
      <div class="method-bar-meta">
        <span class="method-bar-label">${m}</span>
        <span class="method-bar-count">${c.toLocaleString()}</span>
      </div>
      <div class="method-bar-track">
        <div class="method-bar-fill" style="width:${((c/max)*100).toFixed(1)}%"></div>
      </div>
    </div>`).join("");
}

function addRecentSession(r) {
  const container = $("recent-sessions-list");
  container.querySelector(".empty-state-small")?.remove();

  const row = document.createElement("div");
  row.className = "session-row";
  row.innerHTML = `
    <div class="session-name">${escapeHtml(r.sessionName)}</div>
    <div class="session-meta">${r.totalInput} in · ${r.totalUnique} kept</div>
    <span class="session-badge ok">${new Date(r.timestamp).toLocaleDateString()}</span>
  `;
  row.addEventListener("click", () => switchTab("results"));
  container.prepend(row);
}

/* ═══════════════════════════════════════════════════
   ❿  AUTH & HISTORY  (Progressive Enhancement)
   ─────────────────────────────────────────────────
   Storage hierarchy:
     1. localStorage  →  always works, guest-friendly
     2. Firestore     →  bonus when signed in + Firebase ready
   History tab is never gated behind sign-in.
═══════════════════════════════════════════════════ */
const LS_KEY = "smr_dedup_history";
const LS_MAX = 50;   // max sessions stored locally

/* ─── localStorage helpers ───────────────────────── */
function lsLoadHistory() {
  try {
    return JSON.parse(localStorage.getItem(LS_KEY) || "[]");
  } catch { return []; }
}

function lsSaveSession(r) {
  try {
    const sessions = lsLoadHistory();
    sessions.unshift({
      id: Date.now().toString(),
      sessionName: r.sessionName,
      timestamp: r.timestamp,
      totalInput: r.totalInput,
      totalUnique: r.totalUnique,
      totalRemoved: r.totalRemoved,
      totalFlagged: r.totalFlagged,
      methodCounts: r.methodCounts,
      fileNames: r.fileResults.map(f => f.name),
      storageType: "local"
    });
    localStorage.setItem(LS_KEY, JSON.stringify(sessions.slice(0, LS_MAX)));
  } catch(e) { console.warn("localStorage save failed:", e); }
}

/* ─── Unified save: localStorage always, Firestore when possible ── */
async function saveSession(r) {
  // Always save locally — instant, no auth needed
  lsSaveSession(r);

  // Additionally save to Firestore if configured and signed in
  if (firebaseReady && db && state.user) {
    try {
      await addDoc(collection(db, "sessions"), {
        uid: state.user.uid,
        sessionName: r.sessionName,
        timestamp: serverTimestamp(),
        totalInput: r.totalInput,
        totalUnique: r.totalUnique,
        totalRemoved: r.totalRemoved,
        totalFlagged: r.totalFlagged,
        methodCounts: r.methodCounts,
        fileNames: r.fileResults.map(f => f.name)
      });
      toast("☁️ Session saved to Cloud + Local.", "success");
    } catch(e) {
      console.warn("Firestore save failed (saved locally):", e);
      toast("💾 Session saved locally.", "success");
    }
  } else {
    toast("💾 Session saved locally.", "success");
  }

  // Refresh history panel
  loadHistory();
}

/* ─── Load history: Firestore if signed in, else localStorage ──── */
async function loadHistory() {
  if (firebaseReady && db && state.user) {
    // Try Firestore first
    try {
      const q = query(
        collection(db, "sessions"),
        fbWhere("uid", "==", state.user.uid),
        orderBy("timestamp", "desc"),
        limit(50)
      );
      const snap = await getDocs(q);
      const cloudSessions = snap.docs.map(d => ({
        id: d.id, ...d.data(),
        timestamp: d.data().timestamp?.toDate?.()?.toISOString() || new Date().toISOString(),
        storageType: "cloud"
      }));
      // Merge: cloud sessions + any local ones not in cloud
      const localSessions = lsLoadHistory();
      const cloudIds = new Set(cloudSessions.map(s => s.sessionName + s.totalInput));
      const uniqueLocal = localSessions.filter(s => !cloudIds.has(s.sessionName + s.totalInput));
      state.history = [...cloudSessions, ...uniqueLocal].slice(0, 50);
      renderHistory();
      return;
    } catch(e) {
      console.warn("Firestore load failed, falling back to localStorage:", e);
    }
  }
  // Fallback: localStorage
  state.history = lsLoadHistory();
  renderHistory();
}

/* ─── Auth UI ─────────────────────────────────────── */
const btnAuth = $("btn-auth");

// Always load local history on startup — no auth needed
loadHistory();

if (firebaseReady && auth) {
  onAuthStateChanged(auth, user => {
    state.user = user;
    updateAuthUI(user);
    if (user) loadHistory(); // reload with cloud data on sign-in
  });

  btnAuth.addEventListener("click", async () => {
    if (state.user) {
      await signOut(auth);
      toast("👋 Signed out. History now shows local sessions.", "success");
      loadHistory();
    } else {
      try {
        await signInWithPopup(auth, provider);
        toast("✅ Signed in! Cloud history loaded.", "success");
      } catch(e) {
        toast("❌ Sign-in failed: " + e.message, "error");
      }
    }
  });
} else {
  // Firebase not configured — show informative button, don't disable
  btnAuth.textContent = "☁️ Enable Cloud Sync";
  btnAuth.title = "Update firebase-config.js to enable Google sign-in and cloud history";
  btnAuth.addEventListener("click", () => {
    toast("ℹ️ Add Firebase config to firebase-config.js to enable cloud sync.", "warn");
  });
}

function updateAuthUI(user) {
  const avatar = $("user-avatar");
  const name   = $("user-name");
  const status = $("user-status");
  const badge  = $("storage-mode-badge");

  if (user) {
    name.textContent   = user.displayName || user.email || "User";
    status.textContent = "Cloud sync active";
    if (user.photoURL) {
      avatar.innerHTML = `<img src="${user.photoURL}" alt="avatar" />`;
    } else {
      avatar.textContent = (user.displayName || "U")[0].toUpperCase();
    }
    btnAuth.textContent = "Sign out";
    if (badge) { badge.textContent = "☁️ Cloud"; badge.className = "storage-badge cloud"; }
  } else {
    name.textContent   = "Guest";
    status.textContent = "Local mode";
    avatar.textContent = "G";
    btnAuth.textContent = firebaseReady ? "Sign in with Google" : "☁️ Enable Cloud Sync";
    if (badge) { badge.textContent = "💾 Local"; badge.className = "storage-badge local"; }
  }
}

async function saveSession(r) {
  if (!firebaseReady || !db || !state.user) return;
  try {
    await addDoc(collection(db, "sessions"), {
      uid: state.user.uid,
      sessionName: r.sessionName,
      timestamp: serverTimestamp(),
      totalInput: r.totalInput,
      totalUnique: r.totalUnique,
      totalRemoved: r.totalRemoved,
      totalFlagged: r.totalFlagged,
      methodCounts: r.methodCounts,
      fileNames: r.fileResults.map(f => f.name)
    });
    toast("☁️ Session saved to Firebase.", "success");
  } catch(e) {
    console.error("Firestore save error:", e);
  }
}

async function loadHistory() {
  if (!firebaseReady || !db || !state.user) return;
  try {
    const q = query(
      collection(db, "sessions"),
      fbWhere("uid", "==", state.user.uid),
      orderBy("timestamp", "desc"),
      limit(20)
    );
    const snap = await getDocs(q);
    state.history = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    renderHistory();
  } catch(e) {
    console.warn("Could not load history:", e);
  }
}

function renderHistory() {
  const container = $("history-content");
  if (!state.history.length) {
    container.innerHTML = `<div class="empty-state">
      <svg width="64" height="64" fill="none" viewBox="0 0 24 24" stroke="#334166"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
      <p>No sessions yet. Run your first deduplication to see history here!</p>
      <p style="font-size:0.75rem;color:var(--text-faint)">Sessions are saved in your browser automatically. Sign in to sync across devices.</p>
    </div>`;
    return;
  }

  container.innerHTML = `
    <div class="history-mode-bar">
      <span>${state.user ? '☁️ Showing cloud + local sessions' : '💾 Showing local sessions (sign in to sync across devices)'}</span>
      <button class="btn-clear-history" id="btn-clear-history">Clear Local</button>
    </div>
    <div class="history-grid">
      ${state.history.map(s => `
        <div class="history-card" id="hc-${s.id}">
          <div class="hc-top">
            <div class="hc-name">${escapeHtml(s.sessionName || "Untitled Session")}</div>
            <span class="hc-storage ${s.storageType === 'cloud' ? 'cloud' : 'local'}">${s.storageType === 'cloud' ? '☁️' : '💾'}</span>
          </div>
          <div class="hc-date">${new Date(s.timestamp).toLocaleString()}</div>
          <div class="hc-stats">
            <div class="hc-stat"><span class="v">${(s.totalInput||0).toLocaleString()}</span><span class="k">Input</span></div>
            <div class="hc-stat"><span class="v">${(s.totalUnique||0).toLocaleString()}</span><span class="k">Unique</span></div>
            <div class="hc-stat"><span class="v">${(s.totalRemoved||0).toLocaleString()}</span><span class="k">Removed</span></div>
            <div class="hc-stat"><span class="v">${(s.totalFlagged||0).toLocaleString()}</span><span class="k">Flagged</span></div>
          </div>
        </div>`).join("")}
    </div>`;

  $("btn-clear-history")?.addEventListener("click", () => {
    if (!confirm("Clear all local session history from this browser?")) return;
    localStorage.removeItem(LS_KEY);
    state.history = state.history.filter(s => s.storageType === "cloud");
    renderHistory();
    toast("🗑️ Local history cleared.", "success");
  });
}

/* ═══════════════════════════════════════════════════
   UTILITIES
═══════════════════════════════════════════════════ */
function readFileAsync(file) {
  return new Promise((res, rej) => {
    const reader = new FileReader();
    reader.onload = e => res(e.target.result);
    reader.onerror = rej;
    reader.readAsText(file, "utf-8");
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function formatBytes(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / 1048576).toFixed(1) + " MB";
}

function escapeHtml(str) {
  return String(str).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}

function shortPath(p) {
  const parts = p.replace(/\\/g, "/").split("/");
  return parts[parts.length - 1];
}

function actionClass(action) {
  if (action === "removed") return "removed";
  if (action === "flagged_retained") return "flagged";
  return "kept";
}
function actionLabel(action) {
  if (action === "flagged_retained") return "Flagged";
  return action.charAt(0).toUpperCase() + action.slice(1);
}
function confClass(c) {
  if (c >= 0.95) return "conf-high";
  if (c >= 0.85) return "conf-med";
  return "conf-low";
}

function setStatus(type, text) {
  const pill = $("status-pill");
  pill.className = `status-pill ${type === "running" ? "running" : type === "error" ? "error" : ""}`;
  $("status-text").textContent = text;
}

function toast(message, type = "success") {
  const icons = { success: "✅", error: "❌", warn: "⚠️" };
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.innerHTML = `<span class="toast-icon">${icons[type] || "ℹ️"}</span><span>${escapeHtml(message)}</span>`;
  $("toast-container").appendChild(el);
  setTimeout(() => {
    el.classList.add("toast-out");
    setTimeout(() => el.remove(), 350);
  }, 4000);
}

function animateCounter(el, target) {
  const start = +el.textContent.replace(/,/g, "") || 0;
  const duration = 600;
  const startTime = performance.now();
  const tick = (now) => {
    const t = Math.min((now - startTime) / duration, 1);
    const ease = 1 - Math.pow(1 - t, 3);
    el.textContent = Math.round(start + (target - start) * ease).toLocaleString();
    if (t < 1) requestAnimationFrame(tick);
  };
  requestAnimationFrame(tick);
}

function downloadJson(obj, filename) {
  const blob = new Blob([JSON.stringify(obj, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

function downloadFile(content, mime, filename) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

// (where is imported from firebase-config as fbWhere above)
