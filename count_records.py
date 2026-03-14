import re
import os
import glob
import pandas as pd
import csv

def count_records(filename):
    ext = os.path.splitext(filename)[1].lower()
    
    try:
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            head = f.read(4096)
            f.seek(0)
            
            if 'PMID-' in head or ext == '.nbib':
                content = f.read()
                return len(re.findall(r'^PMID- ', content, re.MULTILINE)), "PubMed"
            
            if '@' in head and '{' in head:
                content = f.read()
                return len(re.findall(r'@\w+\s*\{', content)), "BibTeX"
            
            if 'TY  -' in head or 'ER  -' in head or ext == '.ris':
                content = f.read()
                return len(re.findall(r'\nER\s+-', content)) + (1 if head.strip().startswith('TY  -') and '\nER  -' not in content else 0), "RIS"
            
            if ext == '.csv':
                try:
                    # Sniff delimiter
                    sample = head.split('\n')[0] + '\n' + head.split('\n')[1]
                    dialect = csv.Sniffer().sniff(sample)
                    df = pd.read_csv(filename, sep=dialect.delimiter)
                    return len(df), "CSV"
                except:
                    df = pd.read_csv(filename, encoding='latin1')
                    return len(df), "CSV"
                    
            if 'PT ' in head and 'AU ' in head: # WoS Tab delimited
                df = pd.read_csv(filename, sep='\t')
                return len(df), "WoS-Tab"
                
    except Exception as e:
        return 0, f"Error: {str(e)}"
        
    return 0, "Unknown"

def main():
    extensions = ['*.txt', '*.bib', '*.ris', '*.csv', '*.nbib', '*.ciw', '*.enw']
    files = []
    for ext in extensions:
        files.extend(glob.glob(ext))
    
    # Exclude script and deduplicated files
    files = [f for f in files if '_deduplicated' not in f and f not in ['deduplicate_files.py', 'count_records.py', 'verify_clean.py', 'verify_clean_deduplicated.py']]
    files.sort()

    if not files:
        print("No input files found to count.")
        return

    print("Record Count Summary:")
    print("-" * 40)
    total = 0
    for f in files:
        count, label = count_records(f)
        print(f"{f:<30} | {count:>5} records ({label})")
        if isinstance(count, int):
            total += count
    print("-" * 40)
    print(f"{'TOTAL':<30} | {total:>5} records")

if __name__ == "__main__":
    main()
