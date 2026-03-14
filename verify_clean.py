import os
import glob
from count_records import count_records

def main():
    # Find all deduplicated files
    files = glob.glob('*_deduplicated.*')
    
    if not files:
        print("No deduplicated files found to verify.")
        return

    print("Verification of Deduplicated Files:")
    print("-" * 40)
    total = 0
    for f in sorted(files):
        count, label = count_records(f)
        print(f"{f:<30} | {count:>5} records")
        total += count
    print("-" * 40)
    print(f"{'TOTAL UNIQUE':<30} | {total:>5} records")

if __name__ == "__main__":
    main()
