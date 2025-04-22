#!/usr/bin/env python3
"""
Reducer: Data Cleansing for Social Media Analytics

This reducer passes through valid records that were pre-filtered by the mapper.
No additional processing is performed; it acts as a simple pass-through.

Input:  UserID<TAB>Timestamp<TAB>ActionType<TAB>ContentID<TAB>Metadata
Output: UserID<TAB>Timestamp<TAB>ActionType<TAB>ContentID<TAB>Metadata
"""

import sys

def main():
    for line in sys.stdin:
        cleaned_line = line.strip()
        if cleaned_line:
            print(cleaned_line)

if __name__ == "__main__":
    main()
