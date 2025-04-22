#!/usr/bin/env python3
"""
User Activity Join Mapper

Processes user activity data for distributed join operation with profile data.
Tags activity records with 'A:' prefix and handles data skew through salting.

Features:
- Identifies and salts skewed keys to prevent reducer overload
- Maintains data lineage through consistent tagging
- Robust error handling with detailed logging

Environment Variables:
- skewed.keys: Comma-separated list of known skewed user IDs

Input Format:
  UserID<TAB>posts:N,likes:N,comments:N,shares:N

Output Format:
  UserID<TAB>A:posts:N,likes:N,comments:N,shares:N  (normal keys)
  UserID_salt<TAB>A:posts:N,likes:N,comments:N,shares:N  (skewed keys)
"""

import sys
import os
from typing import Set

class SkewHandler:
    """Handles data skew mitigation through key salting"""
    def __init__(self):
        self.skewed_keys = self._load_skewed_keys()
        self.num_salts = 10  # Configurable salt count
    
    @staticmethod
    def _load_skewed_keys() -> Set[str]:
        """Load skewed keys from environment variable"""
        skewed_keys_str = os.environ.get('skewed.keys', '')
        return set(skewed_keys_str.split(',')) if skewed_keys_str else set()
    
    def process_key(self, user_id: str) -> list:
        """
        Generate salted keys for skewed users
        
        Args:
            user_id: Original user ID to process
            
        Returns:
            List of keys (original or salted variants)
        """
        if user_id in self.skewed_keys:
            return [f"{user_id}_{i}" for i in range(self.num_salts)]
        return [user_id]

class ActivityMapper:
    """Main mapper logic for processing activity records"""
    def __init__(self):
        self.skew_handler = SkewHandler()
        self.prefix = "A:"  # Activity data identifier
    
    def process_line(self, line: str) -> list:
        """
        Process a single input line
        
        Args:
            line: Input record from STDIN
            
        Returns:
            List of processed output records
        """
        try:
            fields = line.strip().split('\t', 1)
            if len(fields) < 2:
                raise ValueError("Invalid field count")
                
            user_id, activity_data = fields
            output_records = []
            
            for processed_key in self.skew_handler.process_key(user_id):
                output_records.append(
                    f"{processed_key}\t{self.prefix}{activity_data}"
                )
            
            return output_records
            
        except Exception as e:
            sys.stderr.write(
                f"ACTIVITY_MAPPER_ERROR: {str(e)} | Line: {line.strip()}\n"
            )
            return []

def main():
    """Main processing loop"""
    mapper = ActivityMapper()
    
    for line in sys.stdin:
        for output_record in mapper.process_line(line):
            print(output_record)

if __name__ == "__main__":
    main()