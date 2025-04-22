#!/usr/bin/env python3
"""
User Profile Join Mapper

Processes user profile data for distributed join operation with activity data.
Tags profile records with 'P:' prefix and handles data skew through salting.

Features:
- Extracts user ID from composite field (CSV format)
- Handles skewed keys through salting to prevent reducer overload
- Maintains data lineage through consistent tagging
- Robust error handling with detailed logging

Environment Variables:
- skewed.keys: Comma-separated list of known skewed user IDs

Input Format:
  UserID,Name,Location<TAB>OtherProfileData

Output Format:
  UserID<TAB>P:UserID,Name,Location<TAB>OtherProfileData  (normal keys)
  UserID_salt<TAB>P:UserID,Name,Location<TAB>OtherProfileData  (skewed keys)
"""

import sys
import os
from typing import Set, List

class ProfileSkewHandler:
    """Handles data skew mitigation for profile data"""
    def __init__(self, num_salts: int = 10):
        self.skewed_keys = self._load_skewed_keys()
        self.num_salts = num_salts
    
    @staticmethod
    def _load_skewed_keys() -> Set[str]:
        """Load known skewed keys from environment"""
        skewed_keys_str = os.environ.get('skewed.keys', '')
        return set(filter(None, skewed_keys_str.split(',')))
    
    def process_key(self, user_id: str) -> List[str]:
        """
        Generate keys for join operation with skew handling
        
        Args:
            user_id: Original user ID from profile data
            
        Returns:
            List of keys (original or salted variants for skewed users)
        """
        if user_id in self.skewed_keys:
            return [f"{user_id}_{i}" for i in range(self.num_salts)]
        return [user_id]

class ProfileMapper:
    """Core mapper for processing profile records"""
    PROFILE_PREFIX = "P:"
    
    def __init__(self):
        self.skew_handler = ProfileSkewHandler()
    
    def extract_user_id(self, composite_field: str) -> str:
        """
        Extract user ID from composite CSV field
        
        Args:
            composite_field: First field containing UserID,Name,Location
            
        Returns:
            Extracted user ID
            
        Raises:
            ValueError: If user ID cannot be extracted
        """
        try:
            return composite_field.split(',')[0].strip()
        except (IndexError, AttributeError) as e:
            raise ValueError(f"Invalid user ID format: {composite_field}") from e
    
    def process_line(self, line: str) -> List[str]:
        """
        Process a single profile record
        
        Args:
            line: Raw input line from profile data
            
        Returns:
            List of processed output records
        """
        try:
            fields = line.strip().split('\t', 1)
            if not fields:
                raise ValueError("Empty input line")
                
            user_id = self.extract_user_id(fields[0])
            profile_data = self.PROFILE_PREFIX + line.strip()
            
            return [
                f"{processed_key}\t{profile_data}"
                for processed_key in self.skew_handler.process_key(user_id)
            ]
            
        except Exception as e:
            sys.stderr.write(
                f"PROFILE_MAPPER_ERROR: {str(e)} | Line: {line.strip()}\n"
            )
            return []

def main():
    """Main processing loop"""
    mapper = ProfileMapper()
    
    for line in sys.stdin:
        for output_record in mapper.process_line(line):
            print(output_record)

if __name__ == "__main__":
    main()