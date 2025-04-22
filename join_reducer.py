#!/usr/bin/env python3
"""
User Data Join Reducer

Performs an inner join between user activity and profile data.
Processes both regular and salted keys (for skewed data handling).

Features:
- Inner join semantics (only outputs complete records)
- Handles salted keys from skew mitigation
- Robust error handling with detailed logging
- Memory-efficient processing

Input Format:
  UserID<TAB>A:ActivityData  (activity records)
  UserID<TAB>P:ProfileData  (profile records)
  UserID_salt<TAB>A:ActivityData  (salted activity records)
  UserID_salt<TAB>P:ProfileData  (salted profile records)

Output Format:
  UserID<TAB>ProfileData<TAB>ActivityData  (joined records)
"""

import sys
from typing import Optional, Tuple

class UserRecord:
    """Stores and manages user data for joining"""
    __slots__ = ['profile_data', 'activity_data']
    
    def __init__(self):
        self.profile_data = None
        self.activity_data = None
    
    def is_complete(self) -> bool:
        """Check if both profile and activity data are available"""
        return self.profile_data is not None and self.activity_data is not None
    
    def update(self, data_type: str, data: str) -> None:
        """
        Update user record with new data
        
        Args:
            data_type: 'P' for profile or 'A' for activity
            data: The actual data payload
        """
        if data_type == 'P':
            self.profile_data = data
        elif data_type == 'A':
            self.activity_data = data
    
    def get_joined_data(self, user_id: str) -> str:
        """Format complete record for output"""
        return f"{user_id}\t{self.profile_data}\t{self.activity_data}"

class JoinReducer:
    """Core reducer logic for joining user data"""
    def __init__(self):
        self.current_user = None
        self.current_record = None
    
    @staticmethod
    def normalize_user_id(user_id: str) -> str:
        """Remove salt suffix if present"""
        return user_id.split('_')[0]
    
    @staticmethod
    def parse_line(line: str) -> Tuple[str, str, str]:
        """
        Parse input line into components
        
        Returns:
            Tuple of (user_id, data_type, data_payload)
            
        Raises:
            ValueError: If line format is invalid
        """
        try:
            user_id, tagged_data = line.strip().split('\t', 1)
            if len(tagged_data) < 2 or tagged_data[1] != ':':
                raise ValueError("Invalid data tag format")
            return user_id, tagged_data[0], tagged_data[2:]
        except ValueError as e:
            raise ValueError(f"Malformed input line: {line.strip()}") from e
    
    def process_line(self, line: str) -> Optional[str]:
        """
        Process a single input line
        
        Args:
            line: Input line from mapper
            
        Returns:
            Joined record if a complete user record is ready, None otherwise
        """
        try:
            user_id, data_type, data = self.parse_line(line)
            normalized_id = self.normalize_user_id(user_id)
            
            output = None
            
            # Handle user transition
            if normalized_id != self.current_user:
                output = self._finalize_current_record()
                self.current_user = normalized_id
                self.current_record = UserRecord()
            
            # Update current record
            self.current_record.update(data_type, data)
            
            return output
            
        except Exception as e:
            sys.stderr.write(f"JOIN_REDUCER_ERROR: {str(e)}\n")
            return None
    
    def _finalize_current_record(self) -> Optional[str]:
        """Process and clear the current user record"""
        if self.current_user and self.current_record and self.current_record.is_complete():
            return self.current_record.get_joined_data(self.current_user)
        return None
    
    def finalize(self) -> Optional[str]:
        """Process the last user record"""
        return self._finalize_current_record()

def main():
    """Main processing loop"""
    reducer = JoinReducer()
    
    for line in sys.stdin:
        result = reducer.process_line(line)
        if result:
            print(result)
    
    # Process final record
    final_result = reducer.finalize()
    if final_result:
        print(final_result)

if __name__ == "__main__":
    main()