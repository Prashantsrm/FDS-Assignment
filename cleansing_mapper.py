#!/usr/bin/env python3
"""
Social Media Data Cleansing Mapper

Processes raw social media logs to extract and validate:
- Timestamp (ISO 8601 format)
- UserID
- ActionType
- ContentID
- Metadata (JSON string)

Validates each field and filters malformed records while tracking discard statistics
using Hadoop-style counters for monitoring data quality issues.

Input: Tab-separated records from STDIN
Output: Validated records in TSV format to STDOUT
Counters: Discard statistics emitted to STDERR
"""

import sys
import json
import re
from datetime import datetime

class DataQualityCounters:
    """Track and report data quality metrics"""
    def __init__(self):
        self.metrics = {
            'invalid_timestamp': 0,
            'malformed_json': 0,
            'missing_fields': 0,
            'processing_errors': 0,
            'total_processed': 0,
            'total_discarded': 0
        }
    
    def increment(self, counter_name):
        """Increment specified counter"""
        self.metrics[counter_name] += 1
        if counter_name != 'total_processed':
            self.metrics['total_discarded'] += 1
    
    def report(self):
        """Emit all counters to STDERR in Hadoop format"""
        for name, value in self.metrics.items():
            sys.stderr.write(f"reporter:counter:DataQuality,{name},{value}\n")

class RecordValidator:
    """Validate social media log record fields"""
    TIMESTAMP_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?$')
    
    @classmethod
    def validate_timestamp(cls, timestamp_str):
        """Validate ISO 8601 timestamp format"""
        if not cls.TIMESTAMP_PATTERN.match(timestamp_str):
            return False
        
        try:
            clean_ts = timestamp_str.rstrip('Z')
            datetime.strptime(clean_ts, '%Y-%m-%dT%H:%M:%S')
            return True
        except ValueError:
            return False
    
    @classmethod
    def validate_metadata(cls, json_str):
        """Validate JSON metadata structure"""
        try:
            json.loads(json_str)
            return True
        except json.JSONDecodeError:
            return False

def process_record(line, counters):
    """
    Process a single log record line
    
    Args:
        line: Raw input line from log file
        counters: DataQualityCounters instance
        
    Returns:
        Validated TSV string or None if invalid
    """
    counters.increment('total_processed')
    
    try:
        fields = line.strip().split('\t')
        
        # Validate field count
        if len(fields) < 5:
            counters.increment('missing_fields')
            return None
            
        timestamp, user_id, action_type, content_id, metadata = fields[:5]
        
        # Validate timestamp
        if not RecordValidator.validate_timestamp(timestamp):
            counters.increment('invalid_timestamp')
            return None
            
        # Validate metadata JSON
        if not RecordValidator.validate_metadata(metadata):
            counters.increment('malformed_json')
            return None
            
        # Return validated record
        return f"{user_id}\t{timestamp}\t{action_type}\t{content_id}\t{metadata}"
        
    except Exception as e:
        counters.increment('processing_errors')
        sys.stderr.write(f"RECORD_PROCESSING_ERROR: {str(e)}\n")
        return None

def main():
    """Main processing loop"""
    counters = DataQualityCounters()
    
    for line in sys.stdin:
        processed_record = process_record(line, counters)
        if processed_record:
            print(processed_record)
    
    counters.report()

if __name__ == "__main__":
    main()