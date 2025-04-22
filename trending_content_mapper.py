#!/usr/bin/env python3
"""
Trending Content Mapper for Social Media Analytics

Identifies trending content by analyzing engagement metrics (likes, shares) with:
- Configurable engagement weights
- Comprehensive error handling
- Performance monitoring
- Metadata parsing for advanced metrics

Input Format:
  UserID<TAB>Timestamp<TAB>ActionType<TAB>ContentID<TAB>Metadata(JSON)

Output Format:
  ContentID<TAB>WeightedEngagement
"""

import sys
import json
from collections import defaultdict
from typing import DefaultDict, Dict, Tuple
import argparse

class TrendingMapper:
    """Maps social media actions to content engagement scores"""
    
    def __init__(self, weights: Dict[str, float] = None):
        """
        Initialize mapper with engagement weights
        
        Args:
            weights: Dictionary mapping action types to weight values
        """
        self.engagement: DefaultDict[str, float] = defaultdict(float)
        self.weights = weights or {'like': 1.0, 'share': 1.5}
        self.processed = 0
        self.skipped = 0
        self.errors = 0

    def parse_metadata(self, metadata_str: str) -> Dict:
        """Parse JSON metadata with error handling"""
        try:
            return json.loads(metadata_str) if metadata_str else {}
        except json.JSONDecodeError:
            self.errors += 1
            sys.stderr.write("METADATA_PARSE_ERROR: Invalid JSON metadata\n")
            return {}

    def process_record(self, fields: list) -> None:
        """
        Process a single social media record
        
        Args:
            fields: Parsed record fields [UserID, Timestamp, ActionType, ContentID, Metadata]
        """
        try:
            if len(fields) < 5:
                self.skipped += 1
                return

            action_type = fields[2].lower()
            content_id = fields[3]
            metadata = self.parse_metadata(fields[4])

            if action_type in self.weights:
                weight = self.weights[action_type]
                # Apply potential metadata-based modifiers
                if 'engagement_boost' in metadata:
                    weight *= float(metadata['engagement_boost'])
                self.engagement[content_id] += weight
                
            self.processed += 1

        except Exception as e:
            self.errors += 1
            sys.stderr.write(f"RECORD_PROCESS_ERROR: {str(e)}\n")

    def emit_results(self) -> None:
        """Output engagement metrics with performance counters"""
        for content_id, score in self.engagement.items():
            print(f"{content_id}\t{score:.2f}")
        
        # Report performance metrics
        sys.stderr.write(
            f"reporter:counter:TrendingMapper,ProcessedRecords,{self.processed}\n"
            f"reporter:counter:TrendingMapper,SkippedRecords,{self.skipped}\n"
            f"reporter:counter:TrendingMapper,ErrorRecords,{self.errors}\n"
            f"reporter:counter:TrendingMapper,UniqueContent,{len(self.engagement)}\n"
        )

def parse_args() -> Dict[str, float]:
    """Parse command line arguments for engagement weights"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--like-weight', type=float, default=1.0,
                      help='Weight for like actions')
    parser.add_argument('--share-weight', type=float, default=1.5,
                      help='Weight for share actions')
    args = parser.parse_args()
    return {'like': args.like_weight, 'share': args.share_weight}

def main():
    """Main processing loop"""
    weights = parse_args()
    mapper = TrendingMapper(weights)
    
    for line in sys.stdin:
        fields = line.strip().split('\t')
        mapper.process_record(fields)
    
    mapper.emit_results()

if __name__ == "__main__":
    main()