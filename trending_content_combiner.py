#!/usr/bin/env python3
"""
Trending Content Combiner for Social Media Analytics

Performs local aggregation of engagement metrics to optimize shuffle phase.
Handles both like and share counts with configurable weights.

Features:
- In-memory aggregation of engagement metrics
- Configurable weighting for different engagement types
- Comprehensive error handling and logging
- Performance monitoring counters

Input Format:
  ContentID<TAB>EngagementCount

Output Format:
  ContentID<TAB>TotalEngagement
"""

import sys
from collections import defaultdict
from typing import DefaultDict
import argparse

class TrendingCombiner:
    """Handles aggregation of content engagement metrics"""
    
    def __init__(self, like_weight: float = 1.0, share_weight: float = 1.5):
        """
        Initialize combiner with engagement weights
        
        Args:
            like_weight: Weight multiplier for likes
            share_weight: Weight multiplier for shares
        """
        self.engagement_counts: DefaultDict[str, float] = defaultdict(float)
        self.like_weight = like_weight
        self.share_weight = share_weight
        self.processed_records = 0
        self.error_count = 0

    def process_line(self, line: str) -> None:
        """
        Process a single input record
        
        Args:
            line: Input line to process
        """
        try:
            content_id, engagement = line.strip().split('\t')
            engagement_value = float(engagement)
            
            # Apply weights based on engagement type (if identifiable)
            if '_like' in content_id:
                engagement_value *= self.like_weight
            elif '_share' in content_id:
                engagement_value *= self.share_weight
                
            self.engagement_counts[content_id] += engagement_value
            self.processed_records += 1
            
        except ValueError as e:
            self.error_count += 1
            sys.stderr.write(f"COMBINER_ERROR: Invalid data in line {self.processed_records + 1}: {str(e)}\n")
        except Exception as e:
            self.error_count += 1
            sys.stderr.write(f"COMBINER_ERROR: Unexpected error processing line: {str(e)}\n")

    def emit_results(self) -> None:
        """Output aggregated results"""
        for content_id, engagement in self.engagement_counts.items():
            print(f"{content_id}\t{engagement:.2f}")
        
        # Report performance metrics
        sys.stderr.write(
            f"reporter:counter:TrendingCombiner,ProcessedRecords,{self.processed_records}\n"
            f"reporter:counter:TrendingCombiner,ErrorRecords,{self.error_count}\n"
            f"reporter:counter:TrendingCombiner,UniqueContentIDs,{len(self.engagement_counts)}\n"
        )

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--like-weight', type=float, default=1.0,
                       help='Weight multiplier for like engagements')
    parser.add_argument('--share-weight', type=float, default=1.5,
                       help='Weight multiplier for share engagements')
    return parser.parse_args()

def main():
    """Main processing function"""
    args = parse_args()
    combiner = TrendingCombiner(
        like_weight=args.like_weight,
        share_weight=args.share_weight
    )
    
    for line in sys.stdin:
        combiner.process_line(line)
    
    combiner.emit_results()

if __name__ == "__main__":
    main()