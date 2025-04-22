#!/usr/bin/env python3
"""
Trending Content Reducer for Social Media Analytics

Determines trending content using statistical analysis with:
- Configurable threshold methods (fixed/dynamic)
- Multiple dynamic threshold strategies
- Comprehensive performance metrics
- Robust error handling

Input Format:
  ContentID<TAB>EngagementCount

Output Format:
  ContentID<TAB>EngagementCount  (for trending items only)
"""

import sys
import os
import numpy as np
from typing import List, Tuple, Dict
import argparse

class TrendingReducer:
    """Identifies trending content based on engagement thresholds"""
    
    def __init__(self):
        self.all_engagements: List[int] = []
        self.content_data: List[Tuple[str, int]] = []
        self.processed_records = 0
        self.error_count = 0
        self.threshold_strategy = 'percentile'
        self.threshold_value = 90  # Default to 90th percentile

    def load_config(self) -> None:
        """Configure threshold strategy from environment variables"""
        # Priority: Command line > Environment > Defaults
        parser = argparse.ArgumentParser()
        parser.add_argument('--threshold', type=float, default=-1,
                          help='Fixed threshold value')
        parser.add_argument('--strategy', choices=['percentile', 'mad', 'stddev'],
                          default=os.environ.get('TRENDING_STRATEGY', 'percentile'),
                          help='Threshold calculation method')
        parser.add_argument('--value', type=float,
                          default=float(os.environ.get('TRENDING_VALUE', 90)),
                          help='Parameter for threshold calculation')
        args = parser.parse_args()

        if args.threshold > 0:  # Fixed threshold takes precedence
            self.threshold_strategy = 'fixed'
            self.threshold_value = args.threshold
        else:
            self.threshold_strategy = args.strategy
            self.threshold_value = args.value

    def process_record(self, line: str) -> None:
        """Process a single input record"""
        try:
            content_id, engagement = line.strip().split('\t')
            engagement = int(engagement)
            
            self.all_engagements.append(engagement)
            self.content_data.append((content_id, engagement))
            self.processed_records += 1
            
        except ValueError as e:
            self.error_count += 1
            sys.stderr.write(f"REDUCER_ERROR: Invalid data in record: {str(e)}\n")
        except Exception as e:
            self.error_count += 1
            sys.stderr.write(f"REDUCER_ERROR: Unexpected error: {str(e)}\n")

    def calculate_threshold(self) -> float:
        """Calculate trending threshold using configured strategy"""
        if not self.all_engagements:
            return 0
            
        engagements = np.array(self.all_engagements)
        
        if self.threshold_strategy == 'fixed':
            return self.threshold_value
            
        elif self.threshold_strategy == 'percentile':
            return np.percentile(engagements, self.threshold_value)
            
        elif self.threshold_strategy == 'mad':
            median = np.median(engagements)
            mad = np.median(np.abs(engagements - median))
            return median + (self.threshold_value * mad)
            
        elif self.threshold_strategy == 'stddev':
            mean = np.mean(engagements)
            std = np.std(engagements)
            return mean + (self.threshold_value * std)
            
        return 0  # Fallback

    def emit_results(self, threshold: float) -> None:
        """Output trending content and metrics"""
        trending_count = 0
        
        for content_id, engagement in self.content_data:
            if engagement >= threshold:
                print(f"{content_id}\t{engagement}")
                trending_count += 1
        
        # Report analytics
        sys.stderr.write(
            f"reporter:counter:TrendingStats,ProcessedRecords,{self.processed_records}\n"
            f"reporter:counter:TrendingStats,ErrorRecords,{self.error_count}\n"
            f"reporter:counter:TrendingStats,ThresholdUsed,{int(threshold)}\n"
            f"reporter:counter:TrendingStats,TrendingItems,{trending_count}\n"
            f"reporter:counter:TrendingStats,StrategyUsed,{self.threshold_strategy}\n"
        )

def main():
    """Main execution flow"""
    reducer = TrendingReducer()
    reducer.load_config()
    
    # Process all input
    for line in sys.stdin:
        reducer.process_record(line)
    
    # Calculate and apply threshold
    threshold = reducer.calculate_threshold()
    reducer.emit_results(threshold)

if __name__ == "__main__":
    main()