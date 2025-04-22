#!/usr/bin/env python3
"""
Mapper: Action Aggregation for Social Media Analytics

This script reads user action logs, aggregates action counts (posts, likes, comments, shares)
per UserID using in-memory aggregation, and outputs results with a sort key for descending order by posts.

Input:  UserID<TAB>Timestamp<TAB>ActionType<TAB>ContentID<TAB>Metadata
Output: UserID,SortKey<TAB>post_count,like_count,comment_count,share_count
"""

import sys
from collections import defaultdict

# Initialize a dictionary to hold action counts per user
user_action_counts = defaultdict(lambda: {'post': 0, 'like': 0, 'comment': 0, 'share': 0})

# Read and process each input line
for line in sys.stdin:
    try:
        fields = line.strip().split('\t')
        
        if len(fields) >= 3:
            user_id = fields[0]
            action_type = fields[2].lower()

            if action_type in user_action_counts[user_id]:
                user_action_counts[user_id][action_type] += 1

    except Exception as e:
        # Log processing errors but continue execution
        sys.stderr.write(f"Error processing line: {line.strip()} | Error: {e}\n")

# Emit aggregated results
for user_id, actions in user_action_counts.items():
    # Generate a sort key to sort users by post count descending (using 10000-offset trick)
    sort_key = f"{10000 - actions['post']:05d}"

    # Prepare output
    composite_key = f"{user_id},{sort_key}"
    action_values = f"{actions['post']},{actions['like']},{actions['comment']},{actions['share']}"

    print(f"{composite_key}\t{action_values}")
