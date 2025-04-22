#!/usr/bin/env python3
"""
Reducer: Action Aggregation for Social Media Analytics

This script reads aggregated user actions, formats them properly, and outputs the final result.
Assumes the input is pre-sorted by a composite key (UserID, SortKey for descending post count).

Input:  UserID,SortKey<TAB>post_count,like_count,comment_count,share_count
Output: UserID<TAB>posts:N,likes:N,comments:N,shares:N
"""

import sys

current_user = None

# Process each line from standard input
for line in sys.stdin:
    try:
        key, value = line.strip().split('\t')

        # Extract the UserID from the composite key
        user_id = key.split(',')[0]

        # Only emit once per unique user_id
        if user_id != current_user:
            current_user = user_id

            # Parse action counts
            post_count, like_count, comment_count, share_count = map(int, value.split(','))

            # Format the output
            formatted_output = (
                f"{user_id}\t"
                f"posts:{post_count},likes:{like_count},comments:{comment_count},shares:{share_count}"
            )

            print(formatted_output)

    except Exception as e:
        # Log errors but continue processing
        sys.stderr.write(f"Error processing line: {line.strip()} | Error: {e}\n")
