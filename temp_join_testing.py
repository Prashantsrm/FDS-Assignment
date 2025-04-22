#!/usr/bin/env python3
"""
User ID Overlap Analysis Tool

Analyzes the overlap between user IDs in activity data and profile data.
Provides detailed statistics to validate data consistency before join operations.
"""

import os
import sys
from pathlib import Path
from typing import Set, Tuple, Dict, List

def get_project_root() -> Path:
    """Determine the project root directory"""
    script_dir = Path(__file__).parent.resolve()
    return script_dir.parent

def load_user_ids(file_path: Path) -> Set[str]:
    """
    Extract unique user IDs from a data file
    
    Args:
        file_path: Path to the data file
        
    Returns:
        Set of unique user IDs
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file format is invalid
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")
    
    user_ids = set()
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:  # Skip empty lines
                continue
                
            parts = line.split('\t')
            if not parts:
                raise ValueError(f"Invalid format in {file_path}, line {line_num}")
                
            user_ids.add(parts[0])
    
    return user_ids

def analyze_overlap(activity_ids: Set[str], profile_ids: Set[str]) -> Dict[str, any]:
    """
    Analyze the overlap between two sets of user IDs
    
    Args:
        activity_ids: Set of user IDs from activity data
        profile_ids: Set of user IDs from profile data
        
    Returns:
        Dictionary containing analysis results
    """
    overlap = activity_ids & profile_ids
    activity_only = activity_ids - profile_ids
    profiles_only = profile_ids - activity_ids
    
    return {
        'counts': {
            'activity': len(activity_ids),
            'profiles': len(profile_ids),
            'overlap': len(overlap),
            'activity_only': len(activity_only),
            'profiles_only': len(profiles_only)
        },
        'coverage': {
            'activity_in_profiles': len(overlap) / len(activity_ids) if activity_ids else 0,
            'profiles_in_activity': len(overlap) / len(profile_ids) if profile_ids else 0
        },
        'samples': {
            'activity': sorted(activity_ids)[:5],
            'profiles': sorted(profile_ids)[:5],
            'overlap': sorted(overlap)[:5],
            'activity_only': sorted(activity_only)[:5],
            'profiles_only': sorted(profiles_only)[:5]
        }
    }

def print_report(report: Dict[str, any]) -> None:
    """Print the analysis report in a readable format"""
    print("\n=== User ID Overlap Analysis ===")
    print(f"\nTotal User IDs in Activity Data: {report['counts']['activity']}")
    print(f"Total User IDs in Profile Data: {report['counts']['profiles']}")
    print(f"Overlapping User IDs: {report['counts']['overlap']}")
    print(f"User IDs Only in Activity Data: {report['counts']['activity_only']}")
    print(f"User IDs Only in Profile Data: {report['counts']['profiles_only']}")
    
    print("\nCoverage Statistics:")
    print(f"{report['coverage']['activity_in_profiles']:.1%} of activity users exist in profiles")
    print(f"{report['coverage']['profiles_in_activity']:.1%} of profile users exist in activity")
    
    print("\nSample User IDs:")
    print(f"Activity: {report['samples']['activity']}")
    print(f"Profiles: {report['samples']['profiles']}")
    print(f"Overlap: {report['samples']['overlap']}")
    print(f"Only in Activity: {report['samples']['activity_only']}")
    print(f"Only in Profiles: {report['samples']['profiles_only']}")

def main() -> int:
    """Main execution function"""
    try:
        project_root = get_project_root()
        
        # Define file paths
        activity_path = project_root / 'output' / 'user_activity.txt'
        profiles_path = project_root / 'data' / 'user_profiles.txt'
        
        # Load user IDs
        activity_ids = load_user_ids(activity_path)
        profile_ids = load_user_ids(profiles_path)
        
        # Analyze and report
        report = analyze_overlap(activity_ids, profile_ids)
        print_report(report)
        
        return 0
        
    except FileNotFoundError as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"Data format error: {str(e)}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {str(e)}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())