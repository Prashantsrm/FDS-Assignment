#!/usr/bin/env python3
"""
Local MapReduce Simulator for Windows/PyCharm Development

Simulates Hadoop MapReduce workflow locally for development and testing purposes.
Handles complete data processing pipeline including:
- Data cleansing
- Action aggregation
- Trending content identification
- Dataset joining
- Skew detection and mitigation

Features:
- Fully replicates Hadoop MapReduce phases (map, combine, reduce)
- Supports both single and multi-input jobs
- Handles data skew through key salting
- Detailed progress reporting and error handling
- Configurable through command-line arguments
"""

import os
import sys
import subprocess
import argparse
import time
import json
from typing import Dict, Optional, Tuple, List

class MapReduceJob:
    """Base class for MapReduce job execution"""
    
    def __init__(self, src_dir: str, input_dir: str, output_dir: str):
        self.src_dir = src_dir
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.job_env = os.environ.copy()
        
    def _run_process(self, command: List[str], input_data: str = None, 
                    env: Dict[str, str] = None) -> Tuple[str, str]:
        """Execute a subprocess and handle I/O"""
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE if input_data else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env or self.job_env
            )
            stdout, stderr = process.communicate(input=input_data)
            
            if stderr:
                print(f"Process error output:\n{stderr}", file=sys.stderr)
                
            return stdout, stderr
            
        except subprocess.SubprocessError as e:
            print(f"Failed to execute {' '.join(command)}: {str(e)}", file=sys.stderr)
            raise

    def _prepare_input(self, input_path: str) -> str:
        """Handle directory inputs by merging files"""
        if os.path.isdir(input_path):
            merged_path = os.path.join(self.output_dir, "merged_input.tmp")
            with open(merged_path, 'w', encoding='utf-8') as outfile:
                for filename in os.listdir(input_path):
                    filepath = os.path.join(input_path, filename)
                    if os.path.isfile(filepath):
                        with open(filepath, 'r', encoding='utf-8') as infile:
                            outfile.write(infile.read())
                            outfile.write('\n')
            return merged_path
        return input_path

    def execute(self) -> bool:
        """Execute the MapReduce job"""
        raise NotImplementedError

class SingleInputJob(MapReduceJob):
    """Standard MapReduce job with single input source"""
    
    def __init__(self, src_dir: str, input_dir: str, output_dir: str,
                 mapper: str, reducer: str, combiner: Optional[str] = None):
        super().__init__(src_dir, input_dir, output_dir)
        self.mapper = mapper
        self.reducer = reducer
        self.combiner = combiner
        
    def execute(self, input_path: str, output_path: str, 
               env: Optional[Dict[str, str]] = None) -> bool:
        """Run complete MapReduce workflow"""
        start_time = time.time()
        print(f"\nStarting job: {os.path.basename(output_path)}")
        
        try:
            # Prepare input
            processed_input = self._prepare_input(input_path)
            
            # Map phase
            print("Executing mapper...")
            mapper_output, _ = self._run_process(
                [sys.executable, self.mapper],
                input_data=None if os.path.isdir(input_path) else open(processed_input, 'r').read(),
                env=env
            )
            
            if not mapper_output.strip():
                print("Warning: Mapper produced no output", file=sys.stderr)
                open(output_path, 'w').close()  # Create empty file
                return True
                
            # Sort phase
            print("Sorting mapper output...")
            sorted_lines = sorted(mapper_output.strip().split('\n'))
            
            # Combine phase (optional)
            if self.combiner:
                print("Executing combiner...")
                combiner_output, _ = self._run_process(
                    [sys.executable, self.combiner],
                    input_data='\n'.join(sorted_lines),
                    env=env
                )
                sorted_lines = sorted(combiner_output.strip().split('\n'))
                
            # Reduce phase
            print("Executing reducer...")
            reducer_output, _ = self._run_process(
                [sys.executable, self.reducer],
                input_data='\n'.join(sorted_lines),
                env=env
            )
            
            # Write output
            with open(output_path, 'w', encoding='utf-8') as outfile:
                outfile.write(reducer_output)
                
            elapsed = time.time() - start_time
            print(f"Job completed in {elapsed:.2f} seconds")
            return True
            
        except Exception as e:
            print(f"Job failed: {str(e)}", file=sys.stderr)
            return False

class JoinJob(MapReduceJob):
    """MapReduce job for joining two datasets"""
    
    def __init__(self, src_dir: str, input_dir: str, output_dir: str,
                 activity_mapper: str, profile_mapper: str, reducer: str):
        super().__init__(src_dir, input_dir, output_dir)
        self.activity_mapper = activity_mapper
        self.profile_mapper = profile_mapper
        self.reducer = reducer
        
    def execute(self, activity_input: str, profile_input: str, 
               output_path: str, env: Optional[Dict[str, str]] = None) -> bool:
        """Run complete join workflow"""
        start_time = time.time()
        print("\nStarting join job...")
        
        try:
            # Process activity data
            print("Mapping activity data...")
            activity_output, _ = self._run_process(
                [sys.executable, self.activity_mapper],
                input_data=open(activity_input, 'r').read(),
                env=env
            )
            
            # Process profile data
            print("Mapping profile data...")
            profile_output, _ = self._run_process(
                [sys.executable, self.profile_mapper],
                input_data=open(profile_input, 'r').read(),
                env=env
            )
            
            # Combine and sort
            print("Sorting joined data...")
            combined = activity_output.strip() + '\n' + profile_output.strip()
            sorted_lines = sorted(combined.split('\n'))
            
            # Reduce phase
            print("Reducing joined data...")
            reducer_output, _ = self._run_process(
                [sys.executable, self.reducer],
                input_data='\n'.join(sorted_lines),
                env=env
            )
            
            # Write output
            with open(output_path, 'w', encoding='utf-8') as outfile:
                outfile.write(reducer_output)
                
            elapsed = time.time() - start_time
            print(f"Join completed in {elapsed:.2f} seconds")
            return True
            
        except Exception as e:
            print(f"Join failed: {str(e)}", file=sys.stderr)
            return False

class SkewAnalyzer:
    """Handles data skew detection and mitigation"""
    
    def __init__(self, src_dir: str, output_dir: str):
        self.skew_script = os.path.join(src_dir, 'skew_detection.py')
        self.output_dir = output_dir
        
    def analyze(self, input_path: str) -> str:
        """Run skew analysis and return skewed keys"""
        output_path = os.path.join(self.output_dir, 'skew_analysis.json')
        print("\nRunning skew analysis...")
        
        try:
            with open(input_path, 'r') as infile:
                output, _ = subprocess.Popen(
                    [sys.executable, self.skew_script],
                    stdin=infil,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                ).communicate()
                
            with open(output_path, 'w') as outfile:
                outfile.write(output)
                
            try:
                skew_data = json.loads(output)
                return ','.join(skew_data.get('skewed_keys', []))
            except json.JSONDecodeError:
                print("Warning: Could not parse skew analysis output", file=sys.stderr)
                return ""
                
        except Exception as e:
            print(f"Skew analysis failed: {str(e)}", file=sys.stderr)
            return ""

class WorkflowRunner:
    """Orchestrates complete MapReduce workflow"""
    
    def __init__(self, args):
        self.args = args
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Determine project structure
        if os.path.basename(self.base_dir).lower() == 'src':
            self.project_root = os.path.dirname(self.base_dir)
            self.src_dir = self.base_dir
        else:
            self.project_root = self.base_dir
            self.src_dir = os.path.join(self.project_root, 'src')
            
        # Configure paths
        self.input_dir = os.path.join(self.project_root, args.input_dir)
        self.output_dir = os.path.join(self.project_root, args.output_dir)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Job components
        self.job_components = {
            'cleansing': {
                'mapper': 'cleansing_mapper.py',
                'reducer': 'cleansing_reducer.py'
            },
            'aggregation': {
                'mapper': 'action_aggregation_mapper.py',
                'reducer': 'action_aggregation_reducer.py'
            },
            'trending': {
                'mapper': 'trending_content_mapper.py',
                'combiner': 'trending_content_combiner.py',
                'reducer': 'trending_content_reducer.py'
            },
            'join': {
                'activity_mapper': 'join_activity_mapper.py',
                'profile_mapper': 'join_profile_mapper.py',
                'reducer': 'join_reducer.py'
            }
        }

    def _get_job_paths(self, job_name: str) -> Dict[str, str]:
        """Get full paths to job components"""
        return {
            key: os.path.join(self.src_dir, filename)
            for key, filename in self.job_components[job_name].items()
        }

    def run(self) -> int:
        """Execute requested workflow"""
        # Input files
        social_media_logs = os.path.join(self.input_dir, 'social_media_logs.txt')
        user_profiles = os.path.join(self.input_dir, 'user_profiles.txt')
        
        # Output files
        outputs = {
            'cleansing': os.path.join(self.output_dir, 'cleansed_data.txt'),
            'aggregation': os.path.join(self.output_dir, 'user_activity.txt'),
            'trending': os.path.join(self.output_dir, 'trending_content.txt'),
            'join': os.path.join(self.output_dir, 'joined_data.txt')
        }
        
        # Run requested jobs
        success = True
        skewed_keys = ""
        
        if self.args.job in ['cleansing', 'all'] and success:
            paths = self._get_job_paths('cleansing')
            job = SingleInputJob(
                self.src_dir, self.input_dir, self.output_dir,
                paths['mapper'], paths['reducer']
            )
            success = job.execute(social_media_logs, outputs['cleansing'])
            
        if self.args.job in ['aggregation', 'all'] and success:
            paths = self._get_job_paths('aggregation')
            job = SingleInputJob(
                self.src_dir, self.input_dir, self.output_dir,
                paths['mapper'], paths['reducer']
            )
            input_path = outputs['cleansing'] if self.args.job == 'all' else self.input_dir
            success = job.execute(input_path, outputs['aggregation'])
            
        if self.args.job in ['trending', 'all'] and success:
            paths = self._get_job_paths('trending')
            job = SingleInputJob(
                self.src_dir, self.input_dir, self.output_dir,
                paths['mapper'], paths['reducer'], paths['combiner']
            )
            input_path = outputs['cleansing'] if self.args.job == 'all' else self.input_dir
            success = job.execute(
                input_path, outputs['trending'],
                env={'TRENDING_THRESHOLD': '-1'}  # Dynamic threshold
            )
            
        if self.args.job in ['join', 'all'] and success:
            if self.args.job == 'all':
                analyzer = SkewAnalyzer(self.src_dir, self.output_dir)
                skewed_keys = analyzer.analyze(outputs['aggregation'])
                
            paths = self._get_job_paths('join')
            job = JoinJob(
                self.src_dir, self.input_dir, self.output_dir,
                paths['activity_mapper'], paths['profile_mapper'], paths['reducer']
            )
            activity_input = outputs['aggregation'] if self.args.job == 'all' else os.path.join(
                self.input_dir, 'user_activity.txt')
            success = job.execute(
                activity_input, user_profiles, outputs['join'],
                env={'skewed.keys': skewed_keys}
            )
            
        # Report results
        if success:
            print("\nWorkflow completed successfully")
            if self.args.job == 'all':
                print("Generated files:")
                for name, path in outputs.items():
                    print(f"  - {name}: {path}")
            return 0
        else:
            print("\nWorkflow failed", file=sys.stderr)
            return 1

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Local MapReduce Workflow Simulator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--job', required=True,
                       choices=['cleansing', 'aggregation', 'trending', 'join', 'all'],
                       help='Job(s) to execute')
    parser.add_argument('--input-dir', default='data',
                       help='Input directory path')
    parser.add_argument('--output-dir', default='output',
                       help='Output directory path')
    
    args = parser.parse_args()
    runner = WorkflowRunner(args)
    return runner.run()

if __name__ == "__main__":
    sys.exit(main())