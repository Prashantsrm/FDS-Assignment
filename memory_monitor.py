#!/usr/bin/env python3
"""
Memory and Performance Monitor for MapReduce Workflows

A comprehensive monitoring tool that tracks:
- Memory usage (RSS, peak)
- CPU utilization
- Processing throughput
- Execution time

Can operate in two modes:
1. Process Wrapper: Monitors external processes (mappers/reducers)
2. STDIN Processor: Monitors memory during stream processing

Outputs metrics in Hadoop-compatible counter format for integration with job tracking.
"""

import os
import time
import sys
import subprocess
import psutil
from typing import Optional, Tuple, Dict
from dataclasses import dataclass

@dataclass
class PerformanceStats:
    """Container for performance metrics"""
    peak_memory_mb: float = 0.0
    total_records: int = 0
    start_time: float = time.time()
    last_check_time: float = time.time()
    records_since_check: int = 0

    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time

    @property
    def records_per_second(self) -> float:
        elapsed = self.elapsed_time
        return self.total_records / elapsed if elapsed > 0 else 0.0

class ProcessMonitor:
    """Monitors system resource usage for a process"""
    
    def __init__(self, pid: int, stats: PerformanceStats):
        self.pid = pid
        self.stats = stats
        self.process = psutil.Process(pid)
    
    def update_stats(self) -> None:
        """Update performance statistics"""
        try:
            # Get memory usage in MB
            mem_info = self.process.memory_info()
            current_mem = mem_info.rss / (1024 * 1024)
            
            # Update peak memory
            if current_mem > self.stats.peak_memory_mb:
                self.stats.peak_memory_mb = current_mem
            
            # Get CPU usage
            cpu_percent = self.process.cpu_percent()
            
            return current_mem, cpu_percent
            
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return None, None

def monitor_external_process(command: list, interval: float = 5) -> int:
    """
    Execute and monitor an external process
    
    Args:
        command: Command to execute as list of arguments
        interval: Monitoring interval in seconds
        
    Returns:
        Exit code of the monitored process
    """
    stats = PerformanceStats()
    
    try:
        # Start the subprocess
        proc = subprocess.Popen(command)
        monitor = ProcessMonitor(proc.pid, stats)
        
        while proc.poll() is None:
            # Update performance stats
            current_mem, cpu_percent = monitor.update_stats()
            
            if current_mem is not None:
                sys.stderr.write(
                    f"PROCESS_MONITOR: MEM={current_mem:.2f}MB "
                    f"PEAK={stats.peak_memory_mb:.2f}MB "
                    f"CPU={cpu_percent:.1f}%\n"
                )
            
            time.sleep(interval)
        
        # Final report
        report_metrics(stats)
        return proc.returncode
        
    except Exception as e:
        sys.stderr.write(f"PROCESS_MONITOR_ERROR: {str(e)}\n")
        return 1

def monitor_stream_processing(check_interval: int = 1000) -> None:
    """
    Monitor memory during stdin processing
    
    Args:
        check_interval: Check metrics every N records
    """
    stats = PerformanceStats()
    monitor = ProcessMonitor(os.getpid(), stats)
    
    try:
        for line in sys.stdin:
            stats.total_records += 1
            stats.records_since_check += 1
            
            # Process the line
            print(line.strip())
            
            # Periodic performance check
            if stats.total_records % check_interval == 0:
                current_mem, cpu_percent = monitor.update_stats()
                
                sys.stderr.write(
                    f"STREAM_MONITOR: RECORDS={stats.total_records} "
                    f"RPS={stats.records_per_second:.2f} "
                    f"MEM={current_mem:.2f}MB "
                    f"CPU={cpu_percent:.1f}%\n"
                )
                stats.records_since_check = 0
                stats.last_check_time = time.time()
        
        # Final report
        report_metrics(stats)
        
    except Exception as e:
        sys.stderr.write(f"STREAM_MONITOR_ERROR: {str(e)}\n")
        raise

def report_metrics(stats: PerformanceStats) -> None:
    """Output final metrics in Hadoop counter format"""
    sys.stderr.write(
        f"reporter:counter:PerformanceMetrics,PeakMemoryMB,{int(stats.peak_memory_mb)}\n"
        f"reporter:counter:PerformanceMetrics,TotalRecords,{stats.total_records}\n"
        f"reporter:counter:PerformanceMetrics,TotalTimeSec,{int(stats.elapsed_time)}\n"
        f"reporter:counter:PerformanceMetrics,RecordsPerSec,{int(stats.records_per_second)}\n"
    )

def main() -> int:
    """Main entry point"""
    if len(sys.argv) > 1:
        # External process monitoring mode
        return monitor_external_process(sys.argv[1:])
    else:
        # STDIN processing mode
        monitor_stream_processing()
        return 0

if __name__ == "__main__":
    sys.exit(main())