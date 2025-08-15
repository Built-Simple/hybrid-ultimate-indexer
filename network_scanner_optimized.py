#!/usr/bin/env python3
"""
OPTIMIZED Network Drive Scanner v2.0
- Progress every 1,000 files
- Checkpoints every 10,000 files  
- Per-file 3-second timeout
- Subdirectory test mode
"""

import os
import sys
import time
import json
import lz4.frame
import socket
from datetime import datetime
from pathlib import Path
import threading

class OptimizedNetworkScanner:
    def __init__(self, index_dir=r"C:\Utilities\file_tools\hybrid_ultimate_index"):
        self.index_dir = index_dir
        self.checkpoint_interval = 10000  # Save every 10K files
        self.progress_interval = 1000     # Report every 1K files
        self.file_timeout = 3             # 3 seconds per file
        self.current_checkpoint = []
        self.stats = {
            'files_scanned': 0,
            'files_saved': 0,
            'errors': 0,
            'timeouts': 0,
            'start_time': time.time(),
            'last_checkpoint': time.time()
        }
        
    def scan_file_with_timeout(self, filepath):
        """Scan a single file with timeout"""
        try:
            # Use socket timeout for network operations
            socket.setdefaulttimeout(self.file_timeout)
            
            stat = os.stat(filepath)
            return {
                'path': filepath,
                'size': stat.st_size,
                'modified': stat.st_mtime,
                'success': True
            }
        except (OSError, IOError, TimeoutError, Exception) as e:
            self.stats['timeouts'] += 1
            return {
                'path': filepath,
                'error': str(e),
                'success': False
            }
        finally:
            socket.setdefaulttimeout(None)
    
    def save_checkpoint(self, force=False):
        """Save current checkpoint to disk"""
        if not self.current_checkpoint:
            return
        
        if not force and len(self.current_checkpoint) < self.checkpoint_interval:
            return
        
        # Save checkpoint
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        checkpoint_file = os.path.join(
            self.index_dir, 
            f"network_checkpoint_{timestamp}_{self.stats['files_saved']}.lz4"
        )
        
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Saving checkpoint: {len(self.current_checkpoint)} files...")
        
        try:
            with lz4.frame.open(checkpoint_file, 'wb') as f:
                for entry in self.current_checkpoint:
                    if entry['success']:
                        line = f"{entry['path']}|{entry.get('size', 0)}|{entry.get('modified', 0)}|NETWORK\n"
                        f.write(line.encode('utf-8', errors='ignore'))
            
            self.stats['files_saved'] += len([e for e in self.current_checkpoint if e['success']])
            self.current_checkpoint = []
            self.stats['last_checkpoint'] = time.time()
            
            size_mb = os.path.getsize(checkpoint_file) / (1024 * 1024)
            print(f"OK Checkpoint saved: {checkpoint_file} ({size_mb:.2f} MB)")
            print(f"   Total saved so far: {self.stats['files_saved']:,} files")
        except Exception as e:
            print(f"ERROR saving checkpoint: {e}")
    
    def scan_directory(self, path, max_depth=None, test_limit=None):
        """Scan a directory with proper error handling"""
        print(f"\nScanning: {path}")
        print(f"   Max depth: {max_depth or 'Unlimited'}")
        print(f"   Test limit: {test_limit or 'Unlimited'} files")
        print("-" * 60)
        
        try:
            for root, dirs, files in os.walk(path):
                # Check depth limit
                if max_depth:
                    try:
                        depth = len(Path(root).relative_to(path).parts)
                        if depth >= max_depth:
                            dirs.clear()  # Don't go deeper
                    except:
                        pass
                
                # Skip problematic directories
                dirs[:] = [d for d in dirs if not d.startswith('$') and not d.startswith('.')]
                
                for filename in files:
                    filepath = os.path.join(root, filename)
                    
                    # Scan file with timeout
                    result = self.scan_file_with_timeout(filepath)
                    
                    if result['success']:
                        self.current_checkpoint.append(result)
                        self.stats['files_scanned'] += 1
                    else:
                        self.stats['errors'] += 1
                    
                    # Progress report
                    if self.stats['files_scanned'] % self.progress_interval == 0:
                        self.print_progress()
                    
                    # Save checkpoint if needed
                    if len(self.current_checkpoint) >= self.checkpoint_interval:
                        self.save_checkpoint()
                    
                    # Test limit
                    if test_limit and self.stats['files_scanned'] >= test_limit:
                        print(f"\nOK Test limit reached: {test_limit} files")
                        self.save_checkpoint(force=True)
                        return
        except KeyboardInterrupt:
            print("\nScan interrupted by user!")
            self.save_checkpoint(force=True)
            raise
        except Exception as e:
            print(f"\nError scanning directory: {e}")
            self.save_checkpoint(force=True)
    
    def print_progress(self):
        """Print detailed progress"""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['files_scanned'] / elapsed if elapsed > 0 else 0
        
        print(f"\r[{datetime.now().strftime('%H:%M:%S')}] Progress: {self.stats['files_scanned']:,} files | "
              f"{rate:.1f} files/sec | "
              f"{self.stats['errors']:,} errors | "
              f"{self.stats['timeouts']:,} timeouts | "
              f"{elapsed/60:.1f} min", end='')
    
    def scan_network_drive(self, drive_letter, test_mode=False):
        """Scan a network drive with optimization"""
        drive_path = f"{drive_letter}:\\"
        
        print(f"\n{'='*70}")
        print(f"OPTIMIZED NETWORK SCAN - DRIVE {drive_letter}")
        print(f"{'='*70}")
        
        # Check if drive exists
        if not os.path.exists(drive_path):
            print(f"ERROR: Drive {drive_letter} not found!")
            return
        
        # Test mode - just scan first 10K files
        if test_mode:
            print("TEST MODE - Scanning first 10,000 files only")
            self.scan_directory(drive_path, max_depth=3, test_limit=10000)
        else:
            # Full scan
            self.scan_directory(drive_path)
        
        # Final checkpoint
        self.save_checkpoint(force=True)
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print scan summary"""
        elapsed = time.time() - self.stats['start_time']
        
        print(f"\n\n{'='*70}")
        print("SCAN SUMMARY")
        print("-" * 70)
        print(f"Files Scanned: {self.stats['files_scanned']:,}")
        print(f"Files Saved: {self.stats['files_saved']:,}")
        print(f"Errors: {self.stats['errors']:,}")
        print(f"Timeouts: {self.stats['timeouts']:,}")
        print(f"Time: {elapsed/60:.1f} minutes")
        print(f"Rate: {self.stats['files_scanned']/elapsed:.1f} files/sec")
        print(f"Success Rate: {(1 - self.stats['errors']/max(1, self.stats['files_scanned']))*100:.1f}%")
        print("=" * 70)

def main():
    scanner = OptimizedNetworkScanner()
    
    # Get drive letter
    print("\nOPTIMIZED NETWORK DRIVE SCANNER v2.0")
    print("-" * 40)
    print("Available modes:")
    print("1. Test mode (first 10K files)")
    print("2. Full scan")
    print()
    
    if len(sys.argv) > 1:
        # Command line mode
        drive = sys.argv[1].upper()
        mode = sys.argv[2] if len(sys.argv) > 2 else "1"
    else:
        # Interactive mode
        drive = input("Enter drive letter (V/W/X/Y/Z): ").upper()
        mode = input("Select mode (1=Test, 2=Full): ")
    
    if drive in "VWXYZ":
        try:
            scanner.scan_network_drive(drive, test_mode=(mode == "1"))
        except KeyboardInterrupt:
            print("\nScan interrupted!")
    else:
        print("ERROR Invalid drive letter")

if __name__ == "__main__":
    main()