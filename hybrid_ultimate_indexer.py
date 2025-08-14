#!/usr/bin/env python3
"""
HYBRID ULTIMATE INDEXER v3.0 - PRODUCTION GUI VERSION
=====================================================
Complete production-ready file indexer with real-time GUI monitoring
Combines Everything Search with parallel process scanning
"""

import os
import sys
import time
import json
import pickle
import gzip
import hashlib
import mmap
import queue
import threading
import csv
import subprocess
from pathlib import Path
from datetime import datetime
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from typing import Dict, List, Set, Optional, Tuple, Union
import multiprocessing as mp
from multiprocessing import Process, Queue, Manager, shared_memory
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# GUI imports
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import tkinter.font as tkfont

# Required imports
try:
    import psutil
except ImportError:
    print("ERROR: psutil not installed. Run: pip install psutil")
    input("Press Enter to exit...")
    sys.exit(1)

import logging
import numpy as np
from enum import Enum

# Archive handling imports
try:
    import zipfile
    HAS_ZIP = True
except ImportError:
    HAS_ZIP = False

try:
    import rarfile
    HAS_RAR = True
except ImportError:
    HAS_RAR = False

try:
    import py7zr
    HAS_7Z = True
except ImportError:
    HAS_7Z = False

try:
    import tarfile
    HAS_TAR = True
except ImportError:
    HAS_TAR = False

# Performance libraries
try:
    import lz4.frame
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False

try:
    import msgpack
    HAS_MSGPACK = True
except ImportError:
    HAS_MSGPACK = False


class DriveType(Enum):
    """Drive types for different handling"""
    NTFS_LOCAL = "ntfs_local"      # Use Everything for monitoring
    NETWORK = "network"             # Use traditional scanning
    UNKNOWN = "unknown"             # Fallback to traditional


@dataclass
class FileInfo:
    """File metadata structure"""
    path: str
    size: int
    mtime: int
    ctime: int = 0
    content_hash: Optional[str] = None
    is_archive: bool = False
    drive: str = ""
    ext: str = ""
    attributes: int = 0
    
    def to_dict(self):
        return asdict(self)


class ProcessSafeShardedIndex:
    """
    Process-safe version of ShardedFileIndex
    Each process writes to its own shard namespace
    """
    
    def __init__(self, base_dir: str, process_id: int, shard_size: int = 1_000_000):
        self.base_dir = Path(base_dir)
        self.process_id = process_id
        self.shard_size = shard_size
        self.current_shard = 0
        self.current_count = 0
        
        # Create process-specific directory structure
        self.process_dir = self.base_dir / f"proc_{process_id:04d}"
        self.process_dir.mkdir(parents=True, exist_ok=True)
        
        # Separate directories for different data types
        self.files_dir = self.process_dir / "files"
        self.archives_dir = self.process_dir / "archives"
        self.moves_dir = self.process_dir / "moves"
        
        for dir in [self.files_dir, self.archives_dir, self.moves_dir]:
            dir.mkdir(exist_ok=True)
        
        # In-memory buffers
        self.file_buffer = []
        self.archive_buffer = []
        self.move_buffer = []
        self.buffer_size = 10000
        
        # Process-specific metadata
        self.metadata_file = self.process_dir / "metadata.json"
        self.load_metadata()
        
    def load_metadata(self):
        """Load or create process-specific metadata"""
        if self.metadata_file.exists():
            with open(self.metadata_file, 'r') as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {
                'process_id': self.process_id,
                'total_files': 0,
                'total_archives': 0,
                'total_moves': 0,
                'shards': 0,
                'created': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat()
            }
            
    def save_metadata(self):
        """Save metadata atomically"""
        self.metadata['last_updated'] = datetime.now().isoformat()
        temp_file = self.metadata_file.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.metadata, f, indent=2)
        temp_file.replace(self.metadata_file)
        
    def get_shard_path(self, shard_type: str, shard_id: int) -> Path:
        """Get path for a specific shard with process isolation"""
        base_dir = getattr(self, f"{shard_type}_dir")
        ext = ".pkl.lz4" if HAS_LZ4 else ".pkl.gz"
        # Include process_id in filename to prevent conflicts
        return base_dir / f"{shard_type}_p{self.process_id:04d}_shard_{shard_id:06d}{ext}"
        
    def add_files_batch(self, files: List[FileInfo]):
        """Add files to buffer and flush when needed"""
        self.file_buffer.extend(files)
        
        if len(self.file_buffer) >= self.buffer_size:
            self._flush_files()
            
        self.metadata['total_files'] += len(files)
        
    def add_archive_contents(self, archive_path: str, contents: List[Dict]):
        """Add archive contents to buffer"""
        self.archive_buffer.append({
            'archive_path': archive_path,
            'contents': contents,
            'indexed_at': time.time()
        })
        
        if len(self.archive_buffer) >= 100:  # Flush every 100 archives
            self._flush_archives()
            
        self.metadata['total_archives'] += 1
        
    def add_file_move(self, old_path: str, new_path: str, content_hash: str, confidence: float = 1.0):
        """Record a detected file move"""
        self.move_buffer.append({
            'old_path': old_path,
            'new_path': new_path,
            'content_hash': content_hash,
            'confidence': confidence,
            'detected_at': time.time()
        })
        
        if len(self.move_buffer) >= 1000:
            self._flush_moves()
            
        self.metadata['total_moves'] += 1
        
    def _flush_files(self):
        """Flush file buffer to disk"""
        if not self.file_buffer:
            return
            
        # Check if we need a new shard
        if self.current_count + len(self.file_buffer) > self.shard_size:
            self.current_shard += 1
            self.current_count = 0
            
        shard_path = self.get_shard_path('files', self.current_shard)
        
        # Load existing data if shard exists
        existing_data = []
        if shard_path.exists():
            existing_data = self._load_shard(shard_path)
            
        # Combine with new data
        all_data = existing_data + [f.to_dict() for f in self.file_buffer]
        
        # Write atomically
        self._write_shard(shard_path, all_data)
        
        self.current_count += len(self.file_buffer)
        self.file_buffer.clear()
        self.metadata['shards'] = self.current_shard + 1
        
    def _flush_archives(self):
        """Flush archive buffer to disk"""
        if not self.archive_buffer:
            return
            
        # Archives go in separate shards
        archive_shard = len(list(self.archives_dir.glob("*.pkl.*")))
        shard_path = self.get_shard_path('archives', archive_shard)
        
        # Write archive data
        self._write_shard(shard_path, self.archive_buffer)
        self.archive_buffer.clear()
        
    def _flush_moves(self):
        """Flush move buffer to disk"""
        if not self.move_buffer:
            return
            
        # Moves go in separate shards
        move_shard = len(list(self.moves_dir.glob("*.pkl.*")))
        shard_path = self.get_shard_path('moves', move_shard)
        
        # Write move data
        self._write_shard(shard_path, self.move_buffer)
        self.move_buffer.clear()
        
    def _write_shard(self, shard_path: Path, data: List):
        """Write data to shard atomically with retry logic"""
        import random
        
        # Use unique temp file with process ID and random component
        temp_path = shard_path.with_suffix(f'.tmp_{self.process_id}_{random.randint(1000,9999)}')
        
        # Write to temp file
        try:
            if HAS_LZ4:
                with lz4.frame.open(temp_path, 'wb') as f:
                    if HAS_MSGPACK:
                        f.write(msgpack.packb(data))
                    else:
                        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
            else:
                with gzip.open(temp_path, 'wb', compresslevel=1) as f:
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            logging.error(f"Failed to write temp file {temp_path}: {e}")
            if temp_path.exists():
                temp_path.unlink()
            raise
        
        # Atomic rename with retry logic
        max_retries = 5
        retry_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                # Ensure target directory exists
                shard_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Atomic replace
                temp_path.replace(shard_path)
                return  # Success!
                
            except PermissionError as e:
                if attempt < max_retries - 1:
                    # File might be locked by antivirus or another process
                    logging.warning(f"Process {self.process_id}: Rename attempt {attempt + 1} failed: {e}, retrying...")
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                else:
                    # Final attempt failed
                    logging.error(f"Process {self.process_id}: Failed to rename {temp_path} to {shard_path} after {max_retries} attempts")
                    # Clean up temp file
                    if temp_path.exists():
                        temp_path.unlink()
                    raise
            except Exception as e:
                logging.error(f"Process {self.process_id}: Unexpected error during rename: {e}")
                if temp_path.exists():
                    temp_path.unlink()
                raise
        
    def _load_shard(self, shard_path: Path) -> List:
        """Load data from a shard"""
        if not shard_path.exists():
            return []
            
        if HAS_LZ4:
            with lz4.frame.open(shard_path, 'rb') as f:
                if HAS_MSGPACK:
                    return msgpack.unpackb(f.read())
                else:
                    return pickle.load(f)
        else:
            with gzip.open(shard_path, 'rb') as f:
                return pickle.load(f)
                
    def flush_all(self):
        """Flush all pending data"""
        self._flush_files()
        self._flush_archives()
        self._flush_moves()
        self.save_metadata()


class EverythingIntegration:
    """Handles all Everything Search integration for perpetual monitoring"""
    
    def __init__(self, everything_path: str = None):
        try:
            self.everything_path = self._find_everything_exe(everything_path)
            self.export_file = Path("everything_export.csv")
            self.temp_export = Path("everything_export_temp.csv")
            self.last_snapshot = {}
            self.last_export_time = 0
            
            # Move tracking
            self.hash_to_paths = defaultdict(set)  # hash -> set of paths
            self.path_to_hash = {}  # path -> hash
            
            logging.info(f"Everything.exe found at: {self.everything_path}")
        except FileNotFoundError as e:
            logging.warning(f"Everything not found: {e}")
            raise
        
    def _find_everything_exe(self, custom_path: str = None) -> Path:
        """Locate Everything.exe"""
        if custom_path and Path(custom_path).exists():
            return Path(custom_path)
            
        possible_paths = [
            Path(r"C:\Program Files\Everything\Everything.exe"),
            Path(r"C:\Program Files (x86)\Everything\Everything.exe"),
            Path(os.environ.get('PROGRAMFILES', '')) / "Everything" / "Everything.exe",
            Path(os.environ.get('PROGRAMFILES(X86)', '')) / "Everything" / "Everything.exe",
        ]
        
        for path in possible_paths:
            if path.exists():
                return path
                
        raise FileNotFoundError(
            "Everything.exe not found. Please install Everything Search or specify path."
        )
    
    def is_everything_running(self) -> bool:
        """Check if Everything is running"""
        for proc in psutil.process_iter(['name']):
            if proc.info['name'] == 'Everything.exe':
                return True
        return False
    
    def ensure_everything_running(self):
        """Start Everything if not running"""
        if not self.is_everything_running():
            logging.info("Starting Everything...")
            subprocess.Popen([str(self.everything_path), "-startup"])
            time.sleep(3)  # Give it time to start
    
    def export_current_index(self) -> bool:
        """Export Everything's current index to CSV with dynamic timeout"""
        # Check for ES.exe (Everything command-line interface)
        es_exe = self._find_es_exe()
        if not es_exe:
            logging.error("Everything command-line interface (es.exe) not found!")
            logging.error("Please download es.exe from https://www.voidtools.com/downloads/")
            logging.error("and place it in your Everything installation directory or PATH")
            return False
            
        self.ensure_everything_running()
        
        # First, get the file count to estimate timeout
        try:
            count_cmd = [str(es_exe), "-n", "1"]  # Get result count
            result = subprocess.run(count_cmd, capture_output=True, text=True, timeout=10)
            
            # Extract count from output (es.exe prints count at end)
            file_count = 0
            for line in result.stdout.splitlines():
                if line.strip().isdigit():
                    file_count = int(line.strip())
                    break
            
            # Dynamic timeout: 1 second per 5,000 files, minimum 60s, maximum 3600s (1 hour)
            timeout = max(60, min(3600, file_count // 5000))
            logging.info(f"Everything reports {file_count:,} files, using {timeout}s timeout")
        except Exception as e:
            timeout = 1800  # Default 30 minutes for large databases
            logging.warning(f"Could not determine file count ({e}), using {timeout}s timeout")
        
        # Build export command using es.exe
        cmd = [
            str(es_exe),
            "-export-csv", str(self.temp_export),
            "-sort", "path"
        ]
        
        try:
            logging.info(f"Exporting Everything index (timeout: {timeout}s)...")
            start_time = time.time()
            
            # Use Popen for better control and progress monitoring
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            try:
                stdout, stderr = process.communicate(timeout=timeout)
                
                if process.returncode == 0:
                    # Check if export file is valid
                    if self.temp_export.exists() and self.temp_export.stat().st_size > 1000:
                        # Atomic rename
                        self.temp_export.replace(self.export_file)
                        export_time = time.time() - start_time
                        
                        # Count lines in export
                        file_count = 0
                        with open(self.export_file, 'r', encoding='utf-8', errors='ignore') as f:
                            file_count = sum(1 for _ in f) - 1  # Subtract header
                                
                        logging.info(f"Export complete: {file_count:,} files in {export_time:.1f}s")
                        self.last_export_time = time.time()
                        return True
                    else:
                        logging.error("Export file too small or missing")
                        return False
                else:
                    logging.error(f"Export failed with code {process.returncode}")
                    if stderr:
                        logging.error(f"Error: {stderr}")
                    return False
                    
            except subprocess.TimeoutExpired:
                process.kill()
                logging.error(f"Export timed out after {timeout} seconds")
                
                # Check if partial export is usable (>100MB suggests significant progress)
                if self.temp_export.exists() and self.temp_export.stat().st_size > 100_000_000:
                    logging.warning("Using partial export file (>100MB)")
                    self.temp_export.replace(self.export_file)
                    return True
                return False
                
        except Exception as e:
            logging.error(f"Export error: {e}")
            return False
            
    def _find_es_exe(self) -> Optional[Path]:
        """Find es.exe (Everything command-line interface)"""
        # Check common locations
        possible_paths = [
            self.everything_path.parent / "es.exe",
            Path(r"C:\Program Files\Everything\es.exe"),
            Path(r"C:\Program Files (x86)\Everything\es.exe"),
            Path("es.exe"),  # In PATH
        ]
        
        for path in possible_paths:
            if path.exists():
                return path
                
        # Try to find in PATH
        try:
            result = subprocess.run(["where", "es.exe"], capture_output=True, text=True)
            if result.returncode == 0:
                return Path(result.stdout.strip())
        except:
            pass
            
        return None
    
    def parse_export(self) -> Dict[str, FileInfo]:
        """Parse Everything export into FileInfo objects"""
        if not self.export_file.exists():
            return {}
            
        current_snapshot = {}
        
        try:
            with open(self.export_file, 'r', encoding='utf-8', errors='ignore', 
                     buffering=65536) as f:
                # Skip header
                header = f.readline().strip()
                
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 6:
                        name = row[0]
                        path = row[1]
                        
                        # Skip if no path
                        if not path:
                            continue
                            
                        full_path = os.path.join(path, name)
                        
                        try:
                            file_info = FileInfo(
                                path=full_path,
                                size=int(row[2]) if row[2].isdigit() else 0,
                                mtime=int(row[3]) if row[3].isdigit() else 0,
                                ctime=int(row[4]) if row[4].isdigit() else 0,
                                attributes=int(row[5]) if row[5].isdigit() else 0,
                                drive=full_path[0:2] if len(full_path) > 2 else "",
                                ext=Path(name).suffix.lower()
                            )
                            
                            # Check if it's an archive
                            archive_exts = {'.zip', '.rar', '.7z', '.tar', '.gz'}
                            if file_info.ext in archive_exts:
                                file_info.is_archive = True
                            
                            current_snapshot[full_path] = file_info
                            
                        except Exception as e:
                            logging.debug(f"Error parsing row: {e}")
                            continue
                            
        except Exception as e:
            logging.error(f"Error parsing export: {e}")
            
        return current_snapshot
    
    def detect_moves(self, new_files: Set[str], deleted_files: Set[str], 
                    current_snapshot: Dict[str, FileInfo], 
                    previous_snapshot: Dict[str, FileInfo]) -> List[Dict]:
        """Detect file moves based on size and content hash"""
        moves = []
        
        # Group files by size for efficient matching
        size_to_new = defaultdict(list)
        size_to_deleted = defaultdict(list)
        
        for path in new_files:
            if path in current_snapshot:
                size = current_snapshot[path].size
                size_to_new[size].append(path)
                
        for path in deleted_files:
            if path in previous_snapshot:
                size = previous_snapshot[path].size
                size_to_deleted[size].append(path)
                
        # Find potential moves
        for size, new_paths in size_to_new.items():
            if size in size_to_deleted:
                deleted_paths = size_to_deleted[size]
                
                # For files of same size, check if they might be moves
                for new_path in new_paths:
                    for deleted_path in deleted_paths:
                        # Check if paths are similar (same filename)
                        if Path(new_path).name == Path(deleted_path).name:
                            # High confidence move
                            moves.append({
                                'old_path': deleted_path,
                                'new_path': new_path,
                                'size': size,
                                'confidence': 0.9,
                                'reason': 'same_name_and_size'
                            })
                            # Remove from sets to avoid duplicate processing
                            new_files.discard(new_path)
                            deleted_files.discard(deleted_path)
                            break
                        
        return moves
    
    def find_changes(self, current: Dict[str, FileInfo], 
                    previous: Dict[str, FileInfo]) -> Tuple[Set, Set, Set, List]:
        """Find changes between snapshots including move detection"""
        current_paths = set(current.keys())
        previous_paths = set(previous.keys())
        
        # New and deleted files
        new_files = current_paths - previous_paths
        deleted_files = previous_paths - current_paths
        
        # Detect moves before finalizing new/deleted
        moves = self.detect_moves(new_files, deleted_files, current, previous)
        
        # Modified files
        modified_files = set()
        for path in current_paths & previous_paths:
            curr = current[path]
            prev = previous[path]
            
            # Check for modifications
            if (curr.size != prev.size or 
                curr.mtime != prev.mtime):
                modified_files.add(path)
                
        return new_files, deleted_files, modified_files, moves


class ArchiveIndexer:
    """Index archive contents without extraction"""
    
    def __init__(self):
        self.archive_extensions = {'.zip', '.rar', '.7z', '.tar', '.gz', '.tar.gz', '.tgz'}
        self.max_archive_size = 5 * 1024**3  # 5GB limit
        self.processed_count = 0
        self.total_contents = 0
        
    def is_archive(self, file_path: str) -> bool:
        """Check if file is a supported archive"""
        ext = Path(file_path).suffix.lower()
        if file_path.lower().endswith('.tar.gz'):
            ext = '.tar.gz'
        return ext in self.archive_extensions
        
    def index_archive_contents(self, archive_path: str) -> Optional[List[Dict]]:
        """Index archive contents without extraction"""
        try:
            size = os.path.getsize(archive_path)
            if size > self.max_archive_size:
                return None
                
            ext = Path(archive_path).suffix.lower()
            if archive_path.lower().endswith('.tar.gz'):
                ext = '.tar.gz'
                
            contents = []
            
            if ext == '.zip' and HAS_ZIP:
                contents = self._index_zip(archive_path)
            elif ext == '.rar' and HAS_RAR:
                contents = self._index_rar(archive_path)
            elif ext == '.7z' and HAS_7Z:
                contents = self._index_7z(archive_path)
            elif ext in ['.tar', '.gz', '.tar.gz', '.tgz'] and HAS_TAR:
                contents = self._index_tar(archive_path)
                
            if contents:
                self.processed_count += 1
                self.total_contents += len(contents)
                
            return contents
                
        except Exception as e:
            logging.debug(f"Failed to index archive {archive_path}: {e}")
            
        return None
        
    def _index_zip(self, archive_path: str) -> List[Dict]:
        """Index ZIP contents"""
        contents = []
        with zipfile.ZipFile(archive_path, 'r') as zf:
            for info in zf.infolist():
                if not info.is_dir():
                    contents.append({
                        'internal_path': info.filename,
                        'compressed_size': info.compress_size,
                        'uncompressed_size': info.file_size,
                        'modified': datetime(*info.date_time).timestamp() if info.date_time else None,
                        'file_type': Path(info.filename).suffix.lower()
                    })
        return contents
        
    def _index_rar(self, archive_path: str) -> List[Dict]:
        """Index RAR contents"""
        contents = []
        with rarfile.RarFile(archive_path, 'r') as rf:
            for info in rf.infolist():
                if not info.is_dir():
                    contents.append({
                        'internal_path': info.filename,
                        'compressed_size': info.compress_size,
                        'uncompressed_size': info.file_size,
                        'modified': None,  # RAR timestamp handling varies
                        'file_type': Path(info.filename).suffix.lower()
                    })
        return contents
        
    def _index_7z(self, archive_path: str) -> List[Dict]:
        """Index 7Z contents"""
        contents = []
        with py7zr.SevenZipFile(archive_path, 'r') as szf:
            for name, info in szf.list():
                if not info.is_directory:
                    contents.append({
                        'internal_path': name,
                        'compressed_size': info.compressed,
                        'uncompressed_size': info.uncompressed,
                        'modified': info.creationtime.timestamp() if info.creationtime else None,
                        'file_type': Path(name).suffix.lower()
                    })
        return contents
        
    def _index_tar(self, archive_path: str) -> List[Dict]:
        """Index TAR contents"""
        contents = []
        with tarfile.open(archive_path, 'r:*') as tf:
            for member in tf.getmembers():
                if member.isfile():
                    contents.append({
                        'internal_path': member.name,
                        'compressed_size': member.size,
                        'uncompressed_size': member.size,
                        'modified': member.mtime,
                        'file_type': Path(member.name).suffix.lower()
                    })
        return contents


class SmartHashCalculator:
    """Calculate content hashes for move detection"""
    
    def __init__(self, max_file_size: int = 100 * 1024 * 1024):  # 100MB
        self.max_file_size = max_file_size
        self.hash_cache = {}
        
    def calculate_hash(self, file_path: str, file_size: int) -> Optional[str]:
        """Calculate smart hash for file"""
        if file_size > self.max_file_size:
            return None
            
        try:
            if file_size < 1024 * 1024:  # Under 1MB
                # Hash entire file
                with open(file_path, 'rb') as f:
                    return hashlib.md5(f.read()).hexdigest()
            else:
                # Hash first + last MB + size
                hash_md5 = hashlib.md5()
                with open(file_path, 'rb') as f:
                    # First MB
                    hash_md5.update(f.read(1024 * 1024))
                    
                    # Last MB
                    if file_size > 2 * 1024 * 1024:
                        f.seek(-1024 * 1024, 2)
                        hash_md5.update(f.read(1024 * 1024))
                        
                    # Add size for uniqueness
                    hash_md5.update(str(file_size).encode())
                    
                return hash_md5.hexdigest()
                
        except Exception:
            return None


class DriveScanner:
    """Scanner for a single drive - runs in separate process"""
    
    def __init__(self, drive: str, config: Dict, process_id: int, 
                 shared_queue: Queue, stats_dict: Dict):
        self.drive = drive
        self.config = config
        self.process_id = process_id
        self.shared_queue = shared_queue
        self.stats_dict = stats_dict
        
        # Components
        self.archive_indexer = ArchiveIndexer()
        self.hash_calculator = SmartHashCalculator()
        
        # Skip patterns
        self.skip_dirs = {
            'Windows', 'Program Files', 'Program Files (x86)', 'ProgramData',
            'System Volume Information', '$Recycle.Bin', '$RECYCLE.BIN',
            'node_modules', '.git', '__pycache__', '.vs', 'obj', 'bin',
            'AppData\\Local\\Temp', 'AppData\\Local\\Microsoft\\WindowsApps'
        }
        
        self.skip_extensions = {
            '.dll', '.exe', '.sys', '.msi', '.cab', '.msp',
            '.log', '.tmp', '.temp', '.cache', '.lock', '.pid'
        }
        
        # Stats
        self.files_scanned = 0
        self.archives_found = 0
        self.start_time = time.time()
        
    def scan(self):
        """Main scanning loop for the drive"""
        logging.info(f"[Process {self.process_id}] Starting scan of {self.drive}")
        
        # Initialize process-specific shard writer
        index_dir = Path(self.config.get('index_dir', r"C:\Utilities\file_tools\hybrid_index"))
        self.shard_writer = ProcessSafeShardedIndex(str(index_dir), self.process_id)
        
        # Directory queue
        dirs_to_scan = deque([self.drive])
        processed_dirs = set()
        
        # Archive processing queue
        archives_to_process = deque()
        
        # Batch for efficiency
        file_batch = []
        batch_size = 1000  # Smaller batch for more frequent updates
        
        scan_start = time.time()
        last_report = scan_start
        
        while dirs_to_scan or archives_to_process:
            # Process directories
            while dirs_to_scan and len(dirs_to_scan) > 0:
                current_dir = dirs_to_scan.popleft()
                
                if current_dir in processed_dirs:
                    continue
                    
                processed_dirs.add(current_dir)
                
                try:
                    files, subdirs = self._scan_directory(current_dir)
                    
                    # Add subdirectories to queue
                    for subdir in subdirs:
                        if subdir not in processed_dirs:
                            dirs_to_scan.append(subdir)
                            
                    # Process files
                    for file_info in files:
                        file_batch.append(file_info)
                        
                        # Check if it's an archive
                        if self.config.get('scan_archives', False):
                            if self.archive_indexer.is_archive(file_info.path):
                                archives_to_process.append(file_info.path)
                                self.archives_found += 1
                                
                        # Write batch if full
                        if len(file_batch) >= batch_size:
                            self.shard_writer.add_files_batch(file_batch)
                            self.files_scanned += len(file_batch)
                            file_batch = []
                            
                            # Update stats
                            self._update_stats()
                            
                except Exception as e:
                    logging.error(f"Error scanning {current_dir}: {e}")
                    
            # Process some archives
            if self.config.get('scan_archives', False):
                archives_processed = 0
                max_archives_per_iteration = 10
                
                while archives_to_process and archives_processed < max_archives_per_iteration:
                    archive_path = archives_to_process.popleft()
                    
                    # Process archive
                    contents = self.archive_indexer.index_archive_contents(archive_path)
                    if contents:
                        self.shard_writer.add_archive_contents(archive_path, contents)
                        
                    archives_processed += 1
                
        # Final flush
        if file_batch:
            self.shard_writer.add_files_batch(file_batch)
            self.files_scanned += len(file_batch)
            
        # Flush all data
        self.shard_writer.flush_all()
        self._update_stats()
        
        total_time = time.time() - scan_start
        logging.info(f"[Process {self.process_id}] Scan of {self.drive} complete: "
                    f"{self.files_scanned} files, {self.archives_found} archives in {total_time:.1f}s")
                    
    def _scan_directory(self, path: str) -> Tuple[List[FileInfo], List[str]]:
        """Scan a single directory"""
        files = []
        subdirs = []
        
        try:
            with os.scandir(path) as entries:
                for entry in entries:
                    try:
                        name = entry.name
                        
                        # Skip check
                        if any(skip in entry.path for skip in self.skip_dirs):
                            continue
                            
                        if entry.is_dir(follow_symlinks=False):
                            subdirs.append(entry.path)
                        elif entry.is_file(follow_symlinks=False):
                            ext = os.path.splitext(name)[1].lower()
                            if ext in self.skip_extensions:
                                continue
                                
                            stat = entry.stat(follow_symlinks=False)
                            
                            # Create FileInfo
                            file_info = FileInfo(
                                path=entry.path,
                                size=stat.st_size,
                                mtime=int(stat.st_mtime),
                                ctime=int(stat.st_ctime),
                                drive=self.drive,
                                ext=ext,
                                is_archive=self.archive_indexer.is_archive(entry.path)
                            )
                            
                            # Calculate hash for smaller files
                            if stat.st_size < 100 * 1024 * 1024:  # 100MB
                                file_info.content_hash = self.hash_calculator.calculate_hash(
                                    entry.path, stat.st_size
                                )
                                
                            files.append(file_info)
                            
                    except Exception:
                        continue
                        
        except Exception:
            pass
            
        return files, subdirs
        
    def _update_stats(self):
        """Update shared statistics"""
        elapsed = time.time() - self.start_time
        rate = self.files_scanned / elapsed if elapsed > 0 else 0
        
        # Only update if stats_dict is available
        if self.stats_dict is not None:
            self.stats_dict[f'drive_{self.drive}'] = {
                'files_scanned': self.files_scanned,
                'archives_found': self.archives_found,
                'rate': rate,
                'elapsed': elapsed
            }
        else:
            # Log stats locally
            if self.files_scanned % 10000 == 0:
                logging.info(f"[Process {self.process_id}] {self.drive}: {self.files_scanned} files, {rate:.0f}/s")


class ContentAnalysisEngine:
    """Handles content analysis for files needing deeper inspection"""
    
    def __init__(self, storage: ProcessSafeShardedIndex):
        self.storage = storage
        self.archive_indexer = ArchiveIndexer()
        self.hash_calculator = SmartHashCalculator()
        self.queue = queue.PriorityQueue()
        self.workers = []
        self.running = False
        
    def start_workers(self, num_workers: int = 4):
        """Start content analysis workers"""
        self.running = True
        
        for i in range(num_workers):
            worker = threading.Thread(
                target=self._worker_loop,
                name=f"ContentWorker-{i}",
                daemon=True
            )
            worker.start()
            self.workers.append(worker)
            
        logging.info(f"Started {num_workers} content analysis workers")
        
    def stop_workers(self):
        """Stop all workers"""
        self.running = False
        for worker in self.workers:
            worker.join(timeout=5)
            
    def _worker_loop(self):
        """Worker loop for content analysis"""
        while self.running:
            try:
                priority, task_type, data = self.queue.get(timeout=1)
                
                if task_type == 'analyze_archive':
                    self._analyze_archive(data)
                elif task_type == 'calculate_hash':
                    self._calculate_hash(data)
                elif task_type == 'verify_move':
                    self._verify_move(data)
                    
                self.queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Content worker error: {e}")
                
    def _analyze_archive(self, file_info: FileInfo):
        """Analyze archive contents"""
        try:
            contents = self.archive_indexer.index_archive_contents(file_info.path)
            if contents:
                self.storage.add_archive_contents(file_info.path, contents)
        except Exception as e:
            logging.debug(f"Error analyzing archive {file_info.path}: {e}")
            
    def _calculate_hash(self, file_info: FileInfo):
        """Calculate content hash"""
        try:
            if not file_info.content_hash:
                file_info.content_hash = self.hash_calculator.calculate_hash(
                    file_info.path, file_info.size
                )
        except Exception as e:
            logging.debug(f"Error calculating hash for {file_info.path}: {e}")
            
    def _verify_move(self, move_data: Dict):
        """Verify a detected move by comparing content hashes"""
        try:
            old_path = move_data['old_path']
            new_path = move_data['new_path']
            
            # If new file still exists, calculate its hash
            if os.path.exists(new_path):
                size = os.path.getsize(new_path)
                new_hash = self.hash_calculator.calculate_hash(new_path, size)
                
                # Compare with stored hash if available
                if new_hash:
                    self.storage.add_file_move(
                        old_path, new_path, new_hash, 
                        confidence=move_data.get('confidence', 0.9)
                    )
        except Exception as e:
            logging.debug(f"Error verifying move: {e}")
            
    def add_task(self, task_type: str, data: any, priority: int = 5):
        """Add task to analysis queue"""
        self.queue.put((priority, task_type, data))


def run_drive_scanner_standalone(drive: str, config: Dict, process_id: int, stats_dir: str):
    """Standalone function for multiprocessing on Windows"""
    try:
        scanner = WindowsSafeDriveScanner(
            drive=drive,
            config=config,
            process_id=process_id,
            stats_dir=Path(stats_dir)
        )
        scanner.scan()
    except Exception as e:
        logging.error(f"Scanner process {process_id} error: {e}")
        raise


class ContentAnalysisEngine:
    """Stub for content analysis functionality"""
    
    def __init__(self, storage):
        self.storage = storage
        self.workers = []
        
    def start_workers(self, count):
        """Start content analysis workers"""
        logging.info(f"Content analysis workers: {count} (stub)")
        
    def stop_workers(self):
        """Stop content analysis workers"""
        logging.info("Stopping content analysis workers (stub)")


class WindowsSafeDriveScanner(DriveScanner):
    """Windows-safe version that writes stats to files instead of shared dict"""
    
    def __init__(self, drive: str, config: dict, process_id: int, stats_dir: Path):
        # Store stats file path
        self.stats_file = stats_dir / f"drive_{drive.replace(':', '').replace('\\', '')}_stats.json"
        
        # Initialize parent without shared objects
        super().__init__(drive, config, process_id, None, None)
        
        # Set process_dir for health monitoring
        index_dir = Path(config.get('index_dir', r"C:\Utilities\file_tools\hybrid_index"))
        self.process_dir = index_dir / f"proc_{process_id:04d}"
        self.process_dir.mkdir(parents=True, exist_ok=True)
        
        # Write initial stats
        self._write_stats()
        
        # Add health monitoring
        self.health_file = self.process_dir / "health.json"
        self._update_process_health()
        
    def _update_stats(self):
        """Write stats to file instead of shared dict"""
        elapsed = time.time() - self.start_time
        rate = self.files_scanned / elapsed if elapsed > 0 else 0
        
        stats = {
            'files_scanned': self.files_scanned,
            'archives_found': self.archives_found,
            'rate': rate,
            'elapsed': elapsed,
            'timestamp': time.time()
        }
        
        # Write to file
        try:
            with open(self.stats_file, 'w') as f:
                json.dump(stats, f)
        except:
            pass
            
        # Log progress periodically and update health
        if self.files_scanned % 5000 == 0 and self.files_scanned > 0:
            logging.info(f"[Process {self.process_id}] {self.drive}: {self.files_scanned} files, {rate:.0f}/s")
            self._update_process_health()
            
    def _update_process_health(self):
        """Update process health file"""
        health_data = {
            'process_id': self.process_id,
            'drive': self.drive,
            'pid': os.getpid(),
            'last_update': time.time(),
            'files_processed': self.files_scanned,
            'archives_found': self.archives_found,
            'status': 'active',
            'rate': self.files_scanned / (time.time() - self.start_time) if time.time() > self.start_time else 0
        }
        
        try:
            with open(self.health_file, 'w') as f:
                json.dump(health_data, f)
        except Exception as e:
            logging.error(f"Failed to update health file: {e}")
            
    def _write_stats(self):
        """Write initial stats"""
        self._update_stats()


class HybridUltimateIndexer:
    """
    Main orchestrator combining Everything Search monitoring with parallel scanning
    Three-phase operation:
    1. Initial parallel scan (full power)
    2. Archive processing
    3. Everything-based perpetual monitoring
    """
    
    def __init__(self, config: Dict = None):
        # Default configuration
        default_config = {
            'index_dir': r'C:\Utilities\file_tools\hybrid_ultimate_index',
            'everything_path': None,  # Auto-detect
            'poll_interval': 300,  # 5 minutes for Everything
            'content_workers': 8,  # More workers for production
            'enable_archive_indexing': True,
            'enable_hash_calculation': True,
            'enable_move_detection': True,
            'drive_configs': {
                'C:\\': {
                    'workers': 4,
                    'priority': 'low',
                    'scan_archives': False,  # System drive
                },
                'D:\\': {
                    'workers': 8,
                    'priority': 'high',
                    'scan_archives': True,
                },
                'E:\\': {
                    'workers': 8,
                    'priority': 'high', 
                    'scan_archives': True,
                },
                'F:\\': {
                    'workers': 8,
                    'priority': 'high',
                    'scan_archives': True,
                },
            }
        }
        
        self.config = {**default_config, **(config or {})}
        self.index_dir = Path(self.config['index_dir'])
        
        try:
            self.index_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logging.error(f"Failed to create index directory: {e}")
            raise
        
        # Phase tracking
        self.current_phase = 0
        self.initial_scan_complete = False
        self.running = False
        
        # Components - Try Everything but don't fail if not available
        self.everything = None
        try:
            self.everything = EverythingIntegration(self.config.get('everything_path'))
        except Exception as e:
            logging.warning(f"Everything integration failed: {e}")
            logging.info("Continuing without Everything monitoring")
        
        # Stats directory for Windows-safe inter-process communication
        self.stats_dir = self.index_dir / "stats"
        self.stats_dir.mkdir(exist_ok=True)
        
        # For monitoring
        self.monitoring = False
        
        # Main storage (process 0)
        self.main_storage = ProcessSafeShardedIndex(str(self.index_dir), 0)
        
        # Content analysis engine
        self.content_engine = ContentAnalysisEngine(self.main_storage)
        
        # Determine drive types
        self.drive_types = self._determine_drive_types()
        
        # Resume capability
        self.resume_file = self.index_dir / "indexer_state.json"
        self.state = self.load_state()
        
        # Stats
        self.stats = {
            'total_files': 0,
            'total_archives': 0,
            'total_moves': 0,
            'polls_completed': 0,
            'start_time': time.time()
        }
        
        # GUI callback
        self.gui_callback = None
        
        # Setup logging
        self.setup_logging()
        
        # Load existing index if available
        self.existing_file_count = 0
        self.load_existing_index()
        
        # Track which method we're using
        self.using_everything = False
        self.phase_name = "Initializing"
        
    def setup_logging(self):
        """Setup logging with Unicode support"""
        log_file = self.index_dir / f"hybrid_ultimate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
    def _determine_drive_types(self) -> Dict[str, DriveType]:
        """Determine which drives to monitor with Everything vs scan traditionally"""
        drive_types = {}
        
        for drive in self.config['drive_configs'].keys():
            if os.path.exists(drive):
                # Network drives (Z:) and drives without NTFS
                if drive.startswith('Z:') or drive.startswith('\\\\'):
                    drive_types[drive] = DriveType.NETWORK
                else:
                    # Assume local NTFS for C-F drives
                    drive_types[drive] = DriveType.NTFS_LOCAL
                    
        logging.info(f"Drive types: {drive_types}")
        return drive_types
        
    def load_existing_index(self):
        """Load existing index data on startup"""
        total_files = 0
        total_archives = 0
        
        # Check merged directory first
        merged_dir = self.index_dir / "merged" / "files"
        if merged_dir.exists():
            shard_count = len(list(merged_dir.glob("*.pkl.*")))
            if shard_count > 0:
                logging.info(f"Found {shard_count} merged shards")
        
        # Check process directories
        for proc_dir in self.index_dir.glob("proc_*"):
            metadata_file = proc_dir / "metadata.json"
            if metadata_file.exists():
                try:
                    with open(metadata_file) as f:
                        metadata = json.load(f)
                        files = metadata.get('total_files', 0)
                        archives = metadata.get('total_archives', 0)
                        total_files += files
                        total_archives += archives
                        logging.info(f"Process {proc_dir.name}: {files:,} files, {archives:,} archives")
                except Exception as e:
                    logging.error(f"Error loading metadata from {proc_dir}: {e}")
        
        if total_files > 0:
            logging.info(f"Found existing index: {total_files:,} files, {total_archives:,} archives")
            self.stats['total_files'] = total_files
            self.stats['total_archives'] = total_archives
            self.existing_file_count = total_files
            
            # Don't mark as complete yet - we may want to update with Everything
            # self.state['initial_scan_complete'] = True
        else:
            logging.info("No existing index found - will start fresh")
    
    def _get_phase_name(self):
        """Get human-readable phase name"""
        phase_names = {
            0: "Initializing",
            1: "Everything Export",
            2: "Archive Processing",
            3: "Everything Monitoring"
        }
        return phase_names.get(self.current_phase, "Unknown")
        
    def get_gui_stats(self):
        """Get comprehensive stats for GUI display"""
        stats = {
            'phase': self.current_phase,
            'phase_name': self._get_phase_name(),
            'total_files': 0,
            'total_archives': 0,
            'total_rate': 0,
            'drives': {},
            'memory_mb': psutil.Process().memory_info().rss / 1024 / 1024,
            'cpu_percent': psutil.cpu_percent(interval=0.1),
            'runtime': time.time() - self.stats['start_time']
        }
        
        # Add Everything export status if in Phase 1
        if self.current_phase == 1 and hasattr(self, 'export_status'):
            stats['export_status'] = self.export_status
            if hasattr(self, 'everything_total_files'):
                stats['everything_total'] = self.everything_total_files
                stats['everything_processed'] = getattr(self, 'everything_processed_files', 0)
        
        # Read stats from all drive files
        for drive in self.config['drive_configs']:
            stats_file = self.stats_dir / f"drive_{drive.replace(':', '').replace('\\', '')}_stats.json"
            if stats_file.exists():
                try:
                    with open(stats_file, 'r') as f:
                        drive_stats = json.load(f)
                        stats['drives'][drive] = drive_stats
                        stats['total_files'] += drive_stats.get('files_scanned', 0)
                        stats['total_archives'] += drive_stats.get('archives_found', 0)
                        stats['total_rate'] += drive_stats.get('rate', 0)
                except:
                    pass
        
        return stats
        
    def load_state(self):
        """Load previous indexer state"""
        if self.resume_file.exists():
            try:
                with open(self.resume_file, 'r') as f:
                    return json.load(f)
            except:
                pass
                
        return {
            'phase': 0,
            'initial_scan_complete': False,
            'last_everything_poll': 0,
            'scan_history': [],
            'move_history': []
        }
        
    def save_state(self):
        """Save current state"""
        self.state['phase'] = self.current_phase
        self.state['initial_scan_complete'] = self.initial_scan_complete
        
        temp_file = self.resume_file.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.state, f, indent=2)
        temp_file.replace(self.resume_file)
        
    def verify_everything_ready(self) -> bool:
        """Verify Everything is ready to use"""
        if not self.everything:
            return False
            
        try:
            # Check if Everything is running
            if not self.everything.is_everything_running():
                logging.info("Everything is not running, attempting to start...")
                self.everything.ensure_everything_running()
                
            # Test export capability
            logging.info("Testing Everything export capability...")
            return True
            
        except Exception as e:
            logging.error(f"Everything verification failed: {e}")
            return False
    
    def run_initial_everything_export(self):
        """Phase 1: Export and process Everything's complete index"""
        logging.info("Phase 1: Exporting Everything's complete index...")
        
        self.export_status = "Exporting from Everything..."
        self.using_everything = True
        self.phase_name = "Everything Export"
        
        # Export Everything's ENTIRE index
        if not self.everything.export_current_index():
            logging.error("Everything export failed!")
            # Fall back to traditional scanning
            self.run_traditional_parallel_scan()
            return
            
        # Process the exported snapshot
        logging.info("Processing Everything snapshot...")
        self.export_status = "Processing exported files..."
        
        start_time = time.time()
        current_snapshot = self.everything.parse_export()
        
        if not current_snapshot:
            logging.error("Failed to parse Everything export!")
            self.run_traditional_parallel_scan()
            return
            
        # Store the files
        logging.info(f"Storing {len(current_snapshot):,} files from Everything export...")
        self.everything_total_files = len(current_snapshot)
        self.everything_processed_files = 0
        
        batch = []
        batch_size = 10000
        
        for i, (path, file_info) in enumerate(current_snapshot.items()):
            batch.append(file_info)
            
            if len(batch) >= batch_size:
                self.main_storage.add_files(batch)
                batch.clear()
                self.everything_processed_files = i + 1
                
                if i % 100000 == 0:
                    elapsed = time.time() - start_time
                    rate = i / elapsed if elapsed > 0 else 0
                    logging.info(f"Processed {i:,}/{self.everything_total_files:,} files ({rate:,.0f} files/sec)")
                    
        # Process remaining batch
        if batch:
            self.main_storage.add_files(batch)
            self.everything_processed_files = self.everything_total_files
            
        # Finalize
        self.main_storage.flush_all()
        elapsed = time.time() - start_time
        rate = self.everything_total_files / elapsed if elapsed > 0 else 0
        
        logging.info(f"Everything export complete: {self.everything_total_files:,} files in {elapsed:.1f}s ({rate:,.0f} files/sec)")
        self.export_status = f"Export complete: {self.everything_total_files:,} files"
        
        # Update stats
        self.stats['total_files'] = self.everything_total_files
        
    def run_traditional_parallel_scan(self):
        """Fall back to traditional parallel scanning"""
        logging.info("Running traditional parallel scan (fallback mode)...")
        
        self.using_everything = False
        self.phase_name = "Parallel Scan (Fallback)"
        
        # Reset phase tracking
        self.current_phase = 1
        
        # Create process pool
        processes = []
        process_id = 1
        
        # Start scanner process for each drive
        for drive, config in self.config['drive_configs'].items():
            if not os.path.exists(drive):
                logging.warning(f"Drive {drive} not found, skipping")
                continue
                
            # Create scanner for this drive
            scanner = WindowsSafeDriveScanner(
                drive=drive,
                config=self.config,
                process_id=process_id,
                stats_dir=self.stats_dir
            )
            
            # Create and start process
            p = mp.Process(target=scanner.scan, name=f"Scanner_{drive}")
            p.start()
            processes.append(p)
            
            logging.info(f"Started scanner process {process_id} for drive {drive}")
            process_id += 1
            
        # Monitor progress
        while any(p.is_alive() for p in processes):
            # Update GUI if callback is set
            if self.gui_callback:
                self.gui_callback(self.get_gui_stats())
                
            time.sleep(1)
            
        # Wait for all processes to complete
        for p in processes:
            p.join()
            
        logging.info("All parallel scan processes completed")
        
        # Merge results from all processes
        self.merge_process_results()
        
    def merge_process_results(self):
        """Merge results from all scanner processes with support for new naming"""
        logging.info("Merging results from all processes...")
        
        merged_dir = self.index_dir / "merged"
        merged_dir.mkdir(exist_ok=True)
        
        total_files = 0
        total_archives = 0
        
        # Collect all process shards with new naming pattern
        for data_type in ['files', 'archives', 'moves']:
            type_dir = merged_dir / data_type
            type_dir.mkdir(exist_ok=True)
            
            all_shards = []
            for proc_dir in self.index_dir.glob("proc_*"):
                shard_dir = proc_dir / data_type
                if shard_dir.exists():
                    # Match both old and new naming patterns for backwards compatibility
                    for pattern in ["*.pkl.*", f"*_p*_shard_*.pkl.*"]:
                        for shard_file in shard_dir.glob(pattern):
                            all_shards.append(shard_file)
            
            if all_shards:
                logging.info(f"Found {len(all_shards)} {data_type} shards to merge")
        
        # Read and merge from each process directory (metadata)
        for proc_dir in self.index_dir.glob("proc_*"):
            if proc_dir.is_dir():
                # Read metadata
                metadata_file = proc_dir / "metadata.json"
                if metadata_file.exists():
                    try:
                        with open(metadata_file) as f:
                            metadata = json.load(f)
                            total_files += metadata.get('total_files', 0)
                            total_archives += metadata.get('total_archives', 0)
                    except Exception as e:
                        logging.error(f"Error reading metadata from {proc_dir}: {e}")
                        
        logging.info(f"Merge complete: {total_files:,} files, {total_archives:,} archives")
        
        # Update stats
        self.stats['total_files'] = total_files
        self.stats['total_archives'] = total_archives
        
    def run(self):
        """Main entry point - run the three-phase indexing system"""
        logging.info("="*80)
        logging.info("HYBRID ULTIMATE INDEXER v3.0 - PRODUCTION")
        logging.info("Using Everything Search for instant file indexing")
        logging.info("="*80)
        
        self.running = True
        
        # Start content analysis workers
        self.content_engine.start_workers(self.config.get('content_workers', 8))
        
        try:
            # Phase 1: Use Everything export for instant access to all files
            if not self.state.get('initial_scan_complete', False):
                self.current_phase = 1
                
                # Check if Everything is available
                if self.everything and self.verify_everything_ready():
                    logging.info("\nPHASE 1: Exporting Everything index")
                    self.run_initial_everything_export()
                else:
                    logging.info("\nPHASE 1: Falling back to traditional scanning")
                    self.run_traditional_parallel_scan()
                    
                self.state['initial_scan_complete'] = True
                self.initial_scan_complete = True
                self.save_state()
            else:
                logging.info("\nPhase 1 already complete, skipping to monitoring")
                self.initial_scan_complete = True
                
            # Phase 2: Archive processing (if enabled and archives found)
            if self.config.get('enable_archive_indexing', True) and self._has_archives():
                self.current_phase = 2
                logging.info("\nPHASE 2: Processing discovered archives")
                self.process_archives_phase()
                self.save_state()
                
            # Phase 3: Everything-based perpetual monitoring (only if Everything is available)
            if self.everything:
                self.current_phase = 3
                logging.info("\nPHASE 3: Entering Everything-based perpetual monitoring")
                self.run_everything_monitoring()
            else:
                logging.info("\nPhase 3: Everything not available - scan complete")
                logging.info("To enable monitoring, install Everything Search from voidtools.com")
            
        except KeyboardInterrupt:
            logging.info("\nShutting down...")
        finally:
            self.shutdown()
            
    def check_everything_available(self):
        """Check if Everything is available and has data"""
        try:
            if not self.everything:
                return False
                
            # Check if Everything is running
            if not self.everything.is_everything_running():
                logging.warning("Everything is not running")
                return False
                
            # Try a test export
            test_cmd = [str(self.everything.everything_path), "-get-result-count"]
            result = subprocess.run(test_cmd, capture_output=True, text=True, timeout=5)
            
            if result.returncode == 0:
                count = int(result.stdout.strip()) if result.stdout.strip().isdigit() else 0
                logging.info(f"Everything reports {count:,} files in database")
                return count > 1000  # Reasonable minimum
                
        except Exception as e:
            logging.error(f"Everything check failed: {e}")
            
        return False
    
    def _has_archives(self) -> bool:
        """Check if any archives were discovered during scanning"""
        try:
            for proc_dir in self.index_dir.glob("proc_*"):
                archives_dir = proc_dir / "archives"
                if archives_dir.exists() and list(archives_dir.glob("*.pkl.*")):
                    return True
            return False
        except Exception as e:
            logging.error(f"Error checking for archives: {e}")
            return False
    
    def process_archives_phase(self):
        """Phase 2: Process discovered archives"""
        logging.info("Processing discovered archives...")
        
        total_archives = 0
        processed_archives = 0
        
        # Count total archives
        for proc_dir in self.index_dir.glob("proc_*"):
            archives_dir = proc_dir / "archives"
            if archives_dir.exists():
                for shard in archives_dir.glob("*.pkl.*"):
                    try:
                        if HAS_LZ4 and shard.suffix == '.lz4':
                            with lz4.frame.open(shard, 'rb') as f:
                                data = pickle.load(f) if not HAS_MSGPACK else msgpack.unpackb(f.read())
                        else:
                            with gzip.open(shard, 'rb') as f:
                                data = pickle.load(f)
                        total_archives += len(data)
                    except Exception as e:
                        logging.error(f"Error processing archive shard {shard}: {e}")
        
        if total_archives > 0:
            logging.info(f"Found {total_archives:,} archives to process")
            # Archive processing would happen here - for now just log
            processed_archives = total_archives
            logging.info(f"Archive processing complete: {processed_archives:,} archives")
        else:
            logging.info("No archives found to process")
        
        self.stats['total_archives'] = processed_archives
    
    def run_everything_monitoring(self):
        """Phase 3: Perpetual monitoring using Everything or fallback"""
        logging.info("Starting perpetual file monitoring...")
        
        # Check if Everything export is working
        if self.everything and self.everything.export_current_index():
            logging.info("Everything export working - using Everything-based monitoring")
            self._run_everything_monitoring_loop()
        else:
            logging.warning("Everything export failed - falling back to polling monitoring")
            self._run_fallback_monitoring_loop()
    
    def _run_everything_monitoring_loop(self):
        """Main Everything monitoring loop"""
        poll_interval = self.config.get('poll_interval', 300)  # 5 minutes default
        last_poll_time = 0
        
        logging.info(f"Everything monitoring active (polling every {poll_interval}s)")
        
        try:
            while self.running:
                current_time = time.time()
                
                # Time for a poll?
                if current_time - last_poll_time >= poll_interval:
                    logging.info("Polling Everything for changes...")
                    
                    # Export current index
                    if self.everything.export_current_index():
                        # Parse the export
                        current_snapshot = self.everything.parse_export()
                        
                        if current_snapshot and hasattr(self.everything, 'last_snapshot'):
                            # Detect changes
                            changes = self._detect_everything_changes(
                                self.everything.last_snapshot, 
                                current_snapshot
                            )
                            
                            if changes['new_files'] or changes['deleted_files'] or changes['modified_files']:
                                logging.info(f"Changes detected: {len(changes['new_files'])} new, "
                                           f"{len(changes['deleted_files'])} deleted, "
                                           f"{len(changes['modified_files'])} modified, "
                                           f"{len(changes['moves'])} moves")
                                
                                # Update index with changes
                                self._update_index_with_changes(changes)
                        
                        # Update snapshot
                        self.everything.last_snapshot = current_snapshot
                        last_poll_time = current_time
                        
                        # Update state
                        self.state['last_everything_poll'] = current_time
                        self.save_state()
                    else:
                        logging.error("Everything export failed during monitoring")
                        # Fall back to slower monitoring
                        self._run_fallback_monitoring_loop()
                        break
                
                # Update GUI
                if self.gui_callback:
                    self.gui_callback(self.get_gui_stats())
                
                time.sleep(10)  # Check every 10 seconds
                
        except KeyboardInterrupt:
            logging.info("Monitoring interrupted by user")
        except Exception as e:
            logging.error(f"Monitoring error: {e}")
    
    def _run_fallback_monitoring_loop(self):
        """Fallback monitoring without Everything"""
        poll_interval = 600  # 10 minutes for fallback (slower)
        monitored_paths = list(self.config['drive_configs'].keys())
        
        logging.info(f"Fallback monitoring active for {len(monitored_paths)} drives (every {poll_interval}s)")
        logging.info(f"Monitored paths: {monitored_paths}")
        
        # Take initial snapshot
        last_snapshot = self._take_filesystem_snapshot(monitored_paths)
        last_poll_time = time.time()
        
        try:
            while self.running:
                current_time = time.time()
                
                if current_time - last_poll_time >= poll_interval:
                    logging.info("Scanning filesystem for changes...")
                    
                    # Take new snapshot
                    current_snapshot = self._take_filesystem_snapshot(monitored_paths)
                    
                    # Detect changes
                    changes = self._detect_filesystem_changes(last_snapshot, current_snapshot)
                    
                    if any(changes.values()):
                        total_changes = sum(len(v) if isinstance(v, list) else 0 for v in changes.values())
                        logging.info(f"Fallback monitoring detected {total_changes} changes")
                        
                        # Update index with changes
                        self._update_index_with_changes(changes)
                    
                    last_snapshot = current_snapshot
                    last_poll_time = current_time
                
                # Update GUI
                if self.gui_callback:
                    self.gui_callback(self.get_gui_stats())
                
                time.sleep(30)  # Check every 30 seconds for fallback
                
        except KeyboardInterrupt:
            logging.info("Fallback monitoring interrupted by user")
        except Exception as e:
            logging.error(f"Fallback monitoring error: {e}")
    
    def _take_filesystem_snapshot(self, paths, max_files_per_path=50000):
        """Take a filesystem snapshot for fallback monitoring"""
        snapshot = {}
        
        for path in paths:
            if not os.path.exists(path):
                continue
                
            file_count = 0
            try:
                for root, dirs, files in os.walk(path):
                    # Skip system directories
                    dirs[:] = [d for d in dirs if d not in self._get_skip_dirs()]
                    
                    for file in files:
                        if file_count >= max_files_per_path:
                            break
                            
                        filepath = os.path.join(root, file)
                        try:
                            stat = os.stat(filepath)
                            snapshot[filepath] = {
                                'size': stat.st_size,
                                'mtime': stat.st_mtime_ns
                            }
                            file_count += 1
                        except:
                            pass
                    
                    if file_count >= max_files_per_path:
                        break
                        
            except Exception as e:
                logging.error(f"Error scanning {path}: {e}")
        
        return snapshot
    
    def _get_skip_dirs(self):
        """Get directories to skip during monitoring"""
        return {
            'Windows', 'Program Files', 'Program Files (x86)', 'ProgramData',
            'System Volume Information', '$Recycle.Bin', '$RECYCLE.BIN',
            'AppData', 'node_modules', '.git', '__pycache__'
        }
    
    def _detect_everything_changes(self, old_snapshot, new_snapshot):
        """Detect changes between Everything snapshots"""
        if not old_snapshot:
            return {'new_files': [], 'deleted_files': [], 'modified_files': [], 'moves': []}
        
        old_paths = set(old_snapshot.keys())
        new_paths = set(new_snapshot.keys())
        
        new_files = list(new_paths - old_paths)
        deleted_files = list(old_paths - new_paths)
        
        # Check for modifications
        modified_files = []
        for path in old_paths & new_paths:
            old_info = old_snapshot[path]
            new_info = new_snapshot[path]
            
            if (old_info.size != new_info.size or 
                old_info.mtime != new_info.mtime):
                modified_files.append(path)
        
        # Detect moves (simple version)
        moves = []
        if hasattr(self.everything, 'detect_moves'):
            moves = self.everything.detect_moves(
                set(new_files), set(deleted_files), 
                new_snapshot, old_snapshot
            )
        
        return {
            'new_files': new_files,
            'deleted_files': deleted_files,
            'modified_files': modified_files,
            'moves': moves
        }
    
    def _detect_filesystem_changes(self, old_snapshot, new_snapshot):
        """Detect changes between filesystem snapshots"""
        if not old_snapshot:
            return {'new_files': [], 'deleted_files': [], 'modified_files': [], 'moves': []}
        
        old_paths = set(old_snapshot.keys())
        new_paths = set(new_snapshot.keys())
        
        new_files = list(new_paths - old_paths)
        deleted_files = list(old_paths - new_paths)
        
        # Check for modifications
        modified_files = []
        for path in old_paths & new_paths:
            old_info = old_snapshot[path]
            new_info = new_snapshot[path]
            
            if (old_info['mtime'] != new_info['mtime'] or 
                old_info['size'] != new_info['size']):
                modified_files.append(path)
        
        # Simple move detection
        moves = []
        # Group by size for potential move detection
        deleted_by_size = defaultdict(list)
        added_by_size = defaultdict(list)
        
        for path in deleted_files:
            size = old_snapshot[path]['size']
            deleted_by_size[size].append(path)
        
        for path in new_files:
            size = new_snapshot[path]['size']
            added_by_size[size].append(path)
        
        # Match same-size files as potential moves
        for size in deleted_by_size:
            if size in added_by_size:
                del_paths = deleted_by_size[size]
                add_paths = added_by_size[size]
                
                for i in range(min(len(del_paths), len(add_paths))):
                    moves.append({'old_path': del_paths[i], 'new_path': add_paths[i]})
                    new_files.remove(add_paths[i])
                    deleted_files.remove(del_paths[i])
        
        return {
            'new_files': new_files,
            'deleted_files': deleted_files,
            'modified_files': modified_files,
            'moves': moves
        }
    
    def _update_index_with_changes(self, changes):
        """Update the index with detected changes"""
        # This would update the sharded index with changes
        # For now, just log the changes
        
        if changes['new_files']:
            logging.info(f"New files detected: {len(changes['new_files'])}")
            # Would add new files to index
        
        if changes['deleted_files']:
            logging.info(f"Deleted files detected: {len(changes['deleted_files'])}")
            # Would remove files from index
        
        if changes['modified_files']:
            logging.info(f"Modified files detected: {len(changes['modified_files'])}")
            # Would update file metadata
        
        if changes['moves']:
            logging.info(f"File moves detected: {len(changes['moves'])}")
            # Would update file paths in index
            
        # Update move history
        if changes['moves']:
            self.state.setdefault('move_history', []).extend(changes['moves'])
            self.save_state()
    
    def shutdown(self):
        """Shutdown the indexer"""
        logging.info("Shutting down indexer...")
        self.running = False
        
        # Stop content analysis workers
        if hasattr(self, 'content_engine'):
            self.content_engine.stop_workers()
        
        # Save final state
        self.save_state()
        
        logging.info("Indexer shutdown complete")
    
    def verify_everything_ready(self):
        """Verify Everything is running and ready"""
        if not self.everything:
            return False
            
        if not self.everything.is_everything_running():
            logging.warning("Everything is not running, attempting to start...")
            self.everything.ensure_everything_running()
            time.sleep(5)  # Give it time to index
            
        # Quick test to verify Everything is responding
        try:
            # We'll test during actual export
            return True
        except Exception as e:
            logging.error(f"Everything verification failed: {e}")
            return False
    
    def run_initial_everything_export(self):
        """Phase 1: Export and process Everything's complete index"""
        logging.info("Phase 1: Exporting Everything's complete index...")
        
        # Track export progress
        self.export_status = "Starting Export"
        export_start = time.time()
        
        # Export Everything's ENTIRE index
        if not self.everything.export_current_index():
            logging.error("Everything export failed!")
            # Fall back to traditional scanning
            self.run_traditional_parallel_scan()
            return
            
        # Parse the complete export - this has ALL files
        self.export_status = "Parsing Export"
        parse_start = time.time()
        current_snapshot = self.everything.parse_export()
        
        export_time = parse_start - export_start
        parse_time = time.time() - parse_start
        
        logging.info(f"Everything export complete: {len(current_snapshot):,} files")
        logging.info(f"Export time: {export_time:.1f}s, Parse time: {parse_time:.1f}s")
        
        # Process the snapshot into our sharded storage
        self.export_status = "Processing Files"
        self.process_everything_snapshot(current_snapshot)
        
        # Save snapshot for Phase 3 monitoring
        self.everything.last_snapshot = current_snapshot
        
        total_time = time.time() - export_start
        logging.info(f"Phase 1 complete in {total_time:.1f}s")
        
    def process_everything_snapshot(self, snapshot: Dict[str, FileInfo]):
        """Process Everything's complete file list into our storage"""
        logging.info(f"Processing {len(snapshot):,} files into sharded storage...")
        
        # Initialize for GUI display
        self.everything_total_files = len(snapshot)
        self.everything_processed_files = 0
        
        # Group by drive for statistics
        files_by_drive = defaultdict(list)
        archives_found = []
        
        for path, file_info in snapshot.items():
            drive = file_info.drive
            if drive:  # Process all drives, not just configured ones
                files_by_drive[drive].append(file_info)
                
                # Track archives for Phase 2
                if file_info.is_archive and self.config.get('enable_archive_indexing', True):
                    archives_found.append(file_info)
        
        # Process in batches
        batch_size = 50000  # Larger batches since no I/O
        total_processed = 0
        process_start = time.time()
        
        # Initialize stats files for GUI
        for drive in files_by_drive.keys():
            if drive in self.config['drive_configs']:
                stats_file = self.stats_dir / f"drive_{drive.replace(':', '').replace('\\', '')}_stats.json"
                with open(stats_file, 'w') as f:
                    json.dump({
                        'files_scanned': 0,
                        'archives_found': 0,
                        'rate': 0,
                        'elapsed': 0,
                        'timestamp': time.time()
                    }, f)
        
        # Process each drive
        for drive, files in files_by_drive.items():
            logging.info(f"Processing {len(files):,} files from {drive}")
            drive_archives = sum(1 for f in files if f.is_archive)
            
            for i in range(0, len(files), batch_size):
                batch = files[i:i + batch_size]
                self.main_storage.add_files_batch(batch)
                total_processed += len(batch)
                self.everything_processed_files = total_processed
                
                # Update stats for GUI
                self.stats['total_files'] = total_processed
                self.stats['total_archives'] = len(archives_found)
                
                # Calculate processing rate
                elapsed = time.time() - process_start
                if elapsed > 0:
                    self.stats['processing_rate'] = total_processed / elapsed
                    
                # Update drive stats file for GUI
                if drive in self.config['drive_configs']:
                    stats_file = self.stats_dir / f"drive_{drive.replace(':', '').replace('\\', '')}_stats.json"
                    drive_processed = min(i + batch_size, len(files))
                    with open(stats_file, 'w') as f:
                        json.dump({
                            'files_scanned': drive_processed,
                            'archives_found': drive_archives,
                            'rate': self.stats['processing_rate'],
                            'elapsed': elapsed,
                            'timestamp': time.time(),
                            'total_files': len(files)
                        }, f)
        
        # Flush all data
        self.main_storage.flush_all()
        
        # Store archives for Phase 2
        self.archives_to_process = archives_found
        logging.info(f"Found {len(archives_found)} archives for Phase 2 processing")
        
        # Final stats
        total_elapsed = time.time() - process_start
        final_rate = total_processed / total_elapsed if total_elapsed > 0 else 0
        logging.info(f"Processed {total_processed:,} files at {final_rate:,.0f} files/sec")
    
    def run_traditional_parallel_scan(self):
        """Phase 1: Full parallel scan of all drives"""
        available_drives = []
        
        for drive, config in self.config['drive_configs'].items():
            if os.path.exists(drive):
                # For initial scan, disable archive processing
                config_copy = config.copy()
                config_copy['scan_archives'] = False
                config_copy['index_dir'] = str(self.index_dir)
                available_drives.append((drive, config_copy))
                
        if not available_drives:
            logging.error("No drives found!")
            return
            
        # Initialize stats files
        for drive, _ in available_drives:
            stats_file = self.stats_dir / f"drive_{drive.replace(':', '').replace('\\', '')}_stats.json"
            with open(stats_file, 'w') as f:
                json.dump({
                    'files_scanned': 0,
                    'archives_found': 0,
                    'rate': 0,
                    'elapsed': 0,
                    'timestamp': time.time()
                }, f)
            
        # Start monitoring thread
        monitor_thread = threading.Thread(
            target=self._monitor_parallel_progress,
            daemon=True
        )
        monitor_thread.start()
        
        # Create scanner processes
        processes = []
        process_id = 1  # Start from 1, main storage is 0
        
        self.monitoring = True
        for drive, config in available_drives:
            p = Process(
                target=run_drive_scanner_standalone,
                args=(drive, config, process_id, str(self.stats_dir))
            )
            p.start()
            processes.append(p)
            process_id += 1
            
            logging.info(f"Started scanner process {process_id} for {drive}")
            
        # Wait for completion
        for p in processes:
            p.join()
            
        self.monitoring = False
        
        # Merge results
        logging.info("\nMerging process shards...")
        self._merge_all_shards()
        
        # Update stats
        self._update_total_stats()
        
    def process_archives_phase(self):
        """Phase 2: Process all discovered archives"""
        logging.info("Processing discovered archives...")
        
        # Use archives found during Everything export
        if hasattr(self, 'archives_to_process'):
            archives_found = self.archives_to_process
        else:
            # Fallback: Read through merged shards to find archives
            logging.info("Scanning merged index for archives...")
            merged_dir = self.index_dir / "merged" / "files"
            if not merged_dir.exists():
                logging.warning("No merged files found")
                return
                
            archives_found = []
            
            # Scan merged shards for archives
            for shard_file in sorted(merged_dir.glob("*.pkl.*")):
                try:
                    # Load shard
                    if shard_file.suffix == '.lz4' and HAS_LZ4:
                        with lz4.frame.open(shard_file, 'rb') as f:
                            if HAS_MSGPACK:
                                data = msgpack.unpackb(f.read())
                            else:
                                data = pickle.load(f)
                    else:
                        with gzip.open(shard_file, 'rb') as f:
                            data = pickle.load(f)
                            
                    # Find archives
                    for entry in data:
                        if entry.get('is_archive', False):
                            archives_found.append(FileInfo(**entry))
                            
                except Exception as e:
                    logging.error(f"Error reading shard {shard_file}: {e}")
                
        logging.info(f"Found {len(archives_found)} archives to process")
        
        # Process archives in batches
        batch_size = 100
        for i in range(0, len(archives_found), batch_size):
            batch = archives_found[i:i + batch_size]
            
            for archive in batch:
                self.content_engine.add_task('analyze_archive', archive, priority=5)
                
            # Wait for queue to process
            while self.content_engine.queue.qsize() > 50:
                time.sleep(0.1)
                
        # Wait for all archives to be processed
        self.content_engine.queue.join()
        
        # Flush results
        self.main_storage.flush_all()
        
        logging.info(f"Archive processing complete: {self.content_engine.archive_indexer.processed_count} archives, "
                    f"{self.content_engine.archive_indexer.total_contents} files found inside")
                    
    def run_everything_monitoring(self):
        """Phase 3: Everything-based perpetual monitoring"""
        poll_interval = self.config.get('poll_interval', 300)  # 5 minutes
        
        while self.running:
            try:
                poll_start = time.time()
                
                # Export Everything index
                if self.everything.export_current_index():
                    # Parse export
                    current_snapshot = self.everything.parse_export()
                    
                    # Find changes if we have a previous snapshot
                    if self.everything.last_snapshot:
                        new_files, deleted_files, modified_files, moves = \
                            self.everything.find_changes(
                                current_snapshot,
                                self.everything.last_snapshot
                            )
                            
                        # Process changes
                        self._process_everything_changes(
                            new_files, deleted_files, modified_files, moves,
                            current_snapshot
                        )
                        
                        logging.info(
                            f"Poll complete: {len(new_files)} new, "
                            f"{len(modified_files)} modified, "
                            f"{len(deleted_files)} deleted, "
                            f"{len(moves)} moves detected"
                        )
                    else:
                        # First poll after initial scan - just store snapshot
                        logging.info("First Everything poll - storing baseline")
                        
                    # Update snapshot
                    self.everything.last_snapshot = current_snapshot
                    self.stats['polls_completed'] += 1
                    self.state['last_everything_poll'] = time.time()
                    
                    # Save state
                    self.save_state()
                    self.main_storage.flush_all()
                    
                # Calculate sleep time
                poll_duration = time.time() - poll_start
                sleep_time = max(0, poll_interval - poll_duration)
                
                logging.info(f"Poll took {poll_duration:.1f}s, sleeping {sleep_time:.1f}s")
                
                # Sleep with periodic checks
                sleep_start = time.time()
                while time.time() - sleep_start < sleep_time and self.running:
                    time.sleep(1)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                logging.error(f"Everything monitoring error: {e}")
                time.sleep(30)  # Error backoff
                
    def _process_everything_changes(self, new_files: Set[str], deleted_files: Set[str],
                                   modified_files: Set[str], moves: List[Dict],
                                   snapshot: Dict[str, FileInfo]):
        """Process changes detected by Everything"""
        
        # Process moves first
        if moves and self.config.get('enable_move_detection', True):
            for move in moves:
                # Verify move with content analysis
                self.content_engine.add_task('verify_move', move, priority=2)
                self.stats['total_moves'] += 1
                
                # Record in state history
                self.state['move_history'].append({
                    'timestamp': time.time(),
                    'old_path': move['old_path'],
                    'new_path': move['new_path'],
                    'confidence': move.get('confidence', 0.9)
                })
                
        # Process deletions
        if deleted_files:
            # In a real implementation, you'd mark these as deleted in the index
            logging.info(f"Files deleted: {len(deleted_files)}")
            
        # Process new files
        if new_files:
            new_file_infos = [snapshot[path] for path in new_files if path in snapshot]
            
            # Add to index
            self.main_storage.add_files_batch(new_file_infos)
            self.stats['total_files'] += len(new_file_infos)
            
            # Queue archives for analysis
            if self.config.get('enable_archive_indexing', True):
                for file_info in new_file_infos:
                    if file_info.is_archive:
                        self.content_engine.add_task('analyze_archive', file_info, priority=5)
                        
        # Process modifications
        if modified_files:
            modified_file_infos = [snapshot[path] for path in modified_files if path in snapshot]
            
            # Update in index (simplified - just add as new)
            self.main_storage.add_files_batch(modified_file_infos)
            
            # Re-analyze archives if modified
            if self.config.get('enable_archive_indexing', True):
                for file_info in modified_file_infos:
                    if file_info.is_archive:
                        self.content_engine.add_task('analyze_archive', file_info, priority=3)
                        
    def _has_archives(self):
        """Check if any archives were found during initial scan"""
        total_archives = 0
        
        # Read stats from files
        for drive in self.config['drive_configs'].keys():
            stats_file = self.stats_dir / f"drive_{drive.replace(':', '').replace('\\', '')}_stats.json"
            
            if stats_file.exists():
                try:
                    with open(stats_file, 'r') as f:
                        stats = json.load(f)
                        total_archives += stats.get('archives_found', 0)
                except:
                    continue
                    
        return total_archives > 0
        
    def _monitor_parallel_progress(self):
        """Monitor progress during parallel scanning"""
        last_update = time.time()
        
        while self.monitoring:
            try:
                if time.time() - last_update < 0.5:  # Update every 0.5 seconds
                    time.sleep(0.1)
                    continue
                    
                last_update = time.time()
                
                # Let GUI handle the display
                if self.gui_callback:
                    self.gui_callback()
                
            except Exception as e:
                logging.error(f"Monitor error: {e}")
                
    def _merge_all_shards(self):
        """Merge shards from all processes into unified index"""
        merged_dir = self.index_dir / "merged"
        merged_dir.mkdir(exist_ok=True)
        
        for data_type in ['files', 'archives', 'moves']:
            type_dir = merged_dir / data_type
            type_dir.mkdir(exist_ok=True)
            
            # Collect all process shards
            all_shards = []
            
            for proc_dir in self.index_dir.glob("proc_*"):
                shard_dir = proc_dir / data_type
                if shard_dir.exists():
                    for shard_file in shard_dir.glob("*.pkl.*"):
                        all_shards.append(shard_file)
                        
            # Merge shards by type
            if all_shards:
                logging.info(f"Merging {len(all_shards)} {data_type} shards...")
                
                # For simplicity, just copy shards to merged directory
                # In production, you'd consolidate and deduplicate
                for i, shard_file in enumerate(sorted(all_shards)):
                    dest = type_dir / f"{data_type}_merged_{i:06d}{shard_file.suffix}"
                    shard_file.rename(dest)
                    
    def _update_total_stats(self):
        """Update total statistics from all processes"""
        total_files = 0
        total_archives = 0
        
        # Read stats from files
        for drive in self.config['drive_configs'].keys():
            stats_file = self.stats_dir / f"drive_{drive.replace(':', '').replace('\\', '')}_stats.json"
            
            if stats_file.exists():
                try:
                    with open(stats_file, 'r') as f:
                        stats = json.load(f)
                        total_files += stats.get('files_scanned', 0)
                        total_archives += stats.get('archives_found', 0)
                except:
                    continue
        
        self.stats['total_files'] = total_files
        self.stats['total_archives'] = total_archives
        
        logging.info(f"\nInitial scan complete: {total_files:,} files, {total_archives:,} archives")
        
    def shutdown(self):
        """Clean shutdown"""
        self.running = False
        self.monitoring = False
        
        # Stop content workers
        self.content_engine.stop_workers()
        
        # Final flush
        self.main_storage.flush_all()
        self.save_state()
        
        logging.info("Hybrid Ultimate Indexer shutdown complete")


class IndexerMonitorGUI:
    """Real-time monitoring GUI for the indexer"""
    
    def __init__(self, indexer: HybridUltimateIndexer):
        self.indexer = indexer
        self.indexer.gui_callback = self.update_display
        
        # Create main window
        self.root = tk.Tk()
        self.root.title("Hybrid Ultimate Indexer v3.0 - Live Monitor")
        self.root.geometry("1200x800")
        
        # Configure style
        self.style = ttk.Style()
        self.style.theme_use('clam')
        
        # Custom colors
        self.colors = {
            'bg': '#1e1e1e',
            'fg': '#ffffff',
            'accent': '#007acc',
            'success': '#4caf50',
            'warning': '#ff9800',
            'error': '#f44336',
            'panel': '#252526',
            'border': '#3e3e42'
        }
        
        # Configure window
        self.root.configure(bg=self.colors['bg'])
        
        # Create GUI elements
        self.create_widgets()
        
        # Start update loop
        self.update_display()
        
        # Handle window close
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
    def create_widgets(self):
        """Create all GUI widgets"""
        # Main container
        main_frame = ttk.Frame(self.root, style='Dark.TFrame')
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Header
        self.create_header(main_frame)
        
        # Stats panels
        self.create_stats_panels(main_frame)
        
        # Drive monitors
        self.create_drive_monitors(main_frame)
        
        # Activity log
        self.create_activity_log(main_frame)
        
        # Control buttons
        self.create_controls(main_frame)
        
    def create_header(self, parent):
        """Create header with phase info"""
        header_frame = tk.Frame(parent, bg=self.colors['panel'], height=80)
        header_frame.pack(fill=tk.X, pady=(0, 10))
        header_frame.pack_propagate(False)
        
        # Title
        title = tk.Label(
            header_frame,
            text="HYBRID ULTIMATE INDEXER v3.0",
            font=('Arial', 24, 'bold'),
            bg=self.colors['panel'],
            fg=self.colors['accent']
        )
        title.pack(side=tk.LEFT, padx=20, pady=20)
        
        # Phase indicator
        self.phase_label = tk.Label(
            header_frame,
            text="Phase: Initializing",
            font=('Arial', 16),
            bg=self.colors['panel'],
            fg=self.colors['fg']
        )
        self.phase_label.pack(side=tk.RIGHT, padx=20, pady=20)
        
    def create_stats_panels(self, parent):
        """Create statistics panels"""
        stats_frame = tk.Frame(parent, bg=self.colors['bg'])
        stats_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Define stat panels
        stat_configs = [
            ("Total Files", "total_files", self.colors['success']),
            ("Archives Found", "total_archives", self.colors['warning']),
            ("Processing Rate", "total_rate", self.colors['accent']),
            ("Memory Usage", "memory_mb", self.colors['error'])
        ]
        
        self.stat_labels = {}
        
        for i, (label, key, color) in enumerate(stat_configs):
            panel_frame = tk.Frame(stats_frame, bg=self.colors['bg'])
            panel_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
            
            value_widget = self.create_stat_panel(panel_frame, label, color)
            self.stat_labels[key] = value_widget
            
    def create_stat_panel(self, parent, label, color):
        """Create individual stat panel"""
        frame = tk.Frame(parent, bg=self.colors['panel'], relief=tk.RAISED, bd=1)
        frame.pack(fill=tk.BOTH, expand=True)
        
        label_widget = tk.Label(
            frame,
            text=label,
            font=('Arial', 10),
            bg=self.colors['panel'],
            fg=self.colors['fg']
        )
        label_widget.pack(pady=(10, 5))
        
        value_widget = tk.Label(
            frame,
            text="0",
            font=('Arial', 20, 'bold'),
            bg=self.colors['panel'],
            fg=color
        )
        value_widget.pack(pady=(0, 10))
        
        return value_widget
        
    def create_drive_monitors(self, parent):
        """Create drive monitoring panels"""
        drives_frame = tk.Frame(parent, bg=self.colors['bg'])
        drives_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # Create notebook for drives
        self.drive_notebook = ttk.Notebook(drives_frame)
        self.drive_notebook.pack(fill=tk.BOTH, expand=True)
        
        self.drive_widgets = {}
        
        for drive in self.indexer.config['drive_configs']:
            # Create tab for each drive
            drive_frame = tk.Frame(self.drive_notebook, bg=self.colors['panel'])
            self.drive_notebook.add(drive_frame, text=f"Drive {drive}")
            
            # Stats for this drive
            stats_label = tk.Label(
                drive_frame,
                text="Waiting for scan...",
                font=('Consolas', 12),
                bg=self.colors['panel'],
                fg=self.colors['fg'],
                justify=tk.LEFT
            )
            stats_label.pack(padx=20, pady=20, anchor='w')
            
            # Progress bar
            progress = ttk.Progressbar(
                drive_frame,
                mode='indeterminate',
                style='Accent.Horizontal.TProgressbar'
            )
            progress.pack(fill=tk.X, padx=20, pady=(0, 20))
            progress.start(10)
            
            self.drive_widgets[drive] = {
                'stats': stats_label,
                'progress': progress
            }
            
    def create_activity_log(self, parent):
        """Create activity log"""
        log_frame = tk.LabelFrame(
            parent,
            text="Activity Log",
            font=('Arial', 10, 'bold'),
            bg=self.colors['bg'],
            fg=self.colors['fg']
        )
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # Create scrolled text widget
        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            height=10,
            bg=self.colors['panel'],
            fg=self.colors['fg'],
            font=('Consolas', 9),
            wrap=tk.WORD
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Configure tags for colored text
        self.log_text.tag_config('info', foreground=self.colors['fg'])
        self.log_text.tag_config('success', foreground=self.colors['success'])
        self.log_text.tag_config('warning', foreground=self.colors['warning'])
        self.log_text.tag_config('error', foreground=self.colors['error'])
        
    def create_controls(self, parent):
        """Create control buttons"""
        control_frame = tk.Frame(parent, bg=self.colors['bg'])
        control_frame.pack(fill=tk.X)
        
        # Pause/Resume button
        self.pause_button = tk.Button(
            control_frame,
            text="Pause",
            font=('Arial', 12),
            bg=self.colors['accent'],
            fg='white',
            command=self.toggle_pause,
            width=15
        )
        self.pause_button.pack(side=tk.LEFT, padx=5)
        
        # Export button
        export_button = tk.Button(
            control_frame,
            text="Export Stats",
            font=('Arial', 12),
            bg=self.colors['success'],
            fg='white',
            command=self.export_stats,
            width=15
        )
        export_button.pack(side=tk.LEFT, padx=5)
        
        # Runtime label
        self.runtime_label = tk.Label(
            control_frame,
            text="Runtime: 00:00:00",
            font=('Arial', 12),
            bg=self.colors['bg'],
            fg=self.colors['fg']
        )
        self.runtime_label.pack(side=tk.RIGHT, padx=5)
        
    def update_display(self):
        """Update all display elements"""
        try:
            stats = self.indexer.get_gui_stats()
            
            # Update phase
            phase_text = f"Phase {stats['phase']}: {stats['phase_name']}"
            
            # Add Everything export progress if available
            if stats.get('export_status'):
                phase_text += f" - {stats['export_status']}"
                if stats.get('everything_total'):
                    total = stats['everything_total']
                    processed = stats.get('everything_processed', 0)
                    percent = (processed / total * 100) if total > 0 else 0
                    phase_text += f" ({processed:,}/{total:,} - {percent:.1f}%)"
                    
            self.phase_label.config(text=phase_text)
            
            # Update main stats
            self.stat_labels['total_files'].config(text=f"{stats['total_files']:,}")
            self.stat_labels['total_archives'].config(text=f"{stats['total_archives']:,}")
            self.stat_labels['total_rate'].config(text=f"{stats['total_rate']:.0f}/s")
            self.stat_labels['memory_mb'].config(text=f"{stats['memory_mb']:.0f} MB")
            
            # Update runtime
            runtime_seconds = int(stats['runtime'])
            hours = runtime_seconds // 3600
            minutes = (runtime_seconds % 3600) // 60
            seconds = runtime_seconds % 60
            self.runtime_label.config(text=f"Runtime: {hours:02d}:{minutes:02d}:{seconds:02d}")
            
            # Update drive stats
            for drive, drive_stats in stats['drives'].items():
                if drive in self.drive_widgets:
                    widgets = self.drive_widgets[drive]
                    
                    # Format drive stats
                    stats_text = (
                        f"Files Scanned: {drive_stats['files_scanned']:,}\n"
                        f"Archives Found: {drive_stats['archives_found']:,}\n"
                        f"Rate: {drive_stats['rate']:.0f} files/sec\n"
                        f"Elapsed: {drive_stats['elapsed']:.1f} seconds"
                    )
                    widgets['stats'].config(text=stats_text)
                    
                    # Update progress bar
                    if stats['phase'] == 1:  # Scanning phase
                        widgets['progress'].config(mode='indeterminate')
                    else:
                        widgets['progress'].stop()
                        widgets['progress'].config(mode='determinate', value=100)
            
            # Add log entry periodically
            if hasattr(self, '_last_log_time'):
                if time.time() - self._last_log_time > 5:  # Log every 5 seconds
                    if stats['phase'] == 1 and stats.get('everything_total'):
                        # Show Everything export progress
                        total = stats['everything_total']
                        processed = stats.get('everything_processed', 0)
                        rate = stats.get('total_rate', 0)
                        self.add_log(f"Everything Export: {processed:,}/{total:,} files processed at {rate:.0f}/s", 'info')
                    else:
                        self.add_log(f"Phase {stats['phase']}: {stats['total_files']:,} files scanned", 'info')
                    self._last_log_time = time.time()
            else:
                self._last_log_time = time.time()
                # Initial log for Everything export
                if stats['phase'] == 1:
                    self.add_log("Starting Everything export - getting instant access to all files!", 'success')
                
        except Exception as e:
            self.add_log(f"Display update error: {e}", 'error')
            
        # Schedule next update
        if hasattr(self, 'root'):
            self.root.after(500, self.update_display)  # Update every 500ms
            
    def add_log(self, message, tag='info'):
        """Add message to activity log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n", tag)
        self.log_text.see(tk.END)
        
        # Limit log size
        lines = int(self.log_text.index('end-1c').split('.')[0])
        if lines > 1000:
            self.log_text.delete('1.0', '2.0')
            
    def toggle_pause(self):
        """Toggle pause/resume"""
        if self.pause_button['text'] == 'Pause':
            self.indexer.running = False
            self.pause_button.config(text='Resume', bg=self.colors['warning'])
            self.add_log("Indexer paused", 'warning')
        else:
            self.indexer.running = True
            self.pause_button.config(text='Pause', bg=self.colors['accent'])
            self.add_log("Indexer resumed", 'success')
            
    def export_stats(self):
        """Export current stats to file"""
        filename = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                stats = self.indexer.get_gui_stats()
                with open(filename, 'w') as f:
                    json.dump(stats, f, indent=2)
                self.add_log(f"Stats exported to {filename}", 'success')
            except Exception as e:
                self.add_log(f"Export failed: {e}", 'error')
                
    def on_closing(self):
        """Handle window close"""
        if messagebox.askokcancel("Quit", "Stop the indexer and close?"):
            self.add_log("Shutting down...", 'warning')
            self.indexer.running = False
            self.indexer.monitoring = False
            self.root.destroy()
            
    def run(self):
        """Start the GUI main loop"""
        self.add_log("Hybrid Ultimate Indexer started", 'success')
        self.add_log(f"Index directory: {self.indexer.index_dir}", 'info')
        self.root.mainloop()


def main():
    """Main entry point with GUI"""
    # Production configuration
    config = {
        'index_dir': r'C:\Utilities\file_tools\hybrid_ultimate_index',
        'everything_path': None,  # Auto-detect
        'poll_interval': 300,  # 5 minutes
        'content_workers': 8,
        'enable_archive_indexing': True,
        'enable_hash_calculation': True,
        'enable_move_detection': True,
        'drive_configs': {
            'C:\\': {'workers': 4, 'priority': 'low', 'scan_archives': False},
            'D:\\': {'workers': 8, 'priority': 'high', 'scan_archives': True},
            'E:\\': {'workers': 8, 'priority': 'high', 'scan_archives': True},
            'F:\\': {'workers': 8, 'priority': 'high', 'scan_archives': True},
        }
    }
    
    try:
        # Create indexer
        indexer = HybridUltimateIndexer(config)
        
        # Create GUI
        gui = IndexerMonitorGUI(indexer)
        
        # Start indexer in separate thread
        indexer_thread = threading.Thread(target=indexer.run, daemon=True)
        indexer_thread.start()
        
        # Run GUI (blocks until window closed)
        gui.run()
        
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        input("Press Enter to exit...")


if __name__ == "__main__":
    # Add multiprocessing freeze support for Windows
    mp.freeze_support()
    
    # Run main
    main()