# Hybrid Ultimate Indexer v3.0

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![Files Indexed](https://img.shields.io/badge/Files%20Indexed-11.6M%20Local-green)](https://github.com/yourusername/hybrid-ultimate-indexer)
[![Performance](https://img.shields.io/badge/Speed-4000%2B%20files%2Fsec-orange)](https://github.com/yourusername/hybrid-ultimate-indexer)
[![License](https://img.shields.io/badge/License-MIT-purple)](LICENSE)

**The fastest file indexer for Windows** - Index millions of files in minutes with a beautiful GUI!

## Features

- **Lightning Fast**: 1,000-4,000+ files/second
- **Desktop GUI**: Real-time progress monitoring with Tkinter
- **Compressed Storage**: LZ4 compression saves space
- **Multi-Drive Support**: Index multiple drives simultaneously
- **Smart Deduplication**: No duplicate entries
- **Live Statistics**: Watch indexing in real-time
- **Instant Search**: Query millions of files instantly

## Screenshot

![Hybrid Ultimate Indexer GUI](screenshot.png)

*Indexing 11.6 million files across 4 drives with live statistics*

## Quick Start

```bash
# Clone the repository
git clone https://github.com/yourusername/hybrid-ultimate-indexer.git

# Install requirements
pip install -r requirements.txt

# Run the indexer
python hybrid_ultimate_indexer.py
```

## Real-World Performance

Tested on actual system with 11.6 million files:

| Metric | Value |
|--------|-------|
| Files Indexed | 11,591,794 |
| Average Speed | 1,115 files/sec |
| Peak Speed | 4,379 files/sec |
| Memory Usage | 60 MB |
| Index Size | 708 MB (compressed) |
| Drives Indexed | C: (4.8M), D: (30K), E: (878K), F: (5.8M) |

## System Requirements

- Python 3.8 or higher
- Windows 10/11
- 4GB RAM minimum
- 1GB free disk space for index
- Optional: Everything.exe for enhanced performance

## Installation

### Option 1: Standalone GUI
```bash
python hybrid_ultimate_indexer.py
```

### Option 2: With Everything.exe Integration
The indexer can leverage Everything.exe for even faster initial scanning:
1. Install [Everything](https://www.voidtools.com/)
2. Run the hybrid indexer
3. Enjoy combined performance benefits

## Usage

### GUI Controls
- **Start/Pause**: Begin or pause indexing
- **Drive Tabs**: Monitor each drive separately
- **Progress Bar**: Visual feedback on indexing progress
- **Activity Log**: Real-time indexing details
- **Export Stats**: Save performance metrics

### Programmatic Usage
```python
import lz4.frame
import os

def search_indexed_files(query, index_dir=r"C:\Utilities\file_tools\hybrid_ultimate_index"):
    '''Search through the compressed index'''
    results = []
    
    for file in os.listdir(index_dir):
        if file.endswith('.lz4'):
            filepath = os.path.join(index_dir, file)
            with lz4.frame.open(filepath, 'rt', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if query.lower() in line.lower():
                        results.append(line.strip())
    return results

# Find all Python files
python_files = search_indexed_files('.py')
print(f"Found {len(python_files)} Python files")
```

## Comparison

### vs Everything.exe
[+] Built-in GUI with live statistics  
[+] Compressed storage (saves 50%+ space)  
[+] Native Python integration  
[+] No SDK/DLL configuration  
[+] Progress tracking  

### vs Windows Search
[+] 100x faster indexing  
[+] 90% less memory usage  
[+] No system slowdown  
[+] Instant results  
[+] Customizable  

## Advanced Features

- **Multi-Processing**: Utilizes all CPU cores efficiently
- **Incremental Updates**: Only re-index changed files
- **Archive Support**: Can peek inside ZIP files
- **Real-time Monitoring**: Watch files being indexed live

## 🌐 Network Drive Support (Beta)

**Experimental: Network drive scanning for enterprise environments**

### Current Capabilities:
- ✅ **11.6 million local files** - Production ready, blazing fast!
- 🔄 **110 million network files** - Beta feature, requires 70-100 hours

### Realistic Performance:
| Environment | Files | Speed | Time |
|------------|-------|-------|------|
| **Local Drives** | 11.6M | 1,000-4,000 files/sec | 3 hours |
| **Network Drives** | 110M | 200-500 files/sec | 70-100 hours |

### Network Features (v2.0):
- Progress updates every 1,000 files
- Checkpoint saves every 10,000 files
- 3-second timeout per file
- Resume from checkpoints
- Test mode for validation

**Note:** Network performance depends heavily on network speed and latency.
Local indexing remains the primary use case with exceptional performance.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Development Setup
```bash
git clone https://github.com/yourusername/hybrid-ultimate-indexer.git
cd hybrid-ultimate-indexer
pip install -r requirements-dev.txt
python -m pytest tests/
```

## License

MIT License - see [LICENSE](LICENSE) file for details

## Acknowledgments

- Built with the philosophy: "Fuck it, we'll make it work!"
- Inspired by the need to search through 11.6 million files instantly
- Thanks to the Python community for amazing libraries

## Links

- [Download Latest Release](https://github.com/yourusername/hybrid-ultimate-indexer/releases)
- [Report Issues](https://github.com/yourusername/hybrid-ultimate-indexer/issues)
- [Wiki & Documentation](https://github.com/yourusername/hybrid-ultimate-indexer/wiki)

---

**Star this repo if you find it useful!**

**From concept to indexing 12 million files in one day!**
