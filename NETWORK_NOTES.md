# Network Drive Scanning Notes

## Performance Reality Check

After testing, network drive scanning is MUCH slower than local:
- **Local drives:** 1,000-4,000 files/sec ✅
- **Network drives:** 200-500 files/sec ⚠️

## Time Requirements
- **11.6M local files:** 3 hours ✅
- **110M network files:** 70-100 hours ⚠️

## Optimizations in v2.0
- Progress every 1,000 files
- Checkpoints every 10,000 files  
- 3-second timeout per file
- Resume capability

## Recommendation
Use the indexer primarily for local files where it excels.
Network support is available but should be run on dedicated machines over multiple days.