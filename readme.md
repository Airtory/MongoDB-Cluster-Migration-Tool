## MongoDB Cluster Migration Tool

This tool will help you seamlessly migrate data between MongoDB clusters while preserving exact document structure, indexes, and collection options.

### Quick Start Guide

1. **Install the required package:**
   ```bash
   pip install "pymongo[srv]>=4.0.0"
   ```

2. **For synchronization mode (updates only changed documents):**
   ```bash
   python mongodb_migration.py --source "mongodb://source-uri" --target "mongodb://target-uri"
   ```

3. **For full migration mode:**
   ```bash
   python mongodb_migration.py --source "mongodb://source-uri" --target "mongodb://target-uri" --full-migration
   ```

### Key Features

- **Preserves exact document structure** including fields with dots (no key modifications)
- **Synchronization mode** only updates changed documents
- **Full migration mode** copies all documents regardless of target state
- **Recreates all indexes** with their original options
- **Handles MongoDB Atlas clusters** and replica sets
- **Supports excluding** specific databases or collections
- **Detailed progress reporting** and statistics

The script will connect to both MongoDB clusters, identify all databases and collections to migrate, and then process each document while maintaining its exact structure. It will also recreate all indexes and collection options on the target.

For a complete documentation, installation options, and troubleshooting tips, please refer to the guide I've provided in the artifacts.

Would you like me to explain any specific part of the implementation or usage in more detail?