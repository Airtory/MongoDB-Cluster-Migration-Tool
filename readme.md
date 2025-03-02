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

### Handling Clusters

Here's an example URI for connecting to a MongoDB cluster with all the necessary settings:

```
mongodb://username:password@cluster0-shard-00-00.abcde.mongodb.net:27017,cluster0-shard-00-01.abcde.mongodb.net:27017,cluster0-shard-00-02.abcde.mongodb.net:27017/?ssl=true&replicaSet=atlas-123abc&authSource=admin
```

Let me break down the components:

1. **Protocol**: `mongodb://`
2. **Authentication**: `username:password@`
3. **Cluster nodes**: List of all replica set members, separated by commas
   - `cluster0-shard-00-00.abcde.mongodb.net:27017`
   - `cluster0-shard-00-01.abcde.mongodb.net:27017`
   - `cluster0-shard-00-02.abcde.mongodb.net:27017`
4. **Query parameters** (after the `?`):
   - `ssl=true` - Required for secure connections, especially with Atlas
   - `replicaSet=atlas-123abc` - The name of your replica set (find this in Atlas UI)
   - `authSource=admin` - Database used for authentication (typically "admin")

If you're using MongoDB Atlas, you can also use the SRV format, which is shorter:

```
mongodb+srv://username:password@cluster0.abcde.mongodb.net/?authSource=admin
```

The SRV format automatically handles finding all the servers in your cluster and enables SSL by default.

When running the migration script, you would use these URIs like:

```bash
python mongodb_migration.py \
  --source "mongodb://username:password@source-cluster-00-00.abcde.mongodb.net:27017,source-cluster-00-01.abcde.mongodb.net:27017,source-cluster-00-02.abcde.mongodb.net:27017/?ssl=true&replicaSet=atlas-abc123&authSource=admin" \
  --target "mongodb://username:password@target-cluster-00-00.xyzab.mongodb.net:27017,target-cluster-00-01.xyzab.mongodb.net:27017,target-cluster-00-02.xyzab.mongodb.net:27017/?ssl=true&replicaSet=atlas-xyz456&authSource=admin"
```