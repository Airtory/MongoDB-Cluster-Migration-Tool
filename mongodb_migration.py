#!/usr/bin/env python3
"""
MongoDB Collection Migration Script

This script migrates all collections from a source MongoDB cluster to a target MongoDB cluster.
It preserves all documents, indexes, and collection options where possible.
Supports synchronization mode to only update changed documents.

Requirements:
- pymongo (pip install pymongo)
- dnspython (pip install dnspython)
"""

import argparse
import time
import json
try:
    from bson import json_util
except ImportError:
    # When bson is not available, define a basic json_util
    # This is less accurate but will work for basic documents
    json_util = None
    print("Warning: bson module not found. Using basic JSON comparison.")
    print("Install pymongo completely with: pip install pymongo[srv]")
from pymongo import MongoClient, errors, InsertOne, ReplaceOne
from pymongo.database import Database
from pymongo.collection import Collection
from typing import List, Dict, Any, Union, Optional


def connect_to_mongodb(uri: str) -> MongoClient:
    """
    Establish connection to a MongoDB cluster
    
    Args:
        uri: MongoDB connection string URI
    
    Returns:
        MongoClient instance
    """
    try:
        # Check if essential parameters are in the URI and add them if missing
        if "ssl=" not in uri.lower() and "tls=" not in uri.lower():
            uri += "&ssl=true" if "?" in uri else "?ssl=true"
            
        # Set longer timeouts for stability
        client = MongoClient(
            uri,
            connectTimeoutMS=30000,
            socketTimeoutMS=45000,
            serverSelectionTimeoutMS=30000,
            document_class=dict  # Ensure regular Python dicts are used
        )
        
        # Test connection by executing a simple command
        client.admin.command('ping')
        print("Connection successful!")
        return client
    except errors.ConnectionFailure as e:
        raise ConnectionError(f"Failed to connect to MongoDB at {uri}: {e}")
    except Exception as e:
        raise ConnectionError(f"Error connecting to MongoDB: {e}")


def get_all_databases(client: MongoClient, excluded_dbs: List[str] = None) -> List[str]:
    """
    Get all database names from a MongoDB cluster, excluding system databases
    
    Args:
        client: MongoDB client connection
        excluded_dbs: List of database names to exclude
    
    Returns:
        List of database names
    """
    if excluded_dbs is None:
        excluded_dbs = []
    
    # Default system databases to exclude
    system_dbs = ['admin', 'local', 'config']
    all_excluded = system_dbs + excluded_dbs
    
    db_names = client.list_database_names()
    return [db_name for db_name in db_names if db_name not in all_excluded]


def get_collection_info(db: Database, collection_name: str) -> Dict[str, Any]:
    """
    Get collection information including options and indexes
    
    Args:
        db: MongoDB database instance
        collection_name: Name of the collection
    
    Returns:
        Dictionary with collection details
    """
    collection_info = {}
    
    # Get collection options
    options_cmd = {"listCollections": 1, "filter": {"name": collection_name}}
    options_result = db.command(options_cmd)
    if options_result and "cursor" in options_result and "firstBatch" in options_result["cursor"]:
        first_batch = options_result["cursor"]["firstBatch"]
        if first_batch:
            collection_info["options"] = first_batch[0].get("options", {})
    
    # Get collection indexes
    collection = db[collection_name]
    collection_info["indexes"] = list(collection.list_indexes())
    
    return collection_info


def create_collection_with_options(target_db: Database, collection_name: str, options: Dict[str, Any]) -> Collection:
    """
    Create a collection with specific options
    
    Args:
        target_db: Target MongoDB database
        collection_name: Name of the collection to create
        options: Collection options
    
    Returns:
        Created collection
    """
    # Filter out options that can't be used in create_collection
    valid_options = {k: v for k, v in options.items() if k not in ['autoIndexId']}
    
    # Handle capped collections specially
    if 'capped' in valid_options and valid_options['capped']:
        if 'size' not in valid_options:
            valid_options['size'] = 536870912  # Default to 512MB if size not specified
    
    # Create the collection with options
    try:
        return target_db.create_collection(collection_name, **valid_options)
    except errors.CollectionInvalid:
        # Collection may already exist
        return target_db[collection_name]


def create_indexes(collection: Collection, indexes: List[Dict[str, Any]]) -> None:
    """
    Create indexes on a collection
    
    Args:
        collection: MongoDB collection
        indexes: List of index specifications
    """
    for index in indexes:
        # Extract index keys and options
        keys = index['key'].items()
        options = {k: v for k, v in index.items() if k != 'key' and k != 'v' and k != 'ns'}
        
        # Skip _id index as it's created automatically
        if len(keys) == 1 and list(keys)[0][0] == '_id':
            continue
            
        try:
            collection.create_index(keys, **options)
        except errors.OperationFailure as e:
            print(f"Warning: Failed to create index {index['name']}: {e}")


def document_to_json(doc):
    """
    Convert a BSON document to a JSON string using MongoDB Extended JSON format
    
    Args:
        doc: MongoDB document
    
    Returns:
        JSON string representation of the document
    """
    if json_util:
        return json.dumps(doc, default=json_util.default)
    else:
        # Basic JSON serialization when bson is not available
        # This will handle basic types but not special BSON types accurately
        class BasicJSONEncoder(json.JSONEncoder):
            def default(self, obj):
                # Handle ObjectId-like objects (convert to string)
                if hasattr(obj, '__str__'):
                    return str(obj)
                return super().default(obj)
                
        return json.dumps(doc, cls=BasicJSONEncoder)


def documents_are_equal(doc1, doc2):
    """
    Compare two MongoDB documents for equality by converting to JSON and comparing
    Excludes the _id field for comparison
    
    Args:
        doc1: First document
        doc2: Second document
    
    Returns:
        True if documents are equal, False otherwise
    """
    # Create copies to avoid modifying the originals
    doc1_copy = doc1.copy() if doc1 else {}
    doc2_copy = doc2.copy() if doc2 else {}
    
    # Remove _id fields for comparison
    if '_id' in doc1_copy:
        del doc1_copy['_id']
    if '_id' in doc2_copy:
        del doc2_copy['_id']
    
    # Convert to JSON strings and compare
    return document_to_json(doc1_copy) == document_to_json(doc2_copy)


def migrate_collection(source_db: Database, target_db: Database, collection_name: str, 
                      batch_size: int = 1000, show_progress: bool = True, 
                      sync_mode: bool = True) -> Dict[str, int]:
    """
    Migrate a collection from source to target
    
    Args:
        source_db: Source MongoDB database
        target_db: Target MongoDB database
        collection_name: Name of the collection to migrate
        batch_size: Number of documents to process in each batch
        show_progress: Whether to print progress information
        sync_mode: Whether to perform a sync operation instead of full migration
    
    Returns:
        Statistics dictionary
    """
    source_collection = source_db[collection_name]
    total_docs = source_collection.count_documents({})
    
    if total_docs == 0:
        print(f"Collection {collection_name} is empty, creating structure only")
    
    # Get collection information
    collection_info = get_collection_info(source_db, collection_name)
    
    # Create target collection with options
    target_collection = create_collection_with_options(
        target_db, collection_name, collection_info.get("options", {})
    )
    
    # Initialize counters for sync statistics
    stats = {
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
        "errors": 0,
        "total_processed": 0
    }
    
    # Process documents in batches for better performance
    cursor = source_collection.find({}, batch_size=batch_size, no_cursor_timeout=True)
    
    try:
        # Use bulk operations for better performance
        batch = []
        batch_size_counter = 0
        
        for doc in cursor:
            stats["total_processed"] += 1
            doc_id = doc.get("_id")
            
            if sync_mode:
                # Check if document exists in target
                existing_doc = None
                try:
                    existing_doc = target_collection.find_one({"_id": doc_id})
                except Exception as e:
                    print(f"Error finding document with ID {doc_id}: {e}")
                
                if existing_doc:
                    # Document exists, check if it needs updating
                    if not documents_are_equal(doc, existing_doc):
                        # Create a replace operation
                        batch.append(ReplaceOne({"_id": doc_id}, doc))
                        stats["updated"] += 1
                    else:
                        stats["unchanged"] += 1
                else:
                    # Document doesn't exist, insert it
                    batch.append(InsertOne(doc))
                    stats["inserted"] += 1
            else:
                # Full migration mode - just add inserts
                # (MongoDB will handle duplicate key errors)
                batch.append(InsertOne(doc))
                stats["inserted"] += 1
            
            batch_size_counter += 1
            
            # Execute batch when it reaches the specified size
            if batch_size_counter >= batch_size:
                if batch:
                    try:
                        target_collection.bulk_write(batch, ordered=False)
                    except errors.BulkWriteError as bwe:
                        # Count errors but continue processing
                        write_errors = len(bwe.details.get('writeErrors', []))
                        stats["errors"] += write_errors
                        stats["inserted"] -= write_errors
                        
                        for error in bwe.details.get('writeErrors', [])[:5]:  # Show first 5 errors
                            print(f"Error: {error.get('errmsg', 'Unknown error')}")
                
                # Reset batch
                batch = []
                batch_size_counter = 0
                
                # Show progress
                if show_progress and total_docs > 0:
                    progress = (stats["total_processed"] / total_docs) * 100
                    print(f"\r{collection_name}: {stats['total_processed']}/{total_docs} "
                          f"docs ({progress:.2f}%) - Inserted: {stats['inserted']}, "
                          f"Updated: {stats['updated']}, Unchanged: {stats['unchanged']}, "
                          f"Errors: {stats['errors']}", end="", flush=True)
        
        # Process any remaining documents
        if batch:
            try:
                target_collection.bulk_write(batch, ordered=False)
            except errors.BulkWriteError as bwe:
                # Count errors but continue processing
                write_errors = len(bwe.details.get('writeErrors', []))
                stats["errors"] += write_errors
                stats["inserted"] -= write_errors
                
                for error in bwe.details.get('writeErrors', [])[:5]:  # Show first 5 errors
                    print(f"Error: {error.get('errmsg', 'Unknown error')}")
    
    except Exception as e:
        print(f"\nUnexpected error processing collection {collection_name}: {e}")
        stats["errors"] += 1
    finally:
        cursor.close()
    
    if show_progress:
        print()  # New line after progress indicator
    
    # Summary
    print(f"Collection {collection_name} {'sync' if sync_mode else 'migration'} completed:")
    print(f"  - {stats['inserted']} documents inserted")
    print(f"  - {stats['updated']} documents updated")
    print(f"  - {stats['unchanged']} documents unchanged")
    print(f"  - {stats['errors']} documents failed")
    
    # Create indexes after documents are inserted
    create_indexes(target_collection, collection_info.get("indexes", []))
    
    return stats


def migrate_database(source_client: MongoClient, target_client: MongoClient, 
                    db_name: str, excluded_collections: List[str] = None,
                    batch_size: int = 1000, sync_mode: bool = True) -> Dict[str, dict]:
    """
    Migrate all collections in a database
    
    Args:
        source_client: Source MongoDB client
        target_client: Target MongoDB client
        db_name: Name of the database to migrate
        excluded_collections: List of collection names to exclude
        batch_size: Number of documents to process in each batch
        sync_mode: Whether to perform a sync operation
    
    Returns:
        Dictionary with collection names and statistics
    """
    if excluded_collections is None:
        excluded_collections = []
    
    source_db = source_client[db_name]
    target_db = target_client[db_name]
    
    collection_names = source_db.list_collection_names()
    collection_names = [name for name in collection_names if name not in excluded_collections]
    
    results = {}
    
    print(f"Migrating database: {db_name} ({len(collection_names)} collections)")
    
    for collection_name in collection_names:
        try:
            print(f"Starting {'sync' if sync_mode else 'migration'} of collection: {collection_name}")
            start_time = time.time()
            
            stats = migrate_collection(
                source_db, target_db, collection_name, batch_size=batch_size, sync_mode=sync_mode
            )
            
            elapsed_time = time.time() - start_time
            print(f"Completed {'sync' if sync_mode else 'migration'} of {collection_name} "
                  f"in {elapsed_time:.2f} seconds")
            
            results[collection_name] = stats
        except Exception as e:
            print(f"Error processing collection {collection_name}: {e}")
            results[collection_name] = {"errors": 1, "inserted": 0, "updated": 0, "unchanged": 0, "total_processed": 0}
    
    return results


def migrate_mongodb(source_uri: str, target_uri: str, 
                   excluded_dbs: List[str] = None,
                   excluded_collections: List[str] = None,
                   batch_size: int = 1000,
                   sync_mode: bool = True) -> Dict[str, Dict[str, dict]]:
    """
    Migrate all databases and collections from source to target MongoDB cluster
    
    Args:
        source_uri: Source MongoDB connection URI
        target_uri: Target MongoDB connection URI
        excluded_dbs: List of database names to exclude
        excluded_collections: List of collection names to exclude
        batch_size: Number of documents to process in each batch
        sync_mode: Whether to perform a sync operation
    
    Returns:
        Nested dictionary with migration results
    """
    if excluded_dbs is None:
        excluded_dbs = []
    
    if excluded_collections is None:
        excluded_collections = []
    
    print(f"Connecting to source MongoDB: {source_uri}")
    source_client = connect_to_mongodb(source_uri)
    
    print(f"Connecting to target MongoDB: {target_uri}")
    target_client = connect_to_mongodb(target_uri)
    
    try:
        # Get all databases excluding system ones and user-specified exclusions
        db_names = get_all_databases(source_client, excluded_dbs)
        print(f"Found {len(db_names)} databases to migrate: {', '.join(db_names)}")
        
        results = {}
        
        # Migrate each database
        for db_name in db_names:
            db_start_time = time.time()
            print(f"\n{'=' * 50}")
            print(f"Starting {'sync' if sync_mode else 'migration'} of database: {db_name}")
            
            db_results = migrate_database(
                source_client, target_client, db_name, 
                excluded_collections, batch_size, sync_mode
            )
            
            db_elapsed_time = time.time() - db_start_time
            print(f"Completed {'sync' if sync_mode else 'migration'} of database {db_name} in {db_elapsed_time:.2f} seconds")
            
            results[db_name] = db_results
        
        return results
    
    finally:
        source_client.close()
        target_client.close()


def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description='Migrate MongoDB collections between clusters')
    
    parser.add_argument('--source', required=True, help='Source MongoDB URI')
    parser.add_argument('--target', required=True, help='Target MongoDB URI')
    parser.add_argument('--exclude-dbs', nargs='+', default=[], 
                        help='Databases to exclude from migration')
    parser.add_argument('--exclude-collections', nargs='+', default=[],
                        help='Collections to exclude from migration')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Batch size for document migration')
    parser.add_argument('--full-migration', action='store_true',
                        help='Perform full migration instead of smart sync')
    
    args = parser.parse_args()
    
    try:
        start_time = time.time()
        sync_mode = not args.full_migration
        operation_type = "sync" if sync_mode else "full migration"
        print(f"Starting MongoDB {operation_type}")
        
        results = migrate_mongodb(
            args.source, args.target,
            excluded_dbs=args.exclude_dbs,
            excluded_collections=args.exclude_collections,
            batch_size=args.batch_size,
            sync_mode=sync_mode
        )
        
        # Print summary
        print("\n" + "=" * 50)
        print("Migration Summary:")
        
        total_collections = 0
        total_documents = 0
        
        for db_name, collections in results.items():
            db_inserted = sum(stats.get("inserted", 0) for stats in collections.values())
            db_updated = sum(stats.get("updated", 0) for stats in collections.values())
            db_unchanged = sum(stats.get("unchanged", 0) for stats in collections.values())
            db_errors = sum(stats.get("errors", 0) for stats in collections.values())
            db_collections = len(collections)
            
            print(f"Database {db_name}: {db_collections} collections - "
                  f"Inserted: {db_inserted}, Updated: {db_updated}, "
                  f"Unchanged: {db_unchanged}, Errors: {db_errors}")
            
            total_collections += db_collections
            total_documents += (db_inserted + db_updated)
        
        elapsed_time = time.time() - start_time
        operation_verb = "synchronized" if sync_mode else "migrated"
        print(f"\nTotal: {total_documents} documents {operation_verb} across {total_collections} collections "
              f"in {elapsed_time:.2f} seconds")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())