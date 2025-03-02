#!/usr/bin/env python3
"""
MongoDB Collection Migration Script

This script migrates all collections from a source MongoDB cluster to a target MongoDB cluster.
It preserves all documents, indexes, and collection options where possible.
Supports synchronization mode to only update changed documents.
Added features:
- Retry logic for handling timeouts and temporary errors
- History summary of all migration operations

Requirements:
- pymongo (pip install pymongo)
- dnspython (pip install dnspython)
"""

import argparse
import time
import json
import datetime
import os
import logging
from logging.handlers import RotatingFileHandler
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

# Configure logging
LOG_DIR = "logs"
HISTORY_DIR = "history"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Initialize logging
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
if not os.path.exists(HISTORY_DIR):
    os.makedirs(HISTORY_DIR)

log_file = os.path.join(LOG_DIR, f"mongodb_migration_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(log_file, maxBytes=10485760, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("mongodb_migration")


class MigrationHistory:
    """Class to track and store migration history"""
    
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.history = {
            "start_time": self.start_time.isoformat(),
            "end_time": None,
            "databases": {},
            "summary": {
                "total_databases": 0,
                "total_collections": 0,
                "total_documents_inserted": 0,
                "total_documents_updated": 0,
                "total_documents_unchanged": 0,
                "total_errors": 0,
                "total_retries": 0
            }
        }
        self.retry_counter = 0
    
    def add_database(self, db_name):
        """Initialize tracking for a database"""
        self.history["databases"][db_name] = {
            "collections": {},
            "summary": {
                "total_collections": 0,
                "documents_inserted": 0,
                "documents_updated": 0,
                "documents_unchanged": 0,
                "errors": 0,
                "retries": 0
            }
        }
        self.history["summary"]["total_databases"] += 1
    
    def add_collection(self, db_name, collection_name):
        """Initialize tracking for a collection"""
        if db_name not in self.history["databases"]:
            self.add_database(db_name)
            
        self.history["databases"][db_name]["collections"][collection_name] = {
            "start_time": datetime.datetime.now().isoformat(),
            "end_time": None,
            "documents_inserted": 0,
            "documents_updated": 0,
            "documents_unchanged": 0,
            "errors": 0,
            "retries": 0,
            "error_details": []
        }
        self.history["databases"][db_name]["summary"]["total_collections"] += 1
        self.history["summary"]["total_collections"] += 1
    
    def update_collection_stats(self, db_name, collection_name, stats):
        """Update collection statistics"""
        if db_name in self.history["databases"] and collection_name in self.history["databases"][db_name]["collections"]:
            coll_history = self.history["databases"][db_name]["collections"][collection_name]
            db_summary = self.history["databases"][db_name]["summary"]
            global_summary = self.history["summary"]
            
            # Update collection stats
            coll_history["documents_inserted"] += stats.get("inserted", 0)
            coll_history["documents_updated"] += stats.get("updated", 0)
            coll_history["documents_unchanged"] += stats.get("unchanged", 0)
            coll_history["errors"] += stats.get("errors", 0)
            coll_history["retries"] += stats.get("retries", 0)
            
            # Update database summary
            db_summary["documents_inserted"] += stats.get("inserted", 0)
            db_summary["documents_updated"] += stats.get("updated", 0)
            db_summary["documents_unchanged"] += stats.get("unchanged", 0)
            db_summary["errors"] += stats.get("errors", 0)
            db_summary["retries"] += stats.get("retries", 0)
            
            # Update global summary
            global_summary["total_documents_inserted"] += stats.get("inserted", 0)
            global_summary["total_documents_updated"] += stats.get("updated", 0)
            global_summary["total_documents_unchanged"] += stats.get("unchanged", 0)
            global_summary["total_errors"] += stats.get("errors", 0)
            global_summary["total_retries"] += stats.get("retries", 0)
    
    def add_error(self, db_name, collection_name, error_msg):
        """Add error details for a collection"""
        if db_name in self.history["databases"] and collection_name in self.history["databases"][db_name]["collections"]:
            self.history["databases"][db_name]["collections"][collection_name]["error_details"].append({
                "timestamp": datetime.datetime.now().isoformat(),
                "message": str(error_msg)
            })
    
    def complete_collection(self, db_name, collection_name):
        """Mark a collection as completed"""
        if db_name in self.history["databases"] and collection_name in self.history["databases"][db_name]["collections"]:
            self.history["databases"][db_name]["collections"][collection_name]["end_time"] = datetime.datetime.now().isoformat()
    
    def increment_retry(self, db_name=None, collection_name=None):
        """Increment retry counter"""
        self.retry_counter += 1
        
        if db_name and collection_name and db_name in self.history["databases"] and collection_name in self.history["databases"][db_name]["collections"]:
            self.history["databases"][db_name]["collections"][collection_name]["retries"] += 1
            self.history["databases"][db_name]["summary"]["retries"] += 1
            self.history["summary"]["total_retries"] += 1
    
    def save(self):
        """Save history to disk"""
        self.history["end_time"] = datetime.datetime.now().isoformat()
        
        # Calculate duration
        start = datetime.datetime.fromisoformat(self.history["start_time"])
        end = datetime.datetime.fromisoformat(self.history["end_time"])
        duration = end - start
        self.history["duration_seconds"] = duration.total_seconds()
        
        # Save to file
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        history_file = os.path.join(HISTORY_DIR, f"migration_history_{timestamp}.json")
        
        with open(history_file, 'w') as f:
            # Use json_util if available for better BSON type handling
            if json_util:
                f.write(json.dumps(self.history, default=json_util.default, indent=2))
            else:
                f.write(json.dumps(self.history, default=str, indent=2))
        
        logger.info(f"Migration history saved to {history_file}")
        return history_file
    
    def print_summary(self):
        """Print a summary of the migration"""
        summary = self.history["summary"]
        print("\n" + "=" * 50)
        print("MIGRATION HISTORY SUMMARY")
        print("=" * 50)
        print(f"Started: {self.history['start_time']}")
        print(f"Ended: {self.history['end_time']}")
        
        # Calculate duration
        if self.history["end_time"]:
            start = datetime.datetime.fromisoformat(self.history["start_time"])
            end = datetime.datetime.fromisoformat(self.history["end_time"])
            duration = end - start
            print(f"Duration: {duration}")
        
        print(f"\nDatabases processed: {summary['total_databases']}")
        print(f"Collections processed: {summary['total_collections']}")
        print(f"Documents inserted: {summary['total_documents_inserted']}")
        print(f"Documents updated: {summary['total_documents_updated']}")
        print(f"Documents unchanged: {summary['total_documents_unchanged']}")
        print(f"Total errors: {summary['total_errors']}")
        print(f"Total retry attempts: {summary['total_retries']}")
        
        # Print details for each database
        print("\nDatabase Details:")
        for db_name, db_info in self.history["databases"].items():
            db_summary = db_info["summary"]
            print(f"\n  {db_name}:")
            print(f"    Collections: {db_summary['total_collections']}")
            print(f"    Documents inserted: {db_summary['documents_inserted']}")
            print(f"    Documents updated: {db_summary['documents_updated']}")
            print(f"    Documents unchanged: {db_summary['documents_unchanged']}")
            print(f"    Errors: {db_summary['errors']}")
            print(f"    Retry attempts: {db_summary['retries']}")
        
        # Return the path to the saved history file
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(HISTORY_DIR, f"migration_history_{timestamp}.json")


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
        logger.info("Connection successful!")
        return client
    except errors.ConnectionFailure as e:
        logger.error(f"Failed to connect to MongoDB at {uri}: {e}")
        raise ConnectionError(f"Failed to connect to MongoDB at {uri}: {e}")
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
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
            logger.warning(f"Failed to create index {index['name']}: {e}")


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


def execute_with_retry(func, *args, db_name=None, collection_name=None, max_retries=MAX_RETRIES, **kwargs):
    """
    Execute a function with retry logic for handling transient errors
    
    Args:
        func: Function to execute
        *args: Positional arguments for the function
        db_name: Database name for tracking retries
        collection_name: Collection name for tracking retries
        max_retries: Maximum number of retry attempts
        **kwargs: Keyword arguments for the function
    
    Returns:
        Result of the function
    """
    retry_count = 0
    last_error = None
    
    while retry_count <= max_retries:
        try:
            return func(*args, **kwargs)
        except (errors.NetworkTimeout, errors.ConnectionFailure, errors.OperationFailure) as e:
            retry_count += 1
            last_error = e
            
            # Check if we should retry
            if retry_count <= max_retries:
                logger.warning(f"Retrying operation after error: {e}. Attempt {retry_count} of {max_retries}")
                
                # Track retry in history if history object is passed
                if 'history' in kwargs and kwargs['history']:
                    kwargs['history'].increment_retry(db_name, collection_name)
                
                # Wait before retrying
                time.sleep(RETRY_DELAY * retry_count)  # Exponential backoff
            else:
                logger.error(f"Maximum retry attempts reached. Last error: {e}")
                raise e
        except Exception as e:
            # Non-retryable error
            logger.error(f"Non-retryable error: {e}")
            raise e
    
    # If we get here, we've exceeded max retries
    raise last_error


def process_batch(batch, target_collection, stats):
    """
    Process a batch of write operations
    
    Args:
        batch: List of write operations
        target_collection: Target MongoDB collection
        stats: Statistics dictionary
    
    Returns:
        Updated statistics dictionary
    """
    if not batch:
        return stats
        
    try:
        target_collection.bulk_write(batch, ordered=False)
    except errors.BulkWriteError as bwe:
        # Count errors but continue processing
        write_errors = len(bwe.details.get('writeErrors', []))
        stats["errors"] += write_errors
        stats["inserted"] -= write_errors
        
        # Log details of first few errors
        for error in bwe.details.get('writeErrors', [])[:5]:  # Show first 5 errors
            error_msg = error.get('errmsg', 'Unknown error')
            logger.error(f"Error: {error_msg}")
    
    return stats


def migrate_collection(source_db: Database, target_db: Database, collection_name: str, 
                      batch_size: int = 1000, show_progress: bool = True, 
                      sync_mode: bool = True, history: MigrationHistory = None) -> Dict[str, int]:
    """
    Migrate a collection from source to target
    
    Args:
        source_db: Source MongoDB database
        target_db: Target MongoDB database
        collection_name: Name of the collection to migrate
        batch_size: Number of documents to process in each batch
        show_progress: Whether to print progress information
        sync_mode: Whether to perform a sync operation instead of full migration
        history: Migration history tracker
    
    Returns:
        Statistics dictionary
    """
    db_name = source_db.name
    
    # Initialize history tracking for this collection
    if history:
        history.add_collection(db_name, collection_name)
        
    source_collection = source_db[collection_name]
    total_docs = source_collection.count_documents({})
    
    if total_docs == 0:
        logger.info(f"Collection {collection_name} is empty, creating structure only")
    
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
        "retries": 0,
        "total_processed": 0
    }
    
    try:
        # Process documents in batches with cursor timeout handling
        cursor = None
        
        def setup_cursor():
            return source_collection.find({}, batch_size=batch_size, no_cursor_timeout=True)
        
        # Use retry logic for cursor setup
        cursor = execute_with_retry(
            setup_cursor, 
            db_name=db_name, 
            collection_name=collection_name,
            history=history
        )
        
        # Use bulk operations for better performance
        batch = []
        batch_size_counter = 0
        
        try:
            for doc in cursor:
                stats["total_processed"] += 1
                doc_id = doc.get("_id")
                
                if sync_mode:
                    # Check if document exists in target
                    existing_doc = None
                    try:
                        # Use retry logic for finding document
                        existing_doc = execute_with_retry(
                            target_collection.find_one,
                            {"_id": doc_id},
                            db_name=db_name,
                            collection_name=collection_name,
                            history=history
                        )
                    except Exception as e:
                        logger.error(f"Error finding document with ID {doc_id}: {e}")
                        if history:
                            history.add_error(db_name, collection_name, f"Error finding document with ID {doc_id}: {e}")
                    
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
                    batch.append(InsertOne(doc))
                    stats["inserted"] += 1
                
                batch_size_counter += 1
                
                # Execute batch when it reaches the specified size
                if batch_size_counter >= batch_size:
                    if batch:
                        # Use retry logic for batch processing
                        stats = execute_with_retry(
                            process_batch,
                            batch,
                            target_collection,
                            stats,
                            db_name=db_name,
                            collection_name=collection_name,
                            history=history
                        )
                    
                    # Reset batch
                    batch = []
                    batch_size_counter = 0
                    
                    # Update history with current stats
                    if history:
                        history.update_collection_stats(db_name, collection_name, stats)
                    
                    # Show progress
                    if show_progress and total_docs > 0:
                        progress = (stats["total_processed"] / total_docs) * 100
                        logger.info(f"{collection_name}: {stats['total_processed']}/{total_docs} "
                                  f"docs ({progress:.2f}%) - Inserted: {stats['inserted']}, "
                                  f"Updated: {stats['updated']}, Unchanged: {stats['unchanged']}, "
                                  f"Errors: {stats['errors']}, Retries: {stats['retries']}")
            
            # Process any remaining documents
            if batch:
                # Use retry logic for batch processing
                stats = execute_with_retry(
                    process_batch,
                    batch,
                    target_collection,
                    stats,
                    db_name=db_name,
                    collection_name=collection_name,
                    history=history
                )
        
        except errors.CursorNotFound as e:
            # Handle cursor timeout
            logger.warning(f"Cursor timed out: {e} - Reconnecting and continuing")
            if history:
                history.add_error(db_name, collection_name, f"Cursor timed out: {e}")
                stats["retries"] += 1
            
            # Continue from where we left off by skipping processed docs
            if stats["total_processed"] > 0:
                cursor = source_collection.find({}).skip(stats["total_processed"])
                # Continue processing...
    
    except Exception as e:
        logger.error(f"Unexpected error processing collection {collection_name}: {e}")
        if history:
            history.add_error(db_name, collection_name, f"Unexpected error: {e}")
        stats["errors"] += 1
    finally:
        if cursor:
            cursor.close()
    
    # Summary
    logger.info(f"Collection {collection_name} {'sync' if sync_mode else 'migration'} completed:")
    logger.info(f"  - {stats['inserted']} documents inserted")
    logger.info(f"  - {stats['updated']} documents updated")
    logger.info(f"  - {stats['unchanged']} documents unchanged")
    logger.info(f"  - {stats['errors']} documents failed")
    logger.info(f"  - {stats['retries']} retry attempts")
    
    # Create indexes after documents are inserted
    try:
        create_indexes(target_collection, collection_info.get("indexes", []))
    except Exception as e:
        logger.error(f"Error creating indexes for {collection_name}: {e}")
        if history:
            history.add_error(db_name, collection_name, f"Error creating indexes: {e}")
    
    # Mark collection as completed in history
    if history:
        history.update_collection_stats(db_name, collection_name, stats)
        history.complete_collection(db_name, collection_name)
    
    return stats


def migrate_database(source_client: MongoClient, target_client: MongoClient, 
                    db_name: str, excluded_collections: List[str] = None,
                    batch_size: int = 1000, sync_mode: bool = True,
                    history: MigrationHistory = None) -> Dict[str, dict]:
    """
    Migrate all collections in a database
    
    Args:
        source_client: Source MongoDB client
        target_client: Target MongoDB client
        db_name: Name of the database to migrate
        excluded_collections: List of collection names to exclude
        batch_size: Number of documents to process in each batch
        sync_mode: Whether to perform a sync operation
        history: Migration history tracker
    
    Returns:
        Dictionary with collection names and statistics
    """
    if excluded_collections is None:
        excluded_collections = []
    
    # Initialize history tracking for this database
    if history:
        history.add_database(db_name)
    
    source_db = source_client[db_name]
    target_db = target_client[db_name]
    
    collection_names = source_db.list_collection_names()
    collection_names = [name for name in collection_names if name not in excluded_collections]
    
    results = {}
    
    logger.info(f"Migrating database: {db_name} ({len(collection_names)} collections)")
    
    for collection_name in collection_names:
        try:
            logger.info(f"Starting {'sync' if sync_mode else 'migration'} of collection: {collection_name}")
            start_time = time.time()
            
            stats = migrate_collection(
                source_db, target_db, collection_name, 
                batch_size=batch_size, sync_mode=sync_mode,
                history=history
            )
            
            elapsed_time = time.time() - start_time
            logger.info(f"Completed {'sync' if sync_mode else 'migration'} of {collection_name} "
                      f"in {elapsed_time:.2f} seconds")
            
            results[collection_name] = stats
        except Exception as e:
            logger.error(f"Error processing collection {collection_name}: {e}")
            if history:
                history.add_error(db_name, collection_name, f"Fatal error: {e}")
            results[collection_name] = {"errors": 1, "inserted": 0, "updated": 0, "unchanged": 0, "total_processed": 0, "retries": 0}
    
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
        Nested dictionary with migration results and path to history file
    """
    if excluded_dbs is None:
        excluded_dbs = []
    
    if excluded_collections is None:
        excluded_collections = []
    
    # Initialize migration history tracker
    history = MigrationHistory()
    
    logger.info(f"Connecting to source MongoDB: {source_uri}")
    source_client = connect_to_mongodb(source_uri)
    
    logger.info(f"Connecting to target MongoDB: {target_uri}")
    target_client = connect_to_mongodb(target_uri)
    
    try:
        # Get all databases excluding system ones and user-specified exclusions
        db_names = get_all_databases(source_client, excluded_dbs)
        logger.info(f"Found {len(db_names)} databases to migrate: {', '.join(db_names)}")
        
        results = {}
        
        # Migrate each database
        for db_name in db_names:
            db_start_time = time.time()
            logger.info(f"\n{'=' * 50}")
            logger.info(f"Starting {'sync' if sync_mode else 'migration'} of database: {db_name}")
            
            db_results = migrate_database(
                source_client, target_client, db_name, 
                excluded_collections, batch_size, sync_mode,
                history=history
            )
            
            db_elapsed_time = time.time() - db_start_time
            logger.info(f"Completed {'sync' if sync_mode else 'migration'} of database {db_name} in {db_elapsed_time:.2f} seconds")
            
            results[db_name] = db_results
        
        # Save migration history
        history_file = history.save()
        
        # Add history file path to results
        results["_history_file"] = history_file
        
        return results, history_file
    
    finally:
        source_client.close()
        target_client.close()
        
        # Print summary from history
        history.print_summary()