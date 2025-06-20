#!/usr/bin/env python3
"""
Multi-Database Query Combiner
Loads CSV, queries Oracle/Postgres/MongoDB, combines results into new CSV
"""

import pandas as pd
import cx_Oracle
import psycopg2
from pymongo import MongoClient
import json
from typing import Dict, List, Any, Optional
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MultiDBQueryCombiner:
    def __init__(self):
        self.oracle_conn = None
        self.postgres_conn = None
        self.mongo_client = None
        self.input_data = None
        self.results = []
    
    def load_csv(self, file_path: str) -> pd.DataFrame:
        """Load input CSV file"""
        try:
            self.input_data = pd.read_csv(file_path)
            logger.info(f"Loaded {len(self.input_data)} rows from {file_path}")
            return self.input_data
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            raise
    
    def connect_oracle(self, username: str, password: str, dsn: str):
        """Connect to Oracle database"""
        try:
            self.oracle_conn = cx_Oracle.connect(username, password, dsn)
            logger.info("Connected to Oracle database")
        except Exception as e:
            logger.error(f"Oracle connection failed: {e}")
            raise
    
    def connect_postgres(self, host: str, database: str, username: str, password: str, port: int = 5432):
        """Connect to PostgreSQL database"""
        try:
            self.postgres_conn = psycopg2.connect(
                host=host, database=database, user=username, password=password, port=port
            )
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            raise
    
    def connect_mongodb(self, connection_string: str, database_name: str):
        """Connect to MongoDB"""
        try:
            self.mongo_client = MongoClient(connection_string)
            self.mongo_db = self.mongo_client[database_name]
            logger.info("Connected to MongoDB")
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            raise
    
    def query_oracle(self, query: str, params: Dict = None) -> List[Dict]:
        """Execute Oracle query and return results"""
        if not self.oracle_conn:
            logger.warning("Oracle not connected, skipping query")
            return []
        
        try:
            cursor = self.oracle_conn.cursor()
            cursor.execute(query, params or {})
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            logger.info(f"Oracle query returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"Oracle query failed: {e}")
            return []
    
    def query_postgres(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute PostgreSQL query and return results"""
        if not self.postgres_conn:
            logger.warning("PostgreSQL not connected, skipping query")
            return []
        
        try:
            cursor = self.postgres_conn.cursor()
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            logger.info(f"PostgreSQL query returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"PostgreSQL query failed: {e}")
            return []
    
    def query_mongodb(self, collection_name: str, query: Dict, projection: Dict = None) -> List[Dict]:
        """Execute MongoDB query and return results"""
        if not self.mongo_client:
            logger.warning("MongoDB not connected, skipping query")
            return []
        
        try:
            collection = self.mongo_db[collection_name]
            results = list(collection.find(query, projection))
            # Convert ObjectId to string for JSON serialization
            for result in results:
                if '_id' in result:
                    result['_id'] = str(result['_id'])
            logger.info(f"MongoDB query returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"MongoDB query failed: {e}")
            return []
    
    def combine_results(self, row_data: Dict, oracle_results: List[Dict] = None, 
                       postgres_results: List[Dict] = None, mongo_results: List[Dict] = None) -> Dict:
        """Combine results from all databases for a single row"""
        combined = dict(row_data)  # Start with original CSV row
        
        # Add Oracle results with prefix
        if oracle_results:
            for i, result in enumerate(oracle_results):
                for key, value in result.items():
                    combined[f"oracle_{key.lower()}_{i}"] = value
        
        # Add PostgreSQL results with prefix
        if postgres_results:
            for i, result in enumerate(postgres_results):
                for key, value in result.items():
                    combined[f"postgres_{key.lower()}_{i}"] = value
        
        # Add MongoDB results with prefix
        if mongo_results:
            for i, result in enumerate(mongo_results):
                for key, value in result.items():
                    combined[f"mongo_{key.lower()}_{i}"] = value
        
        return combined
    
    def process_data(self, oracle_query_template: str = None, 
                    postgres_query_template: str = None,
                    mongo_collection: str = None, mongo_query_template: Dict = None):
        """Process each row of input data through all databases"""
        if self.input_data is None:
            raise ValueError("No input data loaded. Call load_csv() first.")
        
        self.results = []
        
        for index, row in self.input_data.iterrows():
            logger.info(f"Processing row {index + 1}/{len(self.input_data)}")
            
            oracle_results = []
            postgres_results = []
            mongo_results = []
            
            # Execute Oracle query if template provided
            if oracle_query_template and self.oracle_conn:
                query = oracle_query_template.format(**row.to_dict())
                oracle_results = self.query_oracle(query)
            
            # Execute PostgreSQL query if template provided
            if postgres_query_template and self.postgres_conn:
                query = postgres_query_template.format(**row.to_dict())
                postgres_results = self.query_postgres(query)
            
            # Execute MongoDB query if template provided
            if mongo_query_template and mongo_collection and self.mongo_client:
                # Replace placeholders in mongo query
                query = {}
                for key, value in mongo_query_template.items():
                    if isinstance(value, str) and '{' in value:
                        query[key] = value.format(**row.to_dict())
                    else:
                        query[key] = value
                mongo_results = self.query_mongodb(mongo_collection, query)
            
            # Combine all results
            combined = self.combine_results(row.to_dict(), oracle_results, postgres_results, mongo_results)
            self.results.append(combined)
    
    def save_results(self, output_file: str):
        """Save combined results to CSV"""
        if not self.results:
            logger.warning("No results to save")
            return
        
        df = pd.DataFrame(self.results)
        df.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
    
    def close_connections(self):
        """Close all database connections"""
        if self.oracle_conn:
            self.oracle_conn.close()
            logger.info("Oracle connection closed")
        
        if self.postgres_conn:
            self.postgres_conn.close()
            logger.info("PostgreSQL connection closed")
        
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")

def main():
    """Example usage"""
    combiner = MultiDBQueryCombiner()
    
    try:
        # Load input CSV
        combiner.load_csv('input_data.csv')
        
        # Connect to databases (uncomment and configure as needed)
        # combiner.connect_oracle('username', 'password', 'localhost:1521/xe')
        # combiner.connect_postgres('localhost', 'mydb', 'username', 'password')
        # combiner.connect_mongodb('mongodb://localhost:27017/', 'mydb')
        
        # Define query templates (use {column_name} for CSV column substitution)
        oracle_query = "SELECT * FROM users WHERE user_id = '{user_id}'"
        postgres_query = "SELECT * FROM orders WHERE customer_id = '{customer_id}'"
        mongo_query = {"userId": "{user_id}"}
        
        # Process data
        combiner.process_data(
            oracle_query_template=oracle_query,
            postgres_query_template=postgres_query,
            mongo_collection="user_profiles",
            mongo_query_template=mongo_query
        )
        
        # Save results
        combiner.save_results('combined_results.csv')
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        combiner.close_connections()

if __name__ == "__main__":
    main()
