/**
 * Query Result Store Service
 * Manages storing and retrieving query results using PostgreSQL with support for caching
 */
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
const logger = require('../utils/logger');
const config = require('../config');
const LRUCache = require('lru-cache');

class QueryResultStore {
  constructor(options = {}) {
    // Default configuration
    this.config = {
      host: process.env.PGHOST || 'localhost',
      port: parseInt(process.env.PGPORT || '5432', 10),
      database: process.env.PGDATABASE || 'query_manager',
      user: process.env.PGUSER || 'postgres',
      password: process.env.PGPASSWORD || 'postgres',
      connectionString: process.env.DATABASE_URL,
      tableName: 'query_results',
      resultsCacheSize: 100,   // Number of result sets to cache in memory
      resultsTTL: 3600000,     // Default TTL for cached results (1 hour)
      compressLargeResults: true, // Compress large result sets
      compressionThreshold: 1024 * 100, // 100KB
      ...options
    };
    
    // Initialize LRU cache for result sets
    this.resultsCache = new LRUCache({
      max: this.config.resultsCacheSize,
      ttl: this.config.resultsTTL,
      updateAgeOnGet: true,
      allowStale: true
    });
    
    this.pool = null;
    this.initialized = false;
    this.compressionEnabled = true;
  }
  
  /**
   * Initialize the result store
   */
  async initialize() {
    if (this.initialized) {
      logger.debug('Query result store already initialized');
      return;
    }
    
    try {
      // Create the connection pool
      this.pool = new Pool({
        connectionString: this.config.connectionString,
        host: this.config.host,
        port: this.config.port,
        database: this.config.database,
        user: this.config.user,
        password: this.config.password,
        max: 20, // Maximum number of clients in the pool
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 2000,
      });
      
      // Test the connection
      const client = await this.pool.connect();
      client.release();
      
      // Create tables if they don't exist
      await this._createTables();
      
      // Check if compression is available
      try {
        const zlib = require('zlib');
        this.compressionEnabled = this.config.compressLargeResults;
        logger.debug('Compression for large result sets is enabled');
      } catch (error) {
        this.compressionEnabled = false;
        logger.warn('Compression for large result sets is disabled: zlib not available');
      }
      
      this.initialized = true;
      logger.info(`Query result store initialized (PostgreSQL)`);
    } catch (error) {
      logger.error('Failed to initialize query result store', error);
      throw new Error(`Query result store initialization failed: ${error.message}`);
    }
  }
  
  /**
   * Create the database tables
   * @private
   */
  async _createTables() {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      
      // Query results table
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${this.config.tableName} (
          id TEXT PRIMARY KEY,
          query_id TEXT,
          execution_id TEXT,
          result_type TEXT NOT NULL,
          row_count INTEGER,
          storage_type TEXT NOT NULL,
          data JSONB,
          data_large BYTEA,
          is_compressed BOOLEAN DEFAULT FALSE,
          metadata JSONB,
          tags TEXT[],
          ttl TIMESTAMP WITH TIME ZONE,
          created_at TIMESTAMP WITH TIME ZONE NOT NULL,
          updated_at TIMESTAMP WITH TIME ZONE NOT NULL
        )
      `);
      
      // Index on query_id and execution_id
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_${this.config.tableName}_query_id ON ${this.config.tableName} (query_id)
      `);
      
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_${this.config.tableName}_execution_id ON ${this.config.tableName} (execution_id)
      `);
      
      // Index on tags for efficient tag-based searches
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_${this.config.tableName}_tags ON ${this.config.tableName} USING GIN (tags)
      `);
      
      // TTL index for cleanup
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_${this.config.tableName}_ttl ON ${this.config.tableName} (ttl)
      `);
      
      await client.query('COMMIT');
      logger.debug('Result store tables created or verified');
    } catch (error) {
      await client.query('ROLLBACK');
      logger.error('Error creating result store tables', error);
      throw error;
    } finally {
      client.release();
    }
  }
  
  /**
   * Close the database connection pool
   */
  async close() {
    if (this.pool) {
      await this.pool.end();
      this.initialized = false;
      this.pool = null;
      logger.info('Query result store closed');
    }
  }
  
  /**
   * Store query results
   * @param {Object} resultData - Result data to store
   * @returns {Promise<Object>} Stored result info
   */
  async storeResult(resultData) {
    await this._ensureInitialized();
    
    try {
      const id = resultData.id || uuidv4();
      const now = new Date().toISOString();
      const resultType = resultData.resultType || 'single';
      
      // Calculate TTL if provided
      let ttl = null;
      if (resultData.ttlSeconds) {
        ttl = new Date(Date.now() + (resultData.ttlSeconds * 1000)).toISOString();
      }
      
      // Determine if we need to compress the data
      const dataString = JSON.stringify(resultData.data || []);
      const isLarge = dataString.length > this.config.compressionThreshold;
      const shouldCompress = isLarge && this.compressionEnabled;
      
      // Prepare storage format
      let storageType, data, dataLarge = null, isCompressed = false;
      
      if (isLarge) {
        storageType = 'large';
        if (shouldCompress) {
          // Compress the data
          const zlib = require('zlib');
          dataLarge = zlib.gzipSync(Buffer.from(dataString));
          isCompressed = true;
          
          logger.debug(`Compressed large result set (${dataString.length} bytes -> ${dataLarge.length} bytes)`);
        } else {
          // Store as binary but uncompressed
          dataLarge = Buffer.from(dataString);
        }
        data = null;
      } else {
        storageType = 'json';
        data = resultData.data || [];
        dataLarge = null;
      }
      
      // Execute the database insert
      const result = await this.pool.query(`
        INSERT INTO ${this.config.tableName} (
          id, query_id, execution_id, result_type, row_count, 
          storage_type, data, data_large, is_compressed, 
          metadata, tags, ttl, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        RETURNING id, query_id, execution_id, result_type, row_count, storage_type, 
                  is_compressed, metadata, tags, ttl, created_at
      `, [
        id,
        resultData.queryId,
        resultData.executionId,
        resultType,
        resultData.data ? resultData.data.length : 0,
        storageType,
        storageType === 'json' ? JSON.stringify(data) : null,
        storageType === 'large' ? dataLarge : null,
        isCompressed,
        JSON.stringify(resultData.metadata || {}),
        resultData.tags || [],
        ttl,
        now,
        now
      ]);
      
      const storedResult = result.rows[0];
      
      // Cache the result in memory (store original data)
      this.resultsCache.set(id, {
        id,
        queryId: resultData.queryId,
        executionId: resultData.executionId,
        data: resultData.data,
        metadata: resultData.metadata,
        resultType,
        rowCount: resultData.data ? resultData.data.length : 0,
        createdAt: now
      });
      
      logger.info(`Stored query result: ${id}`, {
        queryId: resultData.queryId,
        executionId: resultData.executionId,
        resultType,
        rowCount: resultData.data ? resultData.data.length : 0,
        storageType,
        isCompressed,
        size: storageType === 'large' ? dataLarge.length : (dataString.length)
      });
      
      return {
        id,
        queryId: resultData.queryId,
        executionId: resultData.executionId,
        resultType,
        rowCount: resultData.data ? resultData.data.length : 0,
        metadata: resultData.metadata,
        tags: resultData.tags,
        ttl,
        createdAt: now
      };
    } catch (error) {
      logger.error('Error storing query result', error);
      throw new Error(`Failed to store query result: ${error.message}`);
    }
  }
  
  /**
   * Store multiple query results as a joined or cascaded result
   * @param {Object} resultsData - Combined results data
   * @returns {Promise<Object>} Stored result info
   */
  async storeMultiQueryResult(resultsData) {
    // Validate input
    if (!resultsData.results || !Array.isArray(resultsData.results)) {
      throw new Error('Invalid results data: missing results array');
    }
    
    // Prepare combined data
    const combinedData = {
      id: resultsData.id || uuidv4(),
      resultType: resultsData.resultType || 'multi',
      data: resultsData.data,
      metadata: {
        ...resultsData.metadata,
        resultSources: resultsData.results.map(result => ({
          queryId: result.queryId,
          executionId: result.executionId,
          rowCount: result.rowCount || 0
        }))
      },
      tags: resultsData.tags || [],
      ttlSeconds: resultsData.ttlSeconds
    };
    
    // Store the combined result
    return this.storeResult(combinedData);
  }
  
  /**
   * Get a result by ID
   * @param {string} id - Result ID
   * @returns {Promise<Object>} Result data
   */
  async getResultById(id) {
    await this._ensureInitialized();
    
    // Check if the result is in cache
    const cachedResult = this.resultsCache.get(id);
    if (cachedResult) {
      logger.debug(`Serving result ${id} from cache`);
      return {
        ...cachedResult,
        fromCache: true
      };
    }
    
    try {
      // Fetch from database
      const result = await this.pool.query(`
        SELECT id, query_id, execution_id, result_type, row_count, 
               storage_type, data, data_large, is_compressed, 
               metadata, tags, ttl, created_at, updated_at
        FROM ${this.config.tableName}
        WHERE id = $1
      `, [id]);
      
      if (result.rows.length === 0) {
        throw new Error(`Result with ID ${id} not found`);
      }
      
      const resultData = result.rows[0];
      
      // Parse the data based on storage type
      let parsedData;
      if (resultData.storage_type === 'json') {
        // Small result stored as JSON
        parsedData = resultData.data;
      } else if (resultData.storage_type === 'large') {
        // Large result potentially compressed
        if (resultData.is_compressed) {
          const zlib = require('zlib');
          const decompressed = zlib.gunzipSync(resultData.data_large);
          parsedData = JSON.parse(decompressed.toString());
        } else {
          parsedData = JSON.parse(resultData.data_large.toString());
        }
      }
      
      // Parse metadata
      const metadata = resultData.metadata;
      
      // Update cache
      const formattedResult = {
        id: resultData.id,
        queryId: resultData.query_id,
        executionId: resultData.execution_id,
        resultType: resultData.result_type,
        rowCount: resultData.row_count,
        data: parsedData,
        metadata,
        tags: resultData.tags,
        ttl: resultData.ttl,
        createdAt: resultData.created_at,
        updatedAt: resultData.updated_at
      };
      
      this.resultsCache.set(id, formattedResult);
      
      logger.info(`Retrieved result: ${id}`, {
        queryId: resultData.query_id,
        rowCount: resultData.row_count,
        fromDatabase: true
      });
      
      return formattedResult;
    } catch (error) {
      logger.error(`Error getting result by ID: ${id}`, error);
      throw error;
    }
  }
  
  /**
   * Get results by query ID
   * @param {string} queryId - Query ID
   * @returns {Promise<Array>} Result metadata (without full data)
   */
  async getResultsByQueryId(queryId) {
    await this._ensureInitialized();
    
    try {
      const result = await this.pool.query(`
        SELECT id, query_id, execution_id, result_type, row_count, 
               metadata, tags, ttl, created_at
        FROM ${this.config.tableName}
        WHERE query_id = $1
        ORDER BY created_at DESC
      `, [queryId]);
      
      return result.rows.map(row => ({
        id: row.id,
        queryId: row.query_id,
        executionId: row.execution_id,
        resultType: row.result_type,
        rowCount: row.row_count,
        metadata: row.metadata,
        tags: row.tags,
        ttl: row.ttl,
        createdAt: row.created_at
      }));
    } catch (error) {
      logger.error(`Error getting results by query ID: ${queryId}`, error);
      throw error;
    }
  }
  
  /**
   * Get results by execution ID
   * @param {string} executionId - Execution ID
   * @returns {Promise<Array>} Result metadata (without full data)
   */
  async getResultsByExecutionId(executionId) {
    await this._ensureInitialized();
    
    try {
      const result = await this.pool.query(`
        SELECT id, query_id, execution_id, result_type, row_count, 
               metadata, tags, ttl, created_at
        FROM ${this.config.tableName}
        WHERE execution_id = $1
        ORDER BY created_at DESC
      `, [executionId]);
      
      return result.rows.map(row => ({
        id: row.id,
        queryId: row.query_id,
        executionId: row.execution_id,
        resultType: row.result_type,
        rowCount: row.row_count,
        metadata: row.metadata,
        tags: row.tags,
        ttl: row.ttl,
        createdAt: row.created_at
      }));
    } catch (error) {
      logger.error(`Error getting results by execution ID: ${executionId}`, error);
      throw error;
    }
  }
  
  /**
   * Get results by tags (exact match)
   * @param {Array<string>} tags - Array of tags to match
   * @param {boolean} matchAll - If true, all tags must match
   * @returns {Promise<Array>} Result metadata (without full data)
   */
  async getResultsByTags(tags, matchAll = true) {
    await this._ensureInitialized();
    
    try {
      let query, params;
      
      if (matchAll) {
        // All tags must be present
        query = `
          SELECT id, query_id, execution_id, result_type, row_count, 
                 metadata, tags, ttl, created_at
          FROM ${this.config.tableName}
          WHERE tags @> $1
          ORDER BY created_at DESC
        `;
        params = [tags];
      } else {
        // Any tag must be present
        query = `
          SELECT id, query_id, execution_id, result_type, row_count, 
                 metadata, tags, ttl, created_at
          FROM ${this.config.tableName}
          WHERE tags && $1
          ORDER BY created_at DESC
        `;
        params = [tags];
      }
      
      const result = await this.pool.query(query, params);
      
      return result.rows.map(row => ({
        id: row.id,
        queryId: row.query_id,
        executionId: row.execution_id,
        resultType: row.result_type,
        rowCount: row.row_count,
        metadata: row.metadata,
        tags: row.tags,
        ttl: row.ttl,
        createdAt: row.created_at
      }));
    } catch (error) {
      logger.error(`Error getting results by tags`, error);
      throw error;
    }
  }
  
  /**
   * Update result metadata and tags
   * @param {string} id - Result ID
   * @param {Object} updateData - Data to update
   * @returns {Promise<Object>} Updated result metadata
   */
  async updateResultMetadata(id, updateData) {
    await this._ensureInitialized();
    
    try {
      // First get the current result
      const currentResult = await this.getResultById(id);
      if (!currentResult) {
        throw new Error(`Result with ID ${id} not found`);
      }
      
      // Prepare update data
      const updates = [];
      const values = [];
      let paramIndex = 1;
      
      // Tags
      if ('tags' in updateData) {
        updates.push(`tags = $${paramIndex++}`);
        values.push(updateData.tags);
      }
      
      // TTL
      if ('ttlSeconds' in updateData) {
        const ttl = updateData.ttlSeconds ? 
          new Date(Date.now() + (updateData.ttlSeconds * 1000)).toISOString() : null;
        
        updates.push(`ttl = $${paramIndex++}`);
        values.push(ttl);
      }
      
      // Metadata - merge with existing
      if ('metadata' in updateData) {
        const mergedMetadata = {
          ...currentResult.metadata,
          ...updateData.metadata
        };
        
        updates.push(`metadata = $${paramIndex++}`);
        values.push(JSON.stringify(mergedMetadata));
      }
      
      // Updated timestamp
      const now = new Date().toISOString();
      updates.push(`updated_at = $${paramIndex++}`);
      values.push(now);
      
      // Execute update if there's anything to update
      if (updates.length > 0) {
        values.push(id);  // For WHERE clause
        
        const result = await this.pool.query(`
          UPDATE ${this.config.tableName}
          SET ${updates.join(', ')}
          WHERE id = $${paramIndex}
          RETURNING id, query_id, execution_id, result_type, row_count, 
                    metadata, tags, ttl, created_at, updated_at
        `, values);
        
        const updatedResult = result.rows[0];
        
        // Update cache
        const cachedResult = this.resultsCache.get(id);
        if (cachedResult) {
          this.resultsCache.set(id, {
            ...cachedResult,
            metadata: updatedResult.metadata,
            tags: updatedResult.tags,
            ttl: updatedResult.ttl,
            updatedAt: updatedResult.updated_at
          });
        }
        
        logger.debug(`Updated result metadata: ${id}`);
        
        return {
          id: updatedResult.id,
          queryId: updatedResult.query_id,
          executionId: updatedResult.execution_id,
          resultType: updatedResult.result_type,
          rowCount: updatedResult.row_count,
          metadata: updatedResult.metadata,
          tags: updatedResult.tags,
          ttl: updatedResult.ttl,
          createdAt: updatedResult.created_at,
          updatedAt: updatedResult.updated_at
        };
      }
      
      // No updates were made
      return {
        id: currentResult.id,
        queryId: currentResult.queryId,
        executionId: currentResult.executionId,
        resultType: currentResult.resultType,
        rowCount: currentResult.rowCount,
        metadata: currentResult.metadata,
        tags: currentResult.tags,
        ttl: currentResult.ttl,
        createdAt: currentResult.createdAt,
        updatedAt: currentResult.updatedAt || currentResult.createdAt
      };
    } catch (error) {
      logger.error(`Error updating result metadata: ${id}`, error);
      throw error;
    }
  }
  
  /**
   * Delete a result by ID
   * @param {string} id - Result ID
   * @returns {Promise<boolean>} Success indicator
   */
  async deleteResult(id) {
    await this._ensureInitialized();
    
    try {
      // Remove from database
      await this.pool.query(`
        DELETE FROM ${this.config.tableName}
        WHERE id = $1
      `, [id]);
      
      // Remove from cache
      this.resultsCache.delete(id);
      
      logger.info(`Deleted result: ${id}`);
      return true;
    } catch (error) {
      logger.error(`Error deleting result: ${id}`, error);
      throw error;
    }
  }
  
  /**
   * Clean up expired results
   * @returns {Promise<number>} Number of deleted results
   */
  async cleanupExpiredResults() {
    await this._ensureInitialized();
    
    try {
      const now = new Date().toISOString();
      
      // Delete expired results
      const result = await this.pool.query(`
        DELETE FROM ${this.config.tableName}
        WHERE ttl IS NOT NULL AND ttl < $1
        RETURNING id
      `, [now]);
      
      const deletedIds = result.rows.map(row => row.id);
      
      // Remove from cache
      deletedIds.forEach(id => this.resultsCache.delete(id));
      
      logger.info(`Cleaned up ${deletedIds.length} expired results`);
      return deletedIds.length;
    } catch (error) {
      logger.error('Error cleaning up expired results', error);
      throw error;
    }
  }
  
  /**
   * Get summary statistics about stored results
   * @returns {Promise<Object>} Statistics about stored results
   */
  async getStatistics() {
    await this._ensureInitialized();
    
    try {
      // Get count of total results
      const totalResult = await this.pool.query(`
        SELECT COUNT(*) as total FROM ${this.config.tableName}
      `);
      
      // Get count by result type
      const typeResult = await this.pool.query(`
        SELECT result_type, COUNT(*) as count
        FROM ${this.config.tableName}
        GROUP BY result_type
      `);
      
      // Get count by storage type
      const storageResult = await this.pool.query(`
        SELECT storage_type, COUNT(*) as count
        FROM ${this.config.tableName}
        GROUP BY storage_type
      `);
      
      // Get count of compressed results
      const compressedResult = await this.pool.query(`
        SELECT COUNT(*) as count
        FROM ${this.config.tableName}
        WHERE is_compressed = true
      `);
      
      // Get size statistics (approximate)
      const sizeResult = await this.pool.query(`
        SELECT 
          SUM(pg_column_size(data)) as json_size,
          SUM(pg_column_size(data_large)) as large_size
        FROM ${this.config.tableName}
      `);
      
      // Get cache statistics
      const cacheStats = {
        size: this.resultsCache.size,
        maxSize: this.config.resultsCacheSize,
        hitRate: this.resultsCache.getRatio(),
        itemCount: this.resultsCache.size
      };
      
      return {
        totalResults: parseInt(totalResult.rows[0].total, 10),
        byResultType: typeResult.rows.reduce((acc, row) => {
          acc[row.result_type] = parseInt(row.count, 10);
          return acc;
        }, {}),
        byStorageType: storageResult.rows.reduce((acc, row) => {
          acc[row.storage_type] = parseInt(row.count, 10);
          return acc;
        }, {}),
        compressedResults: parseInt(compressedResult.rows[0].count, 10),
        approximateDataSizeBytes: {
          json: parseInt(sizeResult.rows[0].json_size || 0, 10),
          large: parseInt(sizeResult.rows[0].large_size || 0, 10),
          total: parseInt((sizeResult.rows[0].json_size || 0), 10) + 
                parseInt((sizeResult.rows[0].large_size || 0), 10)
        },
        cache: cacheStats,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      logger.error('Error getting result store statistics', error);
      throw error;
    }
  }
  
  /**
   * Ensure the store is initialized
   * @private
   */
  async _ensureInitialized() {
    if (!this.initialized) {
      await this.initialize();
    }
  }
}

// Export singleton instance
const resultStore = new QueryResultStore();
module.exports = resultStore;
