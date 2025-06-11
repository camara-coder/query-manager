/**
 * Enhanced Query Store Service
 * Extends the existing query store to support result storage and tracking
 */
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
const logger = require('../utils/logger');

class EnhancedQueryStore {
  constructor(config = {}) {
    // Default configuration
    this.config = {
      host: process.env.PGHOST || 'localhost',
      port: parseInt(process.env.PGPORT || '5432', 10),
      database: process.env.PGDATABASE || 'query_manager',
      user: process.env.PGUSER || 'postgres',
      password: process.env.PGPASSWORD || 'postgres',
      connectionString: process.env.DATABASE_URL,
      ...config
    };
    
    this.pool = null;
    this.initialized = false;
  }
  
  /**
   * Initialize the enhanced query store
   */
  async initialize() {
    if (this.initialized) {
      logger.debug('Enhanced query store already initialized');
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
        max: 20,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 2000,
      });
      
      // Test the connection
      const client = await this.pool.connect();
      client.release();
      
      // Create enhanced tables
      await this._createEnhancedTables();
      
      this.initialized = true;
      logger.info(`Enhanced query store initialized (PostgreSQL)`);
    } catch (error) {
      logger.error('Failed to initialize enhanced query store', error);
      throw new Error(`Enhanced query store initialization failed: ${error.message}`);
    }
  }
  
  /**
   * Create the enhanced database tables
   * @private
   */
  async _createEnhancedTables() {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      
      // Original queries table
      await client.query(`
        CREATE TABLE IF NOT EXISTS queries (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          description TEXT,
          sql TEXT NOT NULL,
          bind_params JSONB,
          options JSONB,
          created_at TIMESTAMP WITH TIME ZONE NOT NULL,
          updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
          query_signature TEXT -- For tracking similar queries
        )
      `);
      
      // Enhanced query executions table with results storage
      await client.query(`
        CREATE TABLE IF NOT EXISTS query_executions (
          id TEXT PRIMARY KEY,
          query_id TEXT NOT NULL,
          query_signature TEXT, -- For tracking across similar queries
          status TEXT NOT NULL,
          started_at TIMESTAMP WITH TIME ZONE NOT NULL,
          completed_at TIMESTAMP WITH TIME ZONE,
          rows_fetched INTEGER DEFAULT 0,
          execution_time INTEGER,
          error TEXT,
          results JSONB, -- Store actual query results
          tracking_info JSONB, -- Store tracking metadata
          bind_params JSONB, -- Store the parameters used
          FOREIGN KEY (query_id) REFERENCES queries (id) ON DELETE CASCADE
        )
      `);
      
      // Create indexes for performance
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_query_executions_query_signature 
        ON query_executions (query_signature)
      `);
      
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_query_executions_completed_at 
        ON query_executions (completed_at)
      `);
      
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_query_executions_status 
        ON query_executions (status)
      `);
      
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_queries_signature 
        ON queries (query_signature)
      `);
      
      // Result tracking summary table for performance optimization
      await client.query(`
        CREATE TABLE IF NOT EXISTS result_tracking_summary (
          id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
          query_signature TEXT NOT NULL,
          row_signature TEXT NOT NULL,
          first_seen TIMESTAMP WITH TIME ZONE NOT NULL,
          last_seen TIMESTAMP WITH TIME ZONE NOT NULL,
          occurrence_count INTEGER DEFAULT 1,
          execution_ids TEXT[], -- Array of execution IDs where this row appeared
          sample_data JSONB, -- Sample of the actual row data
          created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
          updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
      `);
      
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_result_tracking_query_row_signature 
        ON result_tracking_summary (query_signature, row_signature)
      `);
      
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_result_tracking_row_signature 
        ON result_tracking_summary (row_signature)
      `);
      
      await client.query('COMMIT');
      logger.debug('Enhanced database tables created or verified');
    } catch (error) {
      await client.query('ROLLBACK');
      logger.error('Error creating enhanced database tables', error);
      throw error;
    } finally {
      client.release();
    }
  }
  
  /**
   * Update an execution with results and tracking information
   * @param {string} executionId - Execution ID
   * @param {Object} executionData - Updated execution data including results
   * @returns {Promise<Object>} Updated execution record
   */
  async updateExecutionWithResults(executionId, executionData) {
    await this._ensureInitialized();
    
    try {
      // Get current execution to check if it exists
      const currentExecution = await this.pool.query(
        'SELECT * FROM query_executions WHERE id = $1',
        [executionId]
      );
      
      if (currentExecution.rows.length === 0) {
        throw new Error(`Execution with ID ${executionId} not found`);
      }
      
      // Build update query dynamically
      const updates = [];
      const values = [];
      let paramIndex = 1;
      
      // Standard fields
      if ('status' in executionData) {
        updates.push(`status = $${paramIndex++}`);
        values.push(executionData.status);
      }
      
      if ('rowsFetched' in executionData) {
        updates.push(`rows_fetched = $${paramIndex++}`);
        values.push(executionData.rowsFetched);
      }
      
      if ('executionTime' in executionData) {
        updates.push(`execution_time = $${paramIndex++}`);
        values.push(executionData.executionTime);
      }
      
      if ('error' in executionData) {
        updates.push(`error = $${paramIndex++}`);
        values.push(executionData.error);
      }
      
      if ('completedAt' in executionData) {
        updates.push(`completed_at = $${paramIndex++}`);
        values.push(executionData.completedAt);
      }
      
      // Enhanced fields for result storage
      if ('results' in executionData) {
        updates.push(`results = $${paramIndex++}`);
        values.push(JSON.stringify(executionData.results));
      }
      
      if ('tracking' in executionData) {
        updates.push(`tracking_info = $${paramIndex++}`);
        values.push(JSON.stringify(executionData.tracking));
      }
      
      if ('bindParams' in executionData) {
        updates.push(`bind_params = $${paramIndex++}`);
        values.push(JSON.stringify(executionData.bindParams));
      }
      
      if (updates.length === 0) {
        return this._formatExecutionFromDb(currentExecution.rows[0]);
      }
      
      // Add execution ID to values
      values.push(executionId);
      
      // Execute update
      const result = await this.pool.query(
        `UPDATE query_executions SET ${updates.join(', ')} WHERE id = $${paramIndex} RETURNING *`,
        values
      );
      
      // Update tracking summary if results are provided
      if ('results' in executionData && 'tracking' in executionData) {
        await this._updateTrackingSummary(
          executionId, 
          executionData.results, 
          executionData.tracking
        );
      }
      
      return this._formatExecutionFromDb(result.rows[0]);
    } catch (error) {
      logger.error(`Error updating execution with results: ${executionId}`, error);
      throw error;
    }
  }
  
  /**
   * Get previous executions with results for a query signature
   * @param {string} querySignature - Query signature
   * @param {Object} options - Query options
   * @returns {Promise<Array>} Previous executions with results
   */
  async getPreviousExecutionsWithResults(querySignature, options = {}) {
    await this._ensureInitialized();
    
    const queryOptions = {
      limit: 50, // Limit to recent executions for performance
      includeResults: true,
      ...options
    };
    
    try {
      let query = `
        SELECT id, query_id, query_signature, status, started_at, completed_at, 
               rows_fetched, execution_time, results, tracking_info, bind_params
        FROM query_executions 
        WHERE query_signature = $1 
          AND status = 'completed' 
          AND results IS NOT NULL
        ORDER BY completed_at DESC
      `;
      
      const params = [querySignature];
      
      if (queryOptions.limit) {
        query += ` LIMIT $2`;
        params.push(queryOptions.limit);
      }
      
      const result = await this.pool.query(query, params);
      
      return result.rows.map(execution => ({
        id: execution.id,
        queryId: execution.query_id,
        querySignature: execution.query_signature,
        status: execution.status,
        started_at: execution.started_at,
        completed_at: execution.completed_at,
        rowsFetched: execution.rows_fetched,
        executionTime: execution.execution_time,
        results: execution.results,
        trackingInfo: execution.tracking_info,
        bindParams: execution.bind_params
      }));
    } catch (error) {
      logger.error(`Error getting previous executions for signature: ${querySignature}`, error);
      throw error;
    }
  }
  
  /**
   * Get query executions with results
   * @param {string} queryId - Query ID
   * @param {Object} options - Query options
   * @returns {Promise<Array>} Executions with results
   */
  async getQueryExecutionsWithResults(queryId, options = {}) {
    await this._ensureInitialized();
    
    const queryOptions = {
      includeResults: false, // By default don't include large result sets
      limit: 20,
      ...options
    };
    
    try {
      let selectFields = `
        id, query_id, query_signature, status, started_at, completed_at, 
        rows_fetched, execution_time, tracking_info, bind_params
      `;
      
      if (queryOptions.includeResults) {
        selectFields += ', results';
      }
      
      let query = `
        SELECT ${selectFields}
        FROM query_executions 
        WHERE query_id = $1 
        ORDER BY started_at DESC
      `;
      
      const params = [queryId];
      
      if (queryOptions.limit) {
        query += ` LIMIT $2`;
        params.push(queryOptions.limit);
      }
      
      const result = await this.pool.query(query, params);
      
      return result.rows.map(execution => ({
        id: execution.id,
        queryId: execution.query_id,
        querySignature: execution.query_signature,
        status: execution.status,
        started_at: execution.started_at,
        completed_at: execution.completed_at,
        rowsFetched: execution.rows_fetched,
        executionTime: execution.execution_time,
        results: execution.results || null,
        trackingInfo: execution.tracking_info,
        bindParams: execution.bind_params
      }));
    } catch (error) {
      logger.error(`Error getting executions with results for query: ${queryId}`, error);
      throw error;
    }
