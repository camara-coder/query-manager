/**
 * Query Store Service
 * Manages storing and retrieving query definitions using SQLite
 */
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const { v4: uuidv4 } = require('uuid');
const logger = require('../utils/logger');
const path = require('path');
const fs = require('fs');

class QueryStore {
  constructor(config = {}) {
    this.config = {
      inMemory: process.env.QUERY_STORE_IN_MEMORY === 'true' || false,
      dbPath: process.env.QUERY_STORE_PATH || path.join(process.cwd(), 'data', 'queries.db'),
      ...config
    };
    
    this.db = null;
    this.initialized = false;
  }
  
  /**
   * Initialize the query store
   */
  async initialize() {
    if (this.initialized) {
      logger.debug('Query store already initialized');
      return;
    }
    
    try {
      // Ensure directory exists if using file-based database
      if (!this.config.inMemory) {
        const dbDir = path.dirname(this.config.dbPath);
        if (!fs.existsSync(dbDir)) {
          fs.mkdirSync(dbDir, { recursive: true });
        }
      }
      
      // Connect to database
      this.db = await open({
        filename: this.config.inMemory ? ':memory:' : this.config.dbPath,
        driver: sqlite3.Database
      });
      
      // Enable foreign keys
      await this.db.run('PRAGMA foreign_keys = ON');
      
      // Create tables if they don't exist
      await this._createTables();
      
      this.initialized = true;
      logger.info(`Query store initialized (${this.config.inMemory ? 'in-memory' : 'file-based'})`);
    } catch (error) {
      logger.error('Failed to initialize query store', error);
      throw new Error(`Query store initialization failed: ${error.message}`);
    }
  }
  
  /**
   * Create the database tables
   * @private
   */
  async _createTables() {
    // Queries table
    await this.db.exec(`
      CREATE TABLE IF NOT EXISTS queries (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        sql TEXT NOT NULL,
        bind_params TEXT,
        options TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )
    `);
    
    // Query executions table
    await this.db.exec(`
      CREATE TABLE IF NOT EXISTS query_executions (
        id TEXT PRIMARY KEY,
        query_id TEXT NOT NULL,
        status TEXT NOT NULL,
        started_at TEXT NOT NULL,
        completed_at TEXT,
        rows_fetched INTEGER DEFAULT 0,
        execution_time INTEGER,
        error TEXT,
        FOREIGN KEY (query_id) REFERENCES queries (id) ON DELETE CASCADE
      )
    `);
    
    logger.debug('Database tables created or verified');
  }
  
  /**
   * Close the database connection
   */
  async close() {
    if (this.db) {
      await this.db.close();
      this.initialized = false;
      logger.info('Query store closed');
    }
  }
  
  /**
   * Get all queries
   * @returns {Promise<Array>} List of queries
   */
  async getAllQueries() {
    await this._ensureInitialized();
    
    try {
      const queries = await this.db.all(`
        SELECT q.*, 
               e.id as last_execution_id,
               e.status as last_execution_status,
               e.started_at as last_execution_date,
               e.execution_time as last_execution_time
        FROM queries q
        LEFT JOIN (
          SELECT * FROM query_executions e1
          WHERE e1.started_at = (
            SELECT MAX(e2.started_at) FROM query_executions e2 WHERE e2.query_id = e1.query_id
          )
        ) e ON q.id = e.query_id
        ORDER BY q.updated_at DESC
      `);
      
      // Parse JSON fields and format data
      return queries.map(query => this._formatQueryFromDb(query));
    } catch (error) {
      logger.error('Error getting all queries', error);
      throw new Error(`Failed to get queries: ${error.message}`);
    }
  }
  
  /**
   * Get a query by ID
   * @param {string} id - Query ID
   * @returns {Promise<Object>} Query object
   */
  async getQueryById(id) {
    await this._ensureInitialized();
    
    try {
      const query = await this.db.get(`
        SELECT q.*, 
               e.id as last_execution_id,
               e.status as last_execution_status,
               e.started_at as last_execution_date,
               e.execution_time as last_execution_time
        FROM queries q
        LEFT JOIN (
          SELECT * FROM query_executions e1
          WHERE e1.started_at = (
            SELECT MAX(e2.started_at) FROM query_executions e2 WHERE e2.query_id = e1.query_id
          )
        ) e ON q.id = e.query_id
        WHERE q.id = ?
      `, id);
      
      if (!query) {
        throw new Error(`Query with ID ${id} not found`);
      }
      
      return this._formatQueryFromDb(query);
    } catch (error) {
      logger.error(`Error getting query by ID: ${id}`, error);
      throw error;
    }
  }
  
  /**
   * Create a new query
   * @param {Object} queryData - Query data
   * @returns {Promise<Object>} Created query
   */
  async createQuery(queryData) {
    await this._ensureInitialized();
    
    try {
      const id = queryData.id || uuidv4();
      const now = new Date().toISOString();
      
      const query = {
        id,
        name: queryData.name,
        description: queryData.description || '',
        sql: queryData.sql,
        bind_params: JSON.stringify(queryData.bindParams || {}),
        options: JSON.stringify(queryData.options || {}),
        created_at: now,
        updated_at: now
      };
      
      await this.db.run(`
        INSERT INTO queries (id, name, description, sql, bind_params, options, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        query.id,
        query.name,
        query.description,
        query.sql,
        query.bind_params,
        query.options,
        query.created_at,
        query.updated_at
      ]);
      
      return this.getQueryById(id);
    } catch (error) {
      logger.error('Error creating query', error);
      throw new Error(`Failed to create query: ${error.message}`);
    }
  }
  
  /**
   * Update an existing query
   * @param {string} id - Query ID
   * @param {Object} queryData - Updated query data
   * @returns {Promise<Object>} Updated query
   */
  async updateQuery(id, queryData) {
    await this._ensureInitialized();
    
    try {
      // Check if query exists
      const existingQuery = await this.db.get('SELECT id FROM queries WHERE id = ?', id);
      if (!existingQuery) {
        throw new Error(`Query with ID ${id} not found`);
      }
      
      const now = new Date().toISOString();
      
      // Prepare update data
      const updateData = {
        name: queryData.name,
        description: queryData.description || '',
        sql: queryData.sql,
        bind_params: JSON.stringify(queryData.bindParams || {}),
        options: JSON.stringify(queryData.options || {}),
        updated_at: now
      };
      
      // Execute update
      await this.db.run(`
        UPDATE queries
        SET name = ?,
            description = ?,
            sql = ?,
            bind_params = ?,
            options = ?,
            updated_at = ?
        WHERE id = ?
      `, [
        updateData.name,
        updateData.description,
        updateData.sql,
        updateData.bind_params,
        updateData.options,
        updateData.updated_at,
        id
      ]);
      
      return this.getQueryById(id);
    } catch (error) {
      logger.error(`Error updating query: ${id}`, error);
      throw error;
    }
  }
  
  /**
   * Delete a query
   * @param {string} id - Query ID
   * @returns {Promise<boolean>} Success flag
   */
  async deleteQuery(id) {
    await this._ensureInitialized();
    
    try {
      // Check if query exists
      const existingQuery = await this.db.get('SELECT id FROM queries WHERE id = ?', id);
      if (!existingQuery) {
        throw new Error(`Query with ID ${id} not found`);
      }
      
      // Delete query (cascade will delete executions)
      await this.db.run('DELETE FROM queries WHERE id = ?', id);
      
      return true;
    } catch (error) {
      logger.error(`Error deleting query: ${id}`, error);
      throw error;
    }
  }
  
  /**
   * Record a query execution
   * @param {string} queryId - Query ID
   * @param {Object} executionData - Execution data
   * @returns {Promise<Object>} Created execution record
   */
  async recordExecution(queryId, executionData) {
    await this._ensureInitialized();
    
    try {
      const id = executionData.id || uuidv4();
      const now = new Date().toISOString();
      
      const execution = {
        id,
        query_id: queryId,
        status: executionData.status || 'executing',
        started_at: executionData.startedAt || now,
        completed_at: executionData.completedAt || null,
        rows_fetched: executionData.rowsFetched || 0,
        execution_time: executionData.executionTime || null,
        error: executionData.error || null
      };
      
      await this.db.run(`
        INSERT INTO query_executions (id, query_id, status, started_at, completed_at, rows_fetched, execution_time, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        execution.id,
        execution.query_id,
        execution.status,
        execution.started_at,
        execution.completed_at,
        execution.rows_fetched,
        execution.execution_time,
        execution.error
      ]);
      
      return { id: execution.id, ...executionData };
    } catch (error) {
      logger.error(`Error recording execution for query: ${queryId}`, error);
      throw new Error(`Failed to record execution: ${error.message}`);
    }
  }
  
  /**
   * Update a query execution
   * @param {string} executionId - Execution ID
   * @param {Object} executionData - Updated execution data
   * @returns {Promise<Object>} Updated execution record
   */
  async updateExecution(executionId, executionData) {
    await this._ensureInitialized();
    
    try {
      // Get current execution
      const currentExecution = await this.db.get(
        'SELECT * FROM query_executions WHERE id = ?',
        executionId
      );
      
      if (!currentExecution) {
        throw new Error(`Execution with ID ${executionId} not found`);
      }
      
      const updateFields = [];
      const params = [];
      
      // Only update provided fields
      if ('status' in executionData) {
        updateFields.push('status = ?');
        params.push(executionData.status);
      }
      
      if ('rowsFetched' in executionData) {
        updateFields.push('rows_fetched = ?');
        params.push(executionData.rowsFetched);
      }
      
      if ('executionTime' in executionData) {
        updateFields.push('execution_time = ?');
        params.push(executionData.executionTime);
      }
      
      if ('error' in executionData) {
        updateFields.push('error = ?');
        params.push(executionData.error);
      }
      
      // If status is completed or error, set completed_at
      if (executionData.status === 'completed' || executionData.status === 'error' || executionData.status === 'cancelled') {
        updateFields.push('completed_at = ?');
        params.push(executionData.completedAt || new Date().toISOString());
      }
      
      if (updateFields.length === 0) {
        return currentExecution;
      }
      
      // Add execution ID to params
      params.push(executionId);
      
      // Execute update
      await this.db.run(
        `UPDATE query_executions SET ${updateFields.join(', ')} WHERE id = ?`,
        params
      );
      
      // Return updated execution
      const updatedExecution = await this.db.get(
        'SELECT * FROM query_executions WHERE id = ?',
        executionId
      );
      
      return this._formatExecutionFromDb(updatedExecution);
    } catch (error) {
      logger.error(`Error updating execution: ${executionId}`, error);
      throw error;
    }
  }
  
  /**
   * Get execution details
   * @param {string} executionId - Execution ID
   * @returns {Promise<Object>} Execution details
   */
  async getExecution(executionId) {
    await this._ensureInitialized();
    
    try {
      const execution = await this.db.get(
        'SELECT * FROM query_executions WHERE id = ?',
        executionId
      );
      
      if (!execution) {
        throw new Error(`Execution with ID ${executionId} not found`);
      }
      
      return this._formatExecutionFromDb(execution);
    } catch (error) {
      logger.error(`Error getting execution: ${executionId}`, error);
      throw error;
    }
  }
  
  /**
   * Get all executions for a query
   * @param {string} queryId - Query ID
   * @returns {Promise<Array>} List of executions
   */
  async getQueryExecutions(queryId) {
    await this._ensureInitialized();
    
    try {
      const executions = await this.db.all(
        'SELECT * FROM query_executions WHERE query_id = ? ORDER BY started_at DESC',
        queryId
      );
      
      return executions.map(execution => this._formatExecutionFromDb(execution));
    } catch (error) {
      logger.error(`Error getting executions for query: ${queryId}`, error);
      throw error;
    }
  }
  
  /**
   * Format a query from the database
   * @private
   */
  _formatQueryFromDb(query) {
    if (!query) return null;
    
    // Parse JSON fields
    const bindParams = query.bind_params ? JSON.parse(query.bind_params) : {};
    const options = query.options ? JSON.parse(query.options) : {};
    
    // Format last execution if available
    let lastExecution = null;
    if (query.last_execution_id) {
      lastExecution = {
        id: query.last_execution_id,
        status: query.last_execution_status,
        date: query.last_execution_date,
        executionTime: query.last_execution_time
      };
    }
    
    return {
      id: query.id,
      name: query.name,
      description: query.description,
      sql: query.sql,
      bindParams,
      options,
      createdAt: query.created_at,
      updatedAt: query.updated_at,
      lastExecution
    };
  }
  
  /**
   * Format an execution from the database
   * @private
   */
  _formatExecutionFromDb(execution) {
    if (!execution) return null;
    
    return {
      id: execution.id,
      queryId: execution.query_id,
      status: execution.status,
      startedAt: execution.started_at,
      completedAt: execution.completed_at,
      rowsFetched: execution.rows_fetched,
      executionTime: execution.execution_time,
      error: execution.error
    };
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
const queryStore = new QueryStore();
module.exports = queryStore;