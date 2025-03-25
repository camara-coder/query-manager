/**
 * Query Service
 * Higher-level service for executing and managing queries
 */
const queryManager = require('../db/query-manager');
const eventsService = require('./events-service');
const queryStore = require('./query-store');
const config = require('../config');
const logger = require('../utils/logger');

/**
 * Execute a long-running query with progress tracking and error handling
 * @param {string} sql - SQL query to execute
 * @param {Object} bindParams - Bind parameters
 * @param {Object} options - Query options
 * @param {Function} progressCallback - Optional callback for progress updates
 * @returns {Promise<Object>} Query results
 */
async function executeQuery(sql, bindParams = {}, options = {}, progressCallback = null) {
  // Create subscription to progress events if callback provided
  const progressSubscription = progressCallback ? 
    eventsService.subscribe('query:progress', progressCallback) : null;
  
  try {
    // Forward the query to the manager
    const result = await queryManager.executeLongQuery(sql, bindParams, options);
    
    // Process the results if needed (transformations, formatting, etc.)
    return {
      success: true,
      queryId: result.queryId,
      data: result.rows,
      metadata: result.metaData,
      stats: {
        rowCount: result.rows.length,
        executionTime: result.metrics.duration,
        startTime: new Date(result.metrics.startTime).toISOString(),
        endTime: new Date(result.metrics.endTime).toISOString()
      }
    };
  } catch (error) {
    logger.error('Query execution failed in service layer', { 
      error: error.message,
      query: { bindParamKeys: Object.keys(bindParams) }
    });
    
    return {
      success: false,
      error: error.message,
      queryId: error.queryId
    };
  } finally {
    // Clean up subscription if it was created
    if (progressSubscription) {
      eventsService.unsubscribe(progressSubscription);
    }
  }
}

/**
 * Execute a predefined query by its ID
 * @param {string} queryId - ID of the predefined query
 * @param {Object} bindParams - Override default bind parameters
 * @param {Object} options - Override default options
 * @param {Function} progressCallback - Optional callback for progress updates
 * @returns {Promise<Object>} Query results
 */
async function executeQueryById(queryId, bindParams = {}, options = {}, progressCallback = null) {
  try {
    // Get the query definition from the store
    const query = await queryStore.getQueryById(queryId);
    
    // Merge bind parameters and options
    const mergedBindParams = { ...query.bindParams, ...bindParams };
    const mergedOptions = { ...query.options, ...options };
    
    // Log query execution
    logger.info(`Executing query: ${query.name}`, {
      id: queryId,
      description: query.description
    });
    
    // Start recording execution
    const execution = await queryStore.recordExecution(queryId, {
      status: 'executing',
      startedAt: new Date().toISOString()
    });
    
    // Create a progress callback that updates the execution record
    const executionProgressCallback = async (event) => {
      // Update execution record with progress
      await queryStore.updateExecution(execution.id, {
        rowsFetched: event.data.rowsFetched,
        status: 'executing'
      });
      
      // Forward to user-provided callback if any
      if (progressCallback) {
        progressCallback(event);
      }
    };
    
    // Execute using the standard query executor
    const result = await executeQuery(
      query.sql, 
      mergedBindParams, 
      mergedOptions, 
      executionProgressCallback
    );
    
    // Update execution record with results
    if (result.success) {
      await queryStore.updateExecution(execution.id, {
        status: 'completed',
        rowsFetched: result.stats.rowCount,
        executionTime: result.stats.executionTime,
        completedAt: result.stats.endTime
      });
    } else {
      await queryStore.updateExecution(execution.id, {
        status: 'error',
        error: result.error,
        completedAt: new Date().toISOString()
      });
    }
    
    // Return execution ID with results
    return {
      ...result,
      executionId: execution.id
    };
  } catch (error) {
    logger.error(`Failed to execute query: ${queryId}`, { error: error.message });
    
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Execute an ad-hoc SQL query and optionally save it
 * @param {Object} queryData - Query data including SQL and parameters
 * @param {boolean} save - Whether to save the query
 * @returns {Promise<Object>} Query results with execution ID
 */
async function executeAdhocQuery(queryData, save = false) {
  try {
    let queryId = null;
    let executionId = null;
    
    // Save the query if requested
    if (save && queryData.name) {
      const savedQuery = await queryStore.createQuery({
        name: queryData.name,
        description: queryData.description || 'Ad-hoc query',
        sql: queryData.sql,
        bindParams: queryData.bindParams || {}
      });
      
      queryId = savedQuery.id;
      
      // Execute as a saved query
      return executeQueryById(queryId, queryData.bindParams || {});
    }
    
    // Execute as a transient query
    const result = await executeQuery(
      queryData.sql,
      queryData.bindParams || {},
      queryData.options || {}
    );
    
    return result;
  } catch (error) {
    logger.error('Failed to execute ad-hoc query', { error: error.message });
    
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Cancel an executing query
 * @param {string} queryId - ID of the query
 * @param {string} executionId - ID of the execution
 * @returns {Promise<boolean>} Success indicator
 */
async function cancelQuery(queryId, executionId) {
  try {
    // Get execution to find the query manager ID
    const execution = await queryStore.getExecution(executionId);
    
    // Call the query manager to cancel
    const success = await queryManager.cancelQuery(execution.queryManagerId || executionId);
    
    if (success) {
      // Update execution record
      await queryStore.updateExecution(executionId, {
        status: 'cancelled',
        completedAt: new Date().toISOString()
      });
      
      logger.info(`Query cancelled successfully: ${queryId}, execution: ${executionId}`);
      eventsService.publish('query:cancelled', { queryId, executionId });
    } else {
      logger.warn(`Failed to cancel query: ${queryId}, execution: ${executionId}`);
    }
    
    return success;
  } catch (error) {
    logger.error(`Error in cancel query operation: ${queryId}`, { error: error.message });
    return false;
  }
}

/**
 * Get status of a specific execution
 * @param {string} queryId - ID of the query
 * @param {string} executionId - ID of the execution
 * @returns {Promise<Object>} Execution status
 */
async function getExecutionStatus(queryId, executionId) {
  try {
    const execution = await queryStore.getExecution(executionId);
    
    // Calculate progress (approximation)
    const progress = execution.status === 'completed' ? 100 : 
                     execution.status === 'error' || execution.status === 'cancelled' ? 0 :
                     Math.min(95, Math.random() * 20 + 60); // Random progress between 60-80% for ongoing executions
    
    return {
      queryId,
      executionId,
      status: execution.status,
      startedAt: execution.startedAt,
      completedAt: execution.completedAt,
      rowsFetched: execution.rowsFetched,
      executionTime: execution.executionTime,
      progress,
      error: execution.error
    };
  } catch (error) {
    logger.error(`Error getting execution status: ${executionId}`, { error: error.message });
    throw error;
  }
}

/**
 * Get all saved queries
 * @returns {Promise<Array>} List of saved queries
 */
async function getAllQueries() {
  return queryStore.getAllQueries();
}

/**
 * Get a specific query by ID
 * @param {string} queryId - ID of the query
 * @returns {Promise<Object>} Query details
 */
async function getQueryById(queryId) {
  return queryStore.getQueryById(queryId);
}

/**
 * Create a new query
 * @param {Object} queryData - Query data
 * @returns {Promise<Object>} Created query
 */
async function createQuery(queryData) {
  return queryStore.createQuery(queryData);
}

/**
 * Update an existing query
 * @param {string} queryId - ID of the query
 * @param {Object} queryData - Updated query data
 * @returns {Promise<Object>} Updated query
 */
async function updateQuery(queryId, queryData) {
  return queryStore.updateQuery(queryId, queryData);
}

/**
 * Delete a query
 * @param {string} queryId - ID of the query
 * @returns {Promise<boolean>} Success indicator
 */
async function deleteQuery(queryId) {
  return queryStore.deleteQuery(queryId);
}

/**
 * Get query execution history
 * @param {string} queryId - ID of the query
 * @returns {Promise<Array>} List of executions
 */
async function getQueryExecutionHistory(queryId) {
  return queryStore.getQueryExecutions(queryId);
}

/**
 * Get execution results
 * @param {string} queryId - ID of the query
 * @param {string} executionId - ID of the execution
 * @returns {Promise<Object>} Execution results
 */
async function getExecutionResults(queryId, executionId) {
  // Currently, we don't store result data
  // In a real implementation, you would store results in a separate table
  // or retrieve them from a cache
  return {
    success: false,
    error: 'Result retrieval not implemented. Execute the query again to get results.'
  };
}

/**
 * Initialize the query service
 * Sets up event handlers and subscriptions
 */
async function initialize() {
  // Initialize query store
  await queryStore.initialize();
  
  // Forward query manager events to the event service
  queryManager.on('progress', (progressInfo) => {
    eventsService.publish('query:progress', progressInfo);
  });
  
  queryManager.on('complete', (completeInfo) => {
    eventsService.publish('query:complete', completeInfo);
  });
  
  queryManager.on('cancelled', (cancelInfo) => {
    eventsService.publish('query:cancelled', cancelInfo);
  });
  
  logger.info('Query service initialized');
}

/**
 * Shutdown the query service
 */
async function shutdown() {
  try {
    await queryManager.shutdown();
    await queryStore.close();
    logger.info('Query service shutdown complete');
    return true;
  } catch (error) {
    logger.error('Error during query service shutdown', { error: error.message });
    return false;
  }
}

module.exports = {
  executeQuery,
  executeQueryById,
  executeAdhocQuery,
  cancelQuery,
  getExecutionStatus,
  getAllQueries,
  getQueryById,
  createQuery,
  updateQuery,
  deleteQuery,
  getQueryExecutionHistory,
  getExecutionResults,
  initialize,
  shutdown
};