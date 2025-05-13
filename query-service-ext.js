/**
 * Enhanced Query Service with Result Storage
 * Extension of the query service with added result storage functionality
 */
const queryService = require('./query-service');
const resultStore = require('./result-store');
const dataJoinService = require('./data-join-service');
const logger = require('../utils/logger');
const { v4: uuidv4 } = require('uuid');

/**
 * Execute a query and store the results
 * @param {string} sql - SQL query to execute
 * @param {Object} bindParams - Bind parameters
 * @param {Object} options - Query options
 * @param {Function} progressCallback - Optional callback for progress updates
 * @returns {Promise<Object>} Query results with storage info
 */
async function executeQueryWithStorage(sql, bindParams = {}, options = {}, progressCallback = null) {
  // Extract storage options
  const storageOptions = {
    storeResults: options.storeResults !== false, // Default to true
    resultTTL: options.resultTTL || null,
    resultTags: options.resultTags || [],
    resultMetadata: options.resultMetadata || {}
  };
  
  // Execute the query using the original service
  const result = await queryService.executeQuery(sql, bindParams, options, progressCallback);
  
  // Store the result if enabled and query was successful
  if (storageOptions.storeResults && result.success) {
    try {
      const storedResult = await resultStore.storeResult({
        queryId: result.queryId,
        executionId: result.queryId, // Use queryId as executionId for ad-hoc queries
        resultType: 'single',
        data: result.data,
        metadata: {
          ...result.metadata,
          ...storageOptions.resultMetadata,
          stats: result.stats,
          queryInfo: {
            sql: options.includeSqlInMetadata ? sql : null,
            bindParamKeys: Object.keys(bindParams),
            executedAt: result.stats.startTime
          }
        },
        tags: storageOptions.resultTags,
        ttlSeconds: storageOptions.resultTTL
      });
      
      // Add storage information to the result
      result.storage = {
        resultId: storedResult.id,
        stored: true,
        storedAt: storedResult.createdAt,
        ttl: storedResult.ttl
      };
      
      logger.info('Query results stored', {
        queryId: result.queryId,
        resultId: storedResult.id,
        rowCount: result.data.length
      });
    } catch (error) {
      logger.error('Failed to store query results', {
        queryId: result.queryId,
        error: error.message
      });
      
      // Add storage error information but don't fail the query
      result.storage = {
        stored: false,
        error: error.message
      };
    }
  } else if (!storageOptions.storeResults) {
    result.storage = {
      stored: false,
      reason: 'Storage disabled for this query'
    };
  } else if (!result.success) {
    result.storage = {
      stored: false,
      reason: 'Query execution failed'
    };
  }
  
  return result;
}

/**
 * Execute a predefined query and store results
 * @param {string} queryId - ID of the predefined query
 * @param {Object} bindParams - Override default bind parameters
 * @param {Object} options - Override default options
 * @param {Function} progressCallback - Optional callback for progress updates
 * @returns {Promise<Object>} Query results with storage info
 */
async function executeQueryByIdWithStorage(queryId, bindParams = {}, options = {}, progressCallback = null) {
  // Extract storage options
  const storageOptions = {
    storeResults: options.storeResults !== false, // Default to true
    resultTTL: options.resultTTL || null,
    resultTags: options.resultTags || [],
    resultMetadata: options.resultMetadata || {}
  };
  
  // Execute the query using the original service
  const result = await queryService.executeQueryById(queryId, bindParams, options, progressCallback);
  
  // Store the result if enabled and query was successful
  if (storageOptions.storeResults && result.success) {
    try {
      const storedResult = await resultStore.storeResult({
        queryId: queryId,
        executionId: result.executionId,
        resultType: 'single',
        data: result.data,
        metadata: {
          ...result.metadata,
          ...storageOptions.resultMetadata,
          stats: result.stats,
          queryInfo: {
            queryId: queryId,
            bindParamKeys: Object.keys(bindParams),
            executedAt: result.stats.startTime
          }
        },
        tags: storageOptions.resultTags,
        ttlSeconds: storageOptions.resultTTL
      });
      
      // Add storage information to the result
      result.storage = {
        resultId: storedResult.id,
        stored: true,
        storedAt: storedResult.createdAt,
        ttl: storedResult.ttl
      };
      
      logger.info('Predefined query results stored', {
        queryId: queryId,
        executionId: result.executionId,
        resultId: storedResult.id,
        rowCount: result.data.length
      });
    } catch (error) {
      logger.error('Failed to store predefined query results', {
        queryId: queryId,
        executionId: result.executionId,
        error: error.message
      });
      
      // Add storage error information but don't fail the query
      result.storage = {
        stored: false,
        error: error.message
      };
    }
  } else if (!storageOptions.storeResults) {
    result.storage = {
      stored: false,
      reason: 'Storage disabled for this query'
    };
  } else if (!result.success) {
    result.storage = {
      stored: false,
      reason: 'Query execution failed'
    };
  }
  
  return result;
}

/**
 * Execute an ad-hoc SQL query, optionally save it, and store results
 * @param {Object} queryData - Query data including SQL and parameters
 * @param {boolean} save - Whether to save the query
 * @param {Object} options - Options including storage options
 * @returns {Promise<Object>} Query results with storage info
 */
async function executeAdhocQueryWithStorage(queryData, save = false, options = {}) {
  // Extract storage options
  const storageOptions = {
    storeResults: options.storeResults !== false, // Default to true
    resultTTL: options.resultTTL || null,
    resultTags: options.resultTags || [],
    resultMetadata: options.resultMetadata || {}
  };
  
  // Execute via original service
  const result = await queryService.executeAdhocQuery(queryData, save);
  
  // Store the result if enabled and query was successful
  if (storageOptions.storeResults && result.success) {
    try {
      const storedResult = await resultStore.storeResult({
        queryId: result.queryId,
        executionId: result.executionId || result.queryId,
        resultType: 'adhoc',
        data: result.data,
        metadata: {
          ...result.metadata,
          ...storageOptions.resultMetadata,
          stats: result.stats,
          queryInfo: {
            sql: options.includeSqlInMetadata ? queryData.sql : null,
            name: queryData.name,
            description: queryData.description,
            bindParamKeys: Object.keys(queryData.bindParams || {}),
            saved: save,
            executedAt: result.stats?.startTime || new Date().toISOString()
          }
        },
        tags: storageOptions.resultTags,
        ttlSeconds: storageOptions.resultTTL
      });
      
      // Add storage information to the result
      result.storage = {
        resultId: storedResult.id,
        stored: true,
        storedAt: storedResult.createdAt,
        ttl: storedResult.ttl
      };
      
      logger.info('Ad-hoc query results stored', {
        queryId: result.queryId,
        resultId: storedResult.id,
        rowCount: result.data?.length || 0
      });
    } catch (error) {
      logger.error('Failed to store ad-hoc query results', {
        error: error.message
      });
      
      // Add storage error information but don't fail the query
      result.storage = {
        stored: false,
        error: error.message
      };
    }
  } else if (!storageOptions.storeResults) {
    result.storage = {
      stored: false,
      reason: 'Storage disabled for this query'
    };
  } else if (!result.success) {
    result.storage = {
      stored: false,
      reason: 'Query execution failed'
    };
  }
  
  return result;
}

/**
 * Join two queries and store the combined results
 * @param {string|Object} query1 - First query ID or SQL query object
 * @param {string|Object} query2 - Second query ID or SQL query object
 * @param {Array} joinConditions - Array of join conditions or strategies
 * @param {Object} params1 - Parameters for first query
 * @param {Object} params2 - Parameters for second query
 * @param {Object} options - Join and storage options
 * @returns {Promise<Object>} Join results with storage info
 */
async function joinQueriesWithStorage(query1, query2, joinConditions, params1 = {}, params2 = {}, options = {}) {
  // Extract storage options
  const storageOptions = {
    storeResults: options.storeResults !== false, // Default to true
    resultTTL: options.resultTTL || null,
    resultTags: options.resultTags || ['joined'],
    resultMetadata: options.resultMetadata || {}
  };
  
  // Execute join via data join service
  const result = await dataJoinService.joinQueries(query1, query2, joinConditions, params1, params2, options);
  
  // Store the result if enabled and join was successful
  if (storageOptions.storeResults && result.success) {
    try {
      // Use the best join result
      const bestJoinData = result.bestJoin.data;
      
      const storedResult = await resultStore.storeResult({
        queryId: typeof query1 === 'string' ? query1 : null,
        resultType: 'joined',
        data: bestJoinData,
        metadata: {
          ...storageOptions.resultMetadata,
          joinInfo: {
            bestJoinName: result.bestJoin.name,
            bestJoinCount: result.bestJoin.count,
            query1Info: result.metadata.query1,
            query2Info: result.metadata.query2,
            metrics: result.metadata.metrics
          }
        },
        tags: [...storageOptions.resultTags, 'joined'],
        ttlSeconds: storageOptions.resultTTL
      });
      
      // Add storage information to the result
      result.storage = {
        resultId: storedResult.id,
        stored: true,
        storedAt: storedResult.createdAt,
        ttl: storedResult.ttl
      };
      
      logger.info('Joined query results stored', {
        resultId: storedResult.id,
        rowCount: bestJoinData.length,
        joinType: 'dual'
      });
    } catch (error) {
      logger.error('Failed to store joined query results', {
        error: error.message
      });
      
      // Add storage error information but don't fail the query
      result.storage = {
        stored: false,
        error: error.message
      };
    }
  } else if (!storageOptions.storeResults) {
    result.storage = {
      stored: false,
      reason: 'Storage disabled for this join'
    };
  } else if (!result.success) {
    result.storage = {
      stored: false,
      reason: 'Join operation failed'
    };
  }
  
  return result;
}

/**
 * Join multiple queries and store the combined results
 * @param {Array<Object>} querySpecs - Array of query specifications
 * @param {Array<Object>} joinSpecs - Array of join specifications
 * @param {Object} options - Join and storage options
 * @returns {Promise<Object>} Multi-join results with storage info
 */
async function joinMultipleQueriesWithStorage(querySpecs, joinSpecs, options = {}) {
  // Extract storage options
  const storageOptions = {
    storeResults: options.storeResults !== false, // Default to true
    resultTTL: options.resultTTL || null,
    resultTags: options.resultTags || ['multi-joined'],
    resultMetadata: options.resultMetadata || {}
  };
  
  // Execute multi-join via data join service
  const result = await dataJoinService.joinMultipleQueries(querySpecs, joinSpecs, options);
  
  // Store the result if enabled and join was successful
  if (storageOptions.storeResults && result.success) {
    try {
      const storedResult = await resultStore.storeResult({
        resultType: 'multi-joined',
        data: result.data,
        metadata: {
          ...storageOptions.resultMetadata,
          joinInfo: {
            queryCount: querySpecs.length,
            joinCount: joinSpecs.length,
            initialRowCounts: result.metadata.initialRowCounts,
            finalRowCount: result.metadata.finalRowCount,
            metrics: result.metadata.metrics
          }
        },
        tags: [...storageOptions.resultTags, 'multi-joined'],
        ttlSeconds: storageOptions.resultTTL
      });
      
      // Add storage information to the result
      result.storage = {
        resultId: storedResult.id,
        stored: true,
        storedAt: storedResult.createdAt,
        ttl: storedResult.ttl
      };
      
      logger.info('Multi-joined query results stored', {
        resultId: storedResult.id,
        rowCount: result.data.length,
        queryCount: querySpecs.length,
        joinCount: joinSpecs.length
      });
    } catch (error) {
      logger.error('Failed to store multi-joined query results', {
        error: error.message
      });
      
      // Add storage error information but don't fail the query
      result.storage = {
        stored: false,
        error: error.message
      };
    }
  } else if (!storageOptions.storeResults) {
    result.storage = {
      stored: false,
      reason: 'Storage disabled for this multi-join'
    };
  } else if (!result.success) {
    result.storage = {
      stored: false,
      reason: 'Multi-join operation failed'
    };
  }
  
  return result;
}

/**
 * Retrieve stored results by result ID
 * @param {string} resultId - Result ID
 * @returns {Promise<Object>} Stored result data
 */
async function getStoredResult(resultId) {
  return resultStore.getResultById(resultId);
}

/**
 * Get all stored results for a query
 * @param {string} queryId - Query ID
 * @returns {Promise<Array>} List of result metadata 
 */
async function getStoredResultsByQueryId(queryId) {
  return resultStore.getResultsByQueryId(queryId);
}

/**
 * Get all stored results for an execution
 * @param {string} executionId - Execution ID
 * @returns {Promise<Array>} List of result metadata
 */
async function getStoredResultsByExecutionId(executionId) {
  return resultStore.getResultsByExecutionId(executionId);
}

/**
 * Delete a stored result
 * @param {string} resultId - Result ID
 * @returns {Promise<boolean>} Success indicator
 */
async function deleteStoredResult(resultId) {
  return resultStore.deleteResult(resultId);
}

/**
 * Store existing result data 
 * @param {Object} resultData - Result data to store
 * @param {Object} options - Storage options
 * @returns {Promise<Object>} Storage information
 */
async function storeExistingResultData(resultData, options = {}) {
  // Prepare storage options
  const storageOptions = {
    resultTTL: options.resultTTL || null,
    resultTags: options.resultTags || [],
    resultMetadata: options.resultMetadata || {},
    resultType: options.resultType || 'custom'
  };
  
  try {
    const storedResult = await resultStore.storeResult({
      queryId: resultData.queryId,
      executionId: resultData.executionId,
      resultType: storageOptions.resultType,
      data: resultData.data,
      metadata: {
        ...resultData.metadata,
        ...storageOptions.resultMetadata
      },
      tags: storageOptions.resultTags,
      ttlSeconds: storageOptions.resultTTL
    });
    
    logger.info('Custom result data stored', {
      resultId: storedResult.id,
      resultType: storageOptions.resultType,
      rowCount: resultData.data?.length || 0
    });
    
    return {
      resultId: storedResult.id,
      stored: true,
      storedAt: storedResult.createdAt,
      ttl: storedResult.ttl
    };
  } catch (error) {
    logger.error('Failed to store custom result data', {
      error: error.message
    });
    
    return {
      stored: false,
      error: error.message
    };
  }
}

/**
 * Find stored results by tags
 * @param {Array<string>} tags - Tags to search for
 * @param {boolean} matchAll - Whether all tags must be present
 * @returns {Promise<Array>} Matching result metadata
 */
async function findResultsByTags(tags, matchAll = true) {
  return resultStore.getResultsByTags(tags, matchAll);
}

/**
 * Get result store statistics
 * @returns {Promise<Object>} Statistics about the result store
 */
async function getResultStoreStatistics() {
  return resultStore.getStatistics();
}

/**
 * Initialize the enhanced query service with result storage
 */
async function initialize() {
  await resultStore.initialize();
  logger.info('Enhanced query service with result storage initialized');
}

/**
 * Shutdown the enhanced query service
 */
async function shutdown() {
  await resultStore.close();
  logger.info('Enhanced query service with result storage shut down');
}

module.exports = {
  // Original functions
  ...queryService,
  
  // Enhanced functions with storage
  executeQueryWithStorage,
  executeQueryByIdWithStorage,
  executeAdhocQueryWithStorage,
  joinQueriesWithStorage,
  joinMultipleQueriesWithStorage,
  
  // Result storage specific functions
  getStoredResult,
  getStoredResultsByQueryId,
  getStoredResultsByExecutionId,
  deleteStoredResult,
  storeExistingResultData,
  findResultsByTags,
  getResultStoreStatistics,
  
  // Service lifecycle
  initialize,
  shutdown
};
