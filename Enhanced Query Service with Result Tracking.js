/**
 * Enhanced Query Service with Result Tracking
 * Extends the existing query service to mark previously returned results
 */
const queryManager = require('../db/query-manager');
const eventsService = require('./events-service');
const queryStore = require('./query-store');
const config = require('../config');
const logger = require('../utils/logger');
const crypto = require('crypto');

/**
 * Execute a long-running query with result tracking and marking
 * @param {string} sql - SQL query to execute
 * @param {Object} bindParams - Bind parameters
 * @param {Object} options - Query options
 * @param {Function} progressCallback - Optional callback for progress updates
 * @returns {Promise<Object>} Query results with marking information
 */
async function executeQueryWithTracking(sql, bindParams = {}, options = {}, progressCallback = null) {
  const trackingOptions = {
    enableTracking: true,
    markingStrategy: 'hash', // 'hash', 'composite', 'custom'
    hashFields: null, // null = all fields, array = specific fields
    customComparator: null, // custom function for comparison
    includeMarkingStats: true,
    ...options.tracking
  };

  // Create subscription to progress events if callback provided
  const progressSubscription = progressCallback ? 
    eventsService.subscribe('query:progress', progressCallback) : null;
  
  try {
    // Forward the query to the manager
    const result = await queryManager.executeLongQuery(sql, bindParams, options);
    
    let processedData = result.rows;
    let markingStats = null;

    // Process result tracking if enabled
    if (trackingOptions.enableTracking) {
      logger.info(`Processing result tracking for query with ${result.rows.length} rows`);
      
      const trackingResult = await processResultTracking(
        result.rows,
        sql,
        bindParams,
        trackingOptions
      );
      
      processedData = trackingResult.markedData;
      markingStats = trackingResult.stats;
      
      logger.info('Result tracking completed', {
        totalRows: processedData.length,
        newRows: markingStats.newRows,
        previouslySeenRows: markingStats.previouslySeenRows,
        executionsSearched: markingStats.executionsSearched
      });
    }

    // Return enhanced results
    return {
      success: true,
      queryId: result.queryId,
      data: processedData,
      metadata: result.metaData,
      stats: {
        rowCount: processedData.length,
        executionTime: result.metrics.duration,
        startTime: new Date(result.metrics.startTime).toISOString(),
        endTime: new Date(result.metrics.endTime).toISOString()
      },
      tracking: markingStats
    };
  } catch (error) {
    logger.error('Query execution with tracking failed', { 
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
 * Execute a predefined query by its ID with result tracking
 * @param {string} queryId - ID of the predefined query
 * @param {Object} bindParams - Override default bind parameters
 * @param {Object} options - Override default options
 * @param {Function} progressCallback - Optional callback for progress updates
 * @returns {Promise<Object>} Query results with tracking
 */
async function executeQueryByIdWithTracking(queryId, bindParams = {}, options = {}, progressCallback = null) {
  try {
    // Get the query definition from the store
    const query = await queryStore.getQueryById(queryId);
    
    // Merge bind parameters and options
    const mergedBindParams = { ...query.bindParams, ...bindParams };
    const mergedOptions = { ...query.options, ...options };
    
    // Log query execution
    logger.info(`Executing tracked query: ${query.name}`, {
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
    
    // Execute using the enhanced query executor with tracking
    const result = await executeQueryWithTracking(
      query.sql, 
      mergedBindParams, 
      mergedOptions, 
      executionProgressCallback
    );
    
    // Store results in the database
    if (result.success) {
      await queryStore.updateExecutionWithResults(execution.id, {
        status: 'completed',
        rowsFetched: result.stats.rowCount,
        executionTime: result.stats.executionTime,
        completedAt: result.stats.endTime,
        results: result.data, // Store the actual results
        tracking: result.tracking // Store tracking information
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
    logger.error(`Failed to execute tracked query: ${queryId}`, { error: error.message });
    
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Process result tracking by comparing against previous executions
 * @param {Array} currentResults - Current query results
 * @param {string} sql - SQL query
 * @param {Object} bindParams - Bind parameters
 * @param {Object} trackingOptions - Tracking configuration
 * @returns {Promise<Object>} Marked results and statistics
 */
async function processResultTracking(currentResults, sql, bindParams, trackingOptions) {
  const startTime = Date.now();
  
  // Generate a query signature for finding similar executions
  const querySignature = generateQuerySignature(sql, bindParams);
  
  // Get previous executions for this query signature
  const previousExecutions = await queryStore.getPreviousExecutionsWithResults(querySignature);
  
  logger.debug(`Found ${previousExecutions.length} previous executions to compare against`);
  
  // Create a lookup index from all previous results
  const previousResultsIndex = await buildPreviousResultsIndex(
    previousExecutions, 
    trackingOptions
  );
  
  // Mark current results
  const markedResults = [];
  let newCount = 0;
  let previouslySeenCount = 0;
  
  for (const currentRow of currentResults) {
    const rowSignature = generateRowSignature(currentRow, trackingOptions);
    const previousOccurrence = previousResultsIndex.get(rowSignature);
    
    const markedRow = {
      ...currentRow,
      _tracking: {
        isNew: !previousOccurrence,
        signature: rowSignature,
        firstSeen: previousOccurrence ? previousOccurrence.firstSeen : new Date().toISOString(),
        occurrenceCount: previousOccurrence ? previousOccurrence.count + 1 : 1,
        lastSeenBefore: previousOccurrence ? previousOccurrence.lastSeen : null,
        executionIds: previousOccurrence ? [...previousOccurrence.executionIds] : []
      }
    };
    
    if (previousOccurrence) {
      previouslySeenCount++;
    } else {
      newCount++;
    }
    
    markedResults.push(markedRow);
  }
  
  const processingTime = Date.now() - startTime;
  
  // Return marked results and statistics
  return {
    markedData: markedResults,
    stats: {
      totalRows: currentResults.length,
      newRows: newCount,
      previouslySeenRows: previouslySeenCount,
      executionsSearched: previousExecutions.length,
      processingTimeMs: processingTime,
      querySignature,
      newRowPercentage: Math.round((newCount / currentResults.length) * 100),
      trackingStrategy: trackingOptions.markingStrategy
    }
  };
}

/**
 * Build an index of all previous results for efficient lookup
 * @param {Array} previousExecutions - Previous execution records
 * @param {Object} trackingOptions - Tracking configuration
 * @returns {Promise<Map>} Index map for previous results
 */
async function buildPreviousResultsIndex(previousExecutions, trackingOptions) {
  const index = new Map();
  
  for (const execution of previousExecutions) {
    if (!execution.results || !Array.isArray(execution.results)) {
      continue;
    }
    
    for (const row of execution.results) {
      const signature = generateRowSignature(row, trackingOptions);
      
      if (index.has(signature)) {
        // Update existing entry
        const existing = index.get(signature);
        existing.count++;
        existing.lastSeen = execution.completed_at || execution.started_at;
        existing.executionIds.push(execution.id);
      } else {
        // Create new entry
        index.set(signature, {
          signature,
          count: 1,
          firstSeen: execution.completed_at || execution.started_at,
          lastSeen: execution.completed_at || execution.started_at,
          executionIds: [execution.id],
          sampleData: row // Keep a sample for debugging
        });
      }
    }
  }
  
  logger.debug(`Built index with ${index.size} unique row signatures`);
  return index;
}

/**
 * Generate a signature for a query to group similar executions
 * @param {string} sql - SQL query
 * @param {Object} bindParams - Bind parameters
 * @returns {string} Query signature
 */
function generateQuerySignature(sql, bindParams) {
  // Normalize SQL (remove extra whitespace, convert to lowercase)
  const normalizedSql = sql.replace(/\s+/g, ' ').trim().toLowerCase();
  
  // Create a signature that includes normalized SQL and parameter keys
  const paramKeys = Object.keys(bindParams || {}).sort();
  const signatureData = {
    sql: normalizedSql,
    paramKeys
  };
  
  return crypto
    .createHash('sha256')
    .update(JSON.stringify(signatureData))
    .digest('hex');
}

/**
 * Generate a signature for a row to enable comparison
 * @param {Object} row - Data row
 * @param {Object} trackingOptions - Tracking configuration
 * @returns {string} Row signature
 */
function generateRowSignature(row, trackingOptions) {
  let dataToHash;
  
  switch (trackingOptions.markingStrategy) {
    case 'hash':
      // Use specific fields or all fields
      if (trackingOptions.hashFields && Array.isArray(trackingOptions.hashFields)) {
        dataToHash = {};
        for (const field of trackingOptions.hashFields) {
          if (row.hasOwnProperty(field)) {
            dataToHash[field] = row[field];
          }
        }
      } else {
        // Use all fields except internal tracking fields
        dataToHash = {};
        for (const [key, value] of Object.entries(row)) {
          if (!key.startsWith('_')) {
            dataToHash[key] = value;
          }
        }
      }
      break;
      
    case 'composite':
      // Use a composite key of specific important fields
      const compositeFields = trackingOptions.compositeFields || ['id', 'name', 'email'];
      dataToHash = {};
      for (const field of compositeFields) {
        if (row.hasOwnProperty(field)) {
          dataToHash[field] = row[field];
        }
      }
      break;
      
    case 'custom':
      // Use custom comparator function
      if (typeof trackingOptions.customComparator === 'function') {
        try {
          dataToHash = trackingOptions.customComparator(row);
        } catch (error) {
          logger.error('Error in custom comparator', { error: error.message });
          // Fallback to hash strategy
          dataToHash = row;
        }
      } else {
        dataToHash = row;
      }
      break;
      
    default:
      dataToHash = row;
  }
  
  // Create hash
  return crypto
    .createHash('sha256')
    .update(JSON.stringify(dataToHash, Object.keys(dataToHash).sort()))
    .digest('hex');
}

/**
 * Get tracking statistics for a query
 * @param {string} queryId - Query ID
 * @param {Object} options - Options for statistics
 * @returns {Promise<Object>} Tracking statistics
 */
async function getTrackingStatistics(queryId, options = {}) {
  try {
    const executions = await queryStore.getQueryExecutionsWithResults(queryId);
    
    if (executions.length === 0) {
      return {
        success: true,
        statistics: {
          totalExecutions: 0,
          message: 'No executions found for this query'
        }
      };
    }
    
    // Analyze tracking data across executions
    const stats = {
      totalExecutions: executions.length,
      executionsWithResults: executions.filter(e => e.results && e.results.length > 0).length,
      totalUniqueRows: 0,
      averageNewRowsPerExecution: 0,
      executionTrends: [],
      topRepeatingRows: []
    };
    
    // Build a comprehensive index
    const globalRowIndex = new Map();
    
    for (const execution of executions) {
      if (!execution.results || !Array.isArray(execution.results)) continue;
      
      let newRowsInExecution = 0;
      let repeatingRowsInExecution = 0;
      
      for (const row of execution.results) {
        const signature = generateRowSignature(row, { markingStrategy: 'hash' });
        
        if (globalRowIndex.has(signature)) {
          const existing = globalRowIndex.get(signature);
          existing.count++;
          existing.lastSeen = execution.completed_at;
          existing.executionIds.push(execution.id);
          repeatingRowsInExecution++;
        } else {
          globalRowIndex.set(signature, {
            signature,
            count: 1,
            firstSeen: execution.completed_at,
            lastSeen: execution.completed_at,
            executionIds: [execution.id],
            sampleData: row
          });
          newRowsInExecution++;
        }
      }
      
      stats.executionTrends.push({
        executionId: execution.id,
        executionDate: execution.completed_at,
        totalRows: execution.results.length,
        newRows: newRowsInExecution,
        repeatingRows: repeatingRowsInExecution,
        newRowPercentage: Math.round((newRowsInExecution / execution.results.length) * 100)
      });
    }
    
    stats.totalUniqueRows = globalRowIndex.size;
    stats.averageNewRowsPerExecution = stats.executionTrends.reduce(
      (sum, trend) => sum + trend.newRows, 0
    ) / stats.executionTrends.length;
    
    // Find top repeating rows
    const sortedRows = Array.from(globalRowIndex.values())
      .sort((a, b) => b.count - a.count)
      .slice(0, 10);
    
    stats.topRepeatingRows = sortedRows.map(row => ({
      signature: row.signature.substring(0, 12) + '...',
      occurrenceCount: row.count,
      firstSeen: row.firstSeen,
      lastSeen: row.lastSeen,
      executionCount: row.executionIds.length,
      sampleData: options.includeSampleData ? row.sampleData : 'hidden'
    }));
    
    return {
      success: true,
      statistics: stats
    };
  } catch (error) {
    logger.error(`Error getting tracking statistics for query ${queryId}`, { error: error.message });
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Clean up old tracking data
 * @param {Object} options - Cleanup options
 * @returns {Promise<Object>} Cleanup results
 */
async function cleanupTrackingData(options = {}) {
  const cleanupOptions = {
    retentionDays: 30,
    maxExecutionsPerQuery: 100,
    removeResultsOlderThanDays: 7,
    ...options
  };
  
  try {
    logger.info('Starting tracking data cleanup', cleanupOptions);
    
    const cleanupResults = await queryStore.cleanupOldExecutions(cleanupOptions);
    
    logger.info('Tracking data cleanup completed', cleanupResults);
    
    return {
      success: true,
      cleanupResults
    };
  } catch (error) {
    logger.error('Error during tracking data cleanup', { error: error.message });
    return {
      success: false,
      error: error.message
    };
  }
}

module.exports = {
  executeQueryWithTracking,
  executeQueryByIdWithTracking,
  processResultTracking,
  getTrackingStatistics,
  cleanupTrackingData,
  generateQuerySignature,
  generateRowSignature
};
