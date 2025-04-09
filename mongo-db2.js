// src/queries/queryExecutor.js
const logger = require('../utils/logger');

/**
 * Executes queries against MongoDB with connection pooling best practices
 */
async function executeQueries(client, queries) {
  try {
    logger.info(`Executing ${Object.keys(queries).length} queries...`);
    
    const results = {};
    const startTime = Date.now();
    
    // Process queries in parallel if enabled
    const enableParallel = process.env.ENABLE_PARALLEL_QUERIES === 'true';
    
    if (enableParallel) {
      logger.info('Executing queries in parallel');
      
      // Create an array of query execution promises
      const queryPromises = Object.entries(queries).map(([queryName, queryConfig]) => 
        executeQuery(client, queryName, queryConfig)
          .then(result => ({ queryName, result }))
      );
      
      // Execute all queries in parallel
      const queryResults = await Promise.allSettled(queryPromises);
      
      // Process results
      for (const result of queryResults) {
        if (result.status === 'fulfilled') {
          const { queryName, result: queryResult } = result.value;
          results[queryName] = queryResult;
        } else {
          logger.error(`Error executing query: ${result.reason}`);
        }
      }
    } else {
      logger.info('Executing queries sequentially');
      
      // Execute queries sequentially
      for (const [queryName, queryConfig] of Object.entries(queries)) {
        try {
          results[queryName] = await executeQuery(client, queryName, queryConfig);
        } catch (error) {
          logger.error(`Error executing query ${queryName}: ${error.message}`);
          results[queryName] = { error: error.message };
        }
      }
    }
    
    const duration = Date.now() - startTime;
    logger.info(`Executed ${Object.keys(queries).length} queries in ${duration}ms`);
    
    return results;
  } catch (error) {
    logger.error(`Failed to execute queries: ${error.message}`);
    throw error;
  }
}

/**
 * Execute a single query with proper error handling
 */
async function executeQuery(client, queryName, queryConfig) {
  logger.info(`Executing query: ${queryName}`);
  const startTime = Date.now();
  
  try {
    const db = client.db(queryConfig.database);
    const collection = db.collection(queryConfig.collection);
    
    // Add execution options with timeouts
    const executionOptions = {
      maxTimeMS: parseInt(process.env.MONGODB_QUERY_TIMEOUT_MS || '30000', 10)
    };
    
    // Determine the operation to perform
    let result;
    switch (queryConfig.operation) {
      case 'find':
        result = await executeFindQuery(collection, queryConfig, executionOptions);
        break;
        
      case 'aggregate':
        result = await executeAggregateQuery(collection, queryConfig, executionOptions);
        break;
        
      case 'count':
        result = await executeCountQuery(collection, queryConfig, executionOptions);
        break;
        
      case 'distinct':
        result = await executeDistinctQuery(collection, queryConfig, executionOptions);
        break;
        
      default:
        throw new Error(`Unsupported operation: ${queryConfig.operation}`);
    }
    
    const duration = Date.now() - startTime;
    logger.info(`Query ${queryName} executed successfully in ${duration}ms, returned ${Array.isArray(result) ? result.length : 1} result(s)`);
    
    return result;
  } catch (error) {
    const duration = Date.now() - startTime;
    logger.error(`Error executing query ${queryName} after ${duration}ms: ${error.message}`);
    throw error;
  }
}

/**
 * Execute a find query with proper cursor handling
 */
async function executeFindQuery(collection, queryConfig, options) {
  try {
    const cursor = collection.find(queryConfig.filter || {}, options)
      .project(queryConfig.projection || {})
      .sort(queryConfig.sort || {});
    
    // Apply limit if specified
    if (queryConfig.limit) {
      cursor.limit(queryConfig.limit);
    }
    
    // Apply skip if specified (for pagination)
    if (queryConfig.skip) {
      cursor.skip(queryConfig.skip);
    }
    
    // Apply batch size for optimized fetching if specified
    if (queryConfig.batchSize) {
      cursor.batchSize(queryConfig.batchSize);
    }
    
    // Convert cursor to array
    const result = await cursor.toArray();
    
    // Always close the cursor to prevent resource leaks
    await cursor.close();
    
    return result;
  } catch (error) {
    logger.error(`Error in find query: ${error.message}`);
    throw error;
  }
}

/**
 * Execute an aggregate query with proper cursor handling
 */
async function executeAggregateQuery(collection, queryConfig, options) {
  try {
    const aggregateOptions = {
      ...options,
      allowDiskUse: queryConfig.allowDiskUse !== false // Enable disk use by default for large datasets
    };
    
    const cursor = collection.aggregate(queryConfig.pipeline || [], aggregateOptions);
    
    // Apply batch size for optimized fetching if specified
    if (queryConfig.batchSize) {
      cursor.batchSize(queryConfig.batchSize);
    }
    
    // Convert cursor to array
    const result = await cursor.toArray();
    
    // Always close the cursor to prevent resource leaks
    await cursor.close();
    
    return result;
  } catch (error) {
    logger.error(`Error in aggregate query: ${error.message}`);
    throw error;
  }
}

/**
 * Execute a count query
 */
async function executeCountQuery(collection, queryConfig, options) {
  try {
    return await collection.countDocuments(queryConfig.filter || {}, options);
  } catch (error) {
    logger.error(`Error in count query: ${error.message}`);
    throw error;
  }
}

/**
 * Execute a distinct query
 */
async function executeDistinctQuery(collection, queryConfig, options) {
  try {
    return await collection.distinct(queryConfig.field, queryConfig.filter || {}, options);
  } catch (error) {
    logger.error(`Error in distinct query: ${error.message}`);
    throw error;
  }
}

module.exports = {
  executeQueries
};