/**
 * API Routes for stored query results
 * Extends the existing API with endpoints for results storage
 */
const express = require('express');
const enhancedQueryService = require('../services/enhanced-query-service');
const logger = require('../utils/logger');

// Create Express router
const router = express.Router();

// Get all stored results
router.get('/results', async (req, res) => {
  try {
    // Get filters from query params
    const filters = {
      tags: req.query.tags ? req.query.tags.split(',') : null,
      matchAllTags: req.query.matchAll !== 'false', // Default to true
      resultType: req.query.resultType
    };
    
    let results;
    
    // Filter by tags if provided
    if (filters.tags && filters.tags.length > 0) {
      results = await enhancedQueryService.findResultsByTags(filters.tags, filters.matchAllTags);
    } else {
      // TODO: Implement a proper paginated "get all" endpoint
      // For now, inform the client they need to provide a filter
      return res.status(400).json({
        error: 'Please provide at least one filter (tags, queryId, or executionId)'
      });
    }
    
    // Further filter by result type if provided
    if (filters.resultType && results) {
      results = results.filter(r => r.resultType === filters.resultType);
    }
    
    res.json(results);
  } catch (error) {
    logger.error('Error getting stored results', error);
    res.status(500).json({ error: error.message });
  }
});

// Get stored result by ID
router.get('/results/:id', async (req, res) => {
  try {
    const result = await enhancedQueryService.getStoredResult(req.params.id);
    
    if (!result) {
      return res.status(404).json({ error: 'Result not found' });
    }
    
    // Client can request metadata only for large results
    const metadataOnly = req.query.metadataOnly === 'true';
    
    if (metadataOnly) {
      // Return everything except the actual data
      const { data, ...metadata } = result;
      res.json({
        ...metadata,
        metadataOnly: true,
        dataAvailable: Array.isArray(data) && data.length > 0
      });
    } else {
      res.json(result);
    }
  } catch (error) {
    logger.error(`Error getting stored result ${req.params.id}`, error);
    res.status(error.message.includes('not found') ? 404 : 500).json({ error: error.message });
  }
});

// Get stored results by query ID
router.get('/queries/:id/results', async (req, res) => {
  try {
    const results = await enhancedQueryService.getStoredResultsByQueryId(req.params.id);
    res.json(results);
  } catch (error) {
    logger.error(`Error getting results for query ${req.params.id}`, error);
    res.status(500).json({ error: error.message });
  }
});

// Get stored results by execution ID
router.get('/executions/:id/results', async (req, res) => {
  try {
    const results = await enhancedQueryService.getStoredResultsByExecutionId(req.params.id);
    res.json(results);
  } catch (error) {
    logger.error(`Error getting results for execution ${req.params.id}`, error);
    res.status(500).json({ error: error.message });
  }
});

// Execute a predefined query with result storage
router.post('/queries/:id/execute-with-storage', async (req, res) => {
  try {
    const result = await enhancedQueryService.executeQueryByIdWithStorage(
      req.params.id,
      req.body.params || {},
      req.body.options || {}
    );
    
    if (!result.success) {
      return res.status(400).json({
        error: result.error || 'Query execution failed'
      });
    }
    
    // Determine response based on storage success
    const responseData = {
      executionId: result.executionId,
      status: 'executing',
      resultId: result.storage && result.storage.stored ? result.storage.resultId : null,
      storage: result.storage
    };
    
    // Return the result data if it's small, otherwise just return metadata
    const isSmallResult = !result.data || result.data.length <= 100;
    if (isSmallResult) {
      responseData.data = result.data;
      responseData.metadata = result.metadata;
      responseData.stats = result.stats;
    }
    
    res.json(responseData);
  } catch (error) {
    logger.error(`Error executing query ${req.params.id} with storage`, error);
    res.status(500).json({ error: error.message });
  }
});

// Execute an ad-hoc query with result storage
router.post('/queries/execute-adhoc-with-storage', async (req, res) => {
  try {
    const result = await enhancedQueryService.executeAdhocQueryWithStorage(
      req.body,
      req.body.save === true,
      req.body.options || {}
    );
    
    if (!result.success) {
      return res.status(400).json({
        error: result.error || 'Query execution failed'
      });
    }
    
    // Return full or limited result based on size
    const isSmallResult = !result.data || result.data.length <= 100;
    
    const responseData = {
      executionId: result.executionId || result.queryId,
      status: 'executing',
      resultId: result.storage && result.storage.stored ? result.storage.resultId : null,
      storage: result.storage
    };
    
    if (isSmallResult) {
      responseData.data = result.data;
      responseData.metadata = result.metadata;
      responseData.stats = result.stats;
    }
    
    res.json(responseData);
  } catch (error) {
    logger.error('Error executing ad-hoc query with storage', error);
    res.status(500).json({ error: error.message });
  }
});

// Join queries with result storage
router.post('/queries/join-with-storage', async (req, res) => {
  try {
    const { query1, query2, joinConditions, params1, params2, options } = req.body;
    
    if (!query1 || !query2 || !joinConditions) {
      return res.status(400).json({
        error: 'Missing required parameters: query1, query2, joinConditions'
      });
    }
    
    const result = await enhancedQueryService.joinQueriesWithStorage(
      query1,
      query2,
      joinConditions,
      params1 || {},
      params2 || {},
      options || {}
    );
    
    if (!result.success) {
      return res.status(400).json({
        error: result.error || 'Join operation failed'
      });
    }
    
    // Return metadata and storage info, but not the full data if it's large
    const responseData = {
      success: true,
      bestJoin: {
        name: result.bestJoin.name,
        count: result.bestJoin.count
      },
      metadata: result.metadata,
      resultId: result.storage && result.storage.stored ? result.storage.resultId : null,
      storage: result.storage
    };
    
    // Include data if result is small 
    if (result.bestJoin.data && result.bestJoin.data.length <= 100) {
      responseData.bestJoin.data = result.bestJoin.data;
    } else {
      responseData.bestJoin.dataAvailable = true;
      responseData.bestJoin.dataCount = result.bestJoin.data ? result.bestJoin.data.length : 0;
    }
    
    res.json(responseData);
  } catch (error) {
    logger.error('Error joining queries with storage', error);
    res.status(500).json({ error: error.message });
  }
});

// Join multiple queries with result storage
router.post('/queries/join-multiple-with-storage', async (req, res) => {
  try {
    const { querySpecs, joinSpecs, options } = req.body;
    
    if (!querySpecs || !joinSpecs) {
      return res.status(400).json({
        error: 'Missing required parameters: querySpecs, joinSpecs'
      });
    }
    
    const result = await enhancedQueryService.joinMultipleQueriesWithStorage(
      querySpecs,
      joinSpecs,
      options || {}
    );
    
    if (!result.success) {
      return res.status(400).json({
        error: result.error || 'Multi-join operation failed'
      });
    }
    
    // Return metadata and storage info, but not the full data if it's large
    const responseData = {
      success: true,
      metadata: result.metadata,
      resultId: result.storage && result.storage.stored ? result.storage.resultId : null,
      storage: result.storage
    };
    
    // Include data if result is small 
    if (result.data && result.data.length <= 100) {
      responseData.data = result.data;
    } else {
      responseData.dataAvailable = true;
      responseData.dataCount = result.data ? result.data.length : 0;
    }
    
    res.json(responseData);
  } catch (error) {
    logger.error('Error joining multiple queries with storage', error);
    res.status(500).json({ error: error.message });
  }
});

// Store custom result data
router.post('/results', async (req, res) => {
  try {
    const { data, metadata, options } = req.body;
    
    if (!data) {
      return res.status(400).json({
        error: 'Missing required parameter: data'
      });
    }
    
    const result = await enhancedQueryService.storeExistingResultData(
      {
        data,
        metadata: metadata || {},
        queryId: req.body.queryId,
        executionId: req.body.executionId
      },
      options || {}
    );
    
    if (!result.stored) {
      return res.status(400).json({
        error: result.error || 'Failed to store result data'
      });
    }
    
    res.status(201).json(result);
  } catch (error) {
    logger.error('Error storing custom result data', error);
    res.status(500).json({ error: error.message });
  }
});

// Update result metadata
router.patch('/results/:id', async (req, res) => {
  try {
    const updateData = {
      tags: req.body.tags,
      ttlSeconds: req.body.ttlSeconds,
      metadata: req.body.metadata
    };
    
    const result = await enhancedQueryService.resultStore.updateResultMetadata(
      req.params.id,
      updateData
    );
    
    res.json(result);
  } catch (error) {
    logger.error(`Error updating result ${req.params.id}`, error);
    res.status(error.message.includes('not found') ? 404 : 500).json({ error: error.message });
  }
});

// Delete a stored result
router.delete('/results/:id', async (req, res) => {
  try {
    const success = await enhancedQueryService.deleteStoredResult(req.params.id);
    
    if (success) {
      res.status(204).end();
    } else {
      res.status(404).json({ error: 'Result not found or could not be deleted' });
    }
  } catch (error) {
    logger.error(`Error deleting result ${req.params.id}`, error);
    res.status(500).json({ error: error.message });
  }
});

// Get result store statistics
router.get('/results/statistics', async (req, res) => {
  try {
    const stats = await enhancedQueryService.getResultStoreStatistics();
    res.json(stats);
  } catch (error) {
    logger.error('Error getting result store statistics', error);
    res.status(500).json({ error: error.message });
  }
});

// Clean up expired results (admin use)
router.post('/results/cleanup', async (req, res) => {
  try {
    const deletedCount = await enhancedQueryService.resultStore.cleanupExpiredResults();
    res.json({
      success: true,
      deletedCount
    });
  } catch (error) {
    logger.error('Error cleaning up expired results', error);
    res.status(500).json({ error: error.message });
  }
});

// Export the router
module.exports = router;
