/**
 * Express server for Oracle Query Manager API
 * Connects the frontend UI with the Oracle Long Query Manager backend
 */
const express = require('express');
const cors = require('cors');
const { initialize, shutdown, queryService } = require('./src');
const logger = require('./src/utils/logger');
const path = require('path');

// Create Express app
const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json());

// Request logging middleware
app.use((req, res, next) => {
  logger.debug(`${req.method} ${req.url}`);
  next();
});

// API Routes
const apiRouter = express.Router();

// Health check endpoint
apiRouter.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

// ======= Query Routes =======

// Get all queries
apiRouter.get('/queries', async (req, res) => {
  try {
    const queries = await queryService.getAllQueries();
    res.json(queries);
  } catch (error) {
    logger.error('Error getting queries', error);
    res.status(500).json({ error: error.message });
  }
});

// Get a specific query
apiRouter.get('/queries/:id', async (req, res) => {
  try {
    const query = await queryService.getQueryById(req.params.id);
    if (!query) {
      return res.status(404).json({ error: 'Query not found' });
    }
    res.json(query);
  } catch (error) {
    logger.error(`Error getting query ${req.params.id}`, error);
    res.status(error.message.includes('not found') ? 404 : 500).json({ error: error.message });
  }
});

// Create a new query
apiRouter.post('/queries', async (req, res) => {
  try {
    const newQuery = await queryService.createQuery(req.body);
    res.status(201).json(newQuery);
  } catch (error) {
    logger.error('Error creating query', error);
    res.status(400).json({ error: error.message });
  }
});

// Update a query
apiRouter.put('/queries/:id', async (req, res) => {
  try {
    const updatedQuery = await queryService.updateQuery(req.params.id, req.body);
    res.json(updatedQuery);
  } catch (error) {
    logger.error(`Error updating query ${req.params.id}`, error);
    res.status(error.message.includes('not found') ? 404 : 400).json({ error: error.message });
  }
});

// Delete a query
apiRouter.delete('/queries/:id', async (req, res) => {
  try {
    await queryService.deleteQuery(req.params.id);
    res.status(204).end();
  } catch (error) {
    logger.error(`Error deleting query ${req.params.id}`, error);
    res.status(error.message.includes('not found') ? 404 : 500).json({ error: error.message });
  }
});

// Execute a query
apiRouter.post('/queries/:id/execute', async (req, res) => {
  try {
    const result = await queryService.executeQueryById(
      req.params.id,
      req.body.params || {}
    );
    
    if (!result.success) {
      return res.status(400).json({
        error: result.error || 'Query execution failed'
      });
    }
    
    res.json({
      executionId: result.executionId,
      status: 'executing'
    });
  } catch (error) {
    logger.error(`Error executing query ${req.params.id}`, error);
    res.status(500).json({ error: error.message });
  }
});

// Execute an ad-hoc query
apiRouter.post('/queries/execute-adhoc', async (req, res) => {
  try {
    const result = await queryService.executeAdhocQuery(req.body, req.body.save === true);
    
    if (!result.success) {
      return res.status(400).json({
        error: result.error || 'Query execution failed'
      });
    }
    
    res.json({
      executionId: result.executionId || result.queryId,
      status: 'executing',
      data: result.data,
      metadata: result.metadata,
      stats: result.stats
    });
  } catch (error) {
    logger.error('Error executing ad-hoc query', error);
    res.status(500).json({ error: error.message });
  }
});

// Get query execution status
apiRouter.get('/queries/:id/status/:executionId', async (req, res) => {
  try {
    const status = await queryService.getExecutionStatus(
      req.params.id,
      req.params.executionId
    );
    res.json(status);
  } catch (error) {
    logger.error(`Error getting execution status for ${req.params.executionId}`, error);
    res.status(error.message.includes('not found') ? 404 : 500).json({ error: error.message });
  }
});

// Cancel a query
apiRouter.post('/queries/:id/cancel/:executionId', async (req, res) => {
  try {
    const success = await queryService.cancelQuery(
      req.params.id,
      req.params.executionId
    );
    
    if (!success) {
      return res.status(400).json({ error: 'Failed to cancel query' });
    }
    
    res.json({ success: true });
  } catch (error) {
    logger.error(`Error cancelling query ${req.params.id}`, error);
    res.status(500).json({ error: error.message });
  }
});

// Get query results
apiRouter.get('/queries/:id/results/:executionId', async (req, res) => {
  try {
    const results = await queryService.getExecutionResults(
      req.params.id,
      req.params.executionId
    );
    
    if (!results.success) {
      return res.status(404).json({ error: results.error || 'Results not found' });
    }
    
    res.json(results);
  } catch (error) {
    logger.error(`Error getting results for ${req.params.executionId}`, error);
    res.status(500).json({ error: error.message });
  }
});

// Get query execution history
apiRouter.get('/queries/:id/history', async (req, res) => {
  try {
    const history = await queryService.getQueryExecutionHistory(req.params.id);
    res.json(history);
  } catch (error) {
    logger.error(`Error getting history for query ${req.params.id}`, error);
    res.status(500).json({ error: error.message });
  }
});

// Mount API router
app.use('/api', apiRouter);

// Serve static frontend files in production
if (process.env.NODE_ENV === 'production') {
  const frontendPath = process.env.FRONTEND_PATH || path.join(__dirname, 'frontend/build');
  app.use(express.static(frontendPath));
  
  // Handle React routing, return all non-API requests to React app
  app.get('*', (req, res) => {
    res.sendFile(path.join(frontendPath, 'index.html'));
  });
}

// Error handling middleware
app.use((err, req, res, next) => {
  logger.error('Unhandled error', err);
  res.status(500).json({
    error: 'Internal Server Error',
    message: process.env.NODE_ENV === 'production' ? undefined : err.message
  });
});

// Start the server
let server;

async function startServer() {
  try {
    // Initialize the Oracle Long Query Manager
    await initialize();
    logger.info('Oracle Long Query Manager initialized');
    
    // Start Express server
    server = app.listen(PORT, () => {
      logger.info(`Server running on port ${PORT}`);
    });
  } catch (error) {
    logger.error('Failed to start server', error);
    process.exit(1);
  }
}

// Graceful shutdown
async function shutdownServer() {
  logger.info('Shutting down server');
  
  if (server) {
    server.close(() => {
      logger.info('Express server closed');
    });
  }
  
  try {
    await shutdown();
    logger.info('Oracle Long Query Manager shut down successfully');
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown', error);
    process.exit(1);
  }
}

// Handle process signals for graceful shutdown
process.on('SIGINT', shutdownServer);
process.on('SIGTERM', shutdownServer);

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  logger.error('Uncaught exception', error);
  shutdownServer();
});

// Start the server
startServer();