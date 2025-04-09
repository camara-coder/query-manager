// src/database/connection.js
const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');

// Global variable to store the connection pool
let client = null;

/**
 * Creates and returns a MongoDB connection pool
 * Configuration comes from environment variables
 */
async function connectToDatabase() {
  try {
    // If we already have a client, return it (connection pooling)
    if (client) {
      logger.info('Reusing existing MongoDB connection from the pool');
      return client;
    }

    logger.info('Setting up MongoDB connection pool...');
    
    const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017';
    const options = {
      useNewUrlParser: true,
      useUnifiedTopology: true,
      
      // Connection pooling configuration
      maxPoolSize: parseInt(process.env.MONGODB_MAX_POOL_SIZE || '10', 10),
      minPoolSize: parseInt(process.env.MONGODB_MIN_POOL_SIZE || '5', 10),
      maxIdleTimeMS: parseInt(process.env.MONGODB_MAX_IDLE_TIME_MS || '60000', 10),
      waitQueueTimeoutMS: parseInt(process.env.MONGODB_WAIT_QUEUE_TIMEOUT_MS || '5000', 10),
      connectTimeoutMS: parseInt(process.env.MONGODB_CONNECT_TIMEOUT_MS || '10000', 10),
      socketTimeoutMS: parseInt(process.env.MONGODB_SOCKET_TIMEOUT_MS || '45000', 10),
      
      // Retry configuration
      retryWrites: process.env.MONGODB_RETRY_WRITES !== 'false',
      retryReads: process.env.MONGODB_RETRY_READS !== 'false',
      
      // Heartbeat for detecting connection health
      heartbeatFrequencyMS: parseInt(process.env.MONGODB_HEARTBEAT_FREQUENCY_MS || '10000', 10),
      
      // Application name for monitoring/debugging
      appName: 'MongoDBReporter'
    };
    
    // Add SSL/TLS certificate if configured
    if (process.env.MONGODB_USE_TLS === 'true') {
      logger.info('Using TLS for MongoDB connection');
      
      options.tls = true;
      
      // CA Certificate
      if (process.env.MONGODB_CA_CERT) {
        const caPath = path.resolve(process.cwd(), process.env.MONGODB_CA_CERT);
        if (fs.existsSync(caPath)) {
          options.tlsCAFile = caPath;
          logger.info(`Using CA certificate: ${options.tlsCAFile}`);
        } else {
          logger.warn(`CA certificate not found: ${caPath}`);
        }
      }
      
      // Client Certificate
      if (process.env.MONGODB_CLIENT_CERT) {
        const certPath = path.resolve(process.cwd(), process.env.MONGODB_CLIENT_CERT);
        if (fs.existsSync(certPath)) {
          options.tlsCertificateKeyFile = certPath;
          logger.info(`Using client certificate: ${options.tlsCertificateKeyFile}`);
        } else {
          logger.warn(`Client certificate not found: ${certPath}`);
        }
      }
      
      // Client Key
      if (process.env.MONGODB_CLIENT_KEY) {
        const keyPath = path.resolve(process.cwd(), process.env.MONGODB_CLIENT_KEY);
        if (fs.existsSync(keyPath)) {
          options.tlsPrivateKey = fs.readFileSync(keyPath);
          logger.info('Loaded client private key');
        } else {
          logger.warn(`Client key not found: ${keyPath}`);
        }
      }
      
      // Key Passphrase
      if (process.env.MONGODB_KEY_PASSPHRASE) {
        options.tlsPrivateKeyPassword = process.env.MONGODB_KEY_PASSPHRASE;
        logger.info('Using private key passphrase');
      }
    }
    
    // Add authentication if configured
    if (process.env.MONGODB_USER && process.env.MONGODB_PASSWORD) {
      options.auth = {
        user: process.env.MONGODB_USER,
        password: process.env.MONGODB_PASSWORD
      };
      
      if (process.env.MONGODB_AUTH_SOURCE) {
        options.authSource = process.env.MONGODB_AUTH_SOURCE;
      }
      
      logger.info(`Authenticating as user: ${process.env.MONGODB_USER}`);
    }
    
    // Connect to MongoDB with connection pooling
    logger.info(`Connecting to MongoDB: ${uri}`);
    logger.info(`Connection pool config: max=${options.maxPoolSize}, min=${options.minPoolSize}`);
    
    client = new MongoClient(uri, options);
    
    // Connect to the server
    await client.connect();
    
    // Test connection
    await client.db('admin').command({ ping: 1 });
    logger.info('Successfully connected to MongoDB');
    
    // Set up connection monitoring
    monitorConnection(client);
    
    return client;
  } catch (error) {
    logger.error(`Failed to connect to MongoDB: ${error.message}`);
    throw error;
  }
}

/**
 * Monitor the MongoDB connection for events
 */
function monitorConnection(client) {
  const topology = client.topology;
  
  if (!topology) {
    logger.warn('Unable to access topology for connection monitoring');
    return;
  }
  
  // Connection pool created event
  topology.on('connectionPoolCreated', (event) => {
    logger.info(`Connection pool created: ${JSON.stringify(event)}`);
  });
  
  // Connection pool closed event
  topology.on('connectionPoolClosed', (event) => {
    logger.info(`Connection pool closed: ${JSON.stringify(event)}`);
  });
  
  // Connection created event
  topology.on('connectionCreated', (event) => {
    logger.debug(`Connection created: ${JSON.stringify(event)}`);
  });
  
  // Connection ready event
  topology.on('connectionReady', (event) => {
    logger.debug(`Connection ready: ${JSON.stringify(event)}`);
  });
  
  // Connection closed event
  topology.on('connectionClosed', (event) => {
    logger.debug(`Connection closed: ${JSON.stringify(event)}`);
  });
  
  // Connection pool cleared event
  topology.on('connectionPoolCleared', (event) => {
    logger.warn(`Connection pool cleared: ${JSON.stringify(event)}`);
  });
  
  // Server heartbeat succeeded
  topology.on('serverHeartbeatSucceeded', (event) => {
    logger.debug(`Server heartbeat succeeded: ${JSON.stringify({
      durationMS: event.duration,
      connectionId: event.connectionId
    })}`);
  });
  
  // Server heartbeat failed
  topology.on('serverHeartbeatFailed', (event) => {
    logger.warn(`Server heartbeat failed: ${JSON.stringify({
      durationMS: event.duration,
      connectionId: event.connectionId,
      error: event.failure
    })}`);
  });
  
  // Handle process termination - close connections gracefully
  process.on('SIGINT', async () => {
    logger.info('Process termination signal received, closing connection pool');
    await closeConnectionPool();
    process.exit(0);
  });
  
  process.on('SIGTERM', async () => {
    logger.info('Process termination signal received, closing connection pool');
    await closeConnectionPool();
    process.exit(0);
  });
}

/**
 * Close the MongoDB connection pool gracefully
 */
async function closeConnectionPool() {
  if (client) {
    logger.info('Closing MongoDB connection pool');
    try {
      await client.close(true);
      client = null;
      logger.info('MongoDB connection pool closed successfully');
    } catch (error) {
      logger.error(`Error closing MongoDB connection pool: ${error.message}`);
      throw error;
    }
  }
}

module.exports = {
  connectToDatabase,
  closeConnectionPool
};