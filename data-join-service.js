/**
 * Data Join Service with Fuzzy Matching
 * Provides functionality for joining multiple query result sets
 * with configurable join conditions, types, and fuzzy matching
 */
const queryService = require('./query-service');
const eventsService = require('./events-service');
const logger = require('../utils/logger');
const PerformanceTracker = require('../utils/performance-tracker');
const config = require('../config');
const Fuse = require('fuse.js');

class DataJoinService {
  constructor() {
    this.performanceTracker = new PerformanceTracker({ component: 'data-join-service' });
    
    // Default options for fuzzy matching
    this.defaultFuzzyOptions = {
      enabled: false,           // Whether to enable fuzzy matching
      threshold: 0.3,           // Default threshold (0.0 = perfect match, 1.0 = anything matches)
      ignoreLocation: true,     // By default, ignore where in the string the match appears
      useExtendedSearch: true,  // Enable extended search for more powerful queries
      minMatchCharLength: 2,    // Minimum match character length
      distance: 100,            // Maximum edit distance
      maxPatternLength: 32,     // Maximum pattern length
      includeScore: true,       // Include score in result
      includeMatches: false,    // Include match indices in result (useful for highlighting)
      sortFuzzyResults: true,   // Sort fuzzy results by score
      maxResultsPerKey: 5,      // Maximum number of fuzzy results per key
      keySeparator: '.',        // Separator for nested keys
      findAllMatches: false,    // Find all matches rather than stopping at first match
      isCaseSensitive: false,   // Case sensitive matching
      shouldSort: true,         // Sort results by score
      useOrLogic: false,        // Use OR logic instead of AND for multiple conditions
      includeScoreInResult: false, // Include score in the joined row
      tokenize: false,          // Tokenize the search query
      matchAllTokens: false     // Match all tokens when tokenizing
    };
  }
  
  /**
   * Initialize the data join service
   * Sets up event handlers and subscriptions
   */
  async initialize() {
    // Subscribe to query events
    eventsService.subscribe('query:complete', this._handleQueryComplete.bind(this));
    
    // Publish service events
    eventsService.publish('service:initialized', { 
      service: 'data-join-service',
      timestamp: new Date().toISOString()
    });
    
    logger.info('Data join service initialized with Fuse.js fuzzy matching capability');
    return true;
  }
  
  /**
   * Shutdown the data join service
   */
  async shutdown() {
    logger.info('Data join service shutdown');
    return true;
  }
  
  /**
   * Join two queries based on specified join conditions with support for fuzzy matching
   * @param {string|Object} query1 - First query ID or SQL query object
   * @param {string|Object} query2 - Second query ID or SQL query object
   * @param {Array} joinConditions - Array of join conditions or strategies
   * @param {Object} params1 - Parameters for first query
   * @param {Object} params2 - Parameters for second query
   * @param {Object} options - Join options
   * @returns {Promise<Object>} Join results
   */
  async joinQueries(query1, query2, joinConditions, params1 = {}, params2 = {}, options = {}) {
    const operationId = this.performanceTracker.startOperation('join-queries', {
      joinConditionCount: Array.isArray(joinConditions) ? joinConditions.length : 'multiple-strategies'
    });
    
    try {
      // Get join strategies from configuration
      const joinStrategies = this._getJoinStrategies(joinConditions);
      
      // Log start of join operation
      const query1Name = typeof query1 === 'string' ? query1 : 'custom-sql';
      const query2Name = typeof query2 === 'string' ? query2 : 'custom-sql';
      
      logger.info('Starting join operation between two queries', {
        query1: query1Name,
        query2: query2Name,
        strategiesCount: joinStrategies.length
      });
      
      // Execute both queries in parallel
      const [result1, result2] = await Promise.all([
        this._executeQuery(query1, params1, options.query1Options || {}),
        this._executeQuery(query2, params2, options.query2Options || {})
      ]);
      
      if (!result1.success || !result2.success) {
        const errorMsg = !result1.success 
          ? `Error executing first query: ${result1.error}` 
          : `Error executing second query: ${result2.error}`;
        
        throw new Error(errorMsg);
      }
      
      // Validate result datasets
      this._validateDatasets([result1.data, result2.data]);
      
      // Record checkpoint after queries executed
      this.performanceTracker.recordCheckpoint(operationId, 'queries-executed', {
        query1RowCount: result1.data.length,
        query2RowCount: result2.data.length
      });
      
      // Try each join strategy and keep track of results
      const joinResults = {};
      let bestJoinName = null;
      let bestJoinCount = -1;
      
      for (const strategy of joinStrategies) {
        const strategyStartTime = Date.now();
        
        logger.debug(`Starting join strategy "${strategy.name}"`, {
          type: strategy.type || 'inner',
          conditionCount: strategy.conditions.length,
          fuzzyEnabled: strategy.fuzzyOptions?.enabled || false
        });
        
        // Merge fuzzy options from strategy with defaults
        const fuzzyOptions = {
          ...this.defaultFuzzyOptions,
          ...options.fuzzyOptions,
          ...strategy.fuzzyOptions
        };
        
        // Perform join with this strategy
        const joinedRows = await this._performJoin(
          result1.data, 
          result2.data,
          strategy.conditions,
          strategy.type || options.joinType || 'inner',
          operationId,
          strategy.transform,
          fuzzyOptions
        );
        
        // Record performance metrics for this strategy
        this.performanceTracker.recordCheckpoint(operationId, `strategy-${strategy.name}-completed`, {
          executionTimeMs: Date.now() - strategyStartTime,
          rowCount: joinedRows.length,
          matchRatio: joinedRows.length / Math.max(result1.data.length, result2.data.length)
        });
        
        // Store results
        joinResults[strategy.name] = {
          count: joinedRows.length,
          data: joinedRows,
          executionTimeMs: Date.now() - strategyStartTime,
          totalPossibleMatches: Math.max(result1.data.length, result2.data.length)
        };
        
        // Update best join if this one is better
        if (joinedRows.length > bestJoinCount) {
          bestJoinName = strategy.name;
          bestJoinCount = joinedRows.length;
        }
        
        logger.debug(`Join strategy "${strategy.name}" completed with ${joinedRows.length} matches`);
      }
      
      // Complete performance tracking
      const metrics = this.performanceTracker.completeOperation(operationId, 'completed', {
        bestStrategy: bestJoinName,
        bestStrategyMatchCount: bestJoinCount
      });
      
      // Log completion
      logger.info('Join operation completed successfully', {
        bestStrategy: bestJoinName,
        bestStrategyMatchCount: bestJoinCount,
        totalExecutionTimeMs: metrics.duration
      });
      
      // Return results
      return {
        success: true,
        joinResults,
        bestJoin: {
          name: bestJoinName,
          count: bestJoinCount,
          data: bestJoinName ? joinResults[bestJoinName].data : []
        },
        metadata: {
          query1: {
            id: typeof query1 === 'string' ? query1 : null,
            rowCount: result1.data.length,
            metadata: result1.metadata
          },
          query2: {
            id: typeof query2 === 'string' ? query2 : null,
            rowCount: result2.data.length,
            metadata: result2.metadata
          },
          metrics
        }
      };
    } catch (error) {
      logger.error('Error joining query results', { error: error.message, stack: error.stack });
      this.performanceTracker.completeOperation(operationId, 'error', {
        error: error.message
      });
      
      return {
        success: false,
        error: error.message
      };
    }
  }
  
  /**
   * Join multiple queries (3 or more) with specified join conditions
   * @param {Array<Object>} querySpecs - Array of query specifications
   * @param {Array<Object>} joinSpecs - Array of join specifications
   * @param {Object} options - Join options
   * @returns {Promise<Object>} Join results
   */
  async joinMultipleQueries(querySpecs, joinSpecs, options = {}) {
    if (querySpecs.length < 2) {
      return {
        success: false,
        error: 'At least two queries are required for joining'
      };
    }
    
    if (querySpecs.length - 1 !== joinSpecs.length) {
      return {
        success: false,
        error: 'There must be exactly one fewer join specification than the number of queries'
      };
    }
    
    const operationId = this.performanceTracker.startOperation('multi-way-join', {
      queryCount: querySpecs.length,
      joinCount: joinSpecs.length
    });
    
    try {
      // Execute all queries in parallel
      logger.info(`Starting multi-way join with ${querySpecs.length} queries`);
      
      const queryPromises = querySpecs.map(spec => 
        this._executeQuery(spec.query, spec.params || {}, spec.options || {})
      );
      
      const queryResults = await Promise.all(queryPromises);
      
      // Check if any query failed
      const failedQueryIndex = queryResults.findIndex(r => !r.success);
      if (failedQueryIndex >= 0) {
        throw new Error(`Query #${failedQueryIndex + 1} execution failed: ${queryResults[failedQueryIndex].error}`);
      }
      
      // Validate all datasets
      const datasets = queryResults.map(result => result.data);
      this._validateDatasets(datasets);
      
      this.performanceTracker.recordCheckpoint(operationId, 'all-queries-executed', {
        rowCounts: datasets.map(d => d.length)
      });
      
      // Perform multi-way join
      let currentResult = datasets[0];
      let currentMetadata = queryResults[0].metadata;
      
      // Progressively join with each subsequent dataset
      for (let i = 1; i < datasets.length; i++) {
        const nextDataset = datasets[i];
        const joinSpec = joinSpecs[i - 1];
        
        // Validate join spec
        if (!joinSpec.conditions || !Array.isArray(joinSpec.conditions)) {
          throw new Error(`Join specification #${i} is missing valid conditions array`);
        }
        
        this.performanceTracker.recordCheckpoint(operationId, `join-step-${i}-start`, {
          leftSize: currentResult.length,
          rightSize: nextDataset.length,
          conditionCount: joinSpec.conditions.length
        });
        
        // Merge fuzzy options
        const fuzzyOptions = {
          ...this.defaultFuzzyOptions,
          ...options.fuzzyOptions,
          ...joinSpec.fuzzyOptions
        };
        
        // Perform join between current result and next dataset
        currentResult = await this._performJoin(
          currentResult,
          nextDataset,
          joinSpec.conditions,
          joinSpec.type || 'inner',
          operationId,
          joinSpec.transform,
          fuzzyOptions
        );
        
        this.performanceTracker.recordCheckpoint(operationId, `join-step-${i}-complete`, {
          resultSize: currentResult.length
        });
        
        // Combine metadata
        currentMetadata = this._combineMetadata(currentMetadata, queryResults[i].metadata);
      }
      
      // Complete tracking
      const metrics = this.performanceTracker.completeOperation(operationId, 'completed', {
        resultRowCount: currentResult.length
      });
      
      logger.info('Multi-way join completed successfully', {
        initialRowCounts: datasets.map(d => d.length),
        finalRowCount: currentResult.length,
        executionTimeMs: metrics.duration
      });
      
      return {
        success: true,
        data: currentResult,
        metadata: {
          queryCount: querySpecs.length,
          initialRowCounts: datasets.map(d => d.length),
          finalRowCount: currentResult.length,
          combinedMetadata: currentMetadata,
          metrics
        }
      };
    } catch (error) {
      logger.error('Error performing multi-way join', { error: error.message, stack: error.stack });
      this.performanceTracker.completeOperation(operationId, 'error', {
        error: error.message
      });
      
      return {
        success: false,
        error: error.message
      };
    }
  }
  
  /**
   * Execute a query by ID or SQL
   * @param {string|Object} query - Query ID or SQL query object
   * @param {Object} params - Query parameters
   * @param {Object} options - Query options
   * @returns {Promise<Object>} Query result
   * @private
   */
  async _executeQuery(query, params = {}, options = {}) {
    try {
      if (typeof query === 'string') {
        // Query ID provided
        return queryService.executeQueryById(query, params, options);
      } else {
        // SQL query object provided
        return queryService.executeQuery(query.sql, params, options);
      }
    } catch (error) {
      logger.error('Error executing query', { error: error.message, stack: error.stack });
      return {
        success: false,
        error: error.message
      };
    }
  }
  
  /**
   * Normalize join conditions into a standardized array of join strategies
   * @param {Array|Object} joinConfig - Join conditions or strategies configuration
   * @returns {Array} Normalized array of join strategies
   * @private
   */
  _getJoinStrategies(joinConfig) {
    // Case 1: Array of strategies with conditions
    if (Array.isArray(joinConfig) && joinConfig.length > 0 && joinConfig[0].conditions) {
      return joinConfig;
    }
    
    // Case 2: Array of simple condition objects (legacy format)
    if (Array.isArray(joinConfig) && joinConfig.length > 0 && (joinConfig[0].leftKey || joinConfig[0].name)) {
      // Convert legacy format to new format
      return joinConfig.map(condition => {
        if (condition.leftKey && condition.rightKey) {
          // Single condition format
          return {
            name: condition.name || `join_${condition.leftKey}_${condition.rightKey}`,
            conditions: [{ 
              leftKey: condition.leftKey, 
              rightKey: condition.rightKey 
            }],
            type: condition.type || 'inner',
            transform: condition.transform,
            fuzzyOptions: condition.fuzzyOptions
          };
        } else if (condition.name && condition.conditions) {
          // Already in correct format
          return condition;
        } else {
          throw new Error(`Invalid join condition format: ${JSON.stringify(condition)}`);
        }
      });
    }
    
    // Case 3: Single strategy object
    if (!Array.isArray(joinConfig) && joinConfig.conditions) {
      return [joinConfig];
    }
    
    // Invalid format
    throw new Error(`Invalid join configuration format: ${JSON.stringify(joinConfig)}`);
  }
  
  /**
   * Perform a join between two datasets with specific conditions
   * @param {Array} leftDataset - Left dataset rows
   * @param {Array} rightDataset - Right dataset rows
   * @param {Array} joinConditions - Join conditions
   * @param {string} joinType - Type of join ('inner', 'left', 'right', 'full')
   * @param {string} operationId - ID for performance tracking
   * @param {Function} transform - Optional transform function for joined rows
   * @param {Object} fuzzyOptions - Options for fuzzy matching
   * @returns {Array} Joined rows
   * @private
   */
  async _performJoin(leftDataset, rightDataset, joinConditions, joinType, operationId, transform, fuzzyOptions = {}) {
    if (!Array.isArray(leftDataset) || !Array.isArray(rightDataset)) {
      throw new Error('Invalid datasets provided for join operation');
    }
    
    if (!Array.isArray(joinConditions) || joinConditions.length === 0) {
      throw new Error('Invalid or empty join conditions');
    }
    
    const startTime = Date.now();
    let joinMethod = '_performExactJoin';
    
    // Choose join method based on options
    if (fuzzyOptions.enabled) {
      joinMethod = '_performFuzzyJoin';
      
      this.performanceTracker.recordCheckpoint(operationId, 'join-method-selected', {
        method: 'fuzzy',
        threshold: fuzzyOptions.threshold,
        leftSize: leftDataset.length,
        rightSize: rightDataset.length
      });
    } else {
      this.performanceTracker.recordCheckpoint(operationId, 'join-method-selected', {
        method: 'exact',
        leftSize: leftDataset.length,
        rightSize: rightDataset.length
      });
    }
    
    // Execute the selected join method
    const joinedRows = await this[joinMethod](
      leftDataset,
      rightDataset,
      joinConditions,
      joinType,
      transform,
      fuzzyOptions
    );
    
    this.performanceTracker.recordCheckpoint(operationId, 'join-performed', {
      method: fuzzyOptions.enabled ? 'fuzzy' : 'exact',
      matchCount: joinedRows.length,
      leftSize: leftDataset.length,
      rightSize: rightDataset.length,
      joinType,
      executionTimeMs: Date.now() - startTime
    });
    
    return joinedRows;
  }
  
  /**
   * Perform an exact join between two datasets
   * @param {Array} leftDataset - Left dataset rows
   * @param {Array} rightDataset - Right dataset rows
   * @param {Array} joinConditions - Join conditions
   * @param {string} joinType - Type of join ('inner', 'left', 'right', 'full')
   * @param {Function} transform - Optional transform function
   * @returns {Array} Joined rows
   * @private
   */
  async _performExactJoin(leftDataset, rightDataset, joinConditions, joinType, transform) {
    // Create lookup map from right dataset
    const lookupMap = new Map();
    
    // Build lookup map using all specified join keys
    for (const rightRow of rightDataset) {
      // Create a composite key from all join conditions
      const key = this._createMultiConditionKey(rightRow, joinConditions, 'right');
      
      if (!lookupMap.has(key)) {
        lookupMap.set(key, []);
      }
      lookupMap.get(key).push(rightRow);
    }
    
    // Track which right rows were matched (for right/full joins)
    const matchedRightRows = new Set();
    
    // Perform the join
    const joinedRows = [];
    
    for (const leftRow of leftDataset) {
      const key = this._createMultiConditionKey(leftRow, joinConditions, 'left');
      const matches = lookupMap.get(key) || [];
      
      if (matches.length > 0) {
        // We found matches - create joined rows
        for (const rightRow of matches) {
          // Double-check all conditions match (for safety)
          if (this._compareJoinConditions(joinConditions, leftRow, rightRow)) {
            joinedRows.push(this._mergeRows(leftRow, rightRow, transform));
            
            // Mark this right row as matched
            if (joinType === 'right' || joinType === 'full') {
              matchedRightRows.add(rightRow);
            }
          }
        }
      } else if (joinType === 'left' || joinType === 'full') {
        // Left join - include row with null values for right side
        joinedRows.push(this._mergeRows(leftRow, null, transform));
      }
    }
    
    // For right or full outer joins, add right rows that didn't match
    if (joinType === 'right' || joinType === 'full') {
      for (const rightRow of rightDataset) {
        if (!matchedRightRows.has(rightRow)) {
          joinedRows.push(this._mergeRows(null, rightRow, transform));
        }
      }
    }
    
    return joinedRows;
  }
  
  /**
   * Perform a fuzzy join between two datasets
   * @param {Array} leftDataset - Left dataset rows
   * @param {Array} rightDataset - Right dataset rows
   * @param {Array} joinConditions - Join conditions
   * @param {string} joinType - Type of join ('inner', 'left', 'right', 'full')
   * @param {Function} transform - Optional transform function
   * @param {Object} fuzzyOptions - Options for fuzzy matching
   * @returns {Array} Joined rows
   * @private
   */
  async _performFuzzyJoin(leftDataset, rightDataset, joinConditions, joinType, transform, fuzzyOptions) {
    // Create Fuse.js index for the right dataset
    const {fuseIndex, rightRowMap} = this._createFuzzyIndex(rightDataset, joinConditions, fuzzyOptions);
    
    // Track which right rows were matched (for right/full joins)
    const matchedRightRows = new Set();
    
    // Perform the join
    const joinedRows = [];
    
    // For each left row, find fuzzy matches in right dataset
    for (const leftRow of leftDataset) {
      // Create a search pattern for each condition
      const searchPatterns = joinConditions.map(condition => {
        return {
          key: condition.rightKey,
          value: this._getNestedProperty(leftRow, condition.leftKey),
          condition
        };
      }).filter(pattern => pattern.value !== null && pattern.value !== undefined);
      
      // Skip if no valid search patterns
      if (searchPatterns.length === 0) {
        if (joinType === 'left' || joinType === 'full') {
          joinedRows.push(this._mergeRows(leftRow, null, transform));
        }
        continue;
      }
      
      // Find fuzzy matches for each condition
      const conditionMatches = [];
      for (const pattern of searchPatterns) {
        // Skip invalid patterns
        if (pattern.value === null || pattern.value === undefined) continue;
        
        // Search fuse index
        let searchValue = pattern.value;
        if (typeof searchValue !== 'string') {
          searchValue = String(searchValue);
        }
        
        const fuseOptions = {
          keys: [pattern.key],
          threshold: fuzzyOptions.threshold,
          includeScore: true,
          includeMatches: fuzzyOptions.includeMatches,
          findAllMatches: fuzzyOptions.findAllMatches,
          isCaseSensitive: fuzzyOptions.isCaseSensitive,
          ignoreLocation: fuzzyOptions.ignoreLocation,
          useExtendedSearch: fuzzyOptions.useExtendedSearch,
          minMatchCharLength: fuzzyOptions.minMatchCharLength,
          shouldSort: fuzzyOptions.shouldSort,
          location: 0,
          distance: fuzzyOptions.distance
        };
        
        // Create Fuse instance for this specific key
        const fuse = new Fuse(rightDataset, fuseOptions, fuseIndex);
        const matches = fuse.search(searchValue);
        
        // Add matches to condition results
        if (matches.length > 0) {
          conditionMatches.push({
            condition: pattern.condition,
            matches: matches.slice(0, fuzzyOptions.maxResultsPerKey)
          });
        }
      }
      
      // Handle cases with matches
      if (conditionMatches.length > 0) {
        // Find rows that match all conditions (AND logic)
        // or at least one condition if useOrLogic is true
        const allMatchingRows = new Map();
        
        // Process each condition's matches
        for (const {condition, matches} of conditionMatches) {
          for (const match of matches) {
            const rightRow = match.item;
            const matchScore = match.score;
            
            // First condition or using OR logic
            if (!allMatchingRows.has(rightRow) || fuzzyOptions.useOrLogic) {
              allMatchingRows.set(rightRow, {
                row: rightRow,
                conditions: [{condition, score: matchScore}],
                avgScore: matchScore
              });
            } else {
              // Add condition to existing match
              const existing = allMatchingRows.get(rightRow);
              existing.conditions.push({condition, score: matchScore});
              
              // Update average score
              const sum = existing.conditions.reduce((acc, c) => acc + c.score, 0);
              existing.avgScore = sum / existing.conditions.length;
            }
          }
        }
        
        // Filter rows that don't match all conditions (when using AND logic)
        const matchingRows = [];
        for (const [rightRow, matchInfo] of allMatchingRows.entries()) {
          if (fuzzyOptions.useOrLogic || matchInfo.conditions.length === joinConditions.length) {
            matchingRows.push({
              rightRow,
              score: matchInfo.avgScore
            });
          }
        }
        
        // Sort by score if required
        if (fuzzyOptions.sortFuzzyResults) {
          matchingRows.sort((a, b) => a.score - b.score);
        }
        
        // Create joined rows
        for (const {rightRow, score} of matchingRows) {
          // Check if score is within threshold
          if (score <= fuzzyOptions.threshold) {
            // Create joined row
            const joinedRow = this._mergeRows(leftRow, rightRow, transform);
            
            // Add score to the joined row if requested
            if (fuzzyOptions.includeScoreInResult) {
              joinedRow._fuzzyScore = score;
            }
            
            joinedRows.push(joinedRow);
            
            // Mark right row as matched
            if (joinType === 'right' || joinType === 'full') {
              matchedRightRows.add(rightRow);
            }
          }
        }
      } else if (joinType === 'left' || joinType === 'full') {
        // No matches, but it's a left or full join
        joinedRows.push(this._mergeRows(leftRow, null, transform));
      }
    }
    
    // For right or full outer joins, add right rows that didn't match
    if (joinType === 'right' || joinType === 'full') {
      for (const rightRow of rightDataset) {
        if (!matchedRightRows.has(rightRow)) {
          joinedRows.push(this._mergeRows(null, rightRow, transform));
        }
      }
    }
    
    return joinedRows;
  }
  
  /**
   * Create a Fuse.js index for fuzzy searching
   * @param {Array} dataset - The dataset to index
   * @param {Array} joinConditions - Join conditions
   * @param {Object} fuzzyOptions - Fuzzy search options
   * @returns {Object} Fuse index and row mapping
   * @private
   */
  _createFuzzyIndex(dataset, joinConditions, fuzzyOptions) {
    // Get all the keys we need to index
    const keys = joinConditions.map(condition => condition.rightKey);
    
    // Create mapping between rows and their indices
    const rightRowMap = new Map();
    for (let i = 0; i < dataset.length; i++) {
      rightRowMap.set(dataset[i], i);
    }
    
    // Create Fuse options
    const fuseOptions = {
      keys,
      threshold: fuzzyOptions.threshold,
      includeScore: true,
      isCaseSensitive: fuzzyOptions.isCaseSensitive,
      ignoreLocation: fuzzyOptions.ignoreLocation,
      useExtendedSearch: fuzzyOptions.useExtendedSearch,
      minMatchCharLength: fuzzyOptions.minMatchCharLength,
      findAllMatches: fuzzyOptions.findAllMatches,
      location: 0,
      distance: fuzzyOptions.distance,
      includeMatches: fuzzyOptions.includeMatches,
      shouldSort: fuzzyOptions.shouldSort,
      tokenize: fuzzyOptions.tokenize
    };
    
    // Create and return Fuse.js index
    const fuseIndex = Fuse.createIndex(keys, dataset);
    return { fuseIndex, rightRowMap };
  }
  
  /**
   * Create a composite key from multiple properties
   * @param {Object} row - Data row
   * @param {Array} keyPaths - Array of key paths
   * @returns {string} Composite key
   * @private
   */
  _createCompositeKey(row, keyPaths) {
    if (!row) return 'NULL';
    
    return keyPaths.map(keyPath => {
      const value = this._getNestedProperty(row, keyPath);
      return value === undefined || value === null ? 'NULL' : String(value);
    }).join('|');
  }
  
  /**
   * Create a key that combines multiple join conditions (AND logic)
   * @param {Object} row - Data row
   * @param {Array} joinConditions - Array of join conditions
   * @param {string} side - 'left' or 'right' to indicate which side of the join
   * @returns {string} Multi-condition composite key
   * @private
   */
  _createMultiConditionKey(row, joinConditions, side) {
    if (!row) return 'NULL';
    
    const keyParts = joinConditions.map(condition => {
      const keyPath = side === 'left' ? condition.leftKey : condition.rightKey;
      const value = this._getNestedProperty(row, keyPath);
      return value === undefined || value === null ? 'NULL' : String(value);
    });
    
    return keyParts.join('::');
  }
  
  /**
   * Compare join conditions to find matches (AND logic for multiple conditions)
   * @param {Array} joinConditions - Array of join conditions
   * @param {Object} leftRow - Left row
   * @param {Object} rightRow - Right row
   * @returns {boolean} True if ALL conditions match (AND logic)
   * @private
   */
  _compareJoinConditions(joinConditions, leftRow, rightRow) {
    if (!leftRow || !rightRow) return false;
    
    // All conditions must match (AND logic)
    return joinConditions.every(condition => {
      const leftValue = this._getNestedProperty(leftRow, condition.leftKey);
      const rightValue = this._getNestedProperty(rightRow, condition.rightKey);
      
      // Handle custom comparators if specified
      if (condition.comparator && typeof condition.comparator === 'function') {
        return condition.comparator(leftValue, rightValue);
      }
      
      // Handle null/undefined values consistently
      if (leftValue === null || leftValue === undefined) {
        return rightValue === null || rightValue === undefined;
      }
      
      // For date objects, compare their time values
      if (leftValue instanceof Date && rightValue instanceof Date) {
        return leftValue.getTime() === rightValue.getTime();
      }
      
      // For arrays, compare contents (not reference)
      if (Array.isArray(leftValue) && Array.isArray(rightValue)) {
        if (leftValue.length !== rightValue.length) return false;
        return leftValue.every((val, idx) => val === rightValue[idx]);
      }
      
      // For objects, do shallow comparison of properties
      if (typeof leftValue === 'object' && typeof rightValue === 'object') {
        // Both are non-null objects
        if (!leftValue || !rightValue) return leftValue === rightValue;
        
        // Compare keys
        const leftKeys = Object.keys(leftValue);
        const rightKeys = Object.keys(rightValue);
        if (leftKeys.length !== rightKeys.length) return false;
        
        // Compare values
        return leftKeys.every(key => 
          rightValue.hasOwnProperty(key) && leftValue[key] === rightValue[key]
        );
      }
      
      // Default comparison for primitives
      return leftValue === rightValue;
    });
  }
  
  /**
   * Get a nested property from an object using dot notation
   * @param {Object} obj - Source object
   * @param {string} path - Property path with dot notation
   * @returns {*} Property value
   * @private
   */
  _getNestedProperty(obj, path) {
    if (!obj) return null;
    if (!path) return null;
    if (!path.includes('.')) return obj[path];
    
    try {
      return path.split('.').reduce((current, key) => {
        // Handle array indexing in path (e.g., 'items[0].name')
        if (key.includes('[') && key.includes(']')) {
          const propName = key.substring(0, key.indexOf('['));
          const indexStr = key.substring(key.indexOf('[') + 1, key.indexOf(']'));
          const index = parseInt(indexStr, 10);
          
          // Check that current[propName] is an array and index is valid
          if (Array.isArray(current[propName]) && !isNaN(index)) {
            return current[propName][index];
          }
          return null;
        }
        
        return current && (typeof current === 'object') ? 
          (current[key] !== undefined ? current[key] : null) : null;
      }, obj);
    } catch (error) {
      logger.debug(`Error getting nested property "${path}"`, { error: error.message });
      return null;
    }
  }
  
  /**
   * Merge two rows, handling null values and column conflicts
   * @param {Object} leftRow - Left row
   * @param {Object} rightRow - Right row
   * @param {Function} transform - Optional transform function
   * @returns {Object} Merged row
   * @private
   */
  _mergeRows(leftRow, rightRow, transform) {
    // Create empty result if left row is null
    const result = leftRow ? { ...leftRow } : {};
    
    // Add properties from right row if it exists
    if (rightRow) {
      for (const [key, value] of Object.entries(rightRow)) {
        // Skip internal properties
        if (key.startsWith('_')) continue;
        
        // Handle name conflicts
        if (key in result && result[key] !== value) {
          // Different strategies for conflict resolution
          if (Array.isArray(result[key]) && Array.isArray(value)) {
            // Merge arrays
            result[key] = [...result[key], ...value];
          } else if (typeof result[key] === 'object' && typeof value === 'object' && 
                    result[key] !== null && value !== null) {
            // Merge objects recursively
            result[key] = { ...result[key], ...value };
          } else {
            // Use prefix for non-mergeable conflicts
            result[`right_${key}`] = value;
          }
        } else if (!(key in result)) {
          result[key] = value;
        }
      }
    }
    
    // Apply custom transform if provided
    if (typeof transform === 'function') {
      try {
        const transformed = transform(result, leftRow, rightRow);
        return transformed || result;
      } catch (error) {
        logger.error('Error in transform function', { error: error.message });
        return result;
      }
    }
    
    return result;
  }
  
  /**
   * Combine metadata from two queries
   * @param {Object} meta1 - First metadata
   * @param {Object} meta2 - Second metadata
   * @returns {Object} Combined metadata
   * @private
   */
  _combineMetadata(meta1, meta2) {
    if (!meta1) return meta2;
    if (!meta2) return meta1;
    
    // Combine column metadata with conflict resolution
    const combinedColumns = [];
    const columnMap = new Map();
    
    // Add columns from meta1
    if (Array.isArray(meta1.columns)) {
      for (const column of meta1.columns) {
        if (column && column.name) {
          columnMap.set(column.name, column);
          combinedColumns.push(column);
        }
      }
    }
    
    // Add/merge columns from meta2
    if (Array.isArray(meta2.columns)) {
      for (const column of meta2.columns) {
        if (column && column.name) {
          if (columnMap.has(column.name)) {
            // Column already exists, handle conflict
            const existingColumn = columnMap.get(column.name);
            
            // Keep the most specific data type
            if (column.dataType && (!existingColumn.dataType || 
                existingColumn.dataType === 'ANY' || 
                existingColumn.dataType === 'UNKNOWN')) {
              existingColumn.dataType = column.dataType;
            }
            
            // Merge other properties as needed
            if (column.precision && !existingColumn.precision) {
              existingColumn.precision = column.precision;
            }
            
            if (column.scale && !existingColumn.scale) {
              existingColumn.scale = column.scale;
            }
          } else {
            // New column, add it
            columnMap.set(column.name, column);
            combinedColumns.push(column);
          }
        }
      }
    }
    
    // Combine other metadata properties
    return {
      combinedFrom: [meta1, meta2],
      columns: combinedColumns,
      joinType: 'combined',
      timestamp: new Date().toISOString(),
      sourceCount: (meta1.sourceCount || 1) + (meta2.sourceCount || 1)
    };
  }
  
  /**
   * Handle query complete event
   * @param {Object} event - Event data
   * @private
   */
  _handleQueryComplete(event) {
    // Handle query completion if needed
    logger.debug('Query completed event received', {
      queryId: event.queryId,
      status: event.status
    });
  }
  
  /**
   * Validate input datasets
   * @param {Array<Array>} datasets - Array of datasets to validate
   * @throws {Error} If validation fails
   * @private
   */
  _validateDatasets(datasets) {
    if (!Array.isArray(datasets)) {
      throw new Error('Datasets must be an array');
    }
    
    for (let i = 0; i < datasets.length; i++) {
      const dataset = datasets[i];
      
      if (!Array.isArray(dataset)) {
        throw new Error(`Dataset at index ${i} is not an array`);
      }
      
      if (dataset.length === 0) {
        logger.warn(`Dataset at index ${i} is empty`);
      }
    }
  }
  
  /**
   * Analyze join statistics for performance tuning
   * @param {Object} joinResults - Results of join operations
   * @returns {Object} Join statistics
   */
  analyzeJoinStatistics(joinResults) {
    const stats = {
      bestJoin: { name: null, matchCount: 0, matchRatio: 0 },
      allJoins: []
    };
    
    for (const [joinName, result] of Object.entries(joinResults)) {
      const joinStat = {
        name: joinName,
        matchCount: result.count,
        matchRatio: result.count / result.totalPossibleMatches || 0,
        executionTimeMs: result.executionTimeMs || 0
      };
      
      stats.allJoins.push(joinStat);
      
      if (joinStat.matchCount > stats.bestJoin.matchCount) {
        stats.bestJoin = joinStat;
      }
    }
    
    // Add overall statistics
    stats.totalJoins = stats.allJoins.length;
    stats.averageMatchCount = stats.allJoins.reduce((sum, stat) => sum + stat.matchCount, 0) / stats.allJoins.length;
    stats.averageExecutionTimeMs = stats.allJoins.reduce((sum, stat) => sum + stat.executionTimeMs, 0) / stats.allJoins.length;
    
    return stats;
  }
  
  /**
   * Create a debug representation of join conditions
   * @param {Array} joinConditions - Array of join conditions
   * @returns {string} String representation for logging
   * @private
   */
  _getJoinConditionsDebugString(joinConditions) {
    return joinConditions.map(condition => {
      if (typeof condition === 'object') {
        const left = condition.leftKey || '?';
        const right = condition.rightKey || '?';
        return `${left}=${right}`;
      }
      return String(condition);
    }).join(' AND ');
  }
}

// Export singleton instance
const dataJoinService = new DataJoinService();
module.exports = dataJoinService;