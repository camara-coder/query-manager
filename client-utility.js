/**
 * Query Result Client Utility
 * Client-side utilities for working with the Query Result Store
 */
class QueryResultClient {
  constructor(config = {}) {
    this.config = {
      baseUrl: config.baseUrl || '/api',
      defaultTTL: config.defaultTTL || 86400, // 1 day in seconds
      defaultTags: config.defaultTags || [],
      ...config
    };
    
    this.fetchOptions = {
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json'
      },
      ...config.fetchOptions
    };
  }
  
  /**
   * Set authorization token for API requests
   * @param {string} token - Authorization token
   */
  setAuthToken(token) {
    if (token) {
      this.fetchOptions.headers = {
        ...this.fetchOptions.headers,
        'Authorization': `Bearer ${token}`
      };
    } else {
      // Remove Authorization header if token is falsy
      const { Authorization, ...headers } = this.fetchOptions.headers;
      this.fetchOptions.headers = headers;
    }
  }
  
  /**
   * Execute a query with result storage
   * @param {string} queryId - Query ID
   * @param {Object} params - Query parameters
   * @param {Object} options - Query and storage options
   * @returns {Promise<Object>} Query execution result
   */
  async executeQuery(queryId, params = {}, options = {}) {
    const url = `${this.config.baseUrl}/queries/${queryId}/execute-with-storage`;
    
    // Prepare storage options
    const storageOptions = {
      storeResults: options.storeResults !== false, 
      resultTTL: options.resultTTL || this.config.defaultTTL,
      resultTags: [...(this.config.defaultTags || []), ...(options.resultTags || [])],
      resultMetadata: options.resultMetadata || {}
    };
    
    const response = await fetch(url, {
      ...this.fetchOptions,
      method: 'POST',
      body: JSON.stringify({
        params,
        options: {
          ...options,
          ...storageOptions
        }
      })
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || 'Failed to execute query');
    }
    
    return response.json();
  }
  
  /**
   * Execute an ad-hoc query with result storage
   * @param {Object} queryData - Query data
   * @param {Object} options - Query and storage options
   * @returns {Promise<Object>} Query execution result
   */
  async executeAdhocQuery(queryData, options = {}) {
    const url = `${this.config.baseUrl}/queries/execute-adhoc-with-storage`;
    
    // Prepare storage options
    const storageOptions = {
      storeResults: options.storeResults !== false, 
      resultTTL: options.resultTTL || this.config.defaultTTL,
      resultTags: [...(this.config.defaultTags || []), ...(options.resultTags || [])],
      resultMetadata: options.resultMetadata || {}
    };
    
    const response = await fetch(url, {
      ...this.fetchOptions,
      method: 'POST',
      body: JSON.stringify({
        ...queryData,
        options: {
          ...options,
          ...storageOptions
        }
      })
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || 'Failed to execute ad-hoc query');
    }
    
    return response.json();
  }
  
  /**
   * Join two queries with result storage
   * @param {Object} joinData - Join configuration
   * @param {Object} options - Join and storage options
   * @returns {Promise<Object>} Join result
   */
  async joinQueries(joinData, options = {}) {
    const url = `${this.config.baseUrl}/queries/join-with-storage`;
    
    // Prepare storage options
    const storageOptions = {
      storeResults: options.storeResults !== false, 
      resultTTL: options.resultTTL || this.config.defaultTTL,
      resultTags: [...(this.config.defaultTags || []), 'joined', ...(options.resultTags || [])],
      resultMetadata: options.resultMetadata || {}
    };
    
    const response = await fetch(url, {
      ...this.fetchOptions,
      method: 'POST',
      body: JSON.stringify({
        ...joinData,
        options: {
          ...options,
          ...storageOptions
        }
      })
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || 'Failed to join queries');
    }
    
    return response.json();
  }
  
  /**
   * Join multiple queries with result storage
   * @param {Object} joinData - Multi-join configuration
   * @param {Object} options - Join and storage options
   * @returns {Promise<Object>} Multi-join result
   */
  async joinMultipleQueries(joinData, options = {}) {
    const url = `${this.config.baseUrl}/queries/join-multiple-with-storage`;
    
    // Prepare storage options
    const storageOptions = {
      storeResults: options.storeResults !== false, 
      resultTTL: options.resultTTL || this.config.defaultTTL,
      resultTags: [...(this.config.defaultTags || []), 'multi-joined', ...(options.resultTags || [])],
      resultMetadata: options.resultMetadata || {}
    };
    
    const response = await fetch(url, {
      ...this.fetchOptions,
      method: 'POST',
      body: JSON.stringify({
        ...joinData,
        options: {
          ...options,
          ...storageOptions
        }
      })
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || 'Failed to join multiple queries');
    }
    
    return response.json();
  }
  
  /**
   * Get a stored result by ID
   * @param {string} resultId - Result ID
   * @param {boolean} metadataOnly - Get only metadata without data
   * @returns {Promise<Object>} Stored result
   */
  async getResult(resultId, metadataOnly = false) {
    const url = `${this.config.baseUrl}/results/${resultId}${metadataOnly ? '?metadataOnly=true' : ''}`;
    
    const response = await fetch(url, {
      ...this.fetchOptions,
      method: 'GET'
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || `Failed to get result: ${resultId}`);
    }
    
    return response.json();
  }
  
  /**
   * Get all results for a query
   * @param {string} queryId - Query ID
   * @returns {Promise<Array>} Results for the query
   */
  async getResultsByQueryId(queryId) {
    const url = `${this.config.baseUrl}/queries/${queryId}/results`;
    
    const response = await fetch(url, {
      ...this.fetchOptions,
      method: 'GET'
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || `Failed to get results for query: ${queryId}`);
    }
    
    return response.json();
  }
  
  /**
   * Get all results for an execution
   * @param {string} executionId - Execution ID
   * @returns {Promise<Array>} Results for the execution
   */
  async getResultsByExecutionId(executionId) {
    const url = `${this.config.baseUrl}/executions/${executionId}/results`;
    
    const response = await fetch(url, {
      ...this.fetchOptions,
      method: 'GET'
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || `Failed to get results for execution: ${executionId}`);
    }
    
    return response.json();
  }
  
  /**
   * Find results by tags
   * @param {Array<string>} tags - Tags to search for
   * @param {boolean} matchAll - Whether all tags must be present
   * @returns {Promise<Array>} Matching results
   */
  async findResultsByTags(tags, matchAll = true) {
    if (!tags || !Array.isArray(tags) || tags.length === 0) {
      throw new Error('Tags must be a non-empty array');
    }
    
    const tagsParam = tags.join(',');
    const url = `${this.config.baseUrl}/results?tags=${tagsParam}&matchAll=${matchAll}`;
    
    const response = await fetch(url, {
      ...this.fetchOptions,
      method: 'GET'
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || 'Failed to find results by tags');
    }
    
    return response.json();
  }
  
  /**
   * Update result metadata
   * @param {string} resultId - Result ID
   * @param {Object} updateData - Metadata to update
   * @returns {Promise<Object>} Updated result metadata
   */
  async updateResultMetadata(resultId, updateData) {
    const url = `${this.config.baseUrl}/results/${resultId}`;
    
    const response = await fetch(url, {
      ...this.fetchOptions,
      method: 'PATCH',
      body: JSON.stringify(updateData)
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || `Failed to update result metadata: ${resultId}`);
    }
    
    return response.json();
  }
  
  /**
   * Delete a result
   * @param {string} resultId - Result ID
   * @returns {Promise<boolean>} Success indicator
   */
  async deleteResult(resultId) {
    const url = `${this.config.baseUrl}/results/${resultId}`;
    
    const response = await fetch(url, {
      ...this.fetchOptions,
      method: 'DELETE'
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || `Failed to delete result: ${resultId}`);
    }
    
    return true;
  }
  
  /**
   * Store custom result data
   * @param {Object} resultData - Result data to store
   * @param {Object} options - Storage options
   * @returns {Promise<Object>} Storage information
   */
  async storeCustomResult(resultData, options = {}) {
    const url = `${this.config.baseUrl}/results`;
    
    // Prepare storage options
    const storageOptions = {
      resultTTL: options.resultTTL || this.config.defaultTTL,
      resultTags: [...(this.config.defaultTags || []), 'custom', ...(options.resultTags || [])],
      resultType: options.resultType || 'custom'
    };
    
    const response = await fetch(url, {
      ...this.fetchOptions,
      method: 'POST',
      body: JSON.stringify({
        data: resultData.data,
        metadata: resultData.metadata || {},
        queryId: resultData.queryId,
        executionId: resultData.executionId,
        options: storageOptions
      })
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || 'Failed to store custom result');
    }
    
    return response.json();
  }
  
  /**
   * Get statistics for the result store
   * @returns {Promise<Object>} Result store statistics
   */
  async getStatistics() {
    const url = `${this.config.baseUrl}/results/statistics`;
    
    const response = await fetch(url, {
      ...this.fetchOptions,
      method: 'GET'
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || 'Failed to get result store statistics');
    }
    
    return response.json();
  }
  
  /**
   * Clean up expired results (admin only)
   * @returns {Promise<Object>} Cleanup result
   */
  async cleanupExpiredResults() {
    const url = `${this.config.baseUrl}/results/cleanup`;
    
    const response = await fetch(url, {
      ...this.fetchOptions,
      method: 'POST'
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || 'Failed to clean up expired results');
    }
    
    return response.json();
  }
  
  /**
   * Convenience method to execute a query and wait for the result
   * @param {string} queryId - Query ID
   * @param {Object} params - Query parameters
   * @param {Object} options - Query and storage options
   * @param {number} pollInterval - Polling interval in ms
   * @param {number} maxAttempts - Maximum polling attempts
   * @returns {Promise<Object>} Complete query results
   */
  async executeQueryAndWaitForResult(queryId, params = {}, options = {}, pollInterval = 1000, maxAttempts = 60) {
    // Execute the query first
    const execResult = await this.executeQuery(queryId, params, options);
    
    if (!execResult.resultId) {
      throw new Error('Query execution did not return a result ID');
    }
    
    // Poll for result until it's ready
    let attempts = 0;
    while (attempts < maxAttempts) {
      attempts++;
      
      try {
        // Try to get the result - this will succeed when it's ready
        const result = await this.getResult(execResult.resultId);
        
        // If we got here, the result is ready
        return result;
      } catch (error) {
        // If it's not a 404, rethrow
        if (!error.message.includes('not found')) {
          throw error;
        }
        
        // Wait before trying again
        await new Promise(resolve => setTimeout(resolve, pollInterval));
      }
    }
    
    throw new Error(`Result not available after ${maxAttempts} attempts`);
  }
}

// Export for CommonJS environments
if (typeof module !== 'undefined' && module.exports) {
  module.exports = QueryResultClient;
}

// Export for ES modules
if (typeof exports !== 'undefined') {
  exports.QueryResultClient = QueryResultClient;
}

// Make available in browser environments
if (typeof window !== 'undefined') {
  window.QueryResultClient = QueryResultClient;
}
