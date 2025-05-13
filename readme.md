# Query Result Store

The Query Result Store is an extension to the Oracle Long Query Manager that provides storage, retrieval, and management capabilities for query results. It enables persistence of results from single queries, joined queries, and cascading multi-way joins.

## Features

- **Result Persistence**: Store query results from any query type for later retrieval
- **Automatic Storage**: Seamlessly integrate with existing query execution flows
- **Flexible Retrieval**: Access results by ID, query ID, execution ID, or tags
- **Efficient Storage**: Compression for large datasets to minimize storage requirements
- **Memory Caching**: LRU cache for frequently accessed results
- **TTL Support**: Automatically expire results after a specified time
- **Result Tagging**: Organize and categorize results with tags for easier discovery
- **Metadata Support**: Store and retrieve contextual information about results
- **Join Result Storage**: Store results from complex join operations

## API Endpoints

### Query Execution with Storage

- `POST /api/queries/:id/execute-with-storage`: Execute a predefined query and store results
- `POST /api/queries/execute-adhoc-with-storage`: Execute an ad-hoc query and store results
- `POST /api/queries/join-with-storage`: Join two queries and store results
- `POST /api/queries/join-multiple-with-storage`: Join multiple queries and store combined results

### Result Retrieval

- `GET /api/results/:id`: Get a stored result by ID
- `GET /api/queries/:id/results`: Get all results for a specific query
- `GET /api/executions/:id/results`: Get all results for a specific execution
- `GET /api/results?tags=tag1,tag2&matchAll=true`: Find results by tags

### Result Management

- `POST /api/results`: Store custom result data
- `PATCH /api/results/:id`: Update result metadata and tags
- `DELETE /api/results/:id`: Delete a stored result
- `GET /api/results/statistics`: Get result store statistics
- `POST /api/results/cleanup`: Clean up expired results

## Usage Examples

### Executing a Query with Result Storage

```javascript
// Server-side using the enhanced query service
const result = await enhancedQueryService.executeQueryWithStorage(
  'SELECT * FROM employees WHERE department_id = :deptId',
  { deptId: 10 },
  {
    storeResults: true, // Enable result storage (default)
    resultTTL: 3600, // TTL in seconds (1 hour)
    resultTags: ['employees', 'department-10'], // Custom tags
    resultMetadata: { department: 'Finance' } // Custom metadata
  }
);

// Access the result storage information
const resultId = result.storage.resultId;
console.log(`Result stored with ID: ${resultId}`);
```

### Client-side Execution and Retrieval

```javascript
// Initialize the client
const resultClient = new QueryResultClient({
  baseUrl: '/api',
  defaultTTL: 86400, // 1 day
  defaultTags: ['webapp', 'user-123']
});

// Execute a query and store the result
const execution = await resultClient.executeQuery('SALES_BY_REGION', {
  start_date: '2023-01-01',
  end_date: '2023-12-31'
}, {
  resultTags: ['sales', 'annual']
});

// Wait for the result to be ready and get it
const result = await resultClient.getResult(execution.resultId);

// Find all results with specific tags
const salesResults = await resultClient.findResultsByTags(['sales']);
```

### Joining Queries with Result Storage

```javascript
// Join two queries and store the combined result
const joinResult = await resultClient.joinQueries({
  query1: 'SALES_BY_REGION',
  query2: 'CUSTOMER_DEMOGRAPHICS',
  joinConditions: [
    {
      leftKey: 'region_id',
      rightKey: 'region_id'
    }
  ],
  params1: { year: 2023 },
  params2: { active: true }
}, {
  resultTags: ['joined', 'sales-demographics']
});

// Later retrieve the joined result
const joinedData = await resultClient.getResult(joinResult.resultId);
```

### Storing Custom Results

```javascript
// Store custom result data
const storageInfo = await resultClient.storeCustomResult({
  data: processedData,
  metadata: { source: 'client-side-processing', processingTime: 230 }
}, {
  resultTags: ['processed', 'validated'],
  resultTTL: 604800 // 1 week
});

// Update result metadata later
await resultClient.updateResultMetadata(storageInfo.resultId, {
  tags: ['processed', 'validated', 'approved'],
  metadata: { approvedBy: 'user-456' }
});
```

## Configuration

The result store can be configured through environment variables or the configuration system:

```
# Database connection (shared with main application)
PGHOST=localhost
PGPORT=5432
PGDATABASE=query_manager
PGUSER=postgres
PGPASSWORD=postgres

# Result store configuration
RESULTS_TABLE_NAME=query_results
RESULTS_CACHE_SIZE=100
RESULTS_TTL=86400
RESULTS_COMPRESS_THRESHOLD=102400
RESULTS_CLEANUP_INTERVAL=3600000
```

## Implementation Details

The Query Result Store uses PostgreSQL for persistence with the following storage strategies:

1. **Small Results**: Stored directly as JSON in a `data` column
2. **Large Results**: Stored as binary data with optional compression in a `data_large` column
3. **In-Memory Cache**: Frequently accessed results cached using an LRU cache for performance

The storage mechanism is transparent to users - results are automatically retrieved from the appropriate storage location based on their size and access patterns.

## Client-Side Integration

The `QueryResultClient` utility provides a convenient way to interact with the Query Result Store from client applications. It handles:

- Authentication and authorization
- Query execution with storage
- Result retrieval and manipulation
- Error handling and polling

Include the client in your front-end applications to easily leverage the result storage capabilities.

## Benefits

- **Performance**: Save execution time by reusing previous query results
- **Reliability**: Persist important query results for later analysis
- **Sharing**: Enable sharing of query results between different users or systems
- **Analysis**: Store intermediate results in multi-step analytical processes
- **Auditing**: Maintain historical query results for compliance and verification
