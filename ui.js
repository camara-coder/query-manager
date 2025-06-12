import React, { useState, useEffect } from 'react';
import { Play, Download, X, Clock, CheckCircle, AlertCircle, Database } from 'lucide-react';

const QueryExecutorUI = () => {
  const [queries, setQueries] = useState([]);
  const [selectedQuery, setSelectedQuery] = useState('');
  const [adhocSql, setAdhocSql] = useState('');
  const [params, setParams] = useState('{}');
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [executionId, setExecutionId] = useState(null);
  const [mode, setMode] = useState('saved'); // 'saved' or 'adhoc'
  const [statusPolling, setStatusPolling] = useState(false);

  const API_BASE = process.env.REACT_APP_API_URL || 'http://localhost:3001/api';

  // Fetch saved queries on component mount
  useEffect(() => {
    fetchQueries();
  }, []);

  // Poll execution status
  useEffect(() => {
    let interval;
    if (statusPolling && executionId && selectedQuery) {
      interval = setInterval(async () => {
        try {
          const response = await fetch(`${API_BASE}/queries/${selectedQuery}/status/${executionId}`);
          const status = await response.json();
          
          if (status.status === 'completed') {
            setStatusPolling(false);
            setLoading(false);
            // Fetch results if available
            await fetchResults();
          } else if (status.status === 'error' || status.status === 'cancelled') {
            setStatusPolling(false);
            setLoading(false);
            setError(`Query ${status.status}: ${status.error || 'Unknown error'}`);
          }
        } catch (err) {
          console.error('Status polling error:', err);
        }
      }, 2000);
    }

    return () => {
      if (interval) clearInterval(interval);
    };
  }, [statusPolling, executionId, selectedQuery]);

  const fetchQueries = async () => {
    try {
      const response = await fetch(`${API_BASE}/queries`);
      const data = await response.json();
      setQueries(data);
    } catch (err) {
      setError('Failed to fetch saved queries');
    }
  };

  const fetchResults = async () => {
    if (!executionId || !selectedQuery) return;
    
    try {
      const response = await fetch(`${API_BASE}/queries/${selectedQuery}/results/${executionId}`);
      const data = await response.json();
      
      if (data.success) {
        setResults(data);
      } else {
        setError(data.error || 'Failed to fetch results');
      }
    } catch (err) {
      setError('Failed to fetch query results');
    }
  };

  const executeQuery = async () => {
    setLoading(true);
    setError(null);
    setResults(null);

    try {
      let parsedParams = {};
      if (params.trim()) {
        try {
          parsedParams = JSON.parse(params);
        } catch (paramError) {
          throw new Error('Invalid JSON in parameters field');
        }
      }

      let response;
      if (mode === 'saved') {
        if (!selectedQuery) {
          throw new Error('Please select a query');
        }
        response = await fetch(`${API_BASE}/queries/${selectedQuery}/execute`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ params: parsedParams })
        });
      } else {
        if (!adhocSql.trim()) {
          throw new Error('Please enter SQL query');
        }
        response = await fetch(`${API_BASE}/queries/execute-adhoc`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            sql: adhocSql,
            params: parsedParams
          })
        });
      }

      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.error || 'Query execution failed');
      }

      if (data.data) {
        // Ad-hoc query completed immediately
        setResults(data);
        setLoading(false);
      } else {
        // Saved query - need to poll for results
        setExecutionId(data.executionId);
        setStatusPolling(true);
      }
    } catch (err) {
      setError(err.message);
      setLoading(false);
    }
  };

  const cancelQuery = async () => {
    if (!executionId || !selectedQuery) return;

    try {
      const response = await fetch(`${API_BASE}/queries/${selectedQuery}/cancel/${executionId}`, {
        method: 'POST'
      });
      
      if (response.ok) {
        setStatusPolling(false);
        setLoading(false);
        setError('Query cancelled by user');
      }
    } catch (err) {
      setError('Failed to cancel query');
    }
  };

  const downloadCSV = () => {
    if (!results || !results.data || results.data.length === 0) return;

    const data = results.data;
    const headers = Object.keys(data[0]);
    
    // Create CSV content
    const csvContent = [
      headers.join(','),
      ...data.map(row => 
        headers.map(header => {
          const value = row[header];
          const stringValue = value === null || value === undefined ? '' : String(value);
          // Escape quotes and wrap in quotes if contains comma, quote, or newline
          return stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')
            ? `"${stringValue.replace(/"/g, '""')}"`
            : stringValue;
        }).join(',')
      )
    ].join('\n');

    // Create and trigger download
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', `query_results_${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const clearResults = () => {
    setResults(null);
    setError(null);
    setExecutionId(null);
    setStatusPolling(false);
  };

  const selectedQueryObj = queries.find(q => q.id === selectedQuery);

  return (
    <div className="max-w-7xl mx-auto p-6 bg-white">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2 flex items-center gap-2">
          <Database className="h-8 w-8 text-blue-600" />
          Oracle Query Manager
        </h1>
        <p className="text-gray-600">Execute and manage Oracle database queries with real-time results</p>
      </div>

      {/* Query Mode Selection */}
      <div className="mb-6">
        <div className="flex space-x-1 bg-gray-100 p-1 rounded-lg w-fit">
          <button
            onClick={() => setMode('saved')}
            className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
              mode === 'saved'
                ? 'bg-white text-blue-600 shadow-sm'
                : 'text-gray-500 hover:text-gray-700'
            }`}
          >
            Saved Queries
          </button>
          <button
            onClick={() => setMode('adhoc')}
            className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
              mode === 'adhoc'
                ? 'bg-white text-blue-600 shadow-sm'
                : 'text-gray-500 hover:text-gray-700'
            }`}
          >
            Ad-hoc SQL
          </button>
        </div>
      </div>

      {/* Query Input Section */}
      <div className="bg-gray-50 p-6 rounded-lg mb-6">
        {mode === 'saved' ? (
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Select Query
              </label>
              <select
                value={selectedQuery}
                onChange={(e) => setSelectedQuery(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="">Choose a saved query...</option>
                {queries.map((query) => (
                  <option key={query.id} value={query.id}>
                    {query.name} - {query.description}
                  </option>
                ))}
              </select>
            </div>
            
            {selectedQueryObj && (
              <div className="bg-white p-4 rounded border">
                <h4 className="font-medium text-gray-900 mb-2">Query Preview</h4>
                <pre className="text-sm text-gray-600 bg-gray-100 p-3 rounded overflow-x-auto">
                  {selectedQueryObj.sql}
                </pre>
              </div>
            )}
          </div>
        ) : (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              SQL Query
            </label>
            <textarea
              value={adhocSql}
              onChange={(e) => setAdhocSql(e.target.value)}
              placeholder="Enter your SQL query here..."
              rows={8}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent font-mono text-sm"
            />
          </div>
        )}

        <div className="mt-4">
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Parameters (JSON)
          </label>
          <textarea
            value={params}
            onChange={(e) => setParams(e.target.value)}
            placeholder='{"param1": "value1", "param2": "value2"}'
            rows={3}
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent font-mono text-sm"
          />
        </div>

        <div className="mt-6 flex space-x-3">
          <button
            onClick={executeQuery}
            disabled={loading || (!selectedQuery && mode === 'saved') || (!adhocSql.trim() && mode === 'adhoc')}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {loading ? (
              <>
                <Clock className="h-4 w-4 animate-spin" />
                Executing...
              </>
            ) : (
              <>
                <Play className="h-4 w-4" />
                Execute Query
              </>
            )}
          </button>

          {loading && (
            <button
              onClick={cancelQuery}
              className="flex items-center gap-2 px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700 transition-colors"
            >
              <X className="h-4 w-4" />
              Cancel
            </button>
          )}

          {results && (
            <button
              onClick={clearResults}
              className="flex items-center gap-2 px-4 py-2 bg-gray-600 text-white rounded-md hover:bg-gray-700 transition-colors"
            >
              <X className="h-4 w-4" />
              Clear Results
            </button>
          )}
        </div>
      </div>

      {/* Status Messages */}
      {error && (
        <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-md flex items-start gap-2">
          <AlertCircle className="h-5 w-5 text-red-500 mt-0.5 flex-shrink-0" />
          <div>
            <h4 className="text-red-800 font-medium">Error</h4>
            <p className="text-red-700 text-sm">{error}</p>
          </div>
        </div>
      )}

      {loading && (
        <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-md flex items-center gap-2">
          <Clock className="h-5 w-5 text-blue-500 animate-spin" />
          <span className="text-blue-700">
            {statusPolling ? 'Executing query and fetching results...' : 'Starting query execution...'}
          </span>
        </div>
      )}

      {/* Results Section */}
      {results && (
        <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <CheckCircle className="h-5 w-5 text-green-500" />
              <h3 className="text-lg font-medium text-gray-900">Query Results</h3>
              <span className="text-sm text-gray-500">
                ({results.data?.length || 0} rows)
              </span>
            </div>
            <div className="flex items-center gap-2">
              {results.stats && (
                <span className="text-sm text-gray-500">
                  Executed in {results.stats.executionTime}ms
                </span>
              )}
              <button
                onClick={downloadCSV}
                disabled={!results.data || results.data.length === 0}
                className="flex items-center gap-2 px-3 py-1.5 bg-green-600 text-white text-sm rounded-md hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                <Download className="h-4 w-4" />
                Download CSV
              </button>
            </div>
          </div>

          {results.data && results.data.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    {Object.keys(results.data[0]).map((header) => (
                      <th
                        key={header}
                        className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                      >
                        {header}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {results.data.map((row, index) => (
                    <tr key={index} className="hover:bg-gray-50">
                      {Object.values(row).map((value, cellIndex) => (
                        <td
                          key={cellIndex}
                          className="px-6 py-4 whitespace-nowrap text-sm text-gray-900"
                        >
                          {value === null || value === undefined ? (
                            <span className="text-gray-400 italic">null</span>
                          ) : typeof value === 'object' ? (
                            JSON.stringify(value)
                          ) : (
                            String(value)
                          )}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="px-6 py-8 text-center text-gray-500">
              No data returned from query
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default QueryExecutorUI;
