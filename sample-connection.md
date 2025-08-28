"""
Oracle Database Connection Utility with LDAP Directory Naming

This module provides a reusable connection utility for Oracle databases
that use LDAP directory naming service for connection resolution.

Dependencies:
    pip install cx_Oracle

Usage:
    from oracle_ldap_connector import OracleLDAPConnector
    
    connector = OracleLDAPConnector(
        ldap_url="ldap://oraclenames.com:399/SERVICENAME,cn=OracleContext,dc=something,dc=somethingelse"
    )
    
    with connector.get_connection("username", "password") as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM your_table")
        results = cursor.fetchall()
"""

import cx_Oracle
import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any
import os
import re


class OracleLDAPConnector:
    """
    Oracle Database connector with LDAP directory naming support.
    """
    
    def __init__(self, 
                 ldap_url: str,
                 oracle_encoding: str = "UTF-8",
                 connection_timeout: int = 30,
                 auth_mode: Optional[int] = None):
        """
        Initialize the Oracle LDAP connector.
        
        Args:
            ldap_url: LDAP URL in format: ldap://server:port/service,cn=OracleContext,dc=domain,dc=com
            oracle_encoding: Oracle client encoding (default: UTF-8)
            connection_timeout: Connection timeout in seconds
            auth_mode: Oracle authentication mode (e.g., cx_Oracle.SYSDBA)
        """
        self.ldap_url = ldap_url
        self.oracle_encoding = oracle_encoding
        self.connection_timeout = connection_timeout
        self.auth_mode = auth_mode
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        
        # Validate and parse LDAP URL
        self._validate_ldap_url()
        
        # Parse components for logging/debugging
        self._parse_ldap_url()
    
    def _validate_ldap_url(self):
        """Validate the LDAP URL format."""
        if not self.ldap_url.startswith(('ldap://', 'ldaps://')):
            raise ValueError("LDAP URL must start with ldap:// or ldaps://")
        
        # Basic pattern matching for LDAP URL
        pattern = r'^ldaps?://[^/]+/[^,]+,cn=OracleContext'
        if not re.match(pattern, self.ldap_url, re.IGNORECASE):
            self.logger.warning("LDAP URL format may not be standard Oracle LDAP naming format")
    
    def _parse_ldap_url(self):
        """Parse LDAP URL components for debugging."""
        try:
            # Extract server and port
            url_parts = self.ldap_url.split('/')
            server_part = url_parts[2]  # server:port
            
            if ':' in server_part:
                self.ldap_server, self.ldap_port = server_part.split(':')
                self.ldap_port = int(self.ldap_port)
            else:
                self.ldap_server = server_part
                self.ldap_port = 389 if self.ldap_url.startswith('ldap://') else 636
            
            # Extract service and DN
            dn_part = '/'.join(url_parts[3:])  # Everything after server:port
            if ',' in dn_part:
                self.service_name = dn_part.split(',')[0]
                self.oracle_context = ','.join(dn_part.split(',')[1:])
            else:
                self.service_name = dn_part
                self.oracle_context = ""
                
            self.logger.info(f"Parsed LDAP URL - Server: {self.ldap_server}, Port: {self.ldap_port}, Service: {self.service_name}")
            
        except Exception as e:
            self.logger.warning(f"Could not fully parse LDAP URL: {str(e)}")
            self.ldap_server = "unknown"
            self.ldap_port = 0
            self.service_name = "unknown"
    
    def _create_oracle_dsn(self) -> str:
        """
        Create Oracle DSN string using LDAP naming.
        
        Returns:
            str: Oracle DSN string for LDAP naming
        """
        # For LDAP naming, we use the full LDAP URL as the DSN
        return self.ldap_url
    
    def test_connection(self, username: str, password: str) -> Dict[str, Any]:
        """
        Test Oracle database connection using LDAP naming.
        
        Args:
            username: Username for database authentication
            password: Password for database authentication
            
        Returns:
            dict: Test results with status and details
        """
        results = {
            'oracle_connection': False,
            'error': None,
            'oracle_version': None,
            'ldap_resolution': False,
            'service_resolved': None
        }
        
        try:
            dsn = self._create_oracle_dsn()
            self.logger.info(f"Testing connection with DSN: {dsn}")
            
            connection_params = {
                'user': username,
                'password': password,
                'dsn': dsn,
                'encoding': self.oracle_encoding
            }
            
            if self.auth_mode:
                connection_params['mode'] = self.auth_mode
            
            with cx_Oracle.connect(**connection_params) as conn:
                results['oracle_connection'] = True
                results['ldap_resolution'] = True
                results['oracle_version'] = conn.version
                results['service_resolved'] = self.service_name
                
                # Get actual service name from connection
                cursor = conn.cursor()
                cursor.execute("SELECT SYS_CONTEXT('USERENV', 'SERVICE_NAME') FROM DUAL")
                actual_service = cursor.fetchone()[0]
                results['actual_service_name'] = actual_service
                cursor.close()
                
                self.logger.info(f"Connection test successful - Oracle version: {conn.version}")
                
        except cx_Oracle.Error as e:
            error_obj, = e.args
            results['error'] = f"Oracle Error {error_obj.code}: {error_obj.message}"
            self.logger.error(f"Oracle connection test failed: {results['error']}")
            
            # Check if it's likely an LDAP resolution issue
            if 'TNS' in str(error_obj.code) or 'could not resolve' in error_obj.message.lower():
                results['ldap_resolution'] = False
                
        except Exception as e:
            results['error'] = str(e)
            self.logger.error(f"Connection test failed: {str(e)}")
        
        return results
    
    @contextmanager
    def get_connection(self, username: str, password: str):
        """
        Get Oracle database connection using LDAP naming.
        
        Args:
            username: Username for database authentication
            password: Password for database authentication
            
        Yields:
            cx_Oracle.Connection: Database connection object
            
        Raises:
            ConnectionError: If Oracle connection fails
        """
        dsn = self._create_oracle_dsn()
        connection = None
        
        try:
            connection_params = {
                'user': username,
                'password': password,
                'dsn': dsn,
                'encoding': self.oracle_encoding
            }
            
            if self.auth_mode:
                connection_params['mode'] = self.auth_mode
            
            connection = cx_Oracle.connect(**connection_params)
            
            self.logger.info("Oracle connection established via LDAP naming")
            yield connection
            
        except cx_Oracle.Error as e:
            error_obj, = e.args
            error_msg = f"Oracle connection failed - Error {error_obj.code}: {error_obj.message}"
            self.logger.error(error_msg)
            raise ConnectionError(error_msg)
        
        except Exception as e:
            error_msg = f"Database connection failed: {str(e)}"
            self.logger.error(error_msg)
            raise ConnectionError(error_msg)
        
        finally:
            if connection:
                try:
                    connection.close()
                    self.logger.info("Oracle connection closed")
                except Exception as e:
                    self.logger.warning(f"Error closing connection: {str(e)}")
    
    def execute_query(self, username: str, password: str, 
                     query: str, params: tuple = None, 
                     fetch_all: bool = True,
                     arraysize: int = 1000):
        """
        Execute a query and return results.
        
        Args:
            username: Username for database authentication
            password: Password for database authentication
            query: SQL query to execute
            params: Query parameters (optional)
            fetch_all: Whether to fetch all results or return cursor
            arraysize: Cursor arraysize for better performance
            
        Returns:
            Query results, row count, or cursor object
        """
        with self.get_connection(username, password) as conn:
            cursor = conn.cursor()
            cursor.arraysize = arraysize
            
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                query_type = query.strip().upper().split()[0]
                
                if fetch_all and query_type in ('SELECT', 'WITH'):
                    results = cursor.fetchall()
                    return results
                elif query_type in ('INSERT', 'UPDATE', 'DELETE'):
                    conn.commit()
                    return cursor.rowcount
                elif query_type in ('CREATE', 'DROP', 'ALTER'):
                    conn.commit()
                    return "DDL executed successfully"
                else:
                    # For other statements, try to fetch if possible
                    try:
                        results = cursor.fetchall()
                        return results
                    except:
                        conn.commit()
                        return "Statement executed successfully"
                        
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Query execution failed: {str(e)}")
                raise
            finally:
                cursor.close()
    
    def execute_many(self, username: str, password: str,
                    query: str, params_list: list):
        """
        Execute a query with multiple parameter sets.
        
        Args:
            username: Username for database authentication  
            password: Password for database authentication
            query: SQL query to execute
            params_list: List of parameter tuples
            
        Returns:
            int: Total number of rows affected
        """
        with self.get_connection(username, password) as conn:
            cursor = conn.cursor()
            
            try:
                cursor.executemany(query, params_list)
                conn.commit()
                return cursor.rowcount
                
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Batch execution failed: {str(e)}")
                raise
            finally:
                cursor.close()
    
    def get_connection_info(self, username: str, password: str) -> Dict[str, Any]:
        """
        Get detailed connection information.
        
        Args:
            username: Username for database authentication
            password: Password for database authentication
            
        Returns:
            dict: Connection information
        """
        info = {}
        
        try:
            with self.get_connection(username, password) as conn:
                cursor = conn.cursor()
                
                # Get various connection details
                queries = {
                    'instance_name': "SELECT SYS_CONTEXT('USERENV', 'INSTANCE_NAME') FROM DUAL",
                    'service_name': "SELECT SYS_CONTEXT('USERENV', 'SERVICE_NAME') FROM DUAL", 
                    'server_host': "SELECT SYS_CONTEXT('USERENV', 'SERVER_HOST') FROM DUAL",
                    'database_name': "SELECT SYS_CONTEXT('USERENV', 'DB_NAME') FROM DUAL",
                    'session_user': "SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') FROM DUAL",
                    'current_schema': "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL"
                }
                
                for key, query in queries.items():
                    try:
                        cursor.execute(query)
                        result = cursor.fetchone()
                        info[key] = result[0] if result else None
                    except:
                        info[key] = "N/A"
                
                info['oracle_version'] = conn.version
                cursor.close()
                
        except Exception as e:
            info['error'] = str(e)
        
        return info


class ConnectionError(Exception):
    """Raised when Oracle database connection fails."""
    pass


# Configuration helper functions
def create_connector_from_jdbc_url(jdbc_url: str, **kwargs) -> OracleLDAPConnector:
    """
    Create connector from JDBC URL format.
    
    Args:
        jdbc_url: JDBC URL (jdbc:oracle:thin:@ldap://server:port/service,cn=OracleContext,...)
        **kwargs: Additional connector parameters
        
    Returns:
        OracleLDAPConnector: Configured connector instance
    """
    if not jdbc_url.startswith('jdbc:oracle:thin:@'):
        raise ValueError("Invalid JDBC URL format")
    
    # Extract LDAP URL from JDBC URL
    ldap_url = jdbc_url[len('jdbc:oracle:thin:@'):]
    
    return OracleLDAPConnector(ldap_url=ldap_url, **kwargs)


def create_connector_from_env() -> OracleLDAPConnector:
    """
    Create connector from environment variables.
    
    Expected environment variables:
        ORACLE_LDAP_URL or ORACLE_JDBC_URL
    
    Returns:
        OracleLDAPConnector: Configured connector instance
    """
    ldap_url = os.getenv('ORACLE_LDAP_URL')
    jdbc_url = os.getenv('ORACLE_JDBC_URL')
    
    if jdbc_url:
        return create_connector_from_jdbc_url(jdbc_url)
    elif ldap_url:
        return OracleLDAPConnector(ldap_url=ldap_url)
    else:
        raise ValueError("Either ORACLE_LDAP_URL or ORACLE_JDBC_URL environment variable must be set")


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Configuration example with JDBC URL
    jdbc_url = "jdbc:oracle:thin:@ldap://oraclenames.com:399/SERVICENAME,cn=OracleContext,dc=something,dc=somethingelse"
    
    try:
        # Create connector from JDBC URL
        connector = create_connector_from_jdbc_url(jdbc_url)
        
        # Or create directly with LDAP URL
        # ldap_url = "ldap://oraclenames.com:399/SERVICENAME,cn=OracleContext,dc=something,dc=somethingelse"
        # connector = OracleLDAPConnector(ldap_url=ldap_url)
        
        print("Connector created successfully")
        
        # Test connection (replace with actual credentials)
        username = "your_username"
        password = "your_password"
        
        test_results = connector.test_connection(username, password)
        print(f"Test results: {test_results}")
        
        # Get connection info
        if test_results['oracle_connection']:
            conn_info = connector.get_connection_info(username, password)
            print(f"Connection info: {conn_info}")
            
            # Execute a simple query
            results = connector.execute_query(
                username, password, 
                "SELECT SYSDATE, USER FROM DUAL"
            )
            print(f"Query results: {results}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
