package com.jobqueue.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

// Database connection pool for H2 database
public class Database {
    private static final String DB_URL = "jdbc:h2:./jobqueue;AUTO_SERVER=TRUE";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "";
    private static final int POOL_SIZE = 10;
    private static final int CONNECTION_TIMEOUT_SECONDS = 30;

    private final BlockingQueue<Connection> connectionPool;
    private volatile boolean initialized = false;
    private volatile boolean closed = false;

    public Database() {
        this.connectionPool = new ArrayBlockingQueue<>(POOL_SIZE);
    }

    public synchronized void initialize() throws SQLException {
        if (initialized) {
            System.out.println("Database already initialized");
            return;
        }

        System.out.println("Initializing database connection pool...");
        
        try {

            Class.forName("org.h2.Driver");
            System.out.println("H2 Driver loaded successfully");
            
            for (int i = 0; i < POOL_SIZE; i++) {
                Connection conn = createConnection();
                connectionPool.offer(conn);
            }
            
            System.out.println("Connection pool created with " + POOL_SIZE + " connections");
            
            initializeSchema();
            
            initialized = true;
            System.out.println("Database initialization complete");
            
        } catch (ClassNotFoundException e) {
            throw new SQLException("H2 Driver not found", e);
        }
    }

    private Connection createConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
    }

    public Connection getConnection() throws SQLException {
        if (!initialized) {
            throw new SQLException("Database not initialized. Call initialize() first.");
        }
        
        if (closed) {
            throw new SQLException("Database has been closed");
        }

        try {
            Connection conn = connectionPool.poll(CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            if (conn == null) {
                throw new SQLException("Timeout waiting for available connection");
            }
            
            if (conn.isClosed() || !conn.isValid(2)) {
                System.out.println("Connection invalid, creating new one");
                conn = createConnection();
            }
            
            return new PooledConnection(conn, this);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting for connection", e);
        }
    }

    // Return connection to pool (called by PooledConnection.close)
    synchronized void returnConnection(Connection connection) {
        if (closed) {
            closeConnectionSilently(connection);
            return;
        }
        
        if (connection == null) {
            return;
        }

        try {
            // Validate connection health
            if (connection.isClosed() || !connection.isValid(2)) {
                System.out.println("Connection invalid, creating replacement");
                closeConnectionSilently(connection);
                
                // Only create replacement if pool is below capacity
                if (connectionPool.size() < POOL_SIZE) {
                    connection = createConnection();
                } else {
                    System.err.println("WARNING: Pool at capacity, not replacing invalid connection");
                    return;
                }
            }
            
            // Try to return connection to pool
            if (!connectionPool.offer(connection)) {
                // Pool is full - this should not happen but be defensive
                System.err.println("ERROR: Connection pool full, closing connection");
                closeConnectionSilently(connection);
            }
            
        } catch (SQLException e) {
            System.err.println("Error returning connection to pool: " + e.getMessage());
            closeConnectionSilently(connection);
        }
    }
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
    // Close connection without throwing exceptions
    private void closeConnectionSilently(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("Failed to close connection: " + e.getMessage());
            }
        }
    }
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
    // Initialize schema from schema.sql file
    private void initializeSchema() throws SQLException {
        System.out.println("Initializing database schema...");
        
        String schemaPath = "schema.sql";
        
        if (!Files.exists(Paths.get(schemaPath))) {
            System.out.println("Warning: schema.sql file not found at " + schemaPath);
            System.out.println("Skipping schema initialization");
            return;
        }

        try {
            String schema = Files.readString(Paths.get(schemaPath));
            System.out.println("Read schema.sql successfully");
            

            try (Connection conn = connectionPool.take();
                 Statement stmt = conn.createStatement()) {
                
                StringBuilder currentStatement = new StringBuilder();
                int executedCount = 0;
                
                for (String line : schema.split("\n")) {
                    line = line.trim();
                    if (line.startsWith("--") || line.isEmpty()) {
                        continue;
                    }
                    
                    currentStatement.append(line).append(" ");
                    
                    if (line.endsWith(";")) {
                        String sql = currentStatement.toString().trim();
                        sql = sql.substring(0, sql.length() - 1).trim();
                        
                        if (!sql.isEmpty()) {
                            try {
                                stmt.execute(sql);
                                executedCount++;
                            } catch (SQLException e) {
                                System.err.println("Error executing statement: " + e.getMessage());
                                System.err.println("SQL: " + sql.substring(0, Math.min(sql.length(), 100)));
                            }
                        }
                        
                        currentStatement = new StringBuilder();
                    }
                }
                
                System.out.println("Executed " + executedCount + " SQL statements");
                
                connectionPool.offer(conn);
            }
            
            System.out.println("Database schema initialized successfully");
            
        } catch (IOException e) {
            System.err.println("Error reading schema.sql: " + e.getMessage());
            throw new SQLException("Failed to read schema file", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while initializing schema", e);
        }
    }
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
    public synchronized void close() {
        if (closed) {
            System.out.println("Database already closed");
            return;
        }

        System.out.println("Closing database connections...");
        closed = true;

        Connection conn;
        int closedCount = 0;
        
        while ((conn = connectionPool.poll()) != null) {
            try {
                if (!conn.isClosed()) {
                    conn.close();
                    closedCount++;
                }
            } catch (SQLException e) {
                System.err.println("Error closing connection: " + e.getMessage());
            }
        }

        System.out.println("Closed " + closedCount + " database connections");
        System.out.println("Database shutdown complete");
    }

    public boolean isInitialized() {
        return initialized;
    }

    public boolean isClosed() {
        return closed;
    }

    private static class PooledConnection implements Connection {
        private final Connection delegate;
        private final Database database;
        private boolean closed = false;

        public PooledConnection(Connection delegate, Database database) {
            this.delegate = delegate;
            this.database = database;
        }

        @Override
        public void close() throws SQLException {
            if (!closed) {
                closed = true;
                database.returnConnection(delegate);
            }
        }
        @Override
        public Statement createStatement() throws SQLException {
            return delegate.createStatement();
        }

        @Override
        public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException {
            return delegate.prepareStatement(sql);
        }

        @Override
        public java.sql.CallableStatement prepareCall(String sql) throws SQLException {
            return delegate.prepareCall(sql);
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            return delegate.nativeSQL(sql);
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            delegate.setAutoCommit(autoCommit);
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return delegate.getAutoCommit();
        }

        @Override
        public void commit() throws SQLException {
            delegate.commit();
        }

        @Override
        public void rollback() throws SQLException {
            delegate.rollback();
        }

        @Override
        public boolean isClosed() throws SQLException {
            return closed || delegate.isClosed();
        }

        @Override
        public java.sql.DatabaseMetaData getMetaData() throws SQLException {
            return delegate.getMetaData();
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            delegate.setReadOnly(readOnly);
        }

        @Override
        public boolean isReadOnly() throws SQLException {
            return delegate.isReadOnly();
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {
            delegate.setCatalog(catalog);
        }

        @Override
        public String getCatalog() throws SQLException {
            return delegate.getCatalog();
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            delegate.setTransactionIsolation(level);
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return delegate.getTransactionIsolation();
        }

        @Override
        public java.sql.SQLWarning getWarnings() throws SQLException {
            return delegate.getWarnings();
        }

        @Override
        public void clearWarnings() throws SQLException {
            delegate.clearWarnings();
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return delegate.createStatement(resultSetType, resultSetConcurrency);
        }

        @Override
        public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public java.util.Map<String, Class<?>> getTypeMap() throws SQLException {
            return delegate.getTypeMap();
        }

        @Override
        public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException {
            delegate.setTypeMap(map);
        }

        @Override
        public void setHoldability(int holdability) throws SQLException {
            delegate.setHoldability(holdability);
        }

        @Override
        public int getHoldability() throws SQLException {
            return delegate.getHoldability();
        }

        @Override
        public java.sql.Savepoint setSavepoint() throws SQLException {
            return delegate.setSavepoint();
        }

        @Override
        public java.sql.Savepoint setSavepoint(String name) throws SQLException {
            return delegate.setSavepoint(name);
        }

        @Override
        public void rollback(java.sql.Savepoint savepoint) throws SQLException {
            delegate.rollback(savepoint);
        }

        @Override
        public void releaseSavepoint(java.sql.Savepoint savepoint) throws SQLException {
            delegate.releaseSavepoint(savepoint);
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return delegate.prepareStatement(sql, autoGeneratedKeys);
        }

        @Override
        public java.sql.PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return delegate.prepareStatement(sql, columnIndexes);
        }

        @Override
        public java.sql.PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            return delegate.prepareStatement(sql, columnNames);
        }

        @Override
        public java.sql.Clob createClob() throws SQLException {
            return delegate.createClob();
        }

        @Override
        public java.sql.Blob createBlob() throws SQLException {
            return delegate.createBlob();
        }

        @Override
        public java.sql.NClob createNClob() throws SQLException {
            return delegate.createNClob();
        }

        @Override
        public java.sql.SQLXML createSQLXML() throws SQLException {
            return delegate.createSQLXML();
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            return delegate.isValid(timeout);
        }

        @Override
        public void setClientInfo(String name, String value) throws java.sql.SQLClientInfoException {
            delegate.setClientInfo(name, value);
        }

        @Override
        public void setClientInfo(java.util.Properties properties) throws java.sql.SQLClientInfoException {
            delegate.setClientInfo(properties);
        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            return delegate.getClientInfo(name);
        }

        @Override
        public java.util.Properties getClientInfo() throws SQLException {
            return delegate.getClientInfo();
        }

        @Override
        public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return delegate.createArrayOf(typeName, elements);
        }

        @Override
        public java.sql.Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return delegate.createStruct(typeName, attributes);
        }

        @Override
        public void setSchema(String schema) throws SQLException {
            delegate.setSchema(schema);
        }

        @Override
        public String getSchema() throws SQLException {
            return delegate.getSchema();
        }

        @Override
        public void abort(java.util.concurrent.Executor executor) throws SQLException {
            delegate.abort(executor);
        }

        @Override
        public void setNetworkTimeout(java.util.concurrent.Executor executor, int milliseconds) throws SQLException {
            delegate.setNetworkTimeout(executor, milliseconds);
        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            return delegate.getNetworkTimeout();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return delegate.isWrapperFor(iface);
        }
    }
}
