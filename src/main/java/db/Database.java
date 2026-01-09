package db;

/**
 * Database connection manager.
 * 
 * This is a STUB to allow compilation. Student B will implement
 * the actual H2 database connection, schema creation, and connection pooling.
 * 
 * Responsibilities:
 * - Establish connection to H2 database
 * - Create tables if they don't exist (schema initialization)
 * - Provide connection pooling (optional, bonus)
 * - Handle connection cleanup
 */
public class Database {
    
    /**
     * Initializes the database connection.
     * 
     * Tasks:
     * 1. Connect to H2 database (file-based or in-memory)
     * 2. Create tables if not exist (jobs, job_logs)
     * 3. Set up connection pool (optional)
     */
    public Database() {
        System.out.println("[STUB] Database initialized");
        // TODO: Student B will implement H2 connection
        // Example: jdbc:h2:./data/jobqueue;AUTO_SERVER=TRUE
    }
    
    /**
     * Gets a database connection.
     * 
     * @return a JDBC Connection object
     */
    // TODO: public Connection getConnection() throws SQLException
    
    /**
     * Closes the database and all connections.
     */
    public void close() {
        System.out.println("[STUB] Database closed");
        // TODO: Close connection pool and connections
    }
}
