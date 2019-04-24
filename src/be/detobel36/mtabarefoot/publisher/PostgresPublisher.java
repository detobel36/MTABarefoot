package be.detobel36.mtabarefoot.publisher;

import com.bmwcarit.barefoot.util.SourceException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 *
 * @author remy
 */
public class PostgresPublisher {
    
    private final String host;
    private final int port;
    private final String database;
    private final String user;
    private final String password;

    private Connection connection = null;

    /**
     * Creates a PostgresPublisher object.
     *
     * @param host Host name of the database server.
     * @param port Port of the database server.
     * @param database Name of the database.
     * @param user User for accessing the database.
     * @param password Password of the user.
     */
    public PostgresPublisher(String host, int port, String database, String user, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
    }

    /**
     * Checks if the database connection has been established.
     *
     * @return True if database connection is established, false otherwise.
     */
    public boolean isOpen() {
        try {
            return connection != null && connection.isValid(5);
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Connects to the database.
     *
     * @throws SourceException thrown if opening database connection failed.
     */
    public void open() throws SourceException {
        try {
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            // props.setProperty("ssl","true");
            connection = DriverManager.getConnection(url, props);
//            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new SourceException("Opening PostgreSQL connection failed: " + e.getMessage(), e);
        }
    }

    /**
     * Closes database connection.
     *
     * @throws SourceException thrown if closing database connection failed.
     */
    public void close() throws SourceException {
        try {
            connection.close();
            connection = null;
        } catch (SQLException e) {
            throw new SourceException("Closing PostgreSQL connection failed: " + e.getMessage(), e);
        }
    }
    
    public PreparedStatement prepare(final String strRequest) throws SQLException, ClassNotFoundException {
        return connection.prepareStatement(strRequest);
    }

    public boolean execute(final String statement)  throws SQLException, ClassNotFoundException {
        final Statement st = connection.createStatement(1005, 1008);
        st.setFetchSize(100);
        return st.execute(statement);
    }
    
    public ResultSet getResultSet(final String statement) throws SQLException, ClassNotFoundException {
        final Statement st = connection.createStatement(1005, 1008);
        st.setFetchSize(100);
        return st.executeQuery(statement);
    }
    
    public void commit() throws SQLException {
        connection.commit();
    }
    
}
