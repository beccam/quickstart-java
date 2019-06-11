package com.datastax.quickstart;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * This class is meant to be a standalone walkthrough and understand driver v2.
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public class GettingStarted {
    
    /** Logging on Console. */
    private static final Logger LOGGER = LoggerFactory.getLogger(GettingStartedQueryBuilder.class);
    
    // ---> Configuration
    public static final String cassandraHost       = "127.0.0.1";  // Contact point hostname
    public static final int    cassandraPort       = 9042;         // Contact point port
    public static final String localDataCenterName = "dc1";        // DataCente name, required from v2.
    public static final String keyspaceName        = "demo";       // KeySpace Name
    // <---
    
    public static void main(String[] args) {
        
        LOGGER.info("Start demo application", keyspaceName);
        
        // (1) - Creating a Keyspace, no keyspace provided in Session
        try (CqlSession tempSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
                .withLocalDatacenter(localDataCenterName).build()) {
          tempSession.execute(""
                  + "CREATE KEYSPACE IF NOT EXISTS demo "
                  + "WITH replication = {"
                  + "      'class': 'SimpleStrategy', "
                  + "      'replication_factor': '1'"
                  + "}");
          LOGGER.info("+ Keyspace '{}' has been created (if needed)", keyspaceName);
        }
        
        // CqlSession should be unique and properly closed
        try(CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
                .withKeyspace(keyspaceName)  // Now we provide the keyspace 
                .withLocalDatacenter(localDataCenterName)
                .build()) {
            
            // (2) Create the User table
            cqlSession.execute("CREATE TABLE IF NOT EXISTS demo.users (" 
                  + " lastname text PRIMARY KEY," 
                  + " age int," 
                  + " city text," 
                  + " email text," + 
                    " firstname text)");
            LOGGER.info("+ Table '{}' has been created (if needed)", "users");
            
            // (3) - Insertings users data
            // Basic Way, not a good idea except for dynamic queries
            cqlSession.execute(
                    "INSERT INTO demo.users (lastname, age, city, email, firstname) "
                    + "VALUES ('Snow', 35, 'Winterfell', 'jon.snow@got.com', 'Jon')");
            // Please prepared your statements first and bind variables
            PreparedStatement queryInsertUser = cqlSession.prepare(
                    "INSERT INTO demo.users (firstname, lastname, age, city, email) "
                    + "VALUES (?,?,?,?,?)");
            cqlSession.execute(queryInsertUser.bind("Arya", "Stark", 16, "Winterfell", "aray.stark@got.com"));
            cqlSession.execute(queryInsertUser.bind("Jamie", "Lannister", 32, "Kingslandhill", "jamie@got.com"));
            cqlSession.execute(queryInsertUser.bind("Ramsay", "Bolton", 42, "CastleRock", "ramsay@got.com"));
            cqlSession.execute(queryInsertUser.bind("Robert", "Baratheon", 58, "Kingslandhill", "robert@got.com"));
            LOGGER.info("+ 5 users have been inserted");
            
            // (4) - Retrieve all lastnames
            List < String > families = cqlSession.execute("SELECT lastname FROM demo.users")
                      .all() // because I know there is no need for paging
                      .stream()
                      .map(row -> row.getString("lastname"))
                      .collect(Collectors.toList());
            LOGGER.info("+ List of families is {}", families);
            
            // (5) - Find a user by its id (unique record as searched per PK)
            PreparedStatement queryFindUser = cqlSession.prepare("SELECT * from demo.users WHERE lastname = ?");
            ResultSet rs = cqlSession.execute(queryFindUser.bind("DoesNotExist"));
            if (0 == rs.getAvailableWithoutFetching()) {
                LOGGER.info("+ Invalid lastname, 0 results returned.");
            }
            Row jonSnowRow = cqlSession.execute(queryFindUser.bind("Snow")).one();
            LOGGER.info("+ Jon city is '{}'", jonSnowRow.getString("city"));
            
            // (6) - Delete a user
            PreparedStatement deleteUser = cqlSession.prepare("DELETE from demo.users WHERE lastname = ?");
            cqlSession.execute(deleteUser.bind("Snow"));
            LOGGER.info("+ Jon entry has been deleted");
            
            // (7) - Update value
            PreparedStatement updateUser = cqlSession.prepare("UPDATE demo.users SET age = ? WHERE lastname = ?");
            cqlSession.execute(updateUser.bind(20, "Stark"));
            LOGGER.info("+ Arya record has been updated");
        }
        LOGGER.info("Application closed");
    }

}
