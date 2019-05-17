package com.datastax.quickstart;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

/**
 * Sample using the Java Driver.
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class DriverGettingStarted {

    /** Configuration. */
    public static final String cassandraHost  = "127.0.0.1";
    public static final int    cassandraPort  = 9042;
    public static final String dataCenterName = "DC1";
    public static final String keyspaceName   = "demo";
    
    /** Data Modelling. */ 
    public static final CqlIdentifier USERS     = CqlIdentifier.fromCql("users");
    public static final CqlIdentifier LASTNAME  = CqlIdentifier.fromCql("lastname");
    public static final CqlIdentifier FIRSTNAME = CqlIdentifier.fromCql("firstname");
    public static final CqlIdentifier AGE       = CqlIdentifier.fromCql("age");
    public static final CqlIdentifier CITY      = CqlIdentifier.fromCql("city");
    public static final CqlIdentifier EMAIL     = CqlIdentifier.fromCql("email");
    public static final CqlIdentifier KEYSPACE  = CqlIdentifier.fromCql(keyspaceName);
    
    /** Reuse queries. */
    private static PreparedStatement queryInsertUser = null;
    
    public static void main(String[] args) {
        
        // Connection to dse
        try(CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
                .withKeyspace(keyspaceName)
                .withLocalDatacenter(dataCenterName)
                .build()) {
            
            // 1 - Create a table
            createTableUsers(cqlSession);
            
            // 2 - Insert a user
            insertUser(cqlSession, "Cedrick", "lunven",  35, "Paris",   "cel@gmail.com");
            insertUser(cqlSession, "David",   "Gilardi", 36, "Orlando", "lord@gmail.com");
            
            // 3 - Retrieve my user List
            
            // 4 - Delete a user
            
        }
    }
    
    
    
    /** 
     *  CREATE TABLE demo.users (
     *      lastname text PRIMARY KEY,
     *      age int,
     *      city text,
     *      email text,
     *      firstname text);
     */
    protected static void createTableUsers(CqlSession cqlSession) {
        cqlSession.execute(
                SchemaBuilder.createTable(KEYSPACE, USERS)
                .ifNotExists()
                .withPartitionKey(LASTNAME, DataTypes.TEXT)
                .withColumn(AGE, DataTypes.INT)
                .withColumn(CITY, DataTypes.TEXT)
                .withColumn(EMAIL, DataTypes.TEXT)
                .withColumn(FIRSTNAME, DataTypes.TEXT)
                .build());
    }
    
    protected static void insertUser(CqlSession cqlSession, 
            String firstname, String lastname, 
            int age, String city, String email) {
        
        // Init Once
        if (null == queryInsertUser) {
            queryInsertUser = cqlSession.prepare(QueryBuilder.insertInto(KEYSPACE, USERS)
                .value(LASTNAME, bindMarker(LASTNAME))
                .value(AGE, bindMarker(AGE))
                .value(CITY, bindMarker(CITY))
                .value(EMAIL, bindMarker(EMAIL))
                .value(FIRSTNAME, bindMarker(FIRSTNAME))
                .build());
        }
        // Execute
        cqlSession.execute(queryInsertUser.bind(lastname, age, city, email, firstname));
    }

}
