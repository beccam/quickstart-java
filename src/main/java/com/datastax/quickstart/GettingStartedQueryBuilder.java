package com.datastax.quickstart;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.relation.Relation.column;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;

/**
 * Sample using the Java Driver.
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class GettingStartedQueryBuilder {
    
    /** Logging on Console. */
    private static final Logger LOGGER = LoggerFactory.getLogger(GettingStartedQueryBuilder.class);

    // ---> Configuration
    public static final String cassandraHost       = "127.0.0.1";  // Contact point hostname
    public static final int    cassandraPort       = 9042;         // Contact point port
    public static final String localDataCenterName = "dc1";        // DataCente name, required from v2.
    public static final String keyspaceName        = "demo";       // KeySpace Name
    // <---
    
    /** Data Modelling. */ 
    public static final CqlIdentifier USERS     = CqlIdentifier.fromCql("users");
    public static final CqlIdentifier LASTNAME  = CqlIdentifier.fromCql("lastname");
    public static final CqlIdentifier FIRSTNAME = CqlIdentifier.fromCql("firstname");
    public static final CqlIdentifier AGE       = CqlIdentifier.fromCql("age");
    public static final CqlIdentifier CITY      = CqlIdentifier.fromCql("city");
    public static final CqlIdentifier EMAIL     = CqlIdentifier.fromCql("email");
    public static final CqlIdentifier KEYSPACE  = CqlIdentifier.fromCql(keyspaceName);
    
    public static void main(String[] args) {
        
        LOGGER.info("Start demo application", keyspaceName);
        
        // (1) - Creating a Keyspace, no keyspace provided in Session
        try (CqlSession tempSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
                .withLocalDatacenter(localDataCenterName)
                .build()) {
          tempSession.execute(SchemaBuilder.createKeyspace(keyspaceName)
                               .ifNotExists()
                               .withSimpleStrategy(1).build());
          LOGGER.info("+ Keyspace '{}' has been created (if needed)", keyspaceName);
        }
        
        // CqlSession should be unique and properly closed
        try(CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
                .withKeyspace(keyspaceName)
                .withLocalDatacenter(localDataCenterName)
                .build()) {
            
            // (2) Create the User table
            cqlSession.execute(
                    SchemaBuilder.createTable(KEYSPACE, USERS)
                    .ifNotExists()
                    .withPartitionKey(LASTNAME, DataTypes.TEXT)
                    .withColumn(AGE, DataTypes.INT)
                    .withColumn(CITY, DataTypes.TEXT)
                    .withColumn(EMAIL, DataTypes.TEXT)
                    .withColumn(FIRSTNAME, DataTypes.TEXT)
                    .build());
            LOGGER.info("+ Table '{}' has been created (if needed)", USERS.asInternal());
            
            // (3) - Insertings users data
            PreparedStatement queryInsertUser = cqlSession.prepare(QueryBuilder.insertInto(KEYSPACE, USERS)
                    .value(FIRSTNAME, bindMarker(FIRSTNAME))
                    .value(LASTNAME, bindMarker(LASTNAME))
                    .value(AGE, bindMarker(AGE))
                    .value(CITY, bindMarker(CITY))
                    .value(EMAIL, bindMarker(EMAIL))
                    .build());
            cqlSession.execute(queryInsertUser.bind("Jon",    "Snow",      35, "Winterfell",     "jon.snow@got.com" ));
            cqlSession.execute(queryInsertUser.bind("Arya",   "Stark",     16, "Winterfell",     "aray.stark@got.com"));
            cqlSession.execute(queryInsertUser.bind("Jamie",  "Lannister", 32, "Kingslandhill",  "jamie@got.com"));
            cqlSession.execute(queryInsertUser.bind("Ramsay", "Bolton",    42, "CastleRock",     "ramsay@got.com"));
            cqlSession.execute(queryInsertUser.bind("Robert", "Baratheon", 58, "Kingslandhill",  "robert@got.com"));
            LOGGER.info("+ 5 users have been inserted");
            
            // (4) - Retrieve all lastnames
            PreparedStatement queryfindAllLastNames = cqlSession.prepare(selectFrom(KEYSPACE, USERS).column(LASTNAME).build());
            List < String > families = cqlSession.execute(queryfindAllLastNames.bind())
                    .all().stream()
                    .map(row -> row.getString(LASTNAME))
                    .collect(Collectors.toList());
            LOGGER.info("+ List of families is {}", families);
            
            // (5) - Find a user by its id (unique record as searched per PK)
            PreparedStatement queryfindByLastName = cqlSession.prepare(selectFrom(KEYSPACE, USERS).all()
                                    .where(column(LASTNAME).isEqualTo(bindMarker(LASTNAME)))
                                    .build());
            Optional<Row> noexist =
                    Optional.ofNullable(cqlSession.execute(queryfindByLastName.bind("DoesNotExist")).one());
            if (!noexist.isPresent()) {
                LOGGER.info("+ Invalid lastname, 0 results returned.");
            }
            Optional<Row> jonRow = Optional.ofNullable(cqlSession.execute(queryfindByLastName.bind("Snow")).one());
            jonRow.ifPresent(r -> LOGGER.info("+ Jon city is '{}'", r.getString(CITY)));
            
            // (6) - Delete a user
            PreparedStatement queryDeleteByLastName = cqlSession.prepare(
                    deleteFrom(KEYSPACE, USERS)
                    .where(column(LASTNAME).isEqualTo(bindMarker(LASTNAME)))
                    .build());
            cqlSession.execute(queryDeleteByLastName.bind("Snow"));
            LOGGER.info("+ Jon entry has been deleted");
            
            // (7) - Update values
            PreparedStatement queryUpdateAgeLastName = cqlSession.prepare(QueryBuilder
                    .update(USERS).set(Assignment.setColumn(AGE, bindMarker(AGE)))
                    .where(column(LASTNAME).isEqualTo(bindMarker(LASTNAME))).build());
            cqlSession.execute(queryUpdateAgeLastName.bind(20, "Stark"));
            LOGGER.info("+ Arya record has been updated");
        }
        LOGGER.info("Application closed");
    }
    
   
}
