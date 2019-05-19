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
public class DriverGettingStarted {
    
    /** Initialize dedicated connection to ETCD system. */
    private static final Logger LOGGER = LoggerFactory.getLogger(DriverGettingStarted.class);

    /** Configuration. */
    public static final String cassandraHost  = "127.0.0.1";
    public static final int    cassandraPort  = 9042;
    public static final String dataCenterName = "dc1";
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
    private static PreparedStatement queryInsertUser        = null;
    private static PreparedStatement queryfindAllLastNames  = null;
    private static PreparedStatement queryfindByLastName    = null;
    private static PreparedStatement queryDeleteByLastName  = null;
    private static PreparedStatement queryUpdateAgeLastName = null;
    
    public static void main(String[] args) {
        
        LOGGER.info("Starting Application");
        
        createSimpleKeyspace(keyspaceName);
        LOGGER.info("+ Keyspace '{}' created", keyspaceName);
        
        // CqlSession should be unique and properly closed
        try(CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
                .withKeyspace(keyspaceName)
                .withLocalDatacenter(dataCenterName)
                .build()) {
            
            // 1 - Create a table
            createTableUsers(cqlSession);
            LOGGER.info("+ Table '{}' created", USERS.asInternal());
            
            // 2 - Insert users
            insertUser(cqlSession, "Jon",    "Snow",      35, "Winterfell",    "jon.snow@got.com");
            insertUser(cqlSession, "Arya",   "Stark",     62, "Winterfell",    "aray.stark@got.com");
            insertUser(cqlSession, "Jamie",  "Lannister", 30, "Kingslandhill", "jamie@got.com");
            insertUser(cqlSession, "Ramsay", "Bolton",    25, "Stonehenge",    "ramsay@got.com");
            insertUser(cqlSession, "Robert", "Baratheon", 58, "Kingslandhill", "robert@got.com");
            LOGGER.info("+ 5 users inserted");
            
            // 3 - Retrieve all lastnames
            List < String > families = findLastnames(cqlSession);
            LOGGER.info("+ List of families {}", families);
            
            // 4 - Find a user by its id (unique record as searched per PK)
            Optional<Row> jonRow = findUserByLastName(cqlSession, "Snow");
            jonRow.ifPresent(r -> LOGGER.info("+ Jon city is '{}'", r.getString(CITY)));
            
            // 5 - Delete a user
            deleteByLastName(cqlSession, "Snow");
            jonRow = findUserByLastName(cqlSession, "Snow");
            if(!jonRow.isPresent()) {
                LOGGER.info("+ OK, record deleted");
            }
            
            // 6 - Update value
            updateUserAge(cqlSession, "Stark", 150);
            LOGGER.info("+ OK, record updated");
        }
        LOGGER.info("Application closed");
    }
    
    protected static void updateUserAge(CqlSession cqlSession, String lastname, int age) {
        if (null == queryUpdateAgeLastName) {
            queryUpdateAgeLastName = cqlSession.prepare(QueryBuilder
                    .update(USERS).set(Assignment.setColumn(AGE, bindMarker(AGE)))
                    .where(column(LASTNAME).isEqualTo(bindMarker(LASTNAME))).build());
        }
        cqlSession.execute(queryUpdateAgeLastName.bind(age, lastname));
    }
    
    protected static List < String > findLastnames(CqlSession cqlSession) {
        if (null == queryfindAllLastNames) {
            queryfindAllLastNames = cqlSession.prepare(
                            selectFrom(KEYSPACE, USERS).column(LASTNAME).build());
        };
        return cqlSession.execute(queryfindAllLastNames.bind())
                         .all().stream()
                         .map(row -> row.getString(LASTNAME))
                         .collect(Collectors.toList());
    }
    
    protected static void deleteByLastName(CqlSession cqlSession, String lastName) {
        if (null == queryDeleteByLastName) {
            queryDeleteByLastName = cqlSession.prepare(
                    deleteFrom(KEYSPACE, USERS)
                    .where(column(LASTNAME).isEqualTo(bindMarker(LASTNAME)))
                    .build());
        }
        cqlSession.execute(queryDeleteByLastName.bind(lastName));
    }
    
    protected static Optional <Row> findUserByLastName(CqlSession cqlSession, String lastName) {
        if (queryfindByLastName == null ) {
            queryfindByLastName = cqlSession.prepare(
                            selectFrom(KEYSPACE, USERS).all()
                            .where(column(LASTNAME).isEqualTo(bindMarker(LASTNAME)))
                            .build());
        }
        return Optional.ofNullable(cqlSession.execute(queryfindByLastName.bind(lastName)).one());
    }
    
    protected static void createSimpleKeyspace(String myKeyspace) {
        try (CqlSession tempSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
                .withLocalDatacenter(dataCenterName)
                .build()) {
          tempSession.execute(
                  SchemaBuilder.createKeyspace(myKeyspace)
                               .ifNotExists()
                               .withSimpleStrategy(1).build());
        }
    }
    
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
        // Init Once (prepare)
        if (null == queryInsertUser) {
            queryInsertUser = cqlSession.prepare(QueryBuilder.insertInto(KEYSPACE, USERS)
                .value(LASTNAME, bindMarker(LASTNAME))
                .value(AGE, bindMarker(AGE))
                .value(CITY, bindMarker(CITY))
                .value(EMAIL, bindMarker(EMAIL))
                .value(FIRSTNAME, bindMarker(FIRSTNAME))
                .build());
        }
        // PrepareStatement -> BoundStatement
        cqlSession.execute(queryInsertUser.bind(lastname, age, city, email, firstname));
    }
}
