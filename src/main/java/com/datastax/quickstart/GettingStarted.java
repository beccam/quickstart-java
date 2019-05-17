package com.datastax.quickstart;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import java.net.InetSocketAddress;

public class GettingStarted {

    public static void main(String[] args) {

        // TO DO: Fill in address, data center
        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withKeyspace("demo")
                .withLocalDatacenter("datacenter1")
                .build();

     setUser(session, "Jones", 35, "Austin", "bob@example.com", "Bob");

     getUser(session, "Jones");

     updateUser(session, 36, "Jones");

     getUser(session, "Jones");

     deleteUser(session, "Jones");

    session.close();

        }

    public static void setUser(CqlSession session, String lastname, int age, String city, String email, String firstname) {

        //TO DO: execute SimpleStatement that inserts one user into the table
    }

    public static void getUser(CqlSession session, String lastname) {

        //TO DO: execute SimpleStatement that retrieves one user from the table

        //TO DO: print firstname and age of user
    }


    public static void updateUser(CqlSession session, int age, String lastname) {

        //TO DO: execute SimpleStatement that updates the age of one user
    }

    public static void deleteUser(CqlSession session, String lastname) {

       //TO DO: execute SimpleStatement that deletes one user from the table
    }



    }


