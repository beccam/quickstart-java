CREATE KEYSPACE demo
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE demo.users (
    lastname text PRIMARY KEY,
    age int,
    city text,
    email text,
    firstname text);
    
    