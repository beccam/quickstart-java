CREATE KEYSPACE IF NOT EXISTS demo
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE IF NOT EXISTS demo.users (
    lastname text PRIMARY KEY,
    age int,
    city text,
    email text,
    firstname text);
    
    