CREATE TABLE IF NOT EXISTS Orders (
    id serial NOT NULL PRIMARY KEY,
    data jsonb NOT NULL
);