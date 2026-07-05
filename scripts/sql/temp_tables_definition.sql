CREATE table IF NOT EXISTS temp_cities
(
    LIKE cities
    INCLUDING DEFAULTS
);

CREATE table IF NOT EXISTS temp_airlines
(
    LIKE airlines
    INCLUDING DEFAULTS
);

CREATE table IF NOT EXISTS temp_airports
(
    iata varchar(3),
    name text NOT NULL,
    city text NOT NULL,
    latitude numeric(10, 6) NOT NULL,
    longitude numeric(10, 6) NOT NULL
);

CREATE table IF NOT EXISTS temp_flights
(
    LIKE flights
    INCLUDING DEFAULTS
);