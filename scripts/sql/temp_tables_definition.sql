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
    LIKE airports
    INCLUDING DEFAULTS
);

CREATE table IF NOT EXISTS temp_flights
(
    LIKE flights
    INCLUDING DEFAULTS
);