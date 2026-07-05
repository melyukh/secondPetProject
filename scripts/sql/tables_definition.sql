CREATE TABLE IF NOT EXISTS cities
(
    id SERIAL PRIMARY KEY,
    name text UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS airports
(
    iata varchar(3) PRIMARY KEY,
    name text NOT NULL,
    city_id int NOT NULL,
    latitude numeric(10, 6) NOT NULL,
    longitude numeric(10, 6) NOT NULL,

    CONSTRAINT fk_on_city
    FOREIGN KEY (city_id) REFERENCES cities(id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS airlines
(
    iata VARCHAR(2) PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE IF NOT EXISTS flights
(
    flight_date date NOT NULL,
    day_of_week int NOT NULL,

    airline varchar(2) NOT NULL,
    flight_number int NOT NULL,
    tail_number int NOT NULL,
    
    origin_airport varchar(3) NOT NULL,
    destination_airport varchar(3) NOT NULL,

    departure_delay int NOT NULL DEFAULT 0,
    arrival_delay int NOT NULL DEFAULT 0,

    distance numeric(8, 3) NOT NULL DEFAULT 0.0,
    is_long_haul boolean NOT NULL,

    diverted boolean NOT NULL DEFAULT false,
    cancelled boolean NOT NULL DEFAULT false,
    cancellation_reason char(1),

    air_system_delay int NOT NULL DEFAULT 0,
    security_delay int NOT NULL DEFAULT 0,
    airline_delay int NOT NULL DEFAULT 0,
    late_aircraft_delay int NOT NULL DEFAULT 0,
    weather_delay int NOT NULL DEFAULT 0,

    departure_hour int,
    arrival_hour int,

    CONSTRAINT fk_on_origin_airport
    FOREIGN KEY (origin_airport) REFERENCES airports(iata)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CONSTRAINT fk_on_destination_airport
    FOREIGN KEY (destination_airport) REFERENCES airports(iata)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CONSTRAINT fk_on_airline
    FOREIGN KEY (airline) REFERENCES airlines(iata)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_flights_origin_airport ON flights(origin_airport);
CREATE INDEX IF NOT EXISTS idx_flights_destination_airport ON flights(destination_airport);
CREATE INDEX IF NOT EXISTS idx_flights_airline ON flights(airline);