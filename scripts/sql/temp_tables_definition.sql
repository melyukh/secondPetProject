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
    iata varchar(3) PRIMARY KEY,
    name text NOT NULL,
    latitude numeric(10, 6) NOT NULL,
    longitude numeric(10, 6) NOT NULL,
);

CREATE table IF NOT EXISTS temp_flights
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
);