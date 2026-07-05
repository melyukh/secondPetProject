CREATE TABLE IF NOT EXISTS airports_quarantine
(
    iata VARCHAR(3),
    name TEXT,
    city TEXT,
    reject_reason TEXT,
    rejected_at date NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS flights_quarantine
(
    flight_number int NOT NULL,
    airline varchar(2) NOT NULL,
    origin_airport varchar(3) NOT NULL,
    destination_airport varchar(3) NOT NULL,
    reject_reason TEXT,
    rejected_at date NOT NULL DEFAULT NOW()
);