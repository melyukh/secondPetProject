BEGIN;
INSERT INTO cities(name)
SELECT DISTINCT name 
FROM temp_cities
ON CONFLICT(name) DO 
    NOTHING;

INSERT INTO airports(iata, name, city_id, latitude, longitude)
SELECT iata, name, c.id, latitude, longitude
FROM temp_airports ta
JOIN cities c
ON c.name = ta.city
ON CONFLICT(iata) DO
    NOTHING;

INSERT INTO airlines(iata, name)
SELECT iata, name
FROM temp_airlines
ON CONFLICT(iata) DO
    NOTHING;
INSERT INTO flights(
    flight_date,
    day_of_week,
    airline,
    flight_number,
    tail_number,
    origin_airport,
    destination_airport,
    departure_delay,
    arrival_delay, 
    distance,
    is_long_haul,
    diverted,
    cancelled,
    cancellation_reason,
    air_system_delay,
    security_delay,
    airline_delay,
    late_aircraft_delay,
    weather_delay,
    departure_hour,
    arrival_hour
)
SELECT  tf.date,
        tf.day_of_week,
        airlines.iata,
        tf.flight_number,
        tf.tail_number,
        origin.iata,
        destination.iata,
        tf.departure_delay,
        tf.arrival_delay, 
COALESCE(tf.distance, distance_by_longs_and_lats(origin.latitude, origin.longitude, destination.latitude, destination.longitude)),
        is_long(COALESCE(tf.distance, distance_by_longs_and_lats(origin.latitude, origin.longitude, destination.latitude, destination.longitude))),
        tf.diverted,
        tf.cancelled,
        tf.cancellation_reason,
        tf.air_system_delay,
        tf.security_delay,
        tf.airline_delay,
        tf.late_aircraft_delay,
        tf.weather_delay,
        tf.departure_hour,
        tf.arrival_hour
FROM temp_flights AS tf
INNER JOIN airports AS origin
ON origin.iata =  tf.origin_airport
INNER JOIN airports AS destination
ON destination.iata =  tf.destination_airport
INNER JOIN airlines
ON airlines.iata = tf.airline;

--карантин для аэропортов
INSERT INTO airports_quarantine(iata, name, city, reject_reason)
SELECT ta.iata, ta.name, ta.city, 'city not found in cities table'
FROM temp_airports ta
LEFT JOIN cities c ON c.name = ta.city
WHERE c.id IS NULL;

INSERT INTO flights_quarantine(flight_number, airline, origin_airport, destination_airport, reject_reason)
SELECT tf.flight_number, tf.airline, tf.origin_airport, tf.destination_airport, 
CASE
WHEN origin.iata IS NULL      THEN 'origin airport not found'
WHEN destination.iata IS NULL THEN 'destination airport not found'
WHEN airlines.iata IS NULL    THEN 'airline not found'
END
FROM temp_flights tf
LEFT JOIN temp_airports AS origin
ON tf.origin_airport = origin.iata
LEFT JOIN temp_airports AS destination
ON tf.destination_airport = destination.iata
LEFT JOIN temp_airlines
ON temp_airlines.iata = tf.airline
WHERE origin.iata IS NULL
OR destination.iata IS NULL
OR temp_airlines.iata IS NULL;

TRUNCATE TABLE temp_flights;
TRUNCATE TABLE temp_airlines;
TRUNCATE TABLE temp_cities;
TRUNCATE TABLE temp_airports;
COMMIT;
проанализируй и скажи, сколько тут ошибок. без их объяснения