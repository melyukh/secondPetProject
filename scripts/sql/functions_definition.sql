CREATE OR REPLACE FUNCTION is_long(distance numeric(10, 6))
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS
$$
    SELECT distance > 1000;
$$;

CREATE OR REPLACE FUNCTION distance_by_longs_and_lats(
    latitude_o numeric(10, 6),
    longitude_o numeric(10, 6),
    latitude_d numeric(10, 6),
    longitude_d numeric(10, 6)    
)
RETURNS numeric(8, 3)
LANGUAGE plpgsql
IMMUTABLE
AS
$$
BEGIN
    RETURN (6371 * 2 * ASIN(SQRT(
        POWER(SIN(RADIANS(latitude_d::float8 - latitude_o::float8) / 2), 2) +
        COS(RADIANS(latitude_d::float8)) * COS(RADIANS(latitude_o::float8)) *
        POWER(SIN(RADIANS(longitude_d::float8 - longitude_o::float8) / 2), 2)
    )))::numeric(8, 3);
END;
$$;