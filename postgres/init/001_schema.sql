CREATE TABLE IF NOT EXISTS cities (
    city_id      BIGINT PRIMARY KEY,
    city         VARCHAR(100) NOT NULL,
    city_ascii   VARCHAR(100),
    lat          NUMERIC(9,6) NOT NULL,
    lng          NUMERIC(9,6) NOT NULL,
    country      VARCHAR(100) NOT NULL,
    iso2         CHAR(2),
    iso3         CHAR(3),
    admin_name   VARCHAR(100),
    capital      VARCHAR(20),
    population   INT,

    CONSTRAINT uq_city_coordinates UNIQUE (lat, lng)
);

CREATE TABLE IF NOT EXISTS daily_climate (
    id BIGINT GENERATED ALWAYS AS IDENTITY,

    date DATE NOT NULL,
    city_id BIGINT NOT NULL,

    temperature_2m_max NUMERIC(6,2),
    temperature_2m_min NUMERIC(6,2),
    temperature_2m_mean NUMERIC(6,2),

    cloud_cover_mean NUMERIC(5,2),

    relative_humidity_2m_max NUMERIC(5,2),
    relative_humidity_2m_min NUMERIC(5,2),
    relative_humidity_2m_mean NUMERIC(5,2),

    soil_moisture_0_to_10cm_mean NUMERIC(8,5),

    precipitation_sum NUMERIC(8,2),
    rain_sum NUMERIC(8,2),
    snowfall_sum NUMERIC(8,2),

    wind_speed_10m_mean NUMERIC(7,2),
    wind_speed_10m_max NUMERIC(7,2),

    pressure_msl_mean NUMERIC(8,2),
    shortwave_radiation_sum NUMERIC(10,2),

    river_discharge NUMERIC(12,2),

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),

    CONSTRAINT uq_daily_climate_city_date
        UNIQUE (date, city_id),

    CONSTRAINT fk_daily_climate_city
        FOREIGN KEY (city_id)
        REFERENCES cities(city_id)
);

CREATE TABLE IF NOT EXISTS daily_air_quality (
    id BIGINT GENERATED ALWAYS AS IDENTITY,

    city_id BIGINT NOT NULL,
    date DATE NOT NULL,

    pm2_5_mean NUMERIC(8,2),
    pm10_mean NUMERIC(8,2),
    carbon_dioxide_mean NUMERIC(10,2),

    ozone_8h_max NUMERIC(10,2),
    carbon_monoxide_8h_max NUMERIC(10,2),

    nitrogen_dioxide_1h_max NUMERIC(10,2),
    sulphur_dioxide_1h_max NUMERIC(10,2),

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),

    CONSTRAINT uq_daily_air_quality_city_day
        UNIQUE (date, city_id),

    CONSTRAINT fk_daily_air_quality_city
        FOREIGN KEY (city_id)
        REFERENCES cities(city_id)
);

CREATE TABLE IF NOT EXISTS daily_land_surface (
    id BIGINT GENERATED ALWAYS AS IDENTITY,

    city_id BIGINT NOT NULL,
    date DATE NOT NULL,

    surface_pressure NUMERIC(10,2),
    total_precipitable_water NUMERIC(10,2),
    sea_level_pressure NUMERIC(10,2),
    land_surface_temp NUMERIC(8,2),

    root_zone_soil_wetness NUMERIC(8,4),

    surface_longwave_downward_irradiance NUMERIC(10,2),
    surface_shortwave_upward_irradiance NUMERIC(10,2),
    surface_longwave_upward_irradiance NUMERIC(10,2),
    total_solar_irradiance NUMERIC(10,2),

    all_sky_surface_albedo NUMERIC(6,4),

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),

    CONSTRAINT uq_daily_land_surface_city_day
        UNIQUE (date, city_id),

    CONSTRAINT fk_daily_land_surface_city
        FOREIGN KEY (city_id)
        REFERENCES cities(city_id)
);