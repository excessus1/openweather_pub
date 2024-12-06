CREATE FUNCTION public.celsius_to_fahrenheit(temp real) RETURNS real
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    RETURN (temp * 9/5) + 32;
END;
$$;
ALTER FUNCTION public.celsius_to_fahrenheit(temp real) OWNER TO {owner};
CREATE FUNCTION public.extract_date(timestamp with time zone) RETURNS date
    LANGUAGE plpgsql IMMUTABLE
    AS $_$
BEGIN
    RETURN DATE_TRUNC('day', $1)::date;
END;
$_$;
ALTER FUNCTION public.extract_date(timestamp with time zone) OWNER TO {owner};
CREATE FUNCTION public.unix_timestamp_to_timestamp_with_tz(unix_ts integer, tz text, tzoff integer) RETURNS timestamp with time zone
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    RETURN (TO_TIMESTAMP(unix_ts) AT TIME ZONE 'UTC') + INTERVAL '1 second' * tzoff;
END;
$$;
ALTER FUNCTION public.unix_timestamp_to_timestamp_with_tz(unix_ts integer, tz text, tzoff integer) OWNER TO {owner};
CREATE TABLE public.ar_internal_metadata (
    key character varying NOT NULL,
    value character varying,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL
);
ALTER TABLE public.ar_internal_metadata OWNER TO {owner};
CREATE TABLE public.daily_summary_data (
    id integer NOT NULL,
    lat real,
    lon real,
    tzoff integer,
    date integer,
    units text,
    cloud_cover_afternoon integer,
    humidity_afternoon integer,
    precipitation_total real,
    temperature_min real,
    temperature_max real,
    temperature_afternoon real,
    temperature_night real,
    temperature_evening real,
    temperature_morning real,
    pressure_afternoon integer,
    wind_max_speed real,
    wind_max_direction integer,
    location_id integer
);
ALTER TABLE public.daily_summary_data OWNER TO {owner};
CREATE SEQUENCE public.daily_summary_data_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.daily_summary_data_id_seq OWNER TO {owner};
ALTER SEQUENCE public.daily_summary_data_id_seq OWNED BY public.daily_summary_data.id;
CREATE TABLE public.hourly_data (
    id integer NOT NULL,
    dt integer NOT NULL,
    lat real NOT NULL,
    lon real NOT NULL,
    tz text NOT NULL,
    tzoff integer NOT NULL,
    sunrise integer NOT NULL,
    sunset integer NOT NULL,
    temp real NOT NULL,
    feels_like real NOT NULL,
    pressure integer NOT NULL,
    humidity integer NOT NULL,
    dew_point real,
    vis real,
    description text NOT NULL,
    clouds integer,
    wind_speed real,
    wind_deg integer,
    location_id integer
);
ALTER TABLE public.hourly_data OWNER TO {owner};
CREATE TABLE public.locations (
    id integer NOT NULL,
    friendly_name text NOT NULL,
    official_station_name text,
    lat_detail double precision NOT NULL,
    lon_detail double precision NOT NULL,
    lat_rounded real,
    lon_rounded real,
    zip_code text
);
ALTER TABLE public.locations OWNER TO {owner};
CREATE SEQUENCE public.locations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.locations_id_seq OWNER TO {owner};
ALTER SEQUENCE public.locations_id_seq OWNED BY public.locations.id;
CREATE SEQUENCE public.openweather_data_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.openweather_data_id_seq OWNER TO {owner};
ALTER SEQUENCE public.openweather_data_id_seq OWNED BY public.hourly_data.id;
CREATE TABLE public.schema_migrations (
    version character varying NOT NULL
);
ALTER TABLE public.schema_migrations OWNER TO {owner};
ALTER TABLE ONLY public.daily_summary_data ALTER COLUMN id 
ALTER TABLE ONLY public.hourly_data ALTER COLUMN id 
ALTER TABLE ONLY public.locations ALTER COLUMN id 
ALTER TABLE ONLY public.ar_internal_metadata
    ADD CONSTRAINT ar_internal_metadata_pkey PRIMARY KEY (key);
ALTER TABLE ONLY public.daily_summary_data
    ADD CONSTRAINT daily_summary_data_lat_lon_date_key UNIQUE (lat, lon, date);
ALTER TABLE ONLY public.daily_summary_data
    ADD CONSTRAINT daily_summary_data_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.locations
    ADD CONSTRAINT locations_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.hourly_data
    ADD CONSTRAINT openweather_data_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);
CREATE INDEX idx_lat_lon_dt ON public.hourly_data USING btree (lat, lon, dt);
CREATE INDEX idx_openweather_date ON public.daily_summary_data USING btree (date);
CREATE UNIQUE INDEX idx_unique_dt ON public.hourly_data USING btree (dt);
ALTER TABLE ONLY public.daily_summary_data
    ADD CONSTRAINT daily_summary_data_location_id_fkey FOREIGN KEY (location_id) REFERENCES public.locations(id);
ALTER TABLE ONLY public.hourly_data
    ADD CONSTRAINT hourly_data_location_id_fkey FOREIGN KEY (location_id) REFERENCES public.locations(id);
GRANT SELECT ON TABLE public.ar_internal_metadata TO grafana;
GRANT SELECT ON TABLE public.daily_summary_data TO grafana;
GRANT SELECT ON TABLE public.hourly_data TO grafana;
GRANT SELECT ON TABLE public.locations TO grafana;
GRANT SELECT ON TABLE public.schema_migrations TO grafana;

