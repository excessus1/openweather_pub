CREATE TABLE public.api_call_types (
    api_call_type_id integer NOT NULL,
    platform text NOT NULL,
    api_call_type text NOT NULL,
    api_call_prototype text NOT NULL
);
ALTER TABLE public.api_call_types OWNER TO {owner};
CREATE SEQUENCE public.api_call_types_api_call_type_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.api_call_types_api_call_type_id_seq OWNER TO {owner};
ALTER SEQUENCE public.api_call_types_api_call_type_id_seq OWNED BY public.api_call_types.api_call_type_id;
CREATE TABLE public.api_calls (
    api_call_id integer NOT NULL,
    call_timestamp bigint NOT NULL,
    api_call_type_id integer,
    call_event text NOT NULL,
    request_payload text,
    response_code integer,
    response_message text,
    retry_count integer DEFAULT 0,
    call_log_message text
);
ALTER TABLE public.api_calls OWNER TO {owner};
CREATE SEQUENCE public.api_calls_api_call_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.api_calls_api_call_id_seq OWNER TO {owner};
ALTER SEQUENCE public.api_calls_api_call_id_seq OWNED BY public.api_calls.api_call_id;
CREATE TABLE public.api_script_tracking (
    log_id integer NOT NULL,
    script_name text NOT NULL,
    platform text NOT NULL,
    api_call_alt_name text NOT NULL,
    status text NOT NULL,
    last_checked timestamp without time zone NOT NULL,
    requests_made_today integer DEFAULT 0 NOT NULL,
    daily_limit_reached boolean DEFAULT false,
    previous_status text,
    force_restart boolean DEFAULT false,
    stopped_reason text
);
ALTER TABLE public.api_script_tracking OWNER TO {owner};
CREATE SEQUENCE public.api_logging_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.api_logging_log_id_seq OWNER TO {owner};
ALTER SEQUENCE public.api_logging_log_id_seq OWNED BY public.api_script_tracking.log_id;
CREATE TABLE public.credentials (
    key_name text NOT NULL,
    username text NOT NULL,
    password text NOT NULL,
    target_service text,
    notes text,
    dbname text
);
ALTER TABLE public.credentials OWNER TO {owner};
CREATE TABLE public.sql_handling (
    sql_log_id integer NOT NULL,
    api_call_id integer,
    insert_timestamp bigint NOT NULL,
    insert_status text NOT NULL,
    error_message text,
    retry_count integer DEFAULT 0
);
ALTER TABLE public.sql_handling OWNER TO {owner};
CREATE SEQUENCE public.sql_handling_sql_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.sql_handling_sql_log_id_seq OWNER TO {owner};
ALTER SEQUENCE public.sql_handling_sql_log_id_seq OWNED BY public.sql_handling.sql_log_id;
ALTER TABLE ONLY public.api_call_types ALTER COLUMN api_call_type_id 
ALTER TABLE ONLY public.api_calls ALTER COLUMN api_call_id 
ALTER TABLE ONLY public.api_script_tracking ALTER COLUMN log_id 
ALTER TABLE ONLY public.sql_handling ALTER COLUMN sql_log_id 
ALTER TABLE ONLY public.api_call_types
    ADD CONSTRAINT api_call_types_pkey PRIMARY KEY (api_call_type_id);
ALTER TABLE ONLY public.api_calls
    ADD CONSTRAINT api_calls_pkey PRIMARY KEY (api_call_id);
ALTER TABLE ONLY public.api_script_tracking
    ADD CONSTRAINT api_logging_pkey PRIMARY KEY (log_id);
ALTER TABLE ONLY public.credentials
    ADD CONSTRAINT credentials_pkey PRIMARY KEY (key_name);
ALTER TABLE ONLY public.sql_handling
    ADD CONSTRAINT sql_handling_pkey PRIMARY KEY (sql_log_id);
ALTER TABLE ONLY public.api_script_tracking
    ADD CONSTRAINT unique_script_platform_type UNIQUE (script_name, platform, api_call_alt_name);
ALTER TABLE ONLY public.api_calls
    ADD CONSTRAINT api_calls_api_call_type_id_fkey FOREIGN KEY (api_call_type_id) REFERENCES public.api_call_types(api_call_type_id);
ALTER TABLE ONLY public.sql_handling
    ADD CONSTRAINT sql_handling_api_call_id_fkey FOREIGN KEY (api_call_id) REFERENCES public.api_calls(api_call_id);
GRANT USAGE ON SCHEMA public TO grafana;
GRANT SELECT ON TABLE public.api_call_types TO grafana;
GRANT SELECT ON TABLE public.api_calls TO grafana;
GRANT SELECT ON TABLE public.api_script_tracking TO grafana;
GRANT SELECT ON TABLE public.credentials TO grafana;
GRANT SELECT ON TABLE public.sql_handling TO grafana;
ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA public GRANT SELECT ON TABLES TO grafana;

