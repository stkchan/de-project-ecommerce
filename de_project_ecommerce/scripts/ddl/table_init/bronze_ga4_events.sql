CREATE TABLE IF NOT EXISTS bronze.ecommerce.ga4_events (
    event_date      STRING,
    event_timestamp STRING,
    event_name      STRING,
    user_id         STRING,
    user_pseudo_id  STRING,
    geo             STRING,
    device          STRING,
    traffic_source  STRING,
    event_params    STRING,
    items           STRING
)
USING DELTA
LOCATION 'abfss://bronze@stgecommerce.dfs.core.windows.net/ga4/';