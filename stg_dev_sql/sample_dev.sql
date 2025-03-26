{% set env = "DEV" %}
USE SCHEMA TTM_BABEL.BABEL_STG_{{ env }};

CREATE OR REPLACE TABLE sample_tbl(
    id INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    avatar_id NUMBER NULL,
    created_at NUMBER NULL,
    user_avatar_id NUMBER NULL
) AS
SELECT DISTINCT
    id,
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"avatar_id" AS NUMBER) AS avatar_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"user_avatar_id" AS NUMBER) AS user_avatar_id
FROM TTM_BABEL.BABEL_STG_{{ env }}.DBLOG
WHERE LOG_TYPE = 'avatar:get'
LIMIT 10;