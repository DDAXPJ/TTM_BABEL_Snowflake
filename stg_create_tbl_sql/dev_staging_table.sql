
{% if env == "DEV" %}
-- DEVの場合の処理
　USE SCHEMA TTM_BABEL.BABEL_STG_DEV;
{% elif env == "PROD" %}
-- PRODの場合の処理
　USE SCHEMA TTM_BABEL.BABEL_STG_PROD;
{% else %}
-- その他の場合の処理
    {{ raise_error("DEVかPRODを指定してください") }}
{% endif %}
-- Query for LOG_TYPE = avatar:get

CREATE OR REPLACE TABLE LOG_AVATAR_GET(
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
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'avatar:get'
;

-- Query for LOG_TYPE = avatar:use

CREATE OR REPLACE TABLE LOG_AVATAR_USE(
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
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'avatar:use'
;

-- Query for LOG_TYPE = coupon:create

CREATE OR REPLACE TABLE LOG_COUPON_CREATE(
    id INT NOT NULL,
    idx INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    coupon_id NUMBER NULL,
    coupon_code STRING NULL,
    coupon_effect_type NUMBER NULL,
    coupon_effect_amount NUMBER NULL,
    code_type NUMBER NULL,
    created_at NUMBER NULL,
    coupon_group_id NUMBER NULL
) AS
SELECT DISTINCT
    id,
    coupon.index AS idx,
    request_id,
    created,
    user_id as LOGs_userId,
    coupon.value:"id"::NUMBER AS coupon_id,
    coupon.value:"code"::STRING AS coupon_code,
    coupon.value:"effect_type"::NUMBER AS coupon_effect_type,
    coupon.value:"effect_amount"::NUMBER AS coupon_effect_amount,
    CAST(PARSE_JSON(LOG):"code_type" AS NUMBER) AS code_type,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"coupon_group_id" AS NUMBER) AS coupon_group_id
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG,
    LATERAL FLATTEN(input => PARSE_JSON(LOG):"coupon") coupon
WHERE LOG_TYPE = 'coupon:create'
;

-- Query for LOG_TYPE = coupon:use

CREATE OR REPLACE TABLE LOG_COUPON_USE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    coupon_id NUMBER NULL,
    coupon_code STRING NULL,
    coupon_effect_type NUMBER NULL,
    coupon_effect_amount NUMBER NULL,
    user_id STRING NULL,
    code_type NUMBER NULL,
    created_at NUMBER NULL,
    user_coupon_id NUMBER NULL,
    coupon_group_id NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"coupon.id" AS NUMBER) AS coupon_id,
    CAST(PARSE_JSON(LOG):"coupon.code" AS STRING) AS coupon_code,
    CAST(PARSE_JSON(LOG):"coupon.effect_type" AS NUMBER) AS coupon_effect_type,
    CAST(PARSE_JSON(LOG):"coupon.effect_amount" AS NUMBER) AS coupon_effect_amount,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"code_type" AS NUMBER) AS code_type,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"user_coupon_id" AS NUMBER) AS user_coupon_id,
    CAST(PARSE_JSON(LOG):"coupon_group_id" AS NUMBER) AS coupon_group_id
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'coupon:use'
;

-- Query for LOG_TYPE = currency:deposit

CREATE OR REPLACE TABLE LOG_CURRENCY_DEPOSIT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    currency_amount NUMBER NULL,
    currency_total_amount NUMBER NULL,
    currency_before_amount NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"currency.amount" AS NUMBER) AS currency_amount,
    CAST(PARSE_JSON(LOG):"currency.total_amount" AS NUMBER) AS currency_total_amount,
    CAST(PARSE_JSON(LOG):"currency.before_amount" AS NUMBER) AS currency_before_amount,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'currency:deposit'
;

-- Query for LOG_TYPE = currency:paid

CREATE OR REPLACE TABLE LOG_CURRENCY_PAID(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    currency_amount NUMBER NULL,
    currency_total_amount NUMBER NULL,
    currency_before_amount NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"currency.amount" AS NUMBER) AS currency_amount,
    CAST(PARSE_JSON(LOG):"currency.total_amount" AS NUMBER) AS currency_total_amount,
    CAST(PARSE_JSON(LOG):"currency.before_amount" AS NUMBER) AS currency_before_amount,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'currency:paid'
;

-- Query for LOG_TYPE = currency_free:deposit

CREATE OR REPLACE TABLE LOG_CURRENCY_FREE_DEPOSIT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL,
    currency_free_amount NUMBER NULL,
    currency_free_total_amount NUMBER NULL,
    currency_free_before_amount NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"currency_free.amount" AS NUMBER) AS currency_free_amount,
    CAST(PARSE_JSON(LOG):"currency_free.total_amount" AS NUMBER) AS currency_free_total_amount,
    CAST(PARSE_JSON(LOG):"currency_free.before_amount" AS NUMBER) AS currency_free_before_amount
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'currency_free:deposit'
;

-- Query for LOG_TYPE = currency_free:paid

CREATE OR REPLACE TABLE LOG_CURRENCY_FREE_PAID(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL,
    currency_free_amount NUMBER NULL,
    currency_free_total_amount NUMBER NULL,
    currency_free_before_amount NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"currency_free.amount" AS NUMBER) AS currency_free_amount,
    CAST(PARSE_JSON(LOG):"currency_free.total_amount" AS NUMBER) AS currency_free_total_amount,
    CAST(PARSE_JSON(LOG):"currency_free.before_amount" AS NUMBER) AS currency_free_before_amount
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'currency_free:paid'
;

-- Query for LOG_TYPE = demographic:group

CREATE OR REPLACE TABLE LOG_DEMOGRAPHIC_GROUP(
    id INT NOT NULL,
    idx INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    users_user_id STRING NULL,
    party_id STRING NULL,
    created_at NUMBER NULL,
    customer_base NUMBER NULL
) AS
SELECT DISTINCT
    id,
    users.index AS idx,
    request_id,
    created,
    user_id as LOGs_userId,
    users.value:"user_id"::STRING AS users_user_id,
    CAST(PARSE_JSON(LOG):"party_id" AS STRING) AS party_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"customer_base" AS NUMBER) AS customer_base
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG,
    LATERAL FLATTEN(input => PARSE_JSON(LOG):"users") users
WHERE LOG_TYPE = 'demographic:group'
;

-- Query for LOG_TYPE = demographic:user

CREATE OR REPLACE TABLE LOG_DEMOGRAPHIC_USER(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    age NUMBER NULL,
    sex NUMBER NULL,
    region NUMBER NULL,
    user_id STRING NULL,
    created_at FLOAT NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"age" AS NUMBER) AS age,
    CAST(PARSE_JSON(LOG):"sex" AS NUMBER) AS sex,
    CAST(PARSE_JSON(LOG):"region" AS NUMBER) AS region,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS FLOAT) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'demographic:user'
;

-- Query for LOG_TYPE = gacha:draw

CREATE OR REPLACE TABLE LOG_GACHA_DRAW(
    id INT NOT NULL,
    idx INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    items_id NUMBER NULL,
    items_amount NUMBER NULL,
    items_item_id STRING NULL,
    items_item_type STRING NULL,
    user_id STRING NULL,
    draw_num NUMBER NULL,
    gacha_id NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    items.index AS idx,
    request_id,
    created,
    user_id as LOGs_userId,
    items.value:"id"::NUMBER AS items_id,
    items.value:"amount"::NUMBER AS items_amount,
    items.value:"item_id"::STRING AS items_item_id,
    items.value:"item_type"::STRING AS items_item_type,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"draw_num" AS NUMBER) AS draw_num,
    CAST(PARSE_JSON(LOG):"gacha_id" AS NUMBER) AS gacha_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG,
    LATERAL FLATTEN(input => PARSE_JSON(LOG):"items") items
WHERE LOG_TYPE = 'gacha:draw'
;

-- Query for LOG_TYPE = gold:add

CREATE OR REPLACE TABLE LOG_GOLD_ADD(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    amount NUMBER NULL,
    user_id STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"amount" AS NUMBER) AS amount,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'gold:add'
;

-- Query for LOG_TYPE = gold:sub

CREATE OR REPLACE TABLE LOG_GOLD_SUB(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    amount NUMBER NULL,
    user_id STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"amount" AS NUMBER) AS amount,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'gold:sub'
;

-- Query for LOG_TYPE = inbox:get

CREATE OR REPLACE TABLE LOG_INBOX_GET(
    id INT NOT NULL,
    idx INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    items_id NUMBER NULL,
    items_amount NUMBER NULL,
    items_item_id STRING NULL,
    items_item_type STRING NULL,
    is_read BOOLEAN NULL,
    read_at NUMBER NULL,
    user_id STRING NULL,
    expire_at NUMBER NULL,
    created_at NUMBER NULL,
    message_id NUMBER NULL,
    user_message_id NUMBER NULL
) AS
SELECT DISTINCT
    id,
    items.index AS idx,
    request_id,
    created,
    user_id as LOGs_userId,
    items.value:"id"::NUMBER AS items_id,
    items.value:"amount"::NUMBER AS items_amount,
    items.value:"item_id"::STRING AS items_item_id,
    items.value:"item_type"::STRING AS items_item_type,
    CAST(PARSE_JSON(LOG):"is_read" AS BOOLEAN) AS is_read,
    CAST(PARSE_JSON(LOG):"read_at" AS NUMBER) AS read_at,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"expire_at" AS NUMBER) AS expire_at,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"message_id" AS NUMBER) AS message_id,
    CAST(PARSE_JSON(LOG):"user_message_id" AS NUMBER) AS user_message_id
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG,
    LATERAL FLATTEN(input => PARSE_JSON(LOG):"items") items
WHERE LOG_TYPE = 'inbox:get'
;

-- Query for LOG_TYPE = item:get

CREATE OR REPLACE TABLE LOG_ITEM_GET(
    id INT NOT NULL,
    idx INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    items_id NUMBER NULL,
    items_amount NUMBER NULL,
    items_item_id STRING NULL,
    items_item_type STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    items.index AS idx,
    request_id,
    created,
    user_id as LOGs_userId,
    items.value:"id"::NUMBER AS items_id,
    items.value:"amount"::NUMBER AS items_amount,
    items.value:"item_id"::STRING AS items_item_id,
    items.value:"item_type"::STRING AS items_item_type,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG,
    LATERAL FLATTEN(input => PARSE_JSON(LOG):"items") items
WHERE LOG_TYPE = 'item:get'
;

-- Query for LOG_TYPE = item:select

CREATE OR REPLACE TABLE LOG_ITEM_SELECT(
    id INT NOT NULL,
    idx INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    items_id NUMBER NULL,
    items_item_id STRING NULL,
    items_item_type STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    items.index AS idx,
    request_id,
    created,
    user_id as LOGs_userId,
    items.value:"id"::NUMBER AS items_id,
    items.value:"item_id"::STRING AS items_item_id,
    items.value:"item_type"::STRING AS items_item_type,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG,
    LATERAL FLATTEN(input => PARSE_JSON(LOG):"items") items
WHERE LOG_TYPE = 'item:select'
;

-- Query for LOG_TYPE = item:use

CREATE OR REPLACE TABLE LOG_ITEM_USE(
    id INT NOT NULL,
    idx INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    items_id NUMBER NULL,
    items_amount NUMBER NULL,
    items_item_id STRING NULL,
    items_item_type STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    items.index AS idx,
    request_id,
    created,
    user_id as LOGs_userId,
    items.value:"id"::NUMBER AS items_id,
    items.value:"amount"::NUMBER AS items_amount,
    items.value:"item_id"::STRING AS items_item_id,
    items.value:"item_type"::STRING AS items_item_type,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG,
    LATERAL FLATTEN(input => PARSE_JSON(LOG):"items") items
WHERE LOG_TYPE = 'item:use'
;

-- Query for LOG_TYPE = ofp:playResults

CREATE OR REPLACE TABLE OFP_PLAYRESULTS(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    rate STRING NULL,
    type STRING NULL,
    clear NUMBER NULL,
    items VARIANT NULL,
    items_onpInfo_effects VARIANT NULL,
    level NUMBER NULL,
    score NUMBER NULL,
    state STRING NULL,
    areaId STRING NULL,
    endedAt NUMBER NULL,
    onpInfo_language NUMBER NULL,
    onpInfo_leaderId STRING NULL,
    onpInfo_behaviours VARIANT NULL,
    onpInfo_behaviours_target VARIANT NULL,
    onpInfo_dataVersion STRING NULL,
    onpInfo_isRankedParty BOOLEAN NULL,
    onpInfo_partyCategory NUMBER NULL,
    players VARIANT NULL,
    players_onpInfo VARIANT NULL,
    players_onpInfo_avatarParams VARIANT NULL,
    questId STRING NULL,
    version STRING NULL,
    laevedAt NUMBER NULL,
    ofpState_uses VARIANT NULL,
    ofpState_result_rate STRING NULL,
    ofpState_result_score NUMBER NULL,
    ofpState_players VARIANT NULL,
    ofpState_fieldLevel NUMBER NULL,
    totalExp NUMBER NULL,
    enteredAt NUMBER NULL,
    sessionId NUMBER NULL,
    startedAt NUMBER NULL,
    totalGold NUMBER NULL,
    dataVersion STRING NULL,
    totalExtraExp NUMBER NULL,
    totalExtraGold NUMBER NULL,
    numberOfPlayers NUMBER NULL,
    totalPlayerLevel NUMBER NULL,
    ofpState_dataVersion STRING NULL,
    fieldId STRING NULL,
    onpInfo_fieldLevel NUMBER NULL,
    onpInfo_isTransform BOOLEAN NULL,
    leavedAt NUMBER NULL,
    created_at NUMBER NULL,
    ofpState VARIANT  NULL,
    ofpState_result_exp NUMBER NULL,
    ofpState_result_A NUMBER NULL,
    ofpState_result_B NUMBER NULL,
    ofpState_result_C NUMBER NULL,
    ofpState_result_D NUMBER NULL,
    ofpState_result_E NUMBER NULL,
    ofpState_result_reachedWave NUMBER NULL,
    ofpState_result_bossDefeated BOOLEAN NULL,
    ofpState_result_playerResults VARIANT NULL,
    ofpState_result_reachedWaveAchievementRate FLOAT NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"rate" AS STRING) AS rate,
    CAST(PARSE_JSON(LOG):"type" AS STRING) AS type,
    CAST(PARSE_JSON(LOG):"clear" AS NUMBER) AS clear,
    PARSE_JSON(LOG):"items" AS items,
    PARSE_JSON(LOG):"items_onpInfo_effects" AS items_onpInfo_effects,
    CAST(PARSE_JSON(LOG):"level" AS NUMBER) AS level,
    CAST(PARSE_JSON(LOG):"score" AS NUMBER) AS score,
    CAST(PARSE_JSON(LOG):"state" AS STRING) AS state,
    CAST(PARSE_JSON(LOG):"areaId" AS STRING) AS areaId,
    CAST(PARSE_JSON(LOG):"endedAt" AS NUMBER) AS endedAt,
    CAST(PARSE_JSON(LOG):"onpInfo.language" AS NUMBER) AS onpInfo_language,
    CAST(PARSE_JSON(LOG):"onpInfo.leaderId" AS STRING) AS onpInfo_leaderId,
    PARSE_JSON(LOG):"onpInfo_behaviours" AS onpInfo_behaviours,
    PARSE_JSON(LOG):"onpInfo_behaviours_target" AS onpInfo_behaviours_target,
    CAST(PARSE_JSON(LOG):"onpInfo.dataVersion" AS STRING) AS onpInfo_dataVersion,
    CAST(PARSE_JSON(LOG):"onpInfo.isRankedParty" AS BOOLEAN) AS onpInfo_isRankedParty,
    CAST(PARSE_JSON(LOG):"onpInfo.partyCategory" AS NUMBER) AS onpInfo_partyCategory,
    PARSE_JSON(LOG):"players" AS players,
    PARSE_JSON(LOG):"players_onpInfo" AS players_onpInfo,
    PARSE_JSON(LOG):"players_onpInfo_avatarParams" AS players_onpInfo_avatarParams,
    CAST(PARSE_JSON(LOG):"questId" AS STRING) AS questId,
    CAST(PARSE_JSON(LOG):"version" AS STRING) AS version,
    CAST(PARSE_JSON(LOG):"laevedAt" AS NUMBER) AS laevedAt,
    PARSE_JSON(LOG):"ofpState_uses" AS ofpState_uses,
    CAST(PARSE_JSON(LOG):"ofpState.result.rate" AS STRING) AS ofpState_result_rate,
    CAST(PARSE_JSON(LOG):"ofpState.result.score" AS NUMBER) AS ofpState_result_score,
    PARSE_JSON(LOG):"ofpState_players" AS ofpState_players,
    CAST(PARSE_JSON(LOG):"ofpState.fieldLevel" AS NUMBER) AS ofpState_fieldLevel,
    CAST(PARSE_JSON(LOG):"totalExp" AS NUMBER) AS totalExp,
    CAST(PARSE_JSON(LOG):"enteredAt" AS NUMBER) AS enteredAt,
    CAST(PARSE_JSON(LOG):"sessionId" AS NUMBER) AS sessionId,
    CAST(PARSE_JSON(LOG):"startedAt" AS NUMBER) AS startedAt,
    CAST(PARSE_JSON(LOG):"totalGold" AS NUMBER) AS totalGold,
    CAST(PARSE_JSON(LOG):"dataVersion" AS STRING) AS dataVersion,
    CAST(PARSE_JSON(LOG):"totalExtraExp" AS NUMBER) AS totalExtraExp,
    CAST(PARSE_JSON(LOG):"totalExtraGold" AS NUMBER) AS totalExtraGold,
    CAST(PARSE_JSON(LOG):"numberOfPlayers" AS NUMBER) AS numberOfPlayers,
    CAST(PARSE_JSON(LOG):"totalPlayerLevel" AS NUMBER) AS totalPlayerLevel,
    CAST(PARSE_JSON(LOG):"ofpState.dataVersion" AS STRING) AS ofpState_dataVersion,
    CAST(PARSE_JSON(LOG):"fieldId" AS STRING) AS fieldId,
    CAST(PARSE_JSON(LOG):"onpInfo.fieldLevel" AS NUMBER) AS onpInfo_fieldLevel,
    CAST(PARSE_JSON(LOG):"onpInfo.isTransform" AS BOOLEAN) AS onpInfo_isTransform,
    CAST(PARSE_JSON(LOG):"leavedAt" AS NUMBER) AS leavedAt,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    PARSE_JSON(LOG):"ofpState" AS ofpState,
    -- ofpState: __EMPTY__ --,
    CAST(PARSE_JSON(LOG):"ofpState.result.exp" AS NUMBER) AS ofpState_result_exp,
    CAST(PARSE_JSON(LOG):"ofpState.result.A" AS NUMBER) AS ofpState_result_A,
    CAST(PARSE_JSON(LOG):"ofpState.result.B" AS NUMBER) AS ofpState_result_B,
    CAST(PARSE_JSON(LOG):"ofpState.result.C" AS NUMBER) AS ofpState_result_C,
    CAST(PARSE_JSON(LOG):"ofpState.result.D" AS NUMBER) AS ofpState_result_D,
    CAST(PARSE_JSON(LOG):"ofpState.result.E" AS NUMBER) AS ofpState_result_E,
    CAST(PARSE_JSON(LOG):"ofpState.result.reachedWave" AS NUMBER) AS ofpState_result_reachedWave,
    CAST(PARSE_JSON(LOG):"ofpState.result.bossDefeated" AS BOOLEAN) AS ofpState_result_bossDefeated,
    PARSE_JSON(LOG):"ofpState_result_playerResults" AS ofpState_result_playerResults,
    CAST(PARSE_JSON(LOG):"ofpState.result.reachedWaveAchievementRate" AS FLOAT) AS ofpState_result_reachedWaveAchievementRate
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'ofp:playResults'
;

-- Query for LOG_TYPE = party:create

CREATE OR REPLACE TABLE LOG_PARTY_CREATE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    party_id STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"party_id" AS STRING) AS party_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'party:create'
;

-- Query for LOG_TYPE = party:delete

CREATE OR REPLACE TABLE LOG_PARTY_DELETE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    party_id STRING NULL,
    deleted_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"party_id" AS STRING) AS party_id,
    CAST(PARSE_JSON(LOG):"deleted_at" AS NUMBER) AS deleted_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'party:delete'
;

-- Query for LOG_TYPE = party:name_create

CREATE OR REPLACE TABLE LOG_PARTY_NAME_CREATE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL,
    party_name STRING NULL,
    party_tts_name STRING NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"party_name" AS STRING) AS party_name,
    CAST(PARSE_JSON(LOG):"party_tts_name" AS STRING) AS party_tts_name
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'party:name_create'
;

-- Query for LOG_TYPE = party:name_edit

CREATE OR REPLACE TABLE LOG_PARTY_NAME_EDIT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL,
    party_name STRING NULL,
    party_tts_name STRING NULL,
    before_party_name STRING NULL,
    before_party_tts_name STRING NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"party_name" AS STRING) AS party_name,
    CAST(PARSE_JSON(LOG):"party_tts_name" AS STRING) AS party_tts_name,
    CAST(PARSE_JSON(LOG):"before_party_name" AS STRING) AS before_party_name,
    CAST(PARSE_JSON(LOG):"before_party_tts_name" AS STRING) AS before_party_tts_name
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'party:name_edit'
;

-- Query for LOG_TYPE = party:select

CREATE OR REPLACE TABLE LOG_PARTY_SELECT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    party_id STRING NULL,
    created_at NUMBER NULL,
    party_code STRING NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"party_id" AS STRING) AS party_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"party_code" AS STRING) AS party_code
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'party:select'
;

-- Query for LOG_TYPE = payment:purchase

CREATE OR REPLACE TABLE LOG_PAYMENT_PURCHASE(
    id INT NOT NULL,
    idx INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    paid_cash_amount NUMBER NULL,
    paid_topolo_amount NUMBER NULL,
    paid_topolo_total_amount NUMBER NULL,
    paid_topolo_before_amount NUMBER NULL,
    paid_topolo_free_amount NUMBER NULL,
    paid_topolo_free_total_amount NUMBER NULL,
    paid_topolo_free_before_amount NUMBER NULL,
    user_id STRING NULL,
    acquired_id NUMBER NULL,
    acquired_num NUMBER NULL,
    acquired_item_id STRING NULL,
    acquired_item_type STRING NULL,
    acquired_user_related_id STRING NULL,
    created_at NUMBER NULL,
    gacha_draw_num NUMBER NULL,
    gacha_gacha_id NUMBER NULL,
    paid_coupon_info_coupon_id NUMBER NULL,
    paid_coupon_info_coupon_code STRING NULL,
    paid_coupon_info_coupon_effect_type NUMBER NULL,
    paid_coupon_info_coupon_effect_amount NUMBER NULL,
    paid_coupon_info_code_type NUMBER NULL,
    paid_coupon_info_user_coupon_id STRING NULL,
    paid_coupon_info_coupon_group_id NUMBER NULL
) AS
SELECT DISTINCT
    id,
    acquired.index AS idx,
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"paid.cash.amount" AS NUMBER) AS paid_cash_amount,
    CAST(PARSE_JSON(LOG):"paid.topolo.amount" AS NUMBER) AS paid_topolo_amount,
    CAST(PARSE_JSON(LOG):"paid.topolo.total_amount" AS NUMBER) AS paid_topolo_total_amount,
    CAST(PARSE_JSON(LOG):"paid.topolo.before_amount" AS NUMBER) AS paid_topolo_before_amount,
    CAST(PARSE_JSON(LOG):"paid.topolo_free.amount" AS NUMBER) AS paid_topolo_free_amount,
    CAST(PARSE_JSON(LOG):"paid.topolo_free.total_amount" AS NUMBER) AS paid_topolo_free_total_amount,
    CAST(PARSE_JSON(LOG):"paid.topolo_free.before_amount" AS NUMBER) AS paid_topolo_free_before_amount,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    acquired.value:"id"::NUMBER AS acquired_id,
    acquired.value:"num"::NUMBER AS acquired_num,
    acquired.value:"item_id"::STRING AS acquired_item_id,
    acquired.value:"item_type"::STRING AS acquired_item_type,
    acquired.value:"user_related_id"::STRING AS acquired_user_related_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"gacha.draw_num" AS NUMBER) AS gacha_draw_num,
    CAST(PARSE_JSON(LOG):"gacha.gacha_id" AS NUMBER) AS gacha_gacha_id,
    CAST(PARSE_JSON(LOG):"paid.coupon_info.coupon.id" AS NUMBER) AS paid_coupon_info_coupon_id,
    CAST(PARSE_JSON(LOG):"paid.coupon_info.coupon.code" AS STRING) AS paid_coupon_info_coupon_code,
    CAST(PARSE_JSON(LOG):"paid.coupon_info.coupon.effect_type" AS NUMBER) AS paid_coupon_info_coupon_effect_type,
    CAST(PARSE_JSON(LOG):"paid.coupon_info.coupon.effect_amount" AS NUMBER) AS paid_coupon_info_coupon_effect_amount,
    CAST(PARSE_JSON(LOG):"paid.coupon_info.code_type" AS NUMBER) AS paid_coupon_info_code_type,
    CAST(PARSE_JSON(LOG):"paid.coupon_info.user_coupon_id" AS STRING) AS paid_coupon_info_user_coupon_id,
    CAST(PARSE_JSON(LOG):"paid.coupon_info.coupon_group_id" AS NUMBER) AS paid_coupon_info_coupon_group_id
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG,
    LATERAL FLATTEN(input => PARSE_JSON(LOG):"acquired") acquired
WHERE LOG_TYPE = 'payment:purchase'
;

-- Query for LOG_TYPE = payment:q5_pay

CREATE OR REPLACE TABLE LOG_PAYMENT_Q5_PAY(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    paid NUMBER NULL,
    user_id STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"paid" AS NUMBER) AS paid,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'payment:q5_pay'
;

-- Query for LOG_TYPE = payment:stripe_paymentIntent

CREATE OR REPLACE TABLE LOG_PAYMENT_STRIPE_PAYMENTINTENT(
    id INT NOT NULL,
    idx INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    type STRING NULL,
    status STRING NULL,
    user_id STRING NULL,
    response_amount NUMBER NULL,
    response_object STRING NULL,
    response_status STRING NULL,
    response_currency STRING NULL,
    response_metadata_uid STRING NULL,
    response_payment_method_types STRING NULL,
    created_at NUMBER NULL,
    payment_id STRING NULL
) AS
SELECT DISTINCT
    id,
    response_payment_method_types.index AS idx,
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"type" AS STRING) AS type,
    CAST(PARSE_JSON(LOG):"status" AS STRING) AS status,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"response.amount" AS NUMBER) AS response_amount,
    CAST(PARSE_JSON(LOG):"response.object" AS STRING) AS response_object,
    CAST(PARSE_JSON(LOG):"response.status" AS STRING) AS response_status,
    CAST(PARSE_JSON(LOG):"response.currency" AS STRING) AS response_currency,
    CAST(PARSE_JSON(LOG):"response.metadata.uid" AS STRING) AS response_metadata_uid,
    response_payment_method_types.value:"payment_method_types"::STRING AS response_payment_method_types,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"payment_id" AS STRING) AS payment_id
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG,
    LATERAL FLATTEN(input => PARSE_JSON(LOG):"response.payment_method_types") response_payment_method_types
WHERE LOG_TYPE = 'payment:stripe_paymentIntent'
;

-- Query for LOG_TYPE = ranked_party:create

CREATE OR REPLACE TABLE LOG_RANKED_PARTY_CREATE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL,
    ranked_party_id STRING NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"ranked_party_id" AS STRING) AS ranked_party_id
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'ranked_party:create'
;

-- Query for LOG_TYPE = ranked_party:delete

CREATE OR REPLACE TABLE LOG_RANKED_PARTY_DELETE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    deleted_at NUMBER NULL,
    ranked_party_id STRING NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"deleted_at" AS NUMBER) AS deleted_at,
    CAST(PARSE_JSON(LOG):"ranked_party_id" AS STRING) AS ranked_party_id
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'ranked_party:delete'
;

-- Query for LOG_TYPE = ranked_party:profile_edit

CREATE OR REPLACE TABLE LOG_RANKED_PARTY_PROFILE_EDIT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    message STRING NULL,
    user_id STRING NULL,
    modified_at NUMBER NULL,
    youtube_url STRING NULL,
    ranked_party_id STRING NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"message" AS STRING) AS message,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"modified_at" AS NUMBER) AS modified_at,
    CAST(PARSE_JSON(LOG):"youtube_url" AS STRING) AS youtube_url,
    CAST(PARSE_JSON(LOG):"ranked_party_id" AS STRING) AS ranked_party_id
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'ranked_party:profile_edit'
;

-- Query for LOG_TYPE = reservation:create

CREATE OR REPLACE TABLE LOG_RESERVATION_CREATE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    persons NUMBER NULL,
    user_id STRING NULL,
    created_at NUMBER NULL,
    reservation_id NUMBER NULL,
    reservation_date DATE NULL,
    reservation_time STRING NULL,
    reservation_type NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"persons" AS NUMBER) AS persons,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"reservation_id" AS NUMBER) AS reservation_id,
    CAST(PARSE_JSON(LOG):"reservation_date" AS DATE) AS reservation_date,
    CAST(PARSE_JSON(LOG):"reservation_time" AS STRING) AS reservation_time,
    CAST(PARSE_JSON(LOG):"reservation_type" AS NUMBER) AS reservation_type
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'reservation:create'
;

-- Query for LOG_TYPE = reservation:use

CREATE OR REPLACE TABLE LOG_RESERVATION_USE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL,
    reservation_id NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"reservation_id" AS NUMBER) AS reservation_id
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'reservation:use'
;

-- Query for LOG_TYPE = session:checkin

CREATE OR REPLACE TABLE LOG_SESSION_CHECKIN(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    response_rfid STRING NULL,
    response_type STRING NULL,
    response_playerId NUMBER NULL,
    response_accountId STRING NULL,
    response_createdAt NUMBER NULL,
    response_sessionId NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"response.rfid" AS STRING) AS response_rfid,
    CAST(PARSE_JSON(LOG):"response.type" AS STRING) AS response_type,
    CAST(PARSE_JSON(LOG):"response.playerId" AS NUMBER) AS response_playerId,
    CAST(PARSE_JSON(LOG):"response.accountId" AS STRING) AS response_accountId,
    CAST(PARSE_JSON(LOG):"response.createdAt" AS NUMBER) AS response_createdAt,
    CAST(PARSE_JSON(LOG):"response.sessionId" AS NUMBER) AS response_sessionId,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'session:checkin'
;

-- Query for LOG_TYPE = session:checkout

CREATE OR REPLACE TABLE LOG_SESSION_CHECKOUT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    response_rfid STRING NULL,
    response_type STRING NULL,
    response_playerId NUMBER NULL,
    response_accountId STRING NULL,
    response_createdAt NUMBER NULL,
    response_sessionId NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"response.rfid" AS STRING) AS response_rfid,
    CAST(PARSE_JSON(LOG):"response.type" AS STRING) AS response_type,
    CAST(PARSE_JSON(LOG):"response.playerId" AS NUMBER) AS response_playerId,
    CAST(PARSE_JSON(LOG):"response.accountId" AS STRING) AS response_accountId,
    CAST(PARSE_JSON(LOG):"response.createdAt" AS NUMBER) AS response_createdAt,
    CAST(PARSE_JSON(LOG):"response.sessionId" AS NUMBER) AS response_sessionId,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'session:checkout'
;

-- Query for LOG_TYPE = session:create

CREATE OR REPLACE TABLE LOG_SESSION_CREATE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    request_items VARIANT NULL,
    request_players VARIANT NULL,
    user_id STRING NULL,
    response_data_players VARIANT NULL,
    response_data_sessionId NUMBER NULL,
    created_at NUMBER NULL,
    user_reservation_id NUMBER NULL,
    request_items_onpInfo_effects VARIANT NULL,
    request_party_name STRING NULL,
    request_party_onpId STRING NULL,
    request_party_onpInfo_language NUMBER NULL,
    request_party_onpInfo_leaderId STRING NULL,
    request_party_onpInfo_partyCode STRING NULL,
    request_party_onpInfo_requestId STRING NULL,
    request_party_onpInfo_dataVersion STRING NULL,
    request_party_onpInfo_isTransform BOOLEAN NULL,
    request_party_onpInfo_isRankedParty BOOLEAN NULL,
    request_party_onpInfo_partyCategory NUMBER NULL,
    request_party_onpInfo_hasNextSession BOOLEAN NULL,
    request_party_ttsName STRING NULL,
    request_party_rankedPartyId STRING NULL,
    request_gameId STRING NULL,
    request_fieldId STRING NULL,
    request_players_onpInfo VARIANT NULL,
    request_players_onpInfo_avatarParams VARIANT NULL,
    request_version STRING NULL,
    request_isReserved BOOLEAN NULL,
    request_sourceSessionId NUMBER NULL,
    response_data_entryNumber NUMBER NULL,
    response_meta_status NUMBER NULL,
    terminal_id NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    PARSE_JSON(LOG):"request_items" AS request_items,
    PARSE_JSON(LOG):"request_players" AS request_players,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    PARSE_JSON(LOG):"response_data_players" AS response_data_players,
    CAST(PARSE_JSON(LOG):"response.data.sessionId" AS NUMBER) AS response_data_sessionId,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"user_reservation_id" AS NUMBER) AS user_reservation_id,
    PARSE_JSON(LOG):"request_items_onpInfo_effects" AS request_items_onpInfo_effects,
    CAST(PARSE_JSON(LOG):"request.party.name" AS STRING) AS request_party_name,
    CAST(PARSE_JSON(LOG):"request.party.onpId" AS STRING) AS request_party_onpId,
    CAST(PARSE_JSON(LOG):"request.party.onpInfo.language" AS NUMBER) AS request_party_onpInfo_language,
    CAST(PARSE_JSON(LOG):"request.party.onpInfo.leaderId" AS STRING) AS request_party_onpInfo_leaderId,
    CAST(PARSE_JSON(LOG):"request.party.onpInfo.partyCode" AS STRING) AS request_party_onpInfo_partyCode,
    CAST(PARSE_JSON(LOG):"request.party.onpInfo.requestId" AS STRING) AS request_party_onpInfo_requestId,
    CAST(PARSE_JSON(LOG):"request.party.onpInfo.dataVersion" AS STRING) AS request_party_onpInfo_dataVersion,
    CAST(PARSE_JSON(LOG):"request.party.onpInfo.isTransform" AS BOOLEAN) AS request_party_onpInfo_isTransform,
    CAST(PARSE_JSON(LOG):"request.party.onpInfo.isRankedParty" AS BOOLEAN) AS request_party_onpInfo_isRankedParty,
    CAST(PARSE_JSON(LOG):"request.party.onpInfo.partyCategory" AS NUMBER) AS request_party_onpInfo_partyCategory,
    CAST(PARSE_JSON(LOG):"request.party.onpInfo.hasNextSession" AS BOOLEAN) AS request_party_onpInfo_hasNextSession,
    CAST(PARSE_JSON(LOG):"request.party.ttsName" AS STRING) AS request_party_ttsName,
    CAST(PARSE_JSON(LOG):"request.party.rankedPartyId" AS STRING) AS request_party_rankedPartyId,
    CAST(PARSE_JSON(LOG):"request.gameId" AS STRING) AS request_gameId,
    CAST(PARSE_JSON(LOG):"request.fieldId" AS STRING) AS request_fieldId,
    PARSE_JSON(LOG):"request_players_onpInfo" AS request_players_onpInfo,
    PARSE_JSON(LOG):"request_players_onpInfo_avatarParams" AS request_players_onpInfo_avatarParams,
    CAST(PARSE_JSON(LOG):"request.version" AS STRING) AS request_version,
    CAST(PARSE_JSON(LOG):"request.isReserved" AS BOOLEAN) AS request_isReserved,
    CAST(PARSE_JSON(LOG):"request.sourceSessionId" AS NUMBER) AS request_sourceSessionId,
    CAST(PARSE_JSON(LOG):"response.data.entryNumber" AS NUMBER) AS response_data_entryNumber,
    CAST(PARSE_JSON(LOG):"response.meta.status" AS NUMBER) AS response_meta_status,
    CAST(PARSE_JSON(LOG):"terminal_id" AS NUMBER) AS terminal_id
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'session:create'
;

-- Query for LOG_TYPE = session:end

CREATE OR REPLACE TABLE LOG_SESSION_END(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    response_type STRING NULL,
    response_items_used VARIANT NULL,
    response_items_acquired VARIANT NULL,
    response_party_onpInfo_fieldLevel NUMBER NULL,
    response_party_onpInfo_isTransform BOOLEAN NULL,
    response_quests VARIANT NULL,
    response_quests_ofpState_uses VARIANT NULL,
    response_quests_ofpState_result VARIANT NULL,
    response_quests_ofpState_players VARIANT NULL,
    response_raeson STRING NULL,
    response_partyId STRING NULL,
    response_createdAt NUMBER NULL,
    response_sessionId NUMBER NULL,
    response_entryNumber NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"response.type" AS STRING) AS response_type,
    PARSE_JSON(LOG):"response_items_used" AS response_items_used,
    PARSE_JSON(LOG):"response_items_acquired" AS response_items_acquired,
    CAST(PARSE_JSON(LOG):"response.party.onpInfo.fieldLevel" AS NUMBER) AS response_party_onpInfo_fieldLevel,
    CAST(PARSE_JSON(LOG):"response.party.onpInfo.isTransform" AS BOOLEAN) AS response_party_onpInfo_isTransform,
    PARSE_JSON(LOG):"response_quests" AS response_quests,
    PARSE_JSON(LOG):"response_quests_ofpState_uses" AS response_quests_ofpState_uses,
    PARSE_JSON(LOG):"response_quests_ofpState_result" AS response_quests_ofpState_result,
    PARSE_JSON(LOG):"response_quests_ofpState_players" AS response_quests_ofpState_players,
    CAST(PARSE_JSON(LOG):"response.raeson" AS STRING) AS response_raeson,
    CAST(PARSE_JSON(LOG):"response.partyId" AS STRING) AS response_partyId,
    CAST(PARSE_JSON(LOG):"response.createdAt" AS NUMBER) AS response_createdAt,
    CAST(PARSE_JSON(LOG):"response.sessionId" AS NUMBER) AS response_sessionId,
    CAST(PARSE_JSON(LOG):"response.entryNumber" AS NUMBER) AS response_entryNumber,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'session:end'
;

-- Query for LOG_TYPE = session:entry

CREATE OR REPLACE TABLE LOG_SESSION_ENTRY(
    id INT NOT NULL,
    idx INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    response_type STRING NULL,
    response_partyId STRING NULL,
    response_players_id NUMBER NULL,
    response_players_name STRING NULL,
    response_players_accountId STRING NULL,
    response_partName STRING NULL,
    response_createdAt NUMBER NULL,
    response_sessionId NUMBER NULL,
    response_entryNumber NUMBER NULL,
    response_partyTtsName STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    response_players.index AS idx,
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"response.type" AS STRING) AS response_type,
    CAST(PARSE_JSON(LOG):"response.partyId" AS STRING) AS response_partyId,
    response_players.value:"id"::NUMBER AS response_players_id,
    response_players.value:"name"::STRING AS response_players_name,
    response_players.value:"accountId"::STRING AS response_players_accountId,
    CAST(PARSE_JSON(LOG):"response.partName" AS STRING) AS response_partName,
    CAST(PARSE_JSON(LOG):"response.createdAt" AS NUMBER) AS response_createdAt,
    CAST(PARSE_JSON(LOG):"response.sessionId" AS NUMBER) AS response_sessionId,
    CAST(PARSE_JSON(LOG):"response.entryNumber" AS NUMBER) AS response_entryNumber,
    CAST(PARSE_JSON(LOG):"response.partyTtsName" AS STRING) AS response_partyTtsName,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG,
    LATERAL FLATTEN(input => PARSE_JSON(LOG):"response.players") response_players
WHERE LOG_TYPE = 'session:entry'
;

-- Query for LOG_TYPE = session:exit

CREATE OR REPLACE TABLE LOG_SESSION_EXIT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    response_type STRING NULL,
    response_partyId STRING NULL,
    response_createdAt NUMBER NULL,
    response_sessionId NUMBER NULL,
    response_entryNumber NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"response.type" AS STRING) AS response_type,
    CAST(PARSE_JSON(LOG):"response.partyId" AS STRING) AS response_partyId,
    CAST(PARSE_JSON(LOG):"response.createdAt" AS NUMBER) AS response_createdAt,
    CAST(PARSE_JSON(LOG):"response.sessionId" AS NUMBER) AS response_sessionId,
    CAST(PARSE_JSON(LOG):"response.entryNumber" AS NUMBER) AS response_entryNumber,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'session:exit'
;

-- Query for LOG_TYPE = session:start

CREATE OR REPLACE TABLE LOG_SESSION_START(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    response_type STRING NULL,
    response_raeson STRING NULL,
    response_partyId STRING NULL,
    response_copyCount NUMBER NULL,
    response_createdAt NUMBER NULL,
    response_sessionId NUMBER NULL,
    response_entryNumber NUMBER NULL,
    response_sourceSessionId NUMBER NULL,
    response_originalSessionId NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"response.type" AS STRING) AS response_type,
    CAST(PARSE_JSON(LOG):"response.raeson" AS STRING) AS response_raeson,
    CAST(PARSE_JSON(LOG):"response.partyId" AS STRING) AS response_partyId,
    CAST(PARSE_JSON(LOG):"response.copyCount" AS NUMBER) AS response_copyCount,
    CAST(PARSE_JSON(LOG):"response.createdAt" AS NUMBER) AS response_createdAt,
    CAST(PARSE_JSON(LOG):"response.sessionId" AS NUMBER) AS response_sessionId,
    CAST(PARSE_JSON(LOG):"response.entryNumber" AS NUMBER) AS response_entryNumber,
    CAST(PARSE_JSON(LOG):"response.sourceSessionId" AS NUMBER) AS response_sourceSessionId,
    CAST(PARSE_JSON(LOG):"response.originalSessionId" AS NUMBER) AS response_originalSessionId,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'session:start'
;

-- Query for LOG_TYPE = smaregi:pos_transactions_temporaries_create

CREATE OR REPLACE TABLE LOG_SMAREGI_POS_TRANSACTIONS_TEMPORARIES_CREATE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    request_memo_party_id STRING NULL,
    request_memo_party_code STRING NULL,
    request_memo_party_name STRING NULL,
    request_memo_entryNumber NUMBER NULL,
    request_preRegistrationName STRING NULL,
    party_id STRING NULL,
    response_barcode STRING NULL,
    response_transactionHeadId STRING NULL,
    response_transactionDateTime STRING NULL,
    coupon_id NUMBER NULL,
    created_at NUMBER NULL,
    party_code STRING NULL,
    user_coupon_id NUMBER NULL,
    coupon_group_id NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"request.memo.party_id" AS STRING) AS request_memo_party_id,
    CAST(PARSE_JSON(LOG):"request.memo.party_code" AS STRING) AS request_memo_party_code,
    CAST(PARSE_JSON(LOG):"request.memo.party_name" AS STRING) AS request_memo_party_name,
    CAST(PARSE_JSON(LOG):"request.memo.entryNumber" AS NUMBER) AS request_memo_entryNumber,
    CAST(PARSE_JSON(LOG):"request.preRegistrationName" AS STRING) AS request_preRegistrationName,
    CAST(PARSE_JSON(LOG):"party_id" AS STRING) AS party_id,
    CAST(PARSE_JSON(LOG):"response.barcode" AS STRING) AS response_barcode,
    CAST(PARSE_JSON(LOG):"response.transactionHeadId" AS STRING) AS response_transactionHeadId,
    CAST(PARSE_JSON(LOG):"response.transactionDateTime" AS STRING) AS response_transactionDateTime,
    CAST(PARSE_JSON(LOG):"coupon_id" AS NUMBER) AS coupon_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"party_code" AS STRING) AS party_code,
    CAST(PARSE_JSON(LOG):"user_coupon_id" AS NUMBER) AS user_coupon_id,
    CAST(PARSE_JSON(LOG):"coupon_group_id" AS NUMBER) AS coupon_group_id
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'smaregi:pos_transactions_temporaries_create'
;

-- Query for LOG_TYPE = smaregi:webhook_pos_adjustments

CREATE OR REPLACE TABLE LOG_SMAREGI_WEBHOOK_POS_ADJUSTMENTS(
    id INT NOT NULL,
    idx INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    response_ids_storeId STRING NULL,
    response_ids_terminalId STRING NULL,
    response_ids_adjustmentDateTime STRING NULL,
    response_event STRING NULL,
    response_action STRING NULL,
    response_contractId STRING NULL,
    response_cashAdjustment BOOLEAN NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    response_ids.index AS idx,
    request_id,
    created,
    user_id as LOGs_userId,
    response_ids.value:"storeId"::STRING AS response_ids_storeId,
    response_ids.value:"terminalId"::STRING AS response_ids_terminalId,
    response_ids.value:"adjustmentDateTime"::STRING AS response_ids_adjustmentDateTime,
    CAST(PARSE_JSON(LOG):"response.event" AS STRING) AS response_event,
    CAST(PARSE_JSON(LOG):"response.action" AS STRING) AS response_action,
    CAST(PARSE_JSON(LOG):"response.contractId" AS STRING) AS response_contractId,
    CAST(PARSE_JSON(LOG):"response.cashAdjustment" AS BOOLEAN) AS response_cashAdjustment,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG,
    LATERAL FLATTEN(input => PARSE_JSON(LOG):"response.ids") response_ids
WHERE LOG_TYPE = 'smaregi:webhook_pos_adjustments'
;

-- Query for LOG_TYPE = spirit:get

CREATE OR REPLACE TABLE LOG_SPIRIT_GET(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    spirit_id NUMBER NULL,
    created_at NUMBER NULL,
    user_spirit_id NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"spirit_id" AS NUMBER) AS spirit_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"user_spirit_id" AS NUMBER) AS user_spirit_id
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'spirit:get'
;

-- Query for LOG_TYPE = spirit:use

CREATE OR REPLACE TABLE LOG_SPIRIT_USE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    spirit_id NUMBER NULL,
    created_at NUMBER NULL,
    user_spirit_id NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"spirit_id" AS NUMBER) AS spirit_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"user_spirit_id" AS NUMBER) AS user_spirit_id
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'spirit:use'
;

-- Query for LOG_TYPE = title:get

CREATE OR REPLACE TABLE LOG_TITLE_GET(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    title_id NUMBER NULL,
    modified_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"title_id" AS NUMBER) AS title_id,
    CAST(PARSE_JSON(LOG):"modified_at" AS NUMBER) AS modified_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'title:get'
;

-- Query for LOG_TYPE = user:adult_child_select

CREATE OR REPLACE TABLE LOG_USER_ADULT_CHILD_SELECT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    selected NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"selected" AS NUMBER) AS selected,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:adult_child_select'
;

-- Query for LOG_TYPE = user:ban

CREATE OR REPLACE TABLE LOG_USER_BAN(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    modified_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"modified_at" AS NUMBER) AS modified_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:ban'
;

-- Query for LOG_TYPE = user:ban_cancel

CREATE OR REPLACE TABLE LOG_USER_BAN_CANCEL(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    modified_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"modified_at" AS NUMBER) AS modified_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:ban_cancel'
;

-- Query for LOG_TYPE = user:create

CREATE OR REPLACE TABLE LOG_USER_CREATE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL,
    auth_code STRING NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"auth_code" AS STRING) AS auth_code
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:create'
;

-- Query for LOG_TYPE = user:delete

CREATE OR REPLACE TABLE LOG_USER_DELETE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    auth_code STRING NULL,
    deleted_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"auth_code" AS STRING) AS auth_code,
    CAST(PARSE_JSON(LOG):"deleted_at" AS NUMBER) AS deleted_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:delete'
;

-- Query for LOG_TYPE = user:flag

CREATE OR REPLACE TABLE LOG_USER_FLAG(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    user_flag NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"user_flag" AS NUMBER) AS user_flag,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:flag'
;

-- Query for LOG_TYPE = user:item

CREATE OR REPLACE TABLE LOG_USER_ITEM(
    id INT NOT NULL,
    idx INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    items_id NUMBER NULL,
    items_amount NUMBER NULL,
    items_item_id STRING NULL,
    items_item_type STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    items.index AS idx,
    request_id,
    created,
    user_id as LOGs_userId,
    items.value:"id"::NUMBER AS items_id,
    items.value:"amount"::NUMBER AS items_amount,
    items.value:"item_id"::STRING AS items_item_id,
    items.value:"item_type"::STRING AS items_item_type,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG,
    LATERAL FLATTEN(input => PARSE_JSON(LOG):"items") items
WHERE LOG_TYPE = 'user:item'
;

-- Query for LOG_TYPE = user:language_select

CREATE OR REPLACE TABLE LOG_USER_LANGUAGE_SELECT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    language NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"language" AS NUMBER) AS language,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:language_select'
;

-- Query for LOG_TYPE = user:login

CREATE OR REPLACE TABLE LOG_USER_LOGIN(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    auth_code STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"auth_code" AS STRING) AS auth_code,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:login'
;

-- Query for LOG_TYPE = user:login_first

CREATE OR REPLACE TABLE LOG_USER_LOGIN_FIRST(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    auth_code STRING NULL,
    created_at NUMBER NULL,
    continue_day NUMBER NULL,
    day_since_last NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"auth_code" AS STRING) AS auth_code,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"continue_day" AS NUMBER) AS continue_day,
    CAST(PARSE_JSON(LOG):"day_since_last" AS NUMBER) AS day_since_last
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:login_first'
;

-- Query for LOG_TYPE = user:logout

CREATE OR REPLACE TABLE LOG_USER_LOGOUT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    auth_code STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"auth_code" AS STRING) AS auth_code,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:logout'
;

-- Query for LOG_TYPE = user:name_create

CREATE OR REPLACE TABLE LOG_USER_NAME_CREATE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    name STRING NULL,
    user_id STRING NULL,
    tts_name STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"name" AS STRING) AS name,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"tts_name" AS STRING) AS tts_name,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:name_create'
;

-- Query for LOG_TYPE = user:name_edit

CREATE OR REPLACE TABLE LOG_USER_NAME_EDIT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    name STRING NULL,
    user_id STRING NULL,
    tts_name STRING NULL,
    created_at NUMBER NULL,
    before_name STRING NULL,
    before_tts_name STRING NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"name" AS STRING) AS name,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"tts_name" AS STRING) AS tts_name,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"before_name" AS STRING) AS before_name,
    CAST(PARSE_JSON(LOG):"before_tts_name" AS STRING) AS before_tts_name
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:name_edit'
;

-- Query for LOG_TYPE = user:profile_edit

CREATE OR REPLACE TABLE LOG_USER_PROFILE_EDIT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    url STRING NULL,
    name STRING NULL,
    message STRING NULL,
    user_id STRING NULL,
    tts_name STRING NULL,
    party_name STRING NULL,
    modified_at NUMBER NULL,
    party_tts_name STRING NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"url" AS STRING) AS url,
    CAST(PARSE_JSON(LOG):"name" AS STRING) AS name,
    CAST(PARSE_JSON(LOG):"message" AS STRING) AS message,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"tts_name" AS STRING) AS tts_name,
    CAST(PARSE_JSON(LOG):"party_name" AS STRING) AS party_name,
    CAST(PARSE_JSON(LOG):"modified_at" AS NUMBER) AS modified_at,
    CAST(PARSE_JSON(LOG):"party_tts_name" AS STRING) AS party_tts_name
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:profile_edit'
;

-- Query for LOG_TYPE = user:score

CREATE OR REPLACE TABLE LOG_USER_SCORE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    level NUMBER NULL,
    user_id STRING NULL,
    total_exp NUMBER NULL,
    created_at NUMBER NULL,
    total_gold NUMBER NULL,
    q1_play_num NUMBER NULL,
    q4_play_num NUMBER NULL,
    total_score NUMBER NULL,
    fq1_play_num NUMBER NULL,
    fq2_play_num NUMBER NULL,
    q2_a_play_num NUMBER NULL,
    q2_b_play_num NUMBER NULL,
    q2_c_play_num NUMBER NULL,
    q3_a_play_num NUMBER NULL,
    q3_b_play_num NUMBER NULL,
    q3_c_play_num NUMBER NULL,
    q3_d_play_num NUMBER NULL,
    q3_e_play_num NUMBER NULL,
    q3_f_play_num NUMBER NULL,
    q3_g_play_num NUMBER NULL,
    q3_h_play_num NUMBER NULL,
    q3_i_play_num NUMBER NULL,
    q5_a_play_num NUMBER NULL,
    q5_b_play_num NUMBER NULL,
    fq1_reached_num NUMBER NULL,
    fq2_reached_num NUMBER NULL,
    q5_a_reached_num NUMBER NULL,
    q5_b_reached_num NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):"level" AS NUMBER) AS level,
    CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):"total_exp" AS NUMBER) AS total_exp,
    CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):"total_gold" AS NUMBER) AS total_gold,
    CAST(PARSE_JSON(LOG):"q1_play_num" AS NUMBER) AS q1_play_num,
    CAST(PARSE_JSON(LOG):"q4_play_num" AS NUMBER) AS q4_play_num,
    CAST(PARSE_JSON(LOG):"total_score" AS NUMBER) AS total_score,
    CAST(PARSE_JSON(LOG):"fq1_play_num" AS NUMBER) AS fq1_play_num,
    CAST(PARSE_JSON(LOG):"fq2_play_num" AS NUMBER) AS fq2_play_num,
    CAST(PARSE_JSON(LOG):"q2_a_play_num" AS NUMBER) AS q2_a_play_num,
    CAST(PARSE_JSON(LOG):"q2_b_play_num" AS NUMBER) AS q2_b_play_num,
    CAST(PARSE_JSON(LOG):"q2_c_play_num" AS NUMBER) AS q2_c_play_num,
    CAST(PARSE_JSON(LOG):"q3_a_play_num" AS NUMBER) AS q3_a_play_num,
    CAST(PARSE_JSON(LOG):"q3_b_play_num" AS NUMBER) AS q3_b_play_num,
    CAST(PARSE_JSON(LOG):"q3_c_play_num" AS NUMBER) AS q3_c_play_num,
    CAST(PARSE_JSON(LOG):"q3_d_play_num" AS NUMBER) AS q3_d_play_num,
    CAST(PARSE_JSON(LOG):"q3_e_play_num" AS NUMBER) AS q3_e_play_num,
    CAST(PARSE_JSON(LOG):"q3_f_play_num" AS NUMBER) AS q3_f_play_num,
    CAST(PARSE_JSON(LOG):"q3_g_play_num" AS NUMBER) AS q3_g_play_num,
    CAST(PARSE_JSON(LOG):"q3_h_play_num" AS NUMBER) AS q3_h_play_num,
    CAST(PARSE_JSON(LOG):"q3_i_play_num" AS NUMBER) AS q3_i_play_num,
    CAST(PARSE_JSON(LOG):"q5_a_play_num" AS NUMBER) AS q5_a_play_num,
    CAST(PARSE_JSON(LOG):"q5_b_play_num" AS NUMBER) AS q5_b_play_num,
    CAST(PARSE_JSON(LOG):"fq1_reached_num" AS NUMBER) AS fq1_reached_num,
    CAST(PARSE_JSON(LOG):"fq2_reached_num" AS NUMBER) AS fq2_reached_num,
    CAST(PARSE_JSON(LOG):"q5_a_reached_num" AS NUMBER) AS q5_a_reached_num,
    CAST(PARSE_JSON(LOG):"q5_b_reached_num" AS NUMBER) AS q5_b_reached_num
FROM TTM_BABEL.BABEL_STG_DEV.DBLOG
WHERE LOG_TYPE = 'user:score'
;

