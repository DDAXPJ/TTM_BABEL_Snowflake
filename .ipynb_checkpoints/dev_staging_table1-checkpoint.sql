
{% if env == "PROD" %}
-- PRODの場合の処理
USE SCHEMA TTM_BABEL.BABEL_STG_PROD;
{% else %}
-- その他の場合の処理
USE SCHEMA TTM_BABEL.BABEL_STG_DEV;
{% endif %}
-- Query for LOG_TYPE = avatar:get

CREATE OR REPLACE TABLE LOG_AVATAR_GET(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    avatar_id NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):avatar_id AS NUMBER) AS avatar_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
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
    use_count NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):avatar_id AS NUMBER) AS avatar_id,
    CAST(PARSE_JSON(LOG):use_count AS NUMBER) AS use_count,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
WHERE LOG_TYPE = 'avatar:use'
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
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):currency.amount AS NUMBER) AS currency_amount,
    CAST(PARSE_JSON(LOG):currency.total_amount AS NUMBER) AS currency_total_amount,
    CAST(PARSE_JSON(LOG):currency.before_amount AS NUMBER) AS currency_before_amount,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
WHERE LOG_TYPE = 'currency:paid'
;

-- Query for LOG_TYPE = currency_free:deposit

CREATE OR REPLACE TABLE LOG_CURRENCY_FREE_DEPOSIT(
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
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):currency.amount AS NUMBER) AS currency_amount,
    CAST(PARSE_JSON(LOG):currency.total_amount AS NUMBER) AS currency_total_amount,
    CAST(PARSE_JSON(LOG):currency.before_amount AS NUMBER) AS currency_before_amount,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
WHERE LOG_TYPE = 'currency_free:deposit'
;

-- Query for LOG_TYPE = currency_free:paid

CREATE OR REPLACE TABLE LOG_CURRENCY_FREE_PAID(
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
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):currency.amount AS NUMBER) AS currency_amount,
    CAST(PARSE_JSON(LOG):currency.total_amount AS NUMBER) AS currency_total_amount,
    CAST(PARSE_JSON(LOG):currency.before_amount AS NUMBER) AS currency_before_amount,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
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
    created_at FLOAT NULL,
    customer_base NUMBER NULL
) AS
SELECT DISTINCT
    id,
    users.index AS idx,
    request_id,
    created,
    user_id as LOGs_userId,
    users.value:user_id::STRING AS users_user_id,
    CAST(PARSE_JSON(LOG):party_id AS STRING) AS party_id,
    CAST(PARSE_JSON(LOG):created_at AS FLOAT) AS created_at,
    CAST(PARSE_JSON(LOG):customer_base AS NUMBER) AS customer_base
FROM DBLOG,
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
    CAST(PARSE_JSON(LOG):age AS NUMBER) AS age,
    CAST(PARSE_JSON(LOG):sex AS NUMBER) AS sex,
    CAST(PARSE_JSON(LOG):region AS NUMBER) AS region,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):created_at AS FLOAT) AS created_at
FROM DBLOG
WHERE LOG_TYPE = 'demographic:user'
;

-- Query for LOG_TYPE = gacha:draw

CREATE OR REPLACE TABLE LOG_GACHA_DRAW(
    id INT NOT NULL,
    idx INT NOT NULL,
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    items_is_new BOOLEAN NULL,
    items_item_id NUMBER NULL,
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
    items.value:is_new::BOOLEAN AS items_is_new,
    items.value:item_id::NUMBER AS items_item_id,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):draw_num AS NUMBER) AS draw_num,
    CAST(PARSE_JSON(LOG):gacha_id AS NUMBER) AS gacha_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG,
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
    CAST(PARSE_JSON(LOG):amount AS NUMBER) AS amount,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
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
    CAST(PARSE_JSON(LOG):amount AS NUMBER) AS amount,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
WHERE LOG_TYPE = 'gold:sub'
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
    onpInfo_blacklist VARIANT NULL,
    onpInfo_partyCode STRING NULL,
    onpInfo_dataVersion STRING NULL,
    onpInfo_isTransform BOOLEAN NULL,
    onpInfo_isRankedParty BOOLEAN NULL,
    onpInfo_partyCategory NUMBER NULL,
    onpInfo_hasNextSession BOOLEAN NULL,
    players VARIANT NULL,
    players_onpInfo VARIANT NULL,
    players_onpInfo_avatarParams VARIANT NULL,
    questId STRING NULL,
    version STRING NULL,
    laevedAt NUMBER NULL,
    ofpState VARIANT  NULL,
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
    ofpState_fieldLevel NUMBER NULL,
    ofpState_uses VARIANT NULL,
    ofpState_result_A NUMBER NULL,
    ofpState_result_B FLOAT NULL,
    ofpState_result_C NUMBER NULL,
    ofpState_result_D NUMBER NULL,
    ofpState_result_E NUMBER NULL,
    ofpState_result_rate STRING NULL,
    ofpState_result_score NUMBER NULL,
    ofpState_result_reachedWave NUMBER NULL,
    ofpState_result_bossDefeated BOOLEAN NULL,
    ofpState_result_playerResults VARIANT NULL,
    ofpState_result_reachedWaveAchievementRate NUMBER NULL,
    ofpState_players VARIANT NULL,
    ofpState_dataVersion STRING NULL,
    fieldId STRING NULL,
    onpInfo_fieldLevel NUMBER NULL,
    leavedAt NUMBER NULL,
    created_at NUMBER NULL,
    onpInfo_behaviours VARIANT NULL,
    onpInfo_behaviours_target VARIANT NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):rate AS STRING) AS rate,
    CAST(PARSE_JSON(LOG):type AS STRING) AS type,
    CAST(PARSE_JSON(LOG):clear AS NUMBER) AS clear,
    PARSE_JSON(LOG):items AS items,
    PARSE_JSON(LOG):items_onpInfo_effects AS items_onpInfo_effects,
    CAST(PARSE_JSON(LOG):level AS NUMBER) AS level,
    CAST(PARSE_JSON(LOG):score AS NUMBER) AS score,
    CAST(PARSE_JSON(LOG):state AS STRING) AS state,
    CAST(PARSE_JSON(LOG):areaId AS STRING) AS areaId,
    CAST(PARSE_JSON(LOG):endedAt AS NUMBER) AS endedAt,
    CAST(PARSE_JSON(LOG):onpInfo.language AS NUMBER) AS onpInfo_language,
    CAST(PARSE_JSON(LOG):onpInfo.leaderId AS STRING) AS onpInfo_leaderId,
    PARSE_JSON(LOG):onpInfo_blacklist AS onpInfo_blacklist,
    CAST(PARSE_JSON(LOG):onpInfo.partyCode AS STRING) AS onpInfo_partyCode,
    CAST(PARSE_JSON(LOG):onpInfo.dataVersion AS STRING) AS onpInfo_dataVersion,
    CAST(PARSE_JSON(LOG):onpInfo.isTransform AS BOOLEAN) AS onpInfo_isTransform,
    CAST(PARSE_JSON(LOG):onpInfo.isRankedParty AS BOOLEAN) AS onpInfo_isRankedParty,
    CAST(PARSE_JSON(LOG):onpInfo.partyCategory AS NUMBER) AS onpInfo_partyCategory,
    CAST(PARSE_JSON(LOG):onpInfo.hasNextSession AS BOOLEAN) AS onpInfo_hasNextSession,
    PARSE_JSON(LOG):players AS players,
    PARSE_JSON(LOG):players_onpInfo AS players_onpInfo,
    PARSE_JSON(LOG):players_onpInfo_avatarParams AS players_onpInfo_avatarParams,
    CAST(PARSE_JSON(LOG):questId AS STRING) AS questId,
    CAST(PARSE_JSON(LOG):version AS STRING) AS version,
    CAST(PARSE_JSON(LOG):laevedAt AS NUMBER) AS laevedAt,
    PARSE_JSON(LOG):ofpState AS ofpState,
    -- ofpState: __EMPTY__ --,
    CAST(PARSE_JSON(LOG):totalExp AS NUMBER) AS totalExp,
    CAST(PARSE_JSON(LOG):enteredAt AS NUMBER) AS enteredAt,
    CAST(PARSE_JSON(LOG):sessionId AS NUMBER) AS sessionId,
    CAST(PARSE_JSON(LOG):startedAt AS NUMBER) AS startedAt,
    CAST(PARSE_JSON(LOG):totalGold AS NUMBER) AS totalGold,
    CAST(PARSE_JSON(LOG):dataVersion AS STRING) AS dataVersion,
    CAST(PARSE_JSON(LOG):totalExtraExp AS NUMBER) AS totalExtraExp,
    CAST(PARSE_JSON(LOG):totalExtraGold AS NUMBER) AS totalExtraGold,
    CAST(PARSE_JSON(LOG):numberOfPlayers AS NUMBER) AS numberOfPlayers,
    CAST(PARSE_JSON(LOG):totalPlayerLevel AS NUMBER) AS totalPlayerLevel,
    CAST(PARSE_JSON(LOG):ofpState.fieldLevel AS NUMBER) AS ofpState_fieldLevel,
    PARSE_JSON(LOG):ofpState_uses AS ofpState_uses,
    CAST(PARSE_JSON(LOG):ofpState.result.A AS NUMBER) AS ofpState_result_A,
    CAST(PARSE_JSON(LOG):ofpState.result.B AS FLOAT) AS ofpState_result_B,
    CAST(PARSE_JSON(LOG):ofpState.result.C AS NUMBER) AS ofpState_result_C,
    CAST(PARSE_JSON(LOG):ofpState.result.D AS NUMBER) AS ofpState_result_D,
    CAST(PARSE_JSON(LOG):ofpState.result.E AS NUMBER) AS ofpState_result_E,
    CAST(PARSE_JSON(LOG):ofpState.result.rate AS STRING) AS ofpState_result_rate,
    CAST(PARSE_JSON(LOG):ofpState.result.score AS NUMBER) AS ofpState_result_score,
    CAST(PARSE_JSON(LOG):ofpState.result.reachedWave AS NUMBER) AS ofpState_result_reachedWave,
    CAST(PARSE_JSON(LOG):ofpState.result.bossDefeated AS BOOLEAN) AS ofpState_result_bossDefeated,
    PARSE_JSON(LOG):ofpState_result_playerResults AS ofpState_result_playerResults,
    CAST(PARSE_JSON(LOG):ofpState.result.reachedWaveAchievementRate AS NUMBER) AS ofpState_result_reachedWaveAchievementRate,
    PARSE_JSON(LOG):ofpState_players AS ofpState_players,
    CAST(PARSE_JSON(LOG):ofpState.dataVersion AS STRING) AS ofpState_dataVersion,
    CAST(PARSE_JSON(LOG):fieldId AS STRING) AS fieldId,
    CAST(PARSE_JSON(LOG):onpInfo.fieldLevel AS NUMBER) AS onpInfo_fieldLevel,
    CAST(PARSE_JSON(LOG):leavedAt AS NUMBER) AS leavedAt,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
    PARSE_JSON(LOG):onpInfo_behaviours AS onpInfo_behaviours,
    PARSE_JSON(LOG):onpInfo_behaviours_target AS onpInfo_behaviours_target
FROM DBLOG
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
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):party_id AS STRING) AS party_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
WHERE LOG_TYPE = 'party:create'
;

-- Query for LOG_TYPE = party:name_create

CREATE OR REPLACE TABLE LOG_PARTY_NAME_CREATE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    party_id STRING NULL,
    created_at NUMBER NULL,
    party_name STRING NULL,
    party_tts_name STRING NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):party_id AS STRING) AS party_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):party_name AS STRING) AS party_name,
    CAST(PARSE_JSON(LOG):party_tts_name AS STRING) AS party_tts_name
FROM DBLOG
WHERE LOG_TYPE = 'party:name_create'
;

-- Query for LOG_TYPE = party:name_edit

CREATE OR REPLACE TABLE LOG_PARTY_NAME_EDIT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    party_id STRING NULL,
    created_at NUMBER NULL,
    party_name STRING NULL,
    before_name STRING NULL,
    party_tts_name STRING NULL,
    before_tts_name STRING NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):party_id AS STRING) AS party_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):party_name AS STRING) AS party_name,
    CAST(PARSE_JSON(LOG):before_name AS STRING) AS before_name,
    CAST(PARSE_JSON(LOG):party_tts_name AS STRING) AS party_tts_name,
    CAST(PARSE_JSON(LOG):before_tts_name AS STRING) AS before_tts_name
FROM DBLOG
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
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):party_id AS STRING) AS party_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):party_code AS STRING) AS party_code
FROM DBLOG
WHERE LOG_TYPE = 'party:select'
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
    CAST(PARSE_JSON(LOG):paid AS NUMBER) AS paid,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
WHERE LOG_TYPE = 'payment:q5_pay'
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
    CAST(PARSE_JSON(LOG):response.rfid AS STRING) AS response_rfid,
    CAST(PARSE_JSON(LOG):response.type AS STRING) AS response_type,
    CAST(PARSE_JSON(LOG):response.playerId AS NUMBER) AS response_playerId,
    CAST(PARSE_JSON(LOG):response.accountId AS STRING) AS response_accountId,
    CAST(PARSE_JSON(LOG):response.createdAt AS NUMBER) AS response_createdAt,
    CAST(PARSE_JSON(LOG):response.sessionId AS NUMBER) AS response_sessionId,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
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
    CAST(PARSE_JSON(LOG):response.rfid AS STRING) AS response_rfid,
    CAST(PARSE_JSON(LOG):response.type AS STRING) AS response_type,
    CAST(PARSE_JSON(LOG):response.playerId AS NUMBER) AS response_playerId,
    CAST(PARSE_JSON(LOG):response.accountId AS STRING) AS response_accountId,
    CAST(PARSE_JSON(LOG):response.createdAt AS NUMBER) AS response_createdAt,
    CAST(PARSE_JSON(LOG):response.sessionId AS NUMBER) AS response_sessionId,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
WHERE LOG_TYPE = 'session:checkout'
;

-- Query for LOG_TYPE = session:create

CREATE OR REPLACE TABLE LOG_SESSION_CREATE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    request_items VARIANT NULL,
    request_items_onpInfo_effects VARIANT NULL,
    request_party_name STRING NULL,
    request_party_onpId STRING NULL,
    request_party_onpInfo_language NUMBER NULL,
    request_party_onpInfo_leaderId STRING NULL,
    request_party_onpInfo_blacklist VARIANT NULL,
    request_party_onpInfo_partyCode STRING NULL,
    request_party_onpInfo_dataVersion STRING NULL,
    request_party_onpInfo_isTransform BOOLEAN NULL,
    request_party_onpInfo_isRankedParty BOOLEAN NULL,
    request_party_onpInfo_partyCategory NUMBER NULL,
    request_party_onpInfo_hasNextSession BOOLEAN NULL,
    request_party_ttsName STRING NULL,
    request_party_rankedPartyId VARIANT  NULL,
    request_gameId STRING NULL,
    request_fieldId STRING NULL,
    request_players VARIANT NULL,
    request_players_onpInfo VARIANT NULL,
    request_players_onpInfo_avatarParams VARIANT NULL,
    request_version STRING NULL,
    request_isReserved BOOLEAN NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    PARSE_JSON(LOG):request_items AS request_items,
    PARSE_JSON(LOG):request_items_onpInfo_effects AS request_items_onpInfo_effects,
    CAST(PARSE_JSON(LOG):request.party.name AS STRING) AS request_party_name,
    CAST(PARSE_JSON(LOG):request.party.onpId AS STRING) AS request_party_onpId,
    CAST(PARSE_JSON(LOG):request.party.onpInfo.language AS NUMBER) AS request_party_onpInfo_language,
    CAST(PARSE_JSON(LOG):request.party.onpInfo.leaderId AS STRING) AS request_party_onpInfo_leaderId,
    PARSE_JSON(LOG):request_party_onpInfo_blacklist AS request_party_onpInfo_blacklist,
    CAST(PARSE_JSON(LOG):request.party.onpInfo.partyCode AS STRING) AS request_party_onpInfo_partyCode,
    CAST(PARSE_JSON(LOG):request.party.onpInfo.dataVersion AS STRING) AS request_party_onpInfo_dataVersion,
    CAST(PARSE_JSON(LOG):request.party.onpInfo.isTransform AS BOOLEAN) AS request_party_onpInfo_isTransform,
    CAST(PARSE_JSON(LOG):request.party.onpInfo.isRankedParty AS BOOLEAN) AS request_party_onpInfo_isRankedParty,
    CAST(PARSE_JSON(LOG):request.party.onpInfo.partyCategory AS NUMBER) AS request_party_onpInfo_partyCategory,
    CAST(PARSE_JSON(LOG):request.party.onpInfo.hasNextSession AS BOOLEAN) AS request_party_onpInfo_hasNextSession,
    CAST(PARSE_JSON(LOG):request.party.ttsName AS STRING) AS request_party_ttsName,
    PARSE_JSON(LOG):request.party.rankedPartyId AS request_party_rankedPartyId,
    -- request.party.rankedPartyId: __EMPTY__ --,
    CAST(PARSE_JSON(LOG):request.gameId AS STRING) AS request_gameId,
    CAST(PARSE_JSON(LOG):request.fieldId AS STRING) AS request_fieldId,
    PARSE_JSON(LOG):request_players AS request_players,
    PARSE_JSON(LOG):request_players_onpInfo AS request_players_onpInfo,
    PARSE_JSON(LOG):request_players_onpInfo_avatarParams AS request_players_onpInfo_avatarParams,
    CAST(PARSE_JSON(LOG):request.version AS STRING) AS request_version,
    CAST(PARSE_JSON(LOG):request.isReserved AS BOOLEAN) AS request_isReserved,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
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
    response_party_onpInfo_language NUMBER NULL,
    response_party_onpInfo_leaderId STRING NULL,
    response_party_onpInfo_blacklist VARIANT NULL,
    response_party_onpInfo_partyCode STRING NULL,
    response_party_onpInfo_dataVersion STRING NULL,
    response_party_onpInfo_isTransform BOOLEAN NULL,
    response_party_onpInfo_isRankedParty BOOLEAN NULL,
    response_party_onpInfo_partyCategory NUMBER NULL,
    response_party_onpInfo_hasNextSession BOOLEAN NULL,
    response_party_ofpState_role STRING NULL,
    response_party_ofpState_rating NUMBER NULL,
    response_party_ofpState_debugLevel NUMBER NULL,
    response_party_ofpState_quest2Ended BOOLEAN NULL,
    response_quests VARIANT NULL,
    response_quests_ofpState_uses VARIANT NULL,
    response_quests_ofpState_result VARIANT NULL,
    response_quests_ofpState_result_playerResults VARIANT NULL,
    response_quests_ofpState_players VARIANT NULL,
    response_quests_ofpState VARIANT NULL,
    response_reason STRING NULL,
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
    CAST(PARSE_JSON(LOG):response.type AS STRING) AS response_type,
    PARSE_JSON(LOG):response_items_used AS response_items_used,
    PARSE_JSON(LOG):response_items_acquired AS response_items_acquired,
    CAST(PARSE_JSON(LOG):response.party.onpInfo.language AS NUMBER) AS response_party_onpInfo_language,
    CAST(PARSE_JSON(LOG):response.party.onpInfo.leaderId AS STRING) AS response_party_onpInfo_leaderId,
    PARSE_JSON(LOG):response_party_onpInfo_blacklist AS response_party_onpInfo_blacklist,
    CAST(PARSE_JSON(LOG):response.party.onpInfo.partyCode AS STRING) AS response_party_onpInfo_partyCode,
    CAST(PARSE_JSON(LOG):response.party.onpInfo.dataVersion AS STRING) AS response_party_onpInfo_dataVersion,
    CAST(PARSE_JSON(LOG):response.party.onpInfo.isTransform AS BOOLEAN) AS response_party_onpInfo_isTransform,
    CAST(PARSE_JSON(LOG):response.party.onpInfo.isRankedParty AS BOOLEAN) AS response_party_onpInfo_isRankedParty,
    CAST(PARSE_JSON(LOG):response.party.onpInfo.partyCategory AS NUMBER) AS response_party_onpInfo_partyCategory,
    CAST(PARSE_JSON(LOG):response.party.onpInfo.hasNextSession AS BOOLEAN) AS response_party_onpInfo_hasNextSession,
    CAST(PARSE_JSON(LOG):response.party.ofpState.role AS STRING) AS response_party_ofpState_role,
    CAST(PARSE_JSON(LOG):response.party.ofpState.rating AS NUMBER) AS response_party_ofpState_rating,
    CAST(PARSE_JSON(LOG):response.party.ofpState.debugLevel AS NUMBER) AS response_party_ofpState_debugLevel,
    CAST(PARSE_JSON(LOG):response.party.ofpState.quest2Ended AS BOOLEAN) AS response_party_ofpState_quest2Ended,
    PARSE_JSON(LOG):response_quests AS response_quests,
    PARSE_JSON(LOG):response_quests_ofpState_uses AS response_quests_ofpState_uses,
    PARSE_JSON(LOG):response_quests_ofpState_result AS response_quests_ofpState_result,
    PARSE_JSON(LOG):response_quests_ofpState_result_playerResults AS response_quests_ofpState_result_playerResults,
    PARSE_JSON(LOG):response_quests_ofpState_players AS response_quests_ofpState_players,
    PARSE_JSON(LOG):response_quests_ofpState AS response_quests_ofpState,
    CAST(PARSE_JSON(LOG):response.reason AS STRING) AS response_reason,
    CAST(PARSE_JSON(LOG):response.partyId AS STRING) AS response_partyId,
    CAST(PARSE_JSON(LOG):response.createdAt AS NUMBER) AS response_createdAt,
    CAST(PARSE_JSON(LOG):response.sessionId AS NUMBER) AS response_sessionId,
    CAST(PARSE_JSON(LOG):response.entryNumber AS NUMBER) AS response_entryNumber,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
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
    response_players_level NUMBER NULL,
    response_players_ttsName VARIANT NULL,
    response_players_avatarId NUMBER NULL,
    response_players_accountId STRING NULL,
    response_players_numberOfEntry NUMBER NULL,
    response_createdAt NUMBER NULL,
    response_partyName STRING NULL,
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
    CAST(PARSE_JSON(LOG):response.type AS STRING) AS response_type,
    CAST(PARSE_JSON(LOG):response.partyId AS STRING) AS response_partyId,
    response_players.value:id::NUMBER AS response_players_id,
    response_players.value:name::STRING AS response_players_name,
    response_players.value:level::NUMBER AS response_players_level,
    response_players.value:ttsName AS response_players_ttsName,
    response_players.value:avatarId::NUMBER AS response_players_avatarId,
    response_players.value:accountId::STRING AS response_players_accountId,
    response_players.value:numberOfEntry::NUMBER AS response_players_numberOfEntry,
    CAST(PARSE_JSON(LOG):response.createdAt AS NUMBER) AS response_createdAt,
    CAST(PARSE_JSON(LOG):response.partyName AS STRING) AS response_partyName,
    CAST(PARSE_JSON(LOG):response.sessionId AS NUMBER) AS response_sessionId,
    CAST(PARSE_JSON(LOG):response.entryNumber AS NUMBER) AS response_entryNumber,
    CAST(PARSE_JSON(LOG):response.partyTtsName AS STRING) AS response_partyTtsName,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG,
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
    response_reason STRING NULL,
    response_result_items_used VARIANT NULL,
    response_result_items_acquired VARIANT NULL,
    response_result_party_onpInfo_language NUMBER NULL,
    response_result_party_onpInfo_leaderId STRING NULL,
    response_result_party_onpInfo_blacklist VARIANT NULL,
    response_result_party_onpInfo_partyCode STRING NULL,
    response_result_party_onpInfo_dataVersion STRING NULL,
    response_result_party_onpInfo_isTransform BOOLEAN NULL,
    response_result_party_onpInfo_isRankedParty BOOLEAN NULL,
    response_result_party_onpInfo_partyCategory NUMBER NULL,
    response_result_party_onpInfo_hasNextSession BOOLEAN NULL,
    response_result_party_ofpState_role STRING NULL,
    response_result_party_ofpState_rating NUMBER NULL,
    response_result_party_ofpState_debugLevel NUMBER NULL,
    response_result_party_ofpState_quest2Ended BOOLEAN NULL,
    response_result_quests VARIANT NULL,
    response_result_quests_ofpState_uses VARIANT NULL,
    response_result_quests_ofpState_result VARIANT NULL,
    response_result_quests_ofpState_result_playerResults VARIANT NULL,
    response_result_quests_ofpState_players VARIANT NULL,
    response_result_quests_ofpState VARIANT NULL,
    response_result_reason STRING NULL,
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
    CAST(PARSE_JSON(LOG):response.type AS STRING) AS response_type,
    CAST(PARSE_JSON(LOG):response.reason AS STRING) AS response_reason,
    PARSE_JSON(LOG):response_result_items_used AS response_result_items_used,
    PARSE_JSON(LOG):response_result_items_acquired AS response_result_items_acquired,
    CAST(PARSE_JSON(LOG):response.result.party.onpInfo.language AS NUMBER) AS response_result_party_onpInfo_language,
    CAST(PARSE_JSON(LOG):response.result.party.onpInfo.leaderId AS STRING) AS response_result_party_onpInfo_leaderId,
    PARSE_JSON(LOG):response_result_party_onpInfo_blacklist AS response_result_party_onpInfo_blacklist,
    CAST(PARSE_JSON(LOG):response.result.party.onpInfo.partyCode AS STRING) AS response_result_party_onpInfo_partyCode,
    CAST(PARSE_JSON(LOG):response.result.party.onpInfo.dataVersion AS STRING) AS response_result_party_onpInfo_dataVersion,
    CAST(PARSE_JSON(LOG):response.result.party.onpInfo.isTransform AS BOOLEAN) AS response_result_party_onpInfo_isTransform,
    CAST(PARSE_JSON(LOG):response.result.party.onpInfo.isRankedParty AS BOOLEAN) AS response_result_party_onpInfo_isRankedParty,
    CAST(PARSE_JSON(LOG):response.result.party.onpInfo.partyCategory AS NUMBER) AS response_result_party_onpInfo_partyCategory,
    CAST(PARSE_JSON(LOG):response.result.party.onpInfo.hasNextSession AS BOOLEAN) AS response_result_party_onpInfo_hasNextSession,
    CAST(PARSE_JSON(LOG):response.result.party.ofpState.role AS STRING) AS response_result_party_ofpState_role,
    CAST(PARSE_JSON(LOG):response.result.party.ofpState.rating AS NUMBER) AS response_result_party_ofpState_rating,
    CAST(PARSE_JSON(LOG):response.result.party.ofpState.debugLevel AS NUMBER) AS response_result_party_ofpState_debugLevel,
    CAST(PARSE_JSON(LOG):response.result.party.ofpState.quest2Ended AS BOOLEAN) AS response_result_party_ofpState_quest2Ended,
    PARSE_JSON(LOG):response_result_quests AS response_result_quests,
    PARSE_JSON(LOG):response_result_quests_ofpState_uses AS response_result_quests_ofpState_uses,
    PARSE_JSON(LOG):response_result_quests_ofpState_result AS response_result_quests_ofpState_result,
    PARSE_JSON(LOG):response_result_quests_ofpState_result_playerResults AS response_result_quests_ofpState_result_playerResults,
    PARSE_JSON(LOG):response_result_quests_ofpState_players AS response_result_quests_ofpState_players,
    PARSE_JSON(LOG):response_result_quests_ofpState AS response_result_quests_ofpState,
    CAST(PARSE_JSON(LOG):response.result.reason AS STRING) AS response_result_reason,
    CAST(PARSE_JSON(LOG):response.partyId AS STRING) AS response_partyId,
    CAST(PARSE_JSON(LOG):response.createdAt AS NUMBER) AS response_createdAt,
    CAST(PARSE_JSON(LOG):response.sessionId AS NUMBER) AS response_sessionId,
    CAST(PARSE_JSON(LOG):response.entryNumber AS NUMBER) AS response_entryNumber,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
WHERE LOG_TYPE = 'session:exit'
;

-- Query for LOG_TYPE = session:start

CREATE OR REPLACE TABLE LOG_SESSION_START(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    response_type STRING NULL,
    response_partyId STRING NULL,
    response_copyCount NUMBER NULL,
    response_createdAt NUMBER NULL,
    response_sessionId NUMBER NULL,
    response_entryNumber NUMBER NULL,
    created_at NUMBER NULL,
    response_reason STRING NULL,
    response_sourceSessionId NUMBER NULL,
    response_originalSessionId NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):response.type AS STRING) AS response_type,
    CAST(PARSE_JSON(LOG):response.partyId AS STRING) AS response_partyId,
    CAST(PARSE_JSON(LOG):response.copyCount AS NUMBER) AS response_copyCount,
    CAST(PARSE_JSON(LOG):response.createdAt AS NUMBER) AS response_createdAt,
    CAST(PARSE_JSON(LOG):response.sessionId AS NUMBER) AS response_sessionId,
    CAST(PARSE_JSON(LOG):response.entryNumber AS NUMBER) AS response_entryNumber,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):response.reason AS STRING) AS response_reason,
    CAST(PARSE_JSON(LOG):response.sourceSessionId AS NUMBER) AS response_sourceSessionId,
    CAST(PARSE_JSON(LOG):response.originalSessionId AS NUMBER) AS response_originalSessionId
FROM DBLOG
WHERE LOG_TYPE = 'session:start'
;



-- Query for LOG_TYPE = spirit:get

CREATE OR REPLACE TABLE LOG_SPIRIT_GET(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    spirit_id NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):spirit_id AS NUMBER) AS spirit_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
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
    use_count NUMBER NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):spirit_id AS NUMBER) AS spirit_id,
    CAST(PARSE_JSON(LOG):use_count AS NUMBER) AS use_count,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
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
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):title_id AS NUMBER) AS title_id,
    CAST(PARSE_JSON(LOG):modified_at AS NUMBER) AS modified_at
FROM DBLOG
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
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):selected AS NUMBER) AS selected,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
WHERE LOG_TYPE = 'user:adult_child_select'
;

-- Query for LOG_TYPE = user:create

CREATE OR REPLACE TABLE LOG_USER_CREATE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
WHERE LOG_TYPE = 'user:create'
;

-- Query for LOG_TYPE = user:delete

CREATE OR REPLACE TABLE LOG_USER_DELETE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    deleted_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):deleted_at AS NUMBER) AS deleted_at
FROM DBLOG
WHERE LOG_TYPE = 'user:delete'
;

-- Query for LOG_TYPE = user:flag

CREATE OR REPLACE TABLE LOG_USER_FLAG(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    flag NUMBER NULL,
    user_id STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):flag AS NUMBER) AS flag,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
WHERE LOG_TYPE = 'user:flag'
;

-- Query for LOG_TYPE = user:language_select

CREATE OR REPLACE TABLE LOG_USER_LANGUAGE_SELECT(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    flag NUMBER NULL,
    user_id STRING NULL,
    created_at NUMBER NULL,
    language NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):flag AS NUMBER) AS flag,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):language AS NUMBER) AS language
FROM DBLOG
WHERE LOG_TYPE = 'user:language_select'
;

-- Query for LOG_TYPE = user:login

CREATE OR REPLACE TABLE LOG_USER_LOGIN(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
WHERE LOG_TYPE = 'user:login'
;

-- Query for LOG_TYPE = user:login_first

CREATE OR REPLACE TABLE LOG_USER_LOGIN_FIRST(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    created_at NUMBER NULL,
    continue_day NUMBER NULL,
    day_since_last NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):continue_day AS NUMBER) AS continue_day,
    CAST(PARSE_JSON(LOG):day_since_last AS NUMBER) AS day_since_last
FROM DBLOG
WHERE LOG_TYPE = 'user:login_first'
;

-- Query for LOG_TYPE = user:name_create

CREATE OR REPLACE TABLE LOG_USER_NAME_CREATE(
    id INT NOT NULL,
    
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    name STRING NULL,
    user_id STRING NULL,
    created_at NUMBER NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):name AS STRING) AS name,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at
FROM DBLOG
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
    created_at NUMBER NULL,
    before_name STRING NULL,
    before_tts_name STRING NULL,
    tts_name STRING NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):name AS STRING) AS name,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
    CAST(PARSE_JSON(LOG):before_name AS STRING) AS before_name,
    CAST(PARSE_JSON(LOG):before_tts_name AS STRING) AS before_tts_name,
    CAST(PARSE_JSON(LOG):tts_name AS STRING) AS tts_name
FROM DBLOG
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
    modified_at NUMBER NULL,
    tts_name STRING NULL
) AS
SELECT DISTINCT
    id,
    
    request_id,
    created,
    user_id as LOGs_userId,
    CAST(PARSE_JSON(LOG):url AS STRING) AS url,
    CAST(PARSE_JSON(LOG):name AS STRING) AS name,
    CAST(PARSE_JSON(LOG):message AS STRING) AS message,
    CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
    CAST(PARSE_JSON(LOG):modified_at AS NUMBER) AS modified_at,
    CAST(PARSE_JSON(LOG):tts_name AS STRING) AS tts_name
FROM DBLOG
WHERE LOG_TYPE = 'user:profile_edit'
;

