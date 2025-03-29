
{% if env == "PROD" %}
-- PRODの場合の処理
USE SCHEMA TTM_BABEL.BABEL_STG_PROD;
{% else %}
-- その他の場合の処理
USE SCHEMA TTM_BABEL.BABEL_STG_DEV;
{% endif %}
-- Query for LOG_TYPE = session:end

CREATE OR REPLACE STREAM LOG_SESSION_END_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SESSION_END_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_SESSION_END AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):response.type AS STRING) AS response_type,
        PARSE_JSON(LOG):response.items.used AS response_items_used,
        PARSE_JSON(LOG):response.items.acquired AS response_items_acquired,
        CAST(PARSE_JSON(LOG):response.party.onpInfo.language AS NUMBER) AS response_party_onpInfo_language,
        CAST(PARSE_JSON(LOG):response.party.onpInfo.leaderId AS STRING) AS response_party_onpInfo_leaderId,
        PARSE_JSON(LOG):response.party.onpInfo.blacklist AS response_party_onpInfo_blacklist,
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
        PARSE_JSON(LOG):response.quests AS response_quests,
        PARSE_JSON(LOG):response.quests.ofpState.uses AS response_quests_ofpState_uses,
        PARSE_JSON(LOG):response.quests.ofpState.result AS response_quests_ofpState_result,
        PARSE_JSON(LOG):response.quests.ofpState.result.playerResults AS response_quests_ofpState_result_playerResults,
        PARSE_JSON(LOG):response.quests.ofpState.players AS response_quests_ofpState_players,
        PARSE_JSON(LOG):response.quests.ofpState AS response_quests_ofpState,
        CAST(PARSE_JSON(LOG):response.reason AS STRING) AS response_reason,
        CAST(PARSE_JSON(LOG):response.partyId AS STRING) AS response_partyId,
        CAST(PARSE_JSON(LOG):response.createdAt AS NUMBER) AS response_createdAt,
        CAST(PARSE_JSON(LOG):response.sessionId AS NUMBER) AS response_sessionId,
        CAST(PARSE_JSON(LOG):response.entryNumber AS NUMBER) AS response_entryNumber,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_SESSION_END_STERAM
    WHERE LOG_TYPE = 'session:end'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.response_type = src.response_type,
    tgt.response_items_used = src.response_items_used,
    tgt.response_items_acquired = src.response_items_acquired,
    tgt.response_party_onpInfo_language = src.response_party_onpInfo_language,
    tgt.response_party_onpInfo_leaderId = src.response_party_onpInfo_leaderId,
    tgt.response_party_onpInfo_blacklist = src.response_party_onpInfo_blacklist,
    tgt.response_party_onpInfo_partyCode = src.response_party_onpInfo_partyCode,
    tgt.response_party_onpInfo_dataVersion = src.response_party_onpInfo_dataVersion,
    tgt.response_party_onpInfo_isTransform = src.response_party_onpInfo_isTransform,
    tgt.response_party_onpInfo_isRankedParty = src.response_party_onpInfo_isRankedParty,
    tgt.response_party_onpInfo_partyCategory = src.response_party_onpInfo_partyCategory,
    tgt.response_party_onpInfo_hasNextSession = src.response_party_onpInfo_hasNextSession,
    tgt.response_party_ofpState_role = src.response_party_ofpState_role,
    tgt.response_party_ofpState_rating = src.response_party_ofpState_rating,
    tgt.response_party_ofpState_debugLevel = src.response_party_ofpState_debugLevel,
    tgt.response_party_ofpState_quest2Ended = src.response_party_ofpState_quest2Ended,
    tgt.response_quests = src.response_quests,
    tgt.response_quests_ofpState_uses = src.response_quests_ofpState_uses,
    tgt.response_quests_ofpState_result = src.response_quests_ofpState_result,
    tgt.response_quests_ofpState_result_playerResults = src.response_quests_ofpState_result_playerResults,
    tgt.response_quests_ofpState_players = src.response_quests_ofpState_players,
    tgt.response_quests_ofpState = src.response_quests_ofpState,
    tgt.response_reason = src.response_reason,
    tgt.response_partyId = src.response_partyId,
    tgt.response_createdAt = src.response_createdAt,
    tgt.response_sessionId = src.response_sessionId,
    tgt.response_entryNumber = src.response_entryNumber,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    response_type,
    response_items_used,
    response_items_acquired,
    response_party_onpInfo_language,
    response_party_onpInfo_leaderId,
    response_party_onpInfo_blacklist,
    response_party_onpInfo_partyCode,
    response_party_onpInfo_dataVersion,
    response_party_onpInfo_isTransform,
    response_party_onpInfo_isRankedParty,
    response_party_onpInfo_partyCategory,
    response_party_onpInfo_hasNextSession,
    response_party_ofpState_role,
    response_party_ofpState_rating,
    response_party_ofpState_debugLevel,
    response_party_ofpState_quest2Ended,
    response_quests,
    response_quests_ofpState_uses,
    response_quests_ofpState_result,
    response_quests_ofpState_result_playerResults,
    response_quests_ofpState_players,
    response_quests_ofpState,
    response_reason,
    response_partyId,
    response_createdAt,
    response_sessionId,
    response_entryNumber,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.response_type,
    src.response_items_used,
    src.response_items_acquired,
    src.response_party_onpInfo_language,
    src.response_party_onpInfo_leaderId,
    src.response_party_onpInfo_blacklist,
    src.response_party_onpInfo_partyCode,
    src.response_party_onpInfo_dataVersion,
    src.response_party_onpInfo_isTransform,
    src.response_party_onpInfo_isRankedParty,
    src.response_party_onpInfo_partyCategory,
    src.response_party_onpInfo_hasNextSession,
    src.response_party_ofpState_role,
    src.response_party_ofpState_rating,
    src.response_party_ofpState_debugLevel,
    src.response_party_ofpState_quest2Ended,
    src.response_quests,
    src.response_quests_ofpState_uses,
    src.response_quests_ofpState_result,
    src.response_quests_ofpState_result_playerResults,
    src.response_quests_ofpState_players,
    src.response_quests_ofpState,
    src.response_reason,
    src.response_partyId,
    src.response_createdAt,
    src.response_sessionId,
    src.response_entryNumber,
    src.created_at
);
--Task再開
ALTER TASK LOG_SESSION_END_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SESSION_END_TASK;

-- Query for LOG_TYPE = session:entry

CREATE OR REPLACE STREAM LOG_SESSION_ENTRY_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SESSION_ENTRY_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_SESSION_ENTRY AS tgt
USING (
    SELECT DISTINCT 
        id,
        response_players.index AS idx,
        request_id,
        created,
        user_id AS LOGs_userId,
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
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_SESSION_ENTRY_STERAM,
        LATERAL FLATTEN(input => PARSE_JSON(LOG):"response.players") response_players
    WHERE LOG_TYPE = 'session:entry'
) src
ON tgt.id = src.id
   AND tgt.idx = src.idx
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.response_type = src.response_type,
    tgt.response_partyId = src.response_partyId,
    tgt.response_players_id = src.response_players_id,
    tgt.response_players_name = src.response_players_name,
    tgt.response_players_level = src.response_players_level,
    tgt.response_players_ttsName = src.response_players_ttsName,
    tgt.response_players_avatarId = src.response_players_avatarId,
    tgt.response_players_accountId = src.response_players_accountId,
    tgt.response_players_numberOfEntry = src.response_players_numberOfEntry,
    tgt.response_createdAt = src.response_createdAt,
    tgt.response_partyName = src.response_partyName,
    tgt.response_sessionId = src.response_sessionId,
    tgt.response_entryNumber = src.response_entryNumber,
    tgt.response_partyTtsName = src.response_partyTtsName,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    idx,
    request_id,
    created,
    LOGs_userId,
    response_type,
    response_partyId,
    response_players_id,
    response_players_name,
    response_players_level,
    response_players_ttsName,
    response_players_avatarId,
    response_players_accountId,
    response_players_numberOfEntry,
    response_createdAt,
    response_partyName,
    response_sessionId,
    response_entryNumber,
    response_partyTtsName,
    created_at
) VALUES (
    src.id,
    src.idx,
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.response_type,
    src.response_partyId,
    src.response_players_id,
    src.response_players_name,
    src.response_players_level,
    src.response_players_ttsName,
    src.response_players_avatarId,
    src.response_players_accountId,
    src.response_players_numberOfEntry,
    src.response_createdAt,
    src.response_partyName,
    src.response_sessionId,
    src.response_entryNumber,
    src.response_partyTtsName,
    src.created_at
);
--Task再開
ALTER TASK LOG_SESSION_ENTRY_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SESSION_ENTRY_TASK;

-- Query for LOG_TYPE = session:exit

CREATE OR REPLACE STREAM LOG_SESSION_EXIT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SESSION_EXIT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_SESSION_EXIT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):response.type AS STRING) AS response_type,
        CAST(PARSE_JSON(LOG):response.reason AS STRING) AS response_reason,
        PARSE_JSON(LOG):response.result.items.used AS response_result_items_used,
        PARSE_JSON(LOG):response.result.items.acquired AS response_result_items_acquired,
        CAST(PARSE_JSON(LOG):response.result.party.onpInfo.language AS NUMBER) AS response_result_party_onpInfo_language,
        CAST(PARSE_JSON(LOG):response.result.party.onpInfo.leaderId AS STRING) AS response_result_party_onpInfo_leaderId,
        PARSE_JSON(LOG):response.result.party.onpInfo.blacklist AS response_result_party_onpInfo_blacklist,
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
        PARSE_JSON(LOG):response.result.quests AS response_result_quests,
        PARSE_JSON(LOG):response.result.quests.ofpState.uses AS response_result_quests_ofpState_uses,
        PARSE_JSON(LOG):response.result.quests.ofpState.result AS response_result_quests_ofpState_result,
        PARSE_JSON(LOG):response.result.quests.ofpState.result.playerResults AS response_result_quests_ofpState_result_playerResults,
        PARSE_JSON(LOG):response.result.quests.ofpState.players AS response_result_quests_ofpState_players,
        PARSE_JSON(LOG):response.result.quests.ofpState AS response_result_quests_ofpState,
        CAST(PARSE_JSON(LOG):response.result.reason AS STRING) AS response_result_reason,
        CAST(PARSE_JSON(LOG):response.partyId AS STRING) AS response_partyId,
        CAST(PARSE_JSON(LOG):response.createdAt AS NUMBER) AS response_createdAt,
        CAST(PARSE_JSON(LOG):response.sessionId AS NUMBER) AS response_sessionId,
        CAST(PARSE_JSON(LOG):response.entryNumber AS NUMBER) AS response_entryNumber,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_SESSION_EXIT_STERAM
    WHERE LOG_TYPE = 'session:exit'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.response_type = src.response_type,
    tgt.response_reason = src.response_reason,
    tgt.response_result_items_used = src.response_result_items_used,
    tgt.response_result_items_acquired = src.response_result_items_acquired,
    tgt.response_result_party_onpInfo_language = src.response_result_party_onpInfo_language,
    tgt.response_result_party_onpInfo_leaderId = src.response_result_party_onpInfo_leaderId,
    tgt.response_result_party_onpInfo_blacklist = src.response_result_party_onpInfo_blacklist,
    tgt.response_result_party_onpInfo_partyCode = src.response_result_party_onpInfo_partyCode,
    tgt.response_result_party_onpInfo_dataVersion = src.response_result_party_onpInfo_dataVersion,
    tgt.response_result_party_onpInfo_isTransform = src.response_result_party_onpInfo_isTransform,
    tgt.response_result_party_onpInfo_isRankedParty = src.response_result_party_onpInfo_isRankedParty,
    tgt.response_result_party_onpInfo_partyCategory = src.response_result_party_onpInfo_partyCategory,
    tgt.response_result_party_onpInfo_hasNextSession = src.response_result_party_onpInfo_hasNextSession,
    tgt.response_result_party_ofpState_role = src.response_result_party_ofpState_role,
    tgt.response_result_party_ofpState_rating = src.response_result_party_ofpState_rating,
    tgt.response_result_party_ofpState_debugLevel = src.response_result_party_ofpState_debugLevel,
    tgt.response_result_party_ofpState_quest2Ended = src.response_result_party_ofpState_quest2Ended,
    tgt.response_result_quests = src.response_result_quests,
    tgt.response_result_quests_ofpState_uses = src.response_result_quests_ofpState_uses,
    tgt.response_result_quests_ofpState_result = src.response_result_quests_ofpState_result,
    tgt.response_result_quests_ofpState_result_playerResults = src.response_result_quests_ofpState_result_playerResults,
    tgt.response_result_quests_ofpState_players = src.response_result_quests_ofpState_players,
    tgt.response_result_quests_ofpState = src.response_result_quests_ofpState,
    tgt.response_result_reason = src.response_result_reason,
    tgt.response_partyId = src.response_partyId,
    tgt.response_createdAt = src.response_createdAt,
    tgt.response_sessionId = src.response_sessionId,
    tgt.response_entryNumber = src.response_entryNumber,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    response_type,
    response_reason,
    response_result_items_used,
    response_result_items_acquired,
    response_result_party_onpInfo_language,
    response_result_party_onpInfo_leaderId,
    response_result_party_onpInfo_blacklist,
    response_result_party_onpInfo_partyCode,
    response_result_party_onpInfo_dataVersion,
    response_result_party_onpInfo_isTransform,
    response_result_party_onpInfo_isRankedParty,
    response_result_party_onpInfo_partyCategory,
    response_result_party_onpInfo_hasNextSession,
    response_result_party_ofpState_role,
    response_result_party_ofpState_rating,
    response_result_party_ofpState_debugLevel,
    response_result_party_ofpState_quest2Ended,
    response_result_quests,
    response_result_quests_ofpState_uses,
    response_result_quests_ofpState_result,
    response_result_quests_ofpState_result_playerResults,
    response_result_quests_ofpState_players,
    response_result_quests_ofpState,
    response_result_reason,
    response_partyId,
    response_createdAt,
    response_sessionId,
    response_entryNumber,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.response_type,
    src.response_reason,
    src.response_result_items_used,
    src.response_result_items_acquired,
    src.response_result_party_onpInfo_language,
    src.response_result_party_onpInfo_leaderId,
    src.response_result_party_onpInfo_blacklist,
    src.response_result_party_onpInfo_partyCode,
    src.response_result_party_onpInfo_dataVersion,
    src.response_result_party_onpInfo_isTransform,
    src.response_result_party_onpInfo_isRankedParty,
    src.response_result_party_onpInfo_partyCategory,
    src.response_result_party_onpInfo_hasNextSession,
    src.response_result_party_ofpState_role,
    src.response_result_party_ofpState_rating,
    src.response_result_party_ofpState_debugLevel,
    src.response_result_party_ofpState_quest2Ended,
    src.response_result_quests,
    src.response_result_quests_ofpState_uses,
    src.response_result_quests_ofpState_result,
    src.response_result_quests_ofpState_result_playerResults,
    src.response_result_quests_ofpState_players,
    src.response_result_quests_ofpState,
    src.response_result_reason,
    src.response_partyId,
    src.response_createdAt,
    src.response_sessionId,
    src.response_entryNumber,
    src.created_at
);
--Task再開
ALTER TASK LOG_SESSION_EXIT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SESSION_EXIT_TASK;

-- Query for LOG_TYPE = session:start

CREATE OR REPLACE STREAM LOG_SESSION_START_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SESSION_START_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_SESSION_START AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):response.type AS STRING) AS response_type,
        CAST(PARSE_JSON(LOG):response.partyId AS STRING) AS response_partyId,
        CAST(PARSE_JSON(LOG):response.copyCount AS NUMBER) AS response_copyCount,
        CAST(PARSE_JSON(LOG):response.createdAt AS NUMBER) AS response_createdAt,
        CAST(PARSE_JSON(LOG):response.sessionId AS NUMBER) AS response_sessionId,
        CAST(PARSE_JSON(LOG):response.entryNumber AS NUMBER) AS response_entryNumber,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):response.reason AS STRING) AS response_reason,
        CAST(PARSE_JSON(LOG):response.sourceSessionId AS NUMBER) AS response_sourceSessionId,
        CAST(PARSE_JSON(LOG):response.originalSessionId AS NUMBER) AS response_originalSessionId,
        metadata$action
    FROM LOG_SESSION_START_STERAM
    WHERE LOG_TYPE = 'session:start'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.response_type = src.response_type,
    tgt.response_partyId = src.response_partyId,
    tgt.response_copyCount = src.response_copyCount,
    tgt.response_createdAt = src.response_createdAt,
    tgt.response_sessionId = src.response_sessionId,
    tgt.response_entryNumber = src.response_entryNumber,
    tgt.created_at = src.created_at,
    tgt.response_reason = src.response_reason,
    tgt.response_sourceSessionId = src.response_sourceSessionId,
    tgt.response_originalSessionId = src.response_originalSessionId
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    response_type,
    response_partyId,
    response_copyCount,
    response_createdAt,
    response_sessionId,
    response_entryNumber,
    created_at,
    response_reason,
    response_sourceSessionId,
    response_originalSessionId
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.response_type,
    src.response_partyId,
    src.response_copyCount,
    src.response_createdAt,
    src.response_sessionId,
    src.response_entryNumber,
    src.created_at,
    src.response_reason,
    src.response_sourceSessionId,
    src.response_originalSessionId
);
--Task再開
ALTER TASK LOG_SESSION_START_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SESSION_START_TASK;

-- Query for LOG_TYPE = smaregi:pos_transactions_temporaries_create

CREATE OR REPLACE STREAM LOG_SMAREGI_POS_TRANSACTIONS_TEMPORARIES_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SMAREGI_POS_TRANSACTIONS_TEMPORARIES_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_SMAREGI_POS_TRANSACTIONS_TEMPORARIES_CREATE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):request.memo AS STRING) AS request_memo,
        CAST(PARSE_JSON(LOG):request.total AS STRING) AS request_total,
        CAST(PARSE_JSON(LOG):request.status AS STRING) AS request_status,
        PARSE_JSON(LOG):request.details AS request_details,
        CAST(PARSE_JSON(LOG):request.storeId AS STRING) AS request_storeId,
        CAST(PARSE_JSON(LOG):request.subtotal AS STRING) AS request_subtotal,
        CAST(PARSE_JSON(LOG):request.terminalId AS STRING) AS request_terminalId,
        CAST(PARSE_JSON(LOG):request.terminalTranId AS STRING) AS request_terminalTranId,
        CAST(PARSE_JSON(LOG):request.preRegistrationName AS STRING) AS request_preRegistrationName,
        CAST(PARSE_JSON(LOG):request.terminalTranDateTime AS TIMESTAMP) AS request_terminalTranDateTime,
        CAST(PARSE_JSON(LOG):request.transactionHeadDivision AS STRING) AS request_transactionHeadDivision,
        CAST(PARSE_JSON(LOG):party_id AS STRING) AS party_id,
        CAST(PARSE_JSON(LOG):response.memo AS STRING) AS response_memo,
        PARSE_JSON(LOG):response.tags AS response_tags,
        -- response.tags: __EMPTY__ --,
        CAST(PARSE_JSON(LOG):response.point AS STRING) AS response_point,
        CAST(PARSE_JSON(LOG):response.total AS STRING) AS response_total,
        CAST(PARSE_JSON(LOG):response.amount AS STRING) AS response_amount,
        CAST(PARSE_JSON(LOG):response.status AS STRING) AS response_status,
        CAST(PARSE_JSON(LOG):response.barcode AS STRING) AS response_barcode,
        PARSE_JSON(LOG):response.details AS response_details,
        PARSE_JSON(LOG):response.details.productAttributes AS response_details_productAttributes,
        PARSE_JSON(LOG):response.staffId AS response_staffId,
        -- response.staffId: __EMPTY__ --,
        CAST(PARSE_JSON(LOG):response.storeId AS STRING) AS response_storeId,
        CAST(PARSE_JSON(LOG):response.taxRate AS STRING) AS response_taxRate,
        CAST(PARSE_JSON(LOG):response.newPoint AS STRING) AS response_newPoint,
        CAST(PARSE_JSON(LOG):response.subtotal AS STRING) AS response_subtotal,
        CAST(PARSE_JSON(LOG):response.costTotal AS STRING) AS response_costTotal,
        PARSE_JSON(LOG):response.customerId AS response_customerId,
        -- response.customerId: __EMPTY__ --,
        CAST(PARSE_JSON(LOG):response.spendPoint AS STRING) AS response_spendPoint,
        CAST(PARSE_JSON(LOG):response.taxExclude AS STRING) AS response_taxExclude,
        CAST(PARSE_JSON(LOG):response.taxInclude AS STRING) AS response_taxInclude,
        CAST(PARSE_JSON(LOG):response.terminalId AS STRING) AS response_terminalId,
        CAST(PARSE_JSON(LOG):response.totalPoint AS STRING) AS response_totalPoint,
        PARSE_JSON(LOG):response.receiptMemo AS response_receiptMemo,
        -- response.receiptMemo: __EMPTY__ --,
        CAST(PARSE_JSON(LOG):response.returnSales AS STRING) AS response_returnSales,
        CAST(PARSE_JSON(LOG):response.taxRounding AS STRING) AS response_taxRounding,
        CAST(PARSE_JSON(LOG):response.guestNumbers AS STRING) AS response_guestNumbers,
        CAST(PARSE_JSON(LOG):response.mileageLabel AS STRING) AS response_mileageLabel,
        CAST(PARSE_JSON(LOG):response.returnAmount AS STRING) AS response_returnAmount,
        CAST(PARSE_JSON(LOG):response.sellDivision AS STRING) AS response_sellDivision,
        CAST(PARSE_JSON(LOG):response.enterDateTime AS TIMESTAMP) AS response_enterDateTime,
        CAST(PARSE_JSON(LOG):response.pointDiscount AS STRING) AS response_pointDiscount,
        CAST(PARSE_JSON(LOG):response.roundingPrice AS STRING) AS response_roundingPrice,
        CAST(PARSE_JSON(LOG):response.cancelDivision AS STRING) AS response_cancelDivision,
        CAST(PARSE_JSON(LOG):response.terminalTranId AS STRING) AS response_terminalTranId,
        PARSE_JSON(LOG):response.customerGroupId AS response_customerGroupId,
        -- response.customerGroupId: __EMPTY__ --,
        CAST(PARSE_JSON(LOG):response.mileageDivision AS STRING) AS response_mileageDivision,
        PARSE_JSON(LOG):response.customerGroupId2 AS response_customerGroupId2,
        -- response.customerGroupId2: __EMPTY__ --,
        PARSE_JSON(LOG):response.customerGroupId3 AS response_customerGroupId3,
        -- response.customerGroupId3: __EMPTY__ --,
        PARSE_JSON(LOG):response.customerGroupId4 AS response_customerGroupId4,
        -- response.customerGroupId4: __EMPTY__ --,
        PARSE_JSON(LOG):response.customerGroupId5 AS response_customerGroupId5,
        -- response.customerGroupId5: __EMPTY__ --,
        PARSE_JSON(LOG):response.guestNumbersMale AS response_guestNumbersMale,
        -- response.guestNumbersMale: __EMPTY__ --,
        CAST(PARSE_JSON(LOG):response.roundingDivision AS STRING) AS response_roundingDivision,
        CAST(PARSE_JSON(LOG):response.sequentialNumber AS STRING) AS response_sequentialNumber,
        CAST(PARSE_JSON(LOG):response.transactionHeadId AS STRING) AS response_transactionHeadId,
        PARSE_JSON(LOG):response.guestNumbersFemale AS response_guestNumbersFemale,
        -- response.guestNumbersFemale: __EMPTY__ --,
        PARSE_JSON(LOG):response.guestNumbersUnknown AS response_guestNumbersUnknown,
        -- response.guestNumbersUnknown: __EMPTY__ --,
        CAST(PARSE_JSON(LOG):response.preRegistrationName AS STRING) AS response_preRegistrationName,
        CAST(PARSE_JSON(LOG):response.subtotalForDiscount AS STRING) AS response_subtotalForDiscount,
        CAST(PARSE_JSON(LOG):response.transactionDateTime AS TIMESTAMP) AS response_transactionDateTime,
        CAST(PARSE_JSON(LOG):response.subtotalDiscountRate AS STRING) AS response_subtotalDiscountRate,
        CAST(PARSE_JSON(LOG):response.taxFreeSalesDivision AS STRING) AS response_taxFreeSalesDivision,
        CAST(PARSE_JSON(LOG):response.terminalTranDateTime AS TIMESTAMP) AS response_terminalTranDateTime,
        CAST(PARSE_JSON(LOG):response.unitDiscountsubtotal AS STRING) AS response_unitDiscountsubtotal,
        CAST(PARSE_JSON(LOG):response.subtotalDiscountPrice AS STRING) AS response_subtotalDiscountPrice,
        CAST(PARSE_JSON(LOG):response.transactionHeadDivision AS STRING) AS response_transactionHeadDivision,
        CAST(PARSE_JSON(LOG):response.unitNonDiscountsubtotal AS STRING) AS response_unitNonDiscountsubtotal,
        CAST(PARSE_JSON(LOG):response.discountRoundingDivision AS STRING) AS response_discountRoundingDivision,
        PARSE_JSON(LOG):response.subtotalDiscountDivision AS response_subtotalDiscountDivision,
        -- response.subtotalDiscountDivision: __EMPTY__ --,
        CAST(PARSE_JSON(LOG):response.netTaxFreeGeneralTaxExclude AS STRING) AS response_netTaxFreeGeneralTaxExclude,
        CAST(PARSE_JSON(LOG):response.netTaxFreeGeneralTaxInclude AS STRING) AS response_netTaxFreeGeneralTaxInclude,
        CAST(PARSE_JSON(LOG):response.netTaxFreeConsumableTaxExclude AS STRING) AS response_netTaxFreeConsumableTaxExclude,
        CAST(PARSE_JSON(LOG):response.netTaxFreeConsumableTaxInclude AS STRING) AS response_netTaxFreeConsumableTaxInclude,
        CAST(PARSE_JSON(LOG):coupon_id AS NUMBER) AS coupon_id,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):party_code AS STRING) AS party_code,
        CAST(PARSE_JSON(LOG):user_coupon_id AS STRING) AS user_coupon_id,
        CAST(PARSE_JSON(LOG):coupon_group_id AS NUMBER) AS coupon_group_id,
        metadata$action
    FROM LOG_SMAREGI_POS_TRANSACTIONS_TEMPORARIES_CREATE_STERAM
    WHERE LOG_TYPE = 'smaregi:pos_transactions_temporaries_create'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.request_memo = src.request_memo,
    tgt.request_total = src.request_total,
    tgt.request_status = src.request_status,
    tgt.request_details = src.request_details,
    tgt.request_storeId = src.request_storeId,
    tgt.request_subtotal = src.request_subtotal,
    tgt.request_terminalId = src.request_terminalId,
    tgt.request_terminalTranId = src.request_terminalTranId,
    tgt.request_preRegistrationName = src.request_preRegistrationName,
    tgt.request_terminalTranDateTime = src.request_terminalTranDateTime,
    tgt.request_transactionHeadDivision = src.request_transactionHeadDivision,
    tgt.party_id = src.party_id,
    tgt.response_memo = src.response_memo,
    tgt.response_tags = src.response_tags,
    tgt.response_point = src.response_point,
    tgt.response_total = src.response_total,
    tgt.response_amount = src.response_amount,
    tgt.response_status = src.response_status,
    tgt.response_barcode = src.response_barcode,
    tgt.response_details = src.response_details,
    tgt.response_details_productAttributes = src.response_details_productAttributes,
    tgt.response_staffId = src.response_staffId,
    tgt.response_storeId = src.response_storeId,
    tgt.response_taxRate = src.response_taxRate,
    tgt.response_newPoint = src.response_newPoint,
    tgt.response_subtotal = src.response_subtotal,
    tgt.response_costTotal = src.response_costTotal,
    tgt.response_customerId = src.response_customerId,
    tgt.response_spendPoint = src.response_spendPoint,
    tgt.response_taxExclude = src.response_taxExclude,
    tgt.response_taxInclude = src.response_taxInclude,
    tgt.response_terminalId = src.response_terminalId,
    tgt.response_totalPoint = src.response_totalPoint,
    tgt.response_receiptMemo = src.response_receiptMemo,
    tgt.response_returnSales = src.response_returnSales,
    tgt.response_taxRounding = src.response_taxRounding,
    tgt.response_guestNumbers = src.response_guestNumbers,
    tgt.response_mileageLabel = src.response_mileageLabel,
    tgt.response_returnAmount = src.response_returnAmount,
    tgt.response_sellDivision = src.response_sellDivision,
    tgt.response_enterDateTime = src.response_enterDateTime,
    tgt.response_pointDiscount = src.response_pointDiscount,
    tgt.response_roundingPrice = src.response_roundingPrice,
    tgt.response_cancelDivision = src.response_cancelDivision,
    tgt.response_terminalTranId = src.response_terminalTranId,
    tgt.response_customerGroupId = src.response_customerGroupId,
    tgt.response_mileageDivision = src.response_mileageDivision,
    tgt.response_customerGroupId2 = src.response_customerGroupId2,
    tgt.response_customerGroupId3 = src.response_customerGroupId3,
    tgt.response_customerGroupId4 = src.response_customerGroupId4,
    tgt.response_customerGroupId5 = src.response_customerGroupId5,
    tgt.response_guestNumbersMale = src.response_guestNumbersMale,
    tgt.response_roundingDivision = src.response_roundingDivision,
    tgt.response_sequentialNumber = src.response_sequentialNumber,
    tgt.response_transactionHeadId = src.response_transactionHeadId,
    tgt.response_guestNumbersFemale = src.response_guestNumbersFemale,
    tgt.response_guestNumbersUnknown = src.response_guestNumbersUnknown,
    tgt.response_preRegistrationName = src.response_preRegistrationName,
    tgt.response_subtotalForDiscount = src.response_subtotalForDiscount,
    tgt.response_transactionDateTime = src.response_transactionDateTime,
    tgt.response_subtotalDiscountRate = src.response_subtotalDiscountRate,
    tgt.response_taxFreeSalesDivision = src.response_taxFreeSalesDivision,
    tgt.response_terminalTranDateTime = src.response_terminalTranDateTime,
    tgt.response_unitDiscountsubtotal = src.response_unitDiscountsubtotal,
    tgt.response_subtotalDiscountPrice = src.response_subtotalDiscountPrice,
    tgt.response_transactionHeadDivision = src.response_transactionHeadDivision,
    tgt.response_unitNonDiscountsubtotal = src.response_unitNonDiscountsubtotal,
    tgt.response_discountRoundingDivision = src.response_discountRoundingDivision,
    tgt.response_subtotalDiscountDivision = src.response_subtotalDiscountDivision,
    tgt.response_netTaxFreeGeneralTaxExclude = src.response_netTaxFreeGeneralTaxExclude,
    tgt.response_netTaxFreeGeneralTaxInclude = src.response_netTaxFreeGeneralTaxInclude,
    tgt.response_netTaxFreeConsumableTaxExclude = src.response_netTaxFreeConsumableTaxExclude,
    tgt.response_netTaxFreeConsumableTaxInclude = src.response_netTaxFreeConsumableTaxInclude,
    tgt.coupon_id = src.coupon_id,
    tgt.created_at = src.created_at,
    tgt.party_code = src.party_code,
    tgt.user_coupon_id = src.user_coupon_id,
    tgt.coupon_group_id = src.coupon_group_id
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    request_memo,
    request_total,
    request_status,
    request_details,
    request_storeId,
    request_subtotal,
    request_terminalId,
    request_terminalTranId,
    request_preRegistrationName,
    request_terminalTranDateTime,
    request_transactionHeadDivision,
    party_id,
    response_memo,
    response_tags,
    response_point,
    response_total,
    response_amount,
    response_status,
    response_barcode,
    response_details,
    response_details_productAttributes,
    response_staffId,
    response_storeId,
    response_taxRate,
    response_newPoint,
    response_subtotal,
    response_costTotal,
    response_customerId,
    response_spendPoint,
    response_taxExclude,
    response_taxInclude,
    response_terminalId,
    response_totalPoint,
    response_receiptMemo,
    response_returnSales,
    response_taxRounding,
    response_guestNumbers,
    response_mileageLabel,
    response_returnAmount,
    response_sellDivision,
    response_enterDateTime,
    response_pointDiscount,
    response_roundingPrice,
    response_cancelDivision,
    response_terminalTranId,
    response_customerGroupId,
    response_mileageDivision,
    response_customerGroupId2,
    response_customerGroupId3,
    response_customerGroupId4,
    response_customerGroupId5,
    response_guestNumbersMale,
    response_roundingDivision,
    response_sequentialNumber,
    response_transactionHeadId,
    response_guestNumbersFemale,
    response_guestNumbersUnknown,
    response_preRegistrationName,
    response_subtotalForDiscount,
    response_transactionDateTime,
    response_subtotalDiscountRate,
    response_taxFreeSalesDivision,
    response_terminalTranDateTime,
    response_unitDiscountsubtotal,
    response_subtotalDiscountPrice,
    response_transactionHeadDivision,
    response_unitNonDiscountsubtotal,
    response_discountRoundingDivision,
    response_subtotalDiscountDivision,
    response_netTaxFreeGeneralTaxExclude,
    response_netTaxFreeGeneralTaxInclude,
    response_netTaxFreeConsumableTaxExclude,
    response_netTaxFreeConsumableTaxInclude,
    coupon_id,
    created_at,
    party_code,
    user_coupon_id,
    coupon_group_id
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.request_memo,
    src.request_total,
    src.request_status,
    src.request_details,
    src.request_storeId,
    src.request_subtotal,
    src.request_terminalId,
    src.request_terminalTranId,
    src.request_preRegistrationName,
    src.request_terminalTranDateTime,
    src.request_transactionHeadDivision,
    src.party_id,
    src.response_memo,
    src.response_tags,
    src.response_point,
    src.response_total,
    src.response_amount,
    src.response_status,
    src.response_barcode,
    src.response_details,
    src.response_details_productAttributes,
    src.response_staffId,
    src.response_storeId,
    src.response_taxRate,
    src.response_newPoint,
    src.response_subtotal,
    src.response_costTotal,
    src.response_customerId,
    src.response_spendPoint,
    src.response_taxExclude,
    src.response_taxInclude,
    src.response_terminalId,
    src.response_totalPoint,
    src.response_receiptMemo,
    src.response_returnSales,
    src.response_taxRounding,
    src.response_guestNumbers,
    src.response_mileageLabel,
    src.response_returnAmount,
    src.response_sellDivision,
    src.response_enterDateTime,
    src.response_pointDiscount,
    src.response_roundingPrice,
    src.response_cancelDivision,
    src.response_terminalTranId,
    src.response_customerGroupId,
    src.response_mileageDivision,
    src.response_customerGroupId2,
    src.response_customerGroupId3,
    src.response_customerGroupId4,
    src.response_customerGroupId5,
    src.response_guestNumbersMale,
    src.response_roundingDivision,
    src.response_sequentialNumber,
    src.response_transactionHeadId,
    src.response_guestNumbersFemale,
    src.response_guestNumbersUnknown,
    src.response_preRegistrationName,
    src.response_subtotalForDiscount,
    src.response_transactionDateTime,
    src.response_subtotalDiscountRate,
    src.response_taxFreeSalesDivision,
    src.response_terminalTranDateTime,
    src.response_unitDiscountsubtotal,
    src.response_subtotalDiscountPrice,
    src.response_transactionHeadDivision,
    src.response_unitNonDiscountsubtotal,
    src.response_discountRoundingDivision,
    src.response_subtotalDiscountDivision,
    src.response_netTaxFreeGeneralTaxExclude,
    src.response_netTaxFreeGeneralTaxInclude,
    src.response_netTaxFreeConsumableTaxExclude,
    src.response_netTaxFreeConsumableTaxInclude,
    src.coupon_id,
    src.created_at,
    src.party_code,
    src.user_coupon_id,
    src.coupon_group_id
);
--Task再開
ALTER TASK LOG_SMAREGI_POS_TRANSACTIONS_TEMPORARIES_CREATE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SMAREGI_POS_TRANSACTIONS_TEMPORARIES_CREATE_TASK;

-- Query for LOG_TYPE = spirit:get

CREATE OR REPLACE STREAM LOG_SPIRIT_GET_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SPIRIT_GET_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_SPIRIT_GET AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):spirit_id AS NUMBER) AS spirit_id,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_SPIRIT_GET_STERAM
    WHERE LOG_TYPE = 'spirit:get'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.spirit_id = src.spirit_id,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    spirit_id,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.spirit_id,
    src.created_at
);
--Task再開
ALTER TASK LOG_SPIRIT_GET_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SPIRIT_GET_TASK;

-- Query for LOG_TYPE = spirit:use

CREATE OR REPLACE STREAM LOG_SPIRIT_USE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SPIRIT_USE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_SPIRIT_USE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):spirit_id AS NUMBER) AS spirit_id,
        CAST(PARSE_JSON(LOG):use_count AS NUMBER) AS use_count,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_SPIRIT_USE_STERAM
    WHERE LOG_TYPE = 'spirit:use'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.spirit_id = src.spirit_id,
    tgt.use_count = src.use_count,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    spirit_id,
    use_count,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.spirit_id,
    src.use_count,
    src.created_at
);
--Task再開
ALTER TASK LOG_SPIRIT_USE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SPIRIT_USE_TASK;

-- Query for LOG_TYPE = title:get

CREATE OR REPLACE STREAM LOG_TITLE_GET_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_TITLE_GET_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_TITLE_GET AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):title_id AS NUMBER) AS title_id,
        CAST(PARSE_JSON(LOG):modified_at AS NUMBER) AS modified_at,
        metadata$action
    FROM LOG_TITLE_GET_STERAM
    WHERE LOG_TYPE = 'title:get'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.title_id = src.title_id,
    tgt.modified_at = src.modified_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    title_id,
    modified_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.title_id,
    src.modified_at
);
--Task再開
ALTER TASK LOG_TITLE_GET_TASK RESUME;

--Task実行
EXECUTE TASK LOG_TITLE_GET_TASK;

-- Query for LOG_TYPE = user:adult_child_select

CREATE OR REPLACE STREAM LOG_USER_ADULT_CHILD_SELECT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_ADULT_CHILD_SELECT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_USER_ADULT_CHILD_SELECT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):selected AS NUMBER) AS selected,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_USER_ADULT_CHILD_SELECT_STERAM
    WHERE LOG_TYPE = 'user:adult_child_select'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.selected = src.selected,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    selected,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.selected,
    src.created_at
);
--Task再開
ALTER TASK LOG_USER_ADULT_CHILD_SELECT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_ADULT_CHILD_SELECT_TASK;

-- Query for LOG_TYPE = user:create

CREATE OR REPLACE STREAM LOG_USER_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_USER_CREATE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_USER_CREATE_STERAM
    WHERE LOG_TYPE = 'user:create'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.created_at
);
--Task再開
ALTER TASK LOG_USER_CREATE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_CREATE_TASK;

-- Query for LOG_TYPE = user:delete

CREATE OR REPLACE STREAM LOG_USER_DELETE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_DELETE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_USER_DELETE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):deleted_at AS NUMBER) AS deleted_at,
        metadata$action
    FROM LOG_USER_DELETE_STERAM
    WHERE LOG_TYPE = 'user:delete'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.deleted_at = src.deleted_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    deleted_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.deleted_at
);
--Task再開
ALTER TASK LOG_USER_DELETE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_DELETE_TASK;

-- Query for LOG_TYPE = user:flag

CREATE OR REPLACE STREAM LOG_USER_FLAG_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_FLAG_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_USER_FLAG AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):flag AS NUMBER) AS flag,
        CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_USER_FLAG_STERAM
    WHERE LOG_TYPE = 'user:flag'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.flag = src.flag,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    flag,
    user_id,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.flag,
    src.user_id,
    src.created_at
);
--Task再開
ALTER TASK LOG_USER_FLAG_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_FLAG_TASK;

-- Query for LOG_TYPE = user:language_select

CREATE OR REPLACE STREAM LOG_USER_LANGUAGE_SELECT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_LANGUAGE_SELECT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_USER_LANGUAGE_SELECT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):flag AS NUMBER) AS flag,
        CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):language AS NUMBER) AS language,
        metadata$action
    FROM LOG_USER_LANGUAGE_SELECT_STERAM
    WHERE LOG_TYPE = 'user:language_select'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.flag = src.flag,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at,
    tgt.language = src.language
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    flag,
    user_id,
    created_at,
    language
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.flag,
    src.user_id,
    src.created_at,
    src.language
);
--Task再開
ALTER TASK LOG_USER_LANGUAGE_SELECT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_LANGUAGE_SELECT_TASK;

-- Query for LOG_TYPE = user:login

CREATE OR REPLACE STREAM LOG_USER_LOGIN_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_LOGIN_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_USER_LOGIN AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_USER_LOGIN_STERAM
    WHERE LOG_TYPE = 'user:login'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.created_at
);
--Task再開
ALTER TASK LOG_USER_LOGIN_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_LOGIN_TASK;

-- Query for LOG_TYPE = user:login_first

CREATE OR REPLACE STREAM LOG_USER_LOGIN_FIRST_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_LOGIN_FIRST_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_USER_LOGIN_FIRST AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):continue_day AS NUMBER) AS continue_day,
        CAST(PARSE_JSON(LOG):day_since_last AS NUMBER) AS day_since_last,
        metadata$action
    FROM LOG_USER_LOGIN_FIRST_STERAM
    WHERE LOG_TYPE = 'user:login_first'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.created_at = src.created_at,
    tgt.continue_day = src.continue_day,
    tgt.day_since_last = src.day_since_last
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    created_at,
    continue_day,
    day_since_last
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.created_at,
    src.continue_day,
    src.day_since_last
);
--Task再開
ALTER TASK LOG_USER_LOGIN_FIRST_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_LOGIN_FIRST_TASK;

-- Query for LOG_TYPE = user:name_create

CREATE OR REPLACE STREAM LOG_USER_NAME_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_NAME_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_USER_NAME_CREATE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):name AS STRING) AS name,
        CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_USER_NAME_CREATE_STERAM
    WHERE LOG_TYPE = 'user:name_create'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.name = src.name,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    name,
    user_id,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.name,
    src.user_id,
    src.created_at
);
--Task再開
ALTER TASK LOG_USER_NAME_CREATE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_NAME_CREATE_TASK;

-- Query for LOG_TYPE = user:name_edit

CREATE OR REPLACE STREAM LOG_USER_NAME_EDIT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_NAME_EDIT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_USER_NAME_EDIT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):name AS STRING) AS name,
        CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):created_at AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):before_name AS STRING) AS before_name,
        CAST(PARSE_JSON(LOG):before_tts_name AS STRING) AS before_tts_name,
        CAST(PARSE_JSON(LOG):tts_name AS STRING) AS tts_name,
        metadata$action
    FROM LOG_USER_NAME_EDIT_STERAM
    WHERE LOG_TYPE = 'user:name_edit'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.name = src.name,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at,
    tgt.before_name = src.before_name,
    tgt.before_tts_name = src.before_tts_name,
    tgt.tts_name = src.tts_name
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    name,
    user_id,
    created_at,
    before_name,
    before_tts_name,
    tts_name
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.name,
    src.user_id,
    src.created_at,
    src.before_name,
    src.before_tts_name,
    src.tts_name
);
--Task再開
ALTER TASK LOG_USER_NAME_EDIT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_NAME_EDIT_TASK;

-- Query for LOG_TYPE = user:profile_edit

CREATE OR REPLACE STREAM LOG_USER_PROFILE_EDIT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_PROFILE_EDIT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_USER_PROFILE_EDIT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):url AS STRING) AS url,
        CAST(PARSE_JSON(LOG):name AS STRING) AS name,
        CAST(PARSE_JSON(LOG):message AS STRING) AS message,
        CAST(PARSE_JSON(LOG):user_id AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):modified_at AS NUMBER) AS modified_at,
        CAST(PARSE_JSON(LOG):tts_name AS STRING) AS tts_name,
        metadata$action
    FROM LOG_USER_PROFILE_EDIT_STERAM
    WHERE LOG_TYPE = 'user:profile_edit'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.url = src.url,
    tgt.name = src.name,
    tgt.message = src.message,
    tgt.user_id = src.user_id,
    tgt.modified_at = src.modified_at,
    tgt.tts_name = src.tts_name
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    url,
    name,
    message,
    user_id,
    modified_at,
    tts_name
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.url,
    src.name,
    src.message,
    src.user_id,
    src.modified_at,
    src.tts_name
);
--Task再開
ALTER TASK LOG_USER_PROFILE_EDIT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_PROFILE_EDIT_TASK;

