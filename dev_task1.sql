
{% if env == "PROD" %}
-- PRODの場合の処理
USE SCHEMA TTM_BABEL.BABEL_STG_PROD;
{% else %}
-- その他の場合の処理
USE SCHEMA TTM_BABEL.BABEL_STG_DEV;
{% endif %}
-- Query for LOG_TYPE = avatar:get

CREATE OR REPLACE STREAM LOG_AVATAR_GET_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_AVATAR_GET_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_AVATAR_GET AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"avatar_id" AS NUMBER) AS avatar_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"user_avatar_id" AS NUMBER) AS user_avatar_id,
        metadata$action
    FROM LOG_AVATAR_GET_STERAM
    WHERE LOG_TYPE = 'avatar:get'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.avatar_id = src.avatar_id,
    tgt.created_at = src.created_at,
    tgt.user_avatar_id = src.user_avatar_id
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    avatar_id,
    created_at,
    user_avatar_id
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.avatar_id,
    src.created_at,
    src.user_avatar_id
);
--Task再開
ALTER TASK LOG_AVATAR_GET_TASK RESUME;

--Task実行
EXECUTE TASK LOG_AVATAR_GET_TASK;

-- Query for LOG_TYPE = avatar:use

CREATE OR REPLACE STREAM LOG_AVATAR_USE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_AVATAR_USE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_AVATAR_USE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"avatar_id" AS NUMBER) AS avatar_id,
        CAST(PARSE_JSON(LOG):"use_count" AS NUMBER) AS use_count,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"user_avatar_id" AS NUMBER) AS user_avatar_id,
        metadata$action
    FROM LOG_AVATAR_USE_STERAM
    WHERE LOG_TYPE = 'avatar:use'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.avatar_id = src.avatar_id,
    tgt.use_count = src.use_count,
    tgt.created_at = src.created_at,
    tgt.user_avatar_id = src.user_avatar_id
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    avatar_id,
    use_count,
    created_at,
    user_avatar_id
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.avatar_id,
    src.use_count,
    src.created_at,
    src.user_avatar_id
);
--Task再開
ALTER TASK LOG_AVATAR_USE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_AVATAR_USE_TASK;

-- Query for LOG_TYPE = coupon:create

CREATE OR REPLACE STREAM LOG_COUPON_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_COUPON_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_COUPON_CREATE AS tgt
USING (
    SELECT DISTINCT 
        id,
        coupon.index AS idx,
        request_id,
        created,
        user_id AS LOGs_userId,
        coupon.value:"id"::NUMBER AS coupon_id,
        coupon.value:"code"::STRING AS coupon_code,
        coupon.value:"effect_type"::NUMBER AS coupon_effect_type,
        coupon.value:"effect_amount"::NUMBER AS coupon_effect_amount,
        CAST(PARSE_JSON(LOG):"code_type" AS NUMBER) AS code_type,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"coupon_group_id" AS NUMBER) AS coupon_group_id,
        metadata$action
    FROM LOG_COUPON_CREATE_STERAM,
        LATERAL FLATTEN(input => PARSE_JSON(LOG):"coupon") coupon
    WHERE LOG_TYPE = 'coupon:create'
) src
ON tgt.id = src.id
   AND tgt.idx = src.idx
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.coupon_id = src.coupon_id,
    tgt.coupon_code = src.coupon_code,
    tgt.coupon_effect_type = src.coupon_effect_type,
    tgt.coupon_effect_amount = src.coupon_effect_amount,
    tgt.code_type = src.code_type,
    tgt.created_at = src.created_at,
    tgt.coupon_group_id = src.coupon_group_id
WHEN NOT MATCHED THEN INSERT (
    id,
    idx,
    request_id,
    created,
    LOGs_userId,
    coupon_id,
    coupon_code,
    coupon_effect_type,
    coupon_effect_amount,
    code_type,
    created_at,
    coupon_group_id
) VALUES (
    src.id,
    src.idx,
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.coupon_id,
    src.coupon_code,
    src.coupon_effect_type,
    src.coupon_effect_amount,
    src.code_type,
    src.created_at,
    src.coupon_group_id
);
--Task再開
ALTER TASK LOG_COUPON_CREATE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_COUPON_CREATE_TASK;

-- Query for LOG_TYPE = coupon:use

CREATE OR REPLACE STREAM LOG_COUPON_USE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_COUPON_USE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_COUPON_USE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"coupon.id" AS NUMBER) AS coupon_id,
        CAST(PARSE_JSON(LOG):"coupon.code" AS STRING) AS coupon_code,
        CAST(PARSE_JSON(LOG):"coupon.effect_type" AS NUMBER) AS coupon_effect_type,
        CAST(PARSE_JSON(LOG):"coupon.effect_amount" AS NUMBER) AS coupon_effect_amount,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"code_type" AS NUMBER) AS code_type,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"user_coupon_id" AS NUMBER) AS user_coupon_id,
        CAST(PARSE_JSON(LOG):"coupon_group_id" AS NUMBER) AS coupon_group_id,
        metadata$action
    FROM LOG_COUPON_USE_STERAM
    WHERE LOG_TYPE = 'coupon:use'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.coupon_id = src.coupon_id,
    tgt.coupon_code = src.coupon_code,
    tgt.coupon_effect_type = src.coupon_effect_type,
    tgt.coupon_effect_amount = src.coupon_effect_amount,
    tgt.user_id = src.user_id,
    tgt.code_type = src.code_type,
    tgt.created_at = src.created_at,
    tgt.user_coupon_id = src.user_coupon_id,
    tgt.coupon_group_id = src.coupon_group_id
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    coupon_id,
    coupon_code,
    coupon_effect_type,
    coupon_effect_amount,
    user_id,
    code_type,
    created_at,
    user_coupon_id,
    coupon_group_id
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.coupon_id,
    src.coupon_code,
    src.coupon_effect_type,
    src.coupon_effect_amount,
    src.user_id,
    src.code_type,
    src.created_at,
    src.user_coupon_id,
    src.coupon_group_id
);
--Task再開
ALTER TASK LOG_COUPON_USE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_COUPON_USE_TASK;

-- Query for LOG_TYPE = currency:deposit

CREATE OR REPLACE STREAM LOG_CURRENCY_DEPOSIT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_CURRENCY_DEPOSIT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_CURRENCY_DEPOSIT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"currency.amount" AS NUMBER) AS currency_amount,
        CAST(PARSE_JSON(LOG):"currency.total_amount" AS NUMBER) AS currency_total_amount,
        CAST(PARSE_JSON(LOG):"currency.before_amount" AS NUMBER) AS currency_before_amount,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_CURRENCY_DEPOSIT_STERAM
    WHERE LOG_TYPE = 'currency:deposit'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.currency_amount = src.currency_amount,
    tgt.currency_total_amount = src.currency_total_amount,
    tgt.currency_before_amount = src.currency_before_amount,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    currency_amount,
    currency_total_amount,
    currency_before_amount,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.currency_amount,
    src.currency_total_amount,
    src.currency_before_amount,
    src.created_at
);
--Task再開
ALTER TASK LOG_CURRENCY_DEPOSIT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_CURRENCY_DEPOSIT_TASK;

-- Query for LOG_TYPE = currency:paid

CREATE OR REPLACE STREAM LOG_CURRENCY_PAID_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_CURRENCY_PAID_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_CURRENCY_PAID AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"currency.amount" AS NUMBER) AS currency_amount,
        CAST(PARSE_JSON(LOG):"currency.total_amount" AS NUMBER) AS currency_total_amount,
        CAST(PARSE_JSON(LOG):"currency.before_amount" AS NUMBER) AS currency_before_amount,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_CURRENCY_PAID_STERAM
    WHERE LOG_TYPE = 'currency:paid'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.currency_amount = src.currency_amount,
    tgt.currency_total_amount = src.currency_total_amount,
    tgt.currency_before_amount = src.currency_before_amount,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    currency_amount,
    currency_total_amount,
    currency_before_amount,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.currency_amount,
    src.currency_total_amount,
    src.currency_before_amount,
    src.created_at
);
--Task再開
ALTER TASK LOG_CURRENCY_PAID_TASK RESUME;

--Task実行
EXECUTE TASK LOG_CURRENCY_PAID_TASK;

-- Query for LOG_TYPE = currency_free:deposit

CREATE OR REPLACE STREAM LOG_CURRENCY_FREE_DEPOSIT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_CURRENCY_FREE_DEPOSIT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_CURRENCY_FREE_DEPOSIT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"currency.amount" AS NUMBER) AS currency_amount,
        CAST(PARSE_JSON(LOG):"currency.total_amount" AS NUMBER) AS currency_total_amount,
        CAST(PARSE_JSON(LOG):"currency.before_amount" AS NUMBER) AS currency_before_amount,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"currency_free.amount" AS NUMBER) AS currency_free_amount,
        CAST(PARSE_JSON(LOG):"currency_free.total_amount" AS NUMBER) AS currency_free_total_amount,
        CAST(PARSE_JSON(LOG):"currency_free.before_amount" AS NUMBER) AS currency_free_before_amount,
        metadata$action
    FROM LOG_CURRENCY_FREE_DEPOSIT_STERAM
    WHERE LOG_TYPE = 'currency_free:deposit'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.currency_amount = src.currency_amount,
    tgt.currency_total_amount = src.currency_total_amount,
    tgt.currency_before_amount = src.currency_before_amount,
    tgt.created_at = src.created_at,
    tgt.currency_free_amount = src.currency_free_amount,
    tgt.currency_free_total_amount = src.currency_free_total_amount,
    tgt.currency_free_before_amount = src.currency_free_before_amount
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    currency_amount,
    currency_total_amount,
    currency_before_amount,
    created_at,
    currency_free_amount,
    currency_free_total_amount,
    currency_free_before_amount
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.currency_amount,
    src.currency_total_amount,
    src.currency_before_amount,
    src.created_at,
    src.currency_free_amount,
    src.currency_free_total_amount,
    src.currency_free_before_amount
);
--Task再開
ALTER TASK LOG_CURRENCY_FREE_DEPOSIT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_CURRENCY_FREE_DEPOSIT_TASK;

-- Query for LOG_TYPE = currency_free:paid

CREATE OR REPLACE STREAM LOG_CURRENCY_FREE_PAID_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_CURRENCY_FREE_PAID_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_CURRENCY_FREE_PAID AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"currency.amount" AS NUMBER) AS currency_amount,
        CAST(PARSE_JSON(LOG):"currency.total_amount" AS NUMBER) AS currency_total_amount,
        CAST(PARSE_JSON(LOG):"currency.before_amount" AS NUMBER) AS currency_before_amount,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"currency_free.amount" AS NUMBER) AS currency_free_amount,
        CAST(PARSE_JSON(LOG):"currency_free.total_amount" AS NUMBER) AS currency_free_total_amount,
        CAST(PARSE_JSON(LOG):"currency_free.before_amount" AS NUMBER) AS currency_free_before_amount,
        metadata$action
    FROM LOG_CURRENCY_FREE_PAID_STERAM
    WHERE LOG_TYPE = 'currency_free:paid'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.currency_amount = src.currency_amount,
    tgt.currency_total_amount = src.currency_total_amount,
    tgt.currency_before_amount = src.currency_before_amount,
    tgt.created_at = src.created_at,
    tgt.currency_free_amount = src.currency_free_amount,
    tgt.currency_free_total_amount = src.currency_free_total_amount,
    tgt.currency_free_before_amount = src.currency_free_before_amount
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    currency_amount,
    currency_total_amount,
    currency_before_amount,
    created_at,
    currency_free_amount,
    currency_free_total_amount,
    currency_free_before_amount
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.currency_amount,
    src.currency_total_amount,
    src.currency_before_amount,
    src.created_at,
    src.currency_free_amount,
    src.currency_free_total_amount,
    src.currency_free_before_amount
);
--Task再開
ALTER TASK LOG_CURRENCY_FREE_PAID_TASK RESUME;

--Task実行
EXECUTE TASK LOG_CURRENCY_FREE_PAID_TASK;

-- Query for LOG_TYPE = demographic:group

CREATE OR REPLACE STREAM LOG_DEMOGRAPHIC_GROUP_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_DEMOGRAPHIC_GROUP_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_DEMOGRAPHIC_GROUP AS tgt
USING (
    SELECT DISTINCT 
        id,
        users.index AS idx,
        request_id,
        created,
        user_id AS LOGs_userId,
        users.value:"user_id"::STRING AS users_user_id,
        CAST(PARSE_JSON(LOG):"party_id" AS STRING) AS party_id,
        CAST(PARSE_JSON(LOG):"created_at" AS FLOAT) AS created_at,
        CAST(PARSE_JSON(LOG):"customer_base" AS NUMBER) AS customer_base,
        metadata$action
    FROM LOG_DEMOGRAPHIC_GROUP_STERAM,
        LATERAL FLATTEN(input => PARSE_JSON(LOG):"users") users
    WHERE LOG_TYPE = 'demographic:group'
) src
ON tgt.id = src.id
   AND tgt.idx = src.idx
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.users_user_id = src.users_user_id,
    tgt.party_id = src.party_id,
    tgt.created_at = src.created_at,
    tgt.customer_base = src.customer_base
WHEN NOT MATCHED THEN INSERT (
    id,
    idx,
    request_id,
    created,
    LOGs_userId,
    users_user_id,
    party_id,
    created_at,
    customer_base
) VALUES (
    src.id,
    src.idx,
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.users_user_id,
    src.party_id,
    src.created_at,
    src.customer_base
);
--Task再開
ALTER TASK LOG_DEMOGRAPHIC_GROUP_TASK RESUME;

--Task実行
EXECUTE TASK LOG_DEMOGRAPHIC_GROUP_TASK;

-- Query for LOG_TYPE = demographic:user

CREATE OR REPLACE STREAM LOG_DEMOGRAPHIC_USER_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_DEMOGRAPHIC_USER_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_DEMOGRAPHIC_USER AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"age" AS NUMBER) AS age,
        CAST(PARSE_JSON(LOG):"sex" AS NUMBER) AS sex,
        CAST(PARSE_JSON(LOG):"region" AS NUMBER) AS region,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"created_at" AS FLOAT) AS created_at,
        metadata$action
    FROM LOG_DEMOGRAPHIC_USER_STERAM
    WHERE LOG_TYPE = 'demographic:user'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.age = src.age,
    tgt.sex = src.sex,
    tgt.region = src.region,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    age,
    sex,
    region,
    user_id,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.age,
    src.sex,
    src.region,
    src.user_id,
    src.created_at
);
--Task再開
ALTER TASK LOG_DEMOGRAPHIC_USER_TASK RESUME;

--Task実行
EXECUTE TASK LOG_DEMOGRAPHIC_USER_TASK;

-- Query for LOG_TYPE = gacha:draw

CREATE OR REPLACE STREAM LOG_GACHA_DRAW_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_GACHA_DRAW_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_GACHA_DRAW AS tgt
USING (
    SELECT DISTINCT 
        id,
        items.index AS idx,
        request_id,
        created,
        user_id AS LOGs_userId,
        items.value:"is_new"::BOOLEAN AS items_is_new,
        items.value:"item_id"::NUMBER AS items_item_id,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"draw_num" AS NUMBER) AS draw_num,
        CAST(PARSE_JSON(LOG):"gacha_id" AS NUMBER) AS gacha_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        items.value:"id"::NUMBER AS items_id,
        items.value:"amount"::NUMBER AS items_amount,
        items.value:"item_type"::STRING AS items_item_type,
        metadata$action
    FROM LOG_GACHA_DRAW_STERAM,
        LATERAL FLATTEN(input => PARSE_JSON(LOG):"items") items
    WHERE LOG_TYPE = 'gacha:draw'
) src
ON tgt.id = src.id
   AND tgt.idx = src.idx
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.items_is_new = src.items_is_new,
    tgt.items_item_id = src.items_item_id,
    tgt.user_id = src.user_id,
    tgt.draw_num = src.draw_num,
    tgt.gacha_id = src.gacha_id,
    tgt.created_at = src.created_at,
    tgt.items_id = src.items_id,
    tgt.items_amount = src.items_amount,
    tgt.items_item_type = src.items_item_type
WHEN NOT MATCHED THEN INSERT (
    id,
    idx,
    request_id,
    created,
    LOGs_userId,
    items_is_new,
    items_item_id,
    user_id,
    draw_num,
    gacha_id,
    created_at,
    items_id,
    items_amount,
    items_item_type
) VALUES (
    src.id,
    src.idx,
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.items_is_new,
    src.items_item_id,
    src.user_id,
    src.draw_num,
    src.gacha_id,
    src.created_at,
    src.items_id,
    src.items_amount,
    src.items_item_type
);
--Task再開
ALTER TASK LOG_GACHA_DRAW_TASK RESUME;

--Task実行
EXECUTE TASK LOG_GACHA_DRAW_TASK;

-- Query for LOG_TYPE = gold:add

CREATE OR REPLACE STREAM LOG_GOLD_ADD_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_GOLD_ADD_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_GOLD_ADD AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"amount" AS NUMBER) AS amount,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_GOLD_ADD_STERAM
    WHERE LOG_TYPE = 'gold:add'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.amount = src.amount,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    amount,
    user_id,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.amount,
    src.user_id,
    src.created_at
);
--Task再開
ALTER TASK LOG_GOLD_ADD_TASK RESUME;

--Task実行
EXECUTE TASK LOG_GOLD_ADD_TASK;

-- Query for LOG_TYPE = gold:sub

CREATE OR REPLACE STREAM LOG_GOLD_SUB_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_GOLD_SUB_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_GOLD_SUB AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"amount" AS NUMBER) AS amount,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_GOLD_SUB_STERAM
    WHERE LOG_TYPE = 'gold:sub'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.amount = src.amount,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    amount,
    user_id,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.amount,
    src.user_id,
    src.created_at
);
--Task再開
ALTER TASK LOG_GOLD_SUB_TASK RESUME;

--Task実行
EXECUTE TASK LOG_GOLD_SUB_TASK;

-- Query for LOG_TYPE = inbox:get

CREATE OR REPLACE STREAM LOG_INBOX_GET_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_INBOX_GET_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_INBOX_GET AS tgt
USING (
    SELECT DISTINCT 
        id,
        items.index AS idx,
        request_id,
        created,
        user_id AS LOGs_userId,
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
        CAST(PARSE_JSON(LOG):"user_message_id" AS NUMBER) AS user_message_id,
        metadata$action
    FROM LOG_INBOX_GET_STERAM,
        LATERAL FLATTEN(input => PARSE_JSON(LOG):"items") items
    WHERE LOG_TYPE = 'inbox:get'
) src
ON tgt.id = src.id
   AND tgt.idx = src.idx
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.items_id = src.items_id,
    tgt.items_amount = src.items_amount,
    tgt.items_item_id = src.items_item_id,
    tgt.items_item_type = src.items_item_type,
    tgt.is_read = src.is_read,
    tgt.read_at = src.read_at,
    tgt.user_id = src.user_id,
    tgt.expire_at = src.expire_at,
    tgt.created_at = src.created_at,
    tgt.message_id = src.message_id,
    tgt.user_message_id = src.user_message_id
WHEN NOT MATCHED THEN INSERT (
    id,
    idx,
    request_id,
    created,
    LOGs_userId,
    items_id,
    items_amount,
    items_item_id,
    items_item_type,
    is_read,
    read_at,
    user_id,
    expire_at,
    created_at,
    message_id,
    user_message_id
) VALUES (
    src.id,
    src.idx,
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.items_id,
    src.items_amount,
    src.items_item_id,
    src.items_item_type,
    src.is_read,
    src.read_at,
    src.user_id,
    src.expire_at,
    src.created_at,
    src.message_id,
    src.user_message_id
);
--Task再開
ALTER TASK LOG_INBOX_GET_TASK RESUME;

--Task実行
EXECUTE TASK LOG_INBOX_GET_TASK;

-- Query for LOG_TYPE = item:get

CREATE OR REPLACE STREAM LOG_ITEM_GET_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_ITEM_GET_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_ITEM_GET AS tgt
USING (
    SELECT DISTINCT 
        id,
        items.index AS idx,
        request_id,
        created,
        user_id AS LOGs_userId,
        items.value:"id"::NUMBER AS items_id,
        items.value:"amount"::NUMBER AS items_amount,
        items.value:"item_id"::STRING AS items_item_id,
        items.value:"item_type"::STRING AS items_item_type,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_ITEM_GET_STERAM,
        LATERAL FLATTEN(input => PARSE_JSON(LOG):"items") items
    WHERE LOG_TYPE = 'item:get'
) src
ON tgt.id = src.id
   AND tgt.idx = src.idx
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.items_id = src.items_id,
    tgt.items_amount = src.items_amount,
    tgt.items_item_id = src.items_item_id,
    tgt.items_item_type = src.items_item_type,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    idx,
    request_id,
    created,
    LOGs_userId,
    items_id,
    items_amount,
    items_item_id,
    items_item_type,
    user_id,
    created_at
) VALUES (
    src.id,
    src.idx,
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.items_id,
    src.items_amount,
    src.items_item_id,
    src.items_item_type,
    src.user_id,
    src.created_at
);
--Task再開
ALTER TASK LOG_ITEM_GET_TASK RESUME;

--Task実行
EXECUTE TASK LOG_ITEM_GET_TASK;

-- Query for LOG_TYPE = item:select

CREATE OR REPLACE STREAM LOG_ITEM_SELECT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_ITEM_SELECT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_ITEM_SELECT AS tgt
USING (
    SELECT DISTINCT 
        id,
        items.index AS idx,
        request_id,
        created,
        user_id AS LOGs_userId,
        items.value:"id"::NUMBER AS items_id,
        items.value:"item_id"::STRING AS items_item_id,
        items.value:"item_type"::STRING AS items_item_type,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_ITEM_SELECT_STERAM,
        LATERAL FLATTEN(input => PARSE_JSON(LOG):"items") items
    WHERE LOG_TYPE = 'item:select'
) src
ON tgt.id = src.id
   AND tgt.idx = src.idx
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.items_id = src.items_id,
    tgt.items_item_id = src.items_item_id,
    tgt.items_item_type = src.items_item_type,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    idx,
    request_id,
    created,
    LOGs_userId,
    items_id,
    items_item_id,
    items_item_type,
    user_id,
    created_at
) VALUES (
    src.id,
    src.idx,
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.items_id,
    src.items_item_id,
    src.items_item_type,
    src.user_id,
    src.created_at
);
--Task再開
ALTER TASK LOG_ITEM_SELECT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_ITEM_SELECT_TASK;

-- Query for LOG_TYPE = item:use

CREATE OR REPLACE STREAM LOG_ITEM_USE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_ITEM_USE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_ITEM_USE AS tgt
USING (
    SELECT DISTINCT 
        id,
        items.index AS idx,
        request_id,
        created,
        user_id AS LOGs_userId,
        items.value:"id"::NUMBER AS items_id,
        items.value:"amount"::NUMBER AS items_amount,
        items.value:"item_id"::STRING AS items_item_id,
        items.value:"item_type"::STRING AS items_item_type,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_ITEM_USE_STERAM,
        LATERAL FLATTEN(input => PARSE_JSON(LOG):"items") items
    WHERE LOG_TYPE = 'item:use'
) src
ON tgt.id = src.id
   AND tgt.idx = src.idx
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.items_id = src.items_id,
    tgt.items_amount = src.items_amount,
    tgt.items_item_id = src.items_item_id,
    tgt.items_item_type = src.items_item_type,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    idx,
    request_id,
    created,
    LOGs_userId,
    items_id,
    items_amount,
    items_item_id,
    items_item_type,
    user_id,
    created_at
) VALUES (
    src.id,
    src.idx,
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.items_id,
    src.items_amount,
    src.items_item_id,
    src.items_item_type,
    src.user_id,
    src.created_at
);
--Task再開
ALTER TASK LOG_ITEM_USE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_ITEM_USE_TASK;

-- Query for LOG_TYPE = ofp:playResults

CREATE OR REPLACE STREAM OFP_PLAYRESULTS_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK OFP_PLAYRESULTS_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO OFP_PLAYRESULTS AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
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
        PARSE_JSON(LOG):"onpInfo_blacklist" AS onpInfo_blacklist,
        CAST(PARSE_JSON(LOG):"onpInfo.partyCode" AS STRING) AS onpInfo_partyCode,
        CAST(PARSE_JSON(LOG):"onpInfo.dataVersion" AS STRING) AS onpInfo_dataVersion,
        CAST(PARSE_JSON(LOG):"onpInfo.isTransform" AS BOOLEAN) AS onpInfo_isTransform,
        CAST(PARSE_JSON(LOG):"onpInfo.isRankedParty" AS BOOLEAN) AS onpInfo_isRankedParty,
        CAST(PARSE_JSON(LOG):"onpInfo.partyCategory" AS NUMBER) AS onpInfo_partyCategory,
        CAST(PARSE_JSON(LOG):"onpInfo.hasNextSession" AS BOOLEAN) AS onpInfo_hasNextSession,
        PARSE_JSON(LOG):"players" AS players,
        PARSE_JSON(LOG):"players_onpInfo" AS players_onpInfo,
        PARSE_JSON(LOG):"players_onpInfo_avatarParams" AS players_onpInfo_avatarParams,
        CAST(PARSE_JSON(LOG):"questId" AS STRING) AS questId,
        CAST(PARSE_JSON(LOG):"version" AS STRING) AS version,
        CAST(PARSE_JSON(LOG):"laevedAt" AS NUMBER) AS laevedAt,
        PARSE_JSON(LOG):"ofpState" AS ofpState,
        -- ofpState: __EMPTY__ --,
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
        PARSE_JSON(LOG):"ofpState_uses" AS ofpState_uses,
        CAST(PARSE_JSON(LOG):"ofpState.result.A" AS NUMBER) AS ofpState_result_A,
        CAST(PARSE_JSON(LOG):"ofpState.result.B" AS NUMBER) AS ofpState_result_B,
        CAST(PARSE_JSON(LOG):"ofpState.result.C" AS NUMBER) AS ofpState_result_C,
        CAST(PARSE_JSON(LOG):"ofpState.result.D" AS NUMBER) AS ofpState_result_D,
        CAST(PARSE_JSON(LOG):"ofpState.result.E" AS NUMBER) AS ofpState_result_E,
        CAST(PARSE_JSON(LOG):"ofpState.result.rate" AS STRING) AS ofpState_result_rate,
        CAST(PARSE_JSON(LOG):"ofpState.result.score" AS NUMBER) AS ofpState_result_score,
        CAST(PARSE_JSON(LOG):"ofpState.result.reachedWave" AS NUMBER) AS ofpState_result_reachedWave,
        CAST(PARSE_JSON(LOG):"ofpState.result.bossDefeated" AS BOOLEAN) AS ofpState_result_bossDefeated,
        PARSE_JSON(LOG):"ofpState_result_playerResults" AS ofpState_result_playerResults,
        CAST(PARSE_JSON(LOG):"ofpState.result.reachedWaveAchievementRate" AS NUMBER) AS ofpState_result_reachedWaveAchievementRate,
        PARSE_JSON(LOG):"ofpState_players" AS ofpState_players,
        CAST(PARSE_JSON(LOG):"ofpState.fieldLevel" AS NUMBER) AS ofpState_fieldLevel,
        CAST(PARSE_JSON(LOG):"ofpState.dataVersion" AS STRING) AS ofpState_dataVersion,
        CAST(PARSE_JSON(LOG):"fieldId" AS STRING) AS fieldId,
        CAST(PARSE_JSON(LOG):"onpInfo.fieldLevel" AS NUMBER) AS onpInfo_fieldLevel,
        CAST(PARSE_JSON(LOG):"leavedAt" AS NUMBER) AS leavedAt,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        PARSE_JSON(LOG):"onpInfo_behaviours" AS onpInfo_behaviours,
        PARSE_JSON(LOG):"onpInfo_behaviours_target" AS onpInfo_behaviours_target,
        metadata$action
    FROM OFP_PLAYRESULTS_STERAM
    WHERE LOG_TYPE = 'ofp:playResults'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.rate = src.rate,
    tgt.type = src.type,
    tgt.clear = src.clear,
    tgt.items = src.items,
    tgt.items_onpInfo_effects = src.items_onpInfo_effects,
    tgt.level = src.level,
    tgt.score = src.score,
    tgt.state = src.state,
    tgt.areaId = src.areaId,
    tgt.endedAt = src.endedAt,
    tgt.onpInfo_language = src.onpInfo_language,
    tgt.onpInfo_leaderId = src.onpInfo_leaderId,
    tgt.onpInfo_blacklist = src.onpInfo_blacklist,
    tgt.onpInfo_partyCode = src.onpInfo_partyCode,
    tgt.onpInfo_dataVersion = src.onpInfo_dataVersion,
    tgt.onpInfo_isTransform = src.onpInfo_isTransform,
    tgt.onpInfo_isRankedParty = src.onpInfo_isRankedParty,
    tgt.onpInfo_partyCategory = src.onpInfo_partyCategory,
    tgt.onpInfo_hasNextSession = src.onpInfo_hasNextSession,
    tgt.players = src.players,
    tgt.players_onpInfo = src.players_onpInfo,
    tgt.players_onpInfo_avatarParams = src.players_onpInfo_avatarParams,
    tgt.questId = src.questId,
    tgt.version = src.version,
    tgt.laevedAt = src.laevedAt,
    tgt.ofpState = src.ofpState,
    tgt.totalExp = src.totalExp,
    tgt.enteredAt = src.enteredAt,
    tgt.sessionId = src.sessionId,
    tgt.startedAt = src.startedAt,
    tgt.totalGold = src.totalGold,
    tgt.dataVersion = src.dataVersion,
    tgt.totalExtraExp = src.totalExtraExp,
    tgt.totalExtraGold = src.totalExtraGold,
    tgt.numberOfPlayers = src.numberOfPlayers,
    tgt.totalPlayerLevel = src.totalPlayerLevel,
    tgt.ofpState_uses = src.ofpState_uses,
    tgt.ofpState_result_A = src.ofpState_result_A,
    tgt.ofpState_result_B = src.ofpState_result_B,
    tgt.ofpState_result_C = src.ofpState_result_C,
    tgt.ofpState_result_D = src.ofpState_result_D,
    tgt.ofpState_result_E = src.ofpState_result_E,
    tgt.ofpState_result_rate = src.ofpState_result_rate,
    tgt.ofpState_result_score = src.ofpState_result_score,
    tgt.ofpState_result_reachedWave = src.ofpState_result_reachedWave,
    tgt.ofpState_result_bossDefeated = src.ofpState_result_bossDefeated,
    tgt.ofpState_result_playerResults = src.ofpState_result_playerResults,
    tgt.ofpState_result_reachedWaveAchievementRate = src.ofpState_result_reachedWaveAchievementRate,
    tgt.ofpState_players = src.ofpState_players,
    tgt.ofpState_fieldLevel = src.ofpState_fieldLevel,
    tgt.ofpState_dataVersion = src.ofpState_dataVersion,
    tgt.fieldId = src.fieldId,
    tgt.onpInfo_fieldLevel = src.onpInfo_fieldLevel,
    tgt.leavedAt = src.leavedAt,
    tgt.created_at = src.created_at,
    tgt.onpInfo_behaviours = src.onpInfo_behaviours,
    tgt.onpInfo_behaviours_target = src.onpInfo_behaviours_target
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    rate,
    type,
    clear,
    items,
    items_onpInfo_effects,
    level,
    score,
    state,
    areaId,
    endedAt,
    onpInfo_language,
    onpInfo_leaderId,
    onpInfo_blacklist,
    onpInfo_partyCode,
    onpInfo_dataVersion,
    onpInfo_isTransform,
    onpInfo_isRankedParty,
    onpInfo_partyCategory,
    onpInfo_hasNextSession,
    players,
    players_onpInfo,
    players_onpInfo_avatarParams,
    questId,
    version,
    laevedAt,
    ofpState,
    totalExp,
    enteredAt,
    sessionId,
    startedAt,
    totalGold,
    dataVersion,
    totalExtraExp,
    totalExtraGold,
    numberOfPlayers,
    totalPlayerLevel,
    ofpState_uses,
    ofpState_result_A,
    ofpState_result_B,
    ofpState_result_C,
    ofpState_result_D,
    ofpState_result_E,
    ofpState_result_rate,
    ofpState_result_score,
    ofpState_result_reachedWave,
    ofpState_result_bossDefeated,
    ofpState_result_playerResults,
    ofpState_result_reachedWaveAchievementRate,
    ofpState_players,
    ofpState_fieldLevel,
    ofpState_dataVersion,
    fieldId,
    onpInfo_fieldLevel,
    leavedAt,
    created_at,
    onpInfo_behaviours,
    onpInfo_behaviours_target
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.rate,
    src.type,
    src.clear,
    src.items,
    src.items_onpInfo_effects,
    src.level,
    src.score,
    src.state,
    src.areaId,
    src.endedAt,
    src.onpInfo_language,
    src.onpInfo_leaderId,
    src.onpInfo_blacklist,
    src.onpInfo_partyCode,
    src.onpInfo_dataVersion,
    src.onpInfo_isTransform,
    src.onpInfo_isRankedParty,
    src.onpInfo_partyCategory,
    src.onpInfo_hasNextSession,
    src.players,
    src.players_onpInfo,
    src.players_onpInfo_avatarParams,
    src.questId,
    src.version,
    src.laevedAt,
    src.ofpState,
    src.totalExp,
    src.enteredAt,
    src.sessionId,
    src.startedAt,
    src.totalGold,
    src.dataVersion,
    src.totalExtraExp,
    src.totalExtraGold,
    src.numberOfPlayers,
    src.totalPlayerLevel,
    src.ofpState_uses,
    src.ofpState_result_A,
    src.ofpState_result_B,
    src.ofpState_result_C,
    src.ofpState_result_D,
    src.ofpState_result_E,
    src.ofpState_result_rate,
    src.ofpState_result_score,
    src.ofpState_result_reachedWave,
    src.ofpState_result_bossDefeated,
    src.ofpState_result_playerResults,
    src.ofpState_result_reachedWaveAchievementRate,
    src.ofpState_players,
    src.ofpState_fieldLevel,
    src.ofpState_dataVersion,
    src.fieldId,
    src.onpInfo_fieldLevel,
    src.leavedAt,
    src.created_at,
    src.onpInfo_behaviours,
    src.onpInfo_behaviours_target
);
--Task再開
ALTER TASK OFP_PLAYRESULTS_TASK RESUME;

--Task実行
EXECUTE TASK OFP_PLAYRESULTS_TASK;

-- Query for LOG_TYPE = party:create

CREATE OR REPLACE STREAM LOG_PARTY_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_PARTY_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_PARTY_CREATE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"party_id" AS STRING) AS party_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_PARTY_CREATE_STERAM
    WHERE LOG_TYPE = 'party:create'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.party_id = src.party_id,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    party_id,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.party_id,
    src.created_at
);
--Task再開
ALTER TASK LOG_PARTY_CREATE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_PARTY_CREATE_TASK;

-- Query for LOG_TYPE = party:delete

CREATE OR REPLACE STREAM LOG_PARTY_DELETE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_PARTY_DELETE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_PARTY_DELETE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"party_id" AS STRING) AS party_id,
        CAST(PARSE_JSON(LOG):"deleted_at" AS NUMBER) AS deleted_at,
        metadata$action
    FROM LOG_PARTY_DELETE_STERAM
    WHERE LOG_TYPE = 'party:delete'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.party_id = src.party_id,
    tgt.deleted_at = src.deleted_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    party_id,
    deleted_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.party_id,
    src.deleted_at
);
--Task再開
ALTER TASK LOG_PARTY_DELETE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_PARTY_DELETE_TASK;

-- Query for LOG_TYPE = party:name_create

CREATE OR REPLACE STREAM LOG_PARTY_NAME_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_PARTY_NAME_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_PARTY_NAME_CREATE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"party_id" AS STRING) AS party_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"party_name" AS STRING) AS party_name,
        CAST(PARSE_JSON(LOG):"party_tts_name" AS STRING) AS party_tts_name,
        metadata$action
    FROM LOG_PARTY_NAME_CREATE_STERAM
    WHERE LOG_TYPE = 'party:name_create'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.party_id = src.party_id,
    tgt.created_at = src.created_at,
    tgt.party_name = src.party_name,
    tgt.party_tts_name = src.party_tts_name
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    party_id,
    created_at,
    party_name,
    party_tts_name
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.party_id,
    src.created_at,
    src.party_name,
    src.party_tts_name
);
--Task再開
ALTER TASK LOG_PARTY_NAME_CREATE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_PARTY_NAME_CREATE_TASK;

-- Query for LOG_TYPE = party:name_edit

CREATE OR REPLACE STREAM LOG_PARTY_NAME_EDIT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_PARTY_NAME_EDIT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_PARTY_NAME_EDIT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"party_id" AS STRING) AS party_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"party_name" AS STRING) AS party_name,
        CAST(PARSE_JSON(LOG):"before_name" AS STRING) AS before_name,
        CAST(PARSE_JSON(LOG):"party_tts_name" AS STRING) AS party_tts_name,
        CAST(PARSE_JSON(LOG):"before_tts_name" AS STRING) AS before_tts_name,
        CAST(PARSE_JSON(LOG):"before_party_name" AS STRING) AS before_party_name,
        CAST(PARSE_JSON(LOG):"before_party_tts_name" AS STRING) AS before_party_tts_name,
        metadata$action
    FROM LOG_PARTY_NAME_EDIT_STERAM
    WHERE LOG_TYPE = 'party:name_edit'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.party_id = src.party_id,
    tgt.created_at = src.created_at,
    tgt.party_name = src.party_name,
    tgt.before_name = src.before_name,
    tgt.party_tts_name = src.party_tts_name,
    tgt.before_tts_name = src.before_tts_name,
    tgt.before_party_name = src.before_party_name,
    tgt.before_party_tts_name = src.before_party_tts_name
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    party_id,
    created_at,
    party_name,
    before_name,
    party_tts_name,
    before_tts_name,
    before_party_name,
    before_party_tts_name
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.party_id,
    src.created_at,
    src.party_name,
    src.before_name,
    src.party_tts_name,
    src.before_tts_name,
    src.before_party_name,
    src.before_party_tts_name
);
--Task再開
ALTER TASK LOG_PARTY_NAME_EDIT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_PARTY_NAME_EDIT_TASK;

-- Query for LOG_TYPE = party:select

CREATE OR REPLACE STREAM LOG_PARTY_SELECT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_PARTY_SELECT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_PARTY_SELECT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"party_id" AS STRING) AS party_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"party_code" AS STRING) AS party_code,
        metadata$action
    FROM LOG_PARTY_SELECT_STERAM
    WHERE LOG_TYPE = 'party:select'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.party_id = src.party_id,
    tgt.created_at = src.created_at,
    tgt.party_code = src.party_code
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    party_id,
    created_at,
    party_code
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.party_id,
    src.created_at,
    src.party_code
);
--Task再開
ALTER TASK LOG_PARTY_SELECT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_PARTY_SELECT_TASK;

-- Query for LOG_TYPE = payment:purchase

CREATE OR REPLACE STREAM LOG_PAYMENT_PURCHASE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_PAYMENT_PURCHASE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_PAYMENT_PURCHASE AS tgt
USING (
    SELECT DISTINCT 
        id,
        acquired.index AS idx,
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"paid.cash.amount" AS NUMBER) AS paid_cash_amount,
        CAST(PARSE_JSON(LOG):"paid.topolo.amount" AS NUMBER) AS paid_topolo_amount,
        CAST(PARSE_JSON(LOG):"paid.topolo.total_amount" AS NUMBER) AS paid_topolo_total_amount,
        CAST(PARSE_JSON(LOG):"paid.topolo.before_amount" AS NUMBER) AS paid_topolo_before_amount,
        CAST(PARSE_JSON(LOG):"paid.coupon_info.coupon.id" AS NUMBER) AS paid_coupon_info_coupon_id,
        CAST(PARSE_JSON(LOG):"paid.coupon_info.coupon.code" AS STRING) AS paid_coupon_info_coupon_code,
        CAST(PARSE_JSON(LOG):"paid.coupon_info.coupon.effect_type" AS NUMBER) AS paid_coupon_info_coupon_effect_type,
        CAST(PARSE_JSON(LOG):"paid.coupon_info.coupon.effect_amount" AS NUMBER) AS paid_coupon_info_coupon_effect_amount,
        CAST(PARSE_JSON(LOG):"paid.coupon_info.code_type" AS NUMBER) AS paid_coupon_info_code_type,
        CAST(PARSE_JSON(LOG):"paid.coupon_info.user_coupon_id" AS STRING) AS paid_coupon_info_user_coupon_id,
        CAST(PARSE_JSON(LOG):"paid.coupon_info.coupon_group_id" AS NUMBER) AS paid_coupon_info_coupon_group_id,
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
        metadata$action
    FROM LOG_PAYMENT_PURCHASE_STERAM,
        LATERAL FLATTEN(input => PARSE_JSON(LOG):"acquired") acquired
    WHERE LOG_TYPE = 'payment:purchase'
) src
ON tgt.id = src.id
   AND tgt.idx = src.idx
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.paid_cash_amount = src.paid_cash_amount,
    tgt.paid_topolo_amount = src.paid_topolo_amount,
    tgt.paid_topolo_total_amount = src.paid_topolo_total_amount,
    tgt.paid_topolo_before_amount = src.paid_topolo_before_amount,
    tgt.paid_coupon_info_coupon_id = src.paid_coupon_info_coupon_id,
    tgt.paid_coupon_info_coupon_code = src.paid_coupon_info_coupon_code,
    tgt.paid_coupon_info_coupon_effect_type = src.paid_coupon_info_coupon_effect_type,
    tgt.paid_coupon_info_coupon_effect_amount = src.paid_coupon_info_coupon_effect_amount,
    tgt.paid_coupon_info_code_type = src.paid_coupon_info_code_type,
    tgt.paid_coupon_info_user_coupon_id = src.paid_coupon_info_user_coupon_id,
    tgt.paid_coupon_info_coupon_group_id = src.paid_coupon_info_coupon_group_id,
    tgt.paid_topolo_free_amount = src.paid_topolo_free_amount,
    tgt.paid_topolo_free_total_amount = src.paid_topolo_free_total_amount,
    tgt.paid_topolo_free_before_amount = src.paid_topolo_free_before_amount,
    tgt.user_id = src.user_id,
    tgt.acquired_id = src.acquired_id,
    tgt.acquired_num = src.acquired_num,
    tgt.acquired_item_id = src.acquired_item_id,
    tgt.acquired_item_type = src.acquired_item_type,
    tgt.acquired_user_related_id = src.acquired_user_related_id,
    tgt.created_at = src.created_at,
    tgt.gacha_draw_num = src.gacha_draw_num,
    tgt.gacha_gacha_id = src.gacha_gacha_id
WHEN NOT MATCHED THEN INSERT (
    id,
    idx,
    request_id,
    created,
    LOGs_userId,
    paid_cash_amount,
    paid_topolo_amount,
    paid_topolo_total_amount,
    paid_topolo_before_amount,
    paid_coupon_info_coupon_id,
    paid_coupon_info_coupon_code,
    paid_coupon_info_coupon_effect_type,
    paid_coupon_info_coupon_effect_amount,
    paid_coupon_info_code_type,
    paid_coupon_info_user_coupon_id,
    paid_coupon_info_coupon_group_id,
    paid_topolo_free_amount,
    paid_topolo_free_total_amount,
    paid_topolo_free_before_amount,
    user_id,
    acquired_id,
    acquired_num,
    acquired_item_id,
    acquired_item_type,
    acquired_user_related_id,
    created_at,
    gacha_draw_num,
    gacha_gacha_id
) VALUES (
    src.id,
    src.idx,
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.paid_cash_amount,
    src.paid_topolo_amount,
    src.paid_topolo_total_amount,
    src.paid_topolo_before_amount,
    src.paid_coupon_info_coupon_id,
    src.paid_coupon_info_coupon_code,
    src.paid_coupon_info_coupon_effect_type,
    src.paid_coupon_info_coupon_effect_amount,
    src.paid_coupon_info_code_type,
    src.paid_coupon_info_user_coupon_id,
    src.paid_coupon_info_coupon_group_id,
    src.paid_topolo_free_amount,
    src.paid_topolo_free_total_amount,
    src.paid_topolo_free_before_amount,
    src.user_id,
    src.acquired_id,
    src.acquired_num,
    src.acquired_item_id,
    src.acquired_item_type,
    src.acquired_user_related_id,
    src.created_at,
    src.gacha_draw_num,
    src.gacha_gacha_id
);
--Task再開
ALTER TASK LOG_PAYMENT_PURCHASE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_PAYMENT_PURCHASE_TASK;

-- Query for LOG_TYPE = payment:q5_pay

CREATE OR REPLACE STREAM LOG_PAYMENT_Q5_PAY_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_PAYMENT_Q5_PAY_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_PAYMENT_Q5_PAY AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"paid" AS NUMBER) AS paid,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_PAYMENT_Q5_PAY_STERAM
    WHERE LOG_TYPE = 'payment:q5_pay'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.paid = src.paid,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    paid,
    user_id,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.paid,
    src.user_id,
    src.created_at
);
--Task再開
ALTER TASK LOG_PAYMENT_Q5_PAY_TASK RESUME;

--Task実行
EXECUTE TASK LOG_PAYMENT_Q5_PAY_TASK;

-- Query for LOG_TYPE = payment:stripe_paymentIntent

CREATE OR REPLACE STREAM LOG_PAYMENT_STRIPE_PAYMENTINTENT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_PAYMENT_STRIPE_PAYMENTINTENT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_PAYMENT_STRIPE_PAYMENTINTENT AS tgt
USING (
    SELECT DISTINCT 
        id,
        response_payment_method_types.index AS idx,
        request_id,
        created,
        user_id AS LOGs_userId,
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
        CAST(PARSE_JSON(LOG):"payment_id" AS STRING) AS payment_id,
        metadata$action
    FROM LOG_PAYMENT_STRIPE_PAYMENTINTENT_STERAM,
        LATERAL FLATTEN(input => PARSE_JSON(LOG):"response.payment_method_types") response_payment_method_types
    WHERE LOG_TYPE = 'payment:stripe_paymentIntent'
) src
ON tgt.id = src.id
   AND tgt.idx = src.idx
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.type = src.type,
    tgt.status = src.status,
    tgt.user_id = src.user_id,
    tgt.response_amount = src.response_amount,
    tgt.response_object = src.response_object,
    tgt.response_status = src.response_status,
    tgt.response_currency = src.response_currency,
    tgt.response_metadata_uid = src.response_metadata_uid,
    tgt.response_payment_method_types = src.response_payment_method_types,
    tgt.created_at = src.created_at,
    tgt.payment_id = src.payment_id
WHEN NOT MATCHED THEN INSERT (
    id,
    idx,
    request_id,
    created,
    LOGs_userId,
    type,
    status,
    user_id,
    response_amount,
    response_object,
    response_status,
    response_currency,
    response_metadata_uid,
    response_payment_method_types,
    created_at,
    payment_id
) VALUES (
    src.id,
    src.idx,
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.type,
    src.status,
    src.user_id,
    src.response_amount,
    src.response_object,
    src.response_status,
    src.response_currency,
    src.response_metadata_uid,
    src.response_payment_method_types,
    src.created_at,
    src.payment_id
);
--Task再開
ALTER TASK LOG_PAYMENT_STRIPE_PAYMENTINTENT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_PAYMENT_STRIPE_PAYMENTINTENT_TASK;

-- Query for LOG_TYPE = ranked_party:create

CREATE OR REPLACE STREAM LOG_RANKED_PARTY_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_RANKED_PARTY_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_RANKED_PARTY_CREATE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"ranked_party_id" AS STRING) AS ranked_party_id,
        metadata$action
    FROM LOG_RANKED_PARTY_CREATE_STERAM
    WHERE LOG_TYPE = 'ranked_party:create'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at,
    tgt.ranked_party_id = src.ranked_party_id
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    created_at,
    ranked_party_id
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.created_at,
    src.ranked_party_id
);
--Task再開
ALTER TASK LOG_RANKED_PARTY_CREATE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_RANKED_PARTY_CREATE_TASK;

-- Query for LOG_TYPE = ranked_party:delete

CREATE OR REPLACE STREAM LOG_RANKED_PARTY_DELETE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_RANKED_PARTY_DELETE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_RANKED_PARTY_DELETE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"deleted_at" AS NUMBER) AS deleted_at,
        CAST(PARSE_JSON(LOG):"ranked_party_id" AS STRING) AS ranked_party_id,
        metadata$action
    FROM LOG_RANKED_PARTY_DELETE_STERAM
    WHERE LOG_TYPE = 'ranked_party:delete'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.deleted_at = src.deleted_at,
    tgt.ranked_party_id = src.ranked_party_id
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    deleted_at,
    ranked_party_id
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.deleted_at,
    src.ranked_party_id
);
--Task再開
ALTER TASK LOG_RANKED_PARTY_DELETE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_RANKED_PARTY_DELETE_TASK;

-- Query for LOG_TYPE = ranked_party:profile_edit

CREATE OR REPLACE STREAM LOG_RANKED_PARTY_PROFILE_EDIT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_RANKED_PARTY_PROFILE_EDIT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_dev_int
SCHEDULE = '24 HOUR'
AS 
MERGE INTO LOG_RANKED_PARTY_PROFILE_EDIT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"message" AS STRING) AS message,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"modified_at" AS NUMBER) AS modified_at,
        CAST(PARSE_JSON(LOG):"youtube_url" AS STRING) AS youtube_url,
        CAST(PARSE_JSON(LOG):"ranked_party_id" AS STRING) AS ranked_party_id,
        metadata$action
    FROM LOG_RANKED_PARTY_PROFILE_EDIT_STERAM
    WHERE LOG_TYPE = 'ranked_party:profile_edit'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.message = src.message,
    tgt.user_id = src.user_id,
    tgt.modified_at = src.modified_at,
    tgt.youtube_url = src.youtube_url,
    tgt.ranked_party_id = src.ranked_party_id
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    message,
    user_id,
    modified_at,
    youtube_url,
    ranked_party_id
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.message,
    src.user_id,
    src.modified_at,
    src.youtube_url,
    src.ranked_party_id
);
--Task再開
ALTER TASK LOG_RANKED_PARTY_PROFILE_EDIT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_RANKED_PARTY_PROFILE_EDIT_TASK;

