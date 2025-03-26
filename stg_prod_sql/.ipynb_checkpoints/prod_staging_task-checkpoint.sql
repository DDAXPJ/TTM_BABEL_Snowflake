-- Query for LOG_TYPE = avatar:get

CREATE OR REPLACE STREAM LOG_AVATAR_GET_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_AVATAR_GET_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ALTER TASK LOG_AVATAR_USE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_AVATAR_USE_TASK;

-- Query for LOG_TYPE = coupon:create

CREATE OR REPLACE STREAM LOG_COUPON_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_COUPON_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_CURRENCY_FREE_DEPOSIT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_CURRENCY_FREE_PAID AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_GACHA_DRAW AS tgt
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
        CAST(PARSE_JSON(LOG):"draw_num" AS NUMBER) AS draw_num,
        CAST(PARSE_JSON(LOG):"gacha_id" AS NUMBER) AS gacha_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
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
    tgt.items_id = src.items_id,
    tgt.items_amount = src.items_amount,
    tgt.items_item_id = src.items_item_id,
    tgt.items_item_type = src.items_item_type,
    tgt.user_id = src.user_id,
    tgt.draw_num = src.draw_num,
    tgt.gacha_id = src.gacha_id,
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
    draw_num,
    gacha_id,
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
    src.draw_num,
    src.gacha_id,
    src.created_at
);

--Task再開
ALTER TASK LOG_GACHA_DRAW_TASK RESUME;

--Task実行
EXECUTE TASK LOG_GACHA_DRAW_TASK;

-- Query for LOG_TYPE = gold:add

CREATE OR REPLACE STREAM LOG_GOLD_ADD_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_GOLD_ADD_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
        CAST(PARSE_JSON(LOG):"ofpState.result.reachedWaveAchievementRate" AS FLOAT) AS ofpState_result_reachedWaveAchievementRate,
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
    tgt.onpInfo_behaviours = src.onpInfo_behaviours,
    tgt.onpInfo_behaviours_target = src.onpInfo_behaviours_target,
    tgt.onpInfo_dataVersion = src.onpInfo_dataVersion,
    tgt.onpInfo_isRankedParty = src.onpInfo_isRankedParty,
    tgt.onpInfo_partyCategory = src.onpInfo_partyCategory,
    tgt.players = src.players,
    tgt.players_onpInfo = src.players_onpInfo,
    tgt.players_onpInfo_avatarParams = src.players_onpInfo_avatarParams,
    tgt.questId = src.questId,
    tgt.version = src.version,
    tgt.laevedAt = src.laevedAt,
    tgt.ofpState_uses = src.ofpState_uses,
    tgt.ofpState_result_rate = src.ofpState_result_rate,
    tgt.ofpState_result_score = src.ofpState_result_score,
    tgt.ofpState_players = src.ofpState_players,
    tgt.ofpState_fieldLevel = src.ofpState_fieldLevel,
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
    tgt.ofpState_dataVersion = src.ofpState_dataVersion,
    tgt.fieldId = src.fieldId,
    tgt.onpInfo_fieldLevel = src.onpInfo_fieldLevel,
    tgt.onpInfo_isTransform = src.onpInfo_isTransform,
    tgt.leavedAt = src.leavedAt,
    tgt.created_at = src.created_at,
    tgt.ofpState = src.ofpState,
    tgt.ofpState_result_exp = src.ofpState_result_exp,
    tgt.ofpState_result_A = src.ofpState_result_A,
    tgt.ofpState_result_B = src.ofpState_result_B,
    tgt.ofpState_result_C = src.ofpState_result_C,
    tgt.ofpState_result_D = src.ofpState_result_D,
    tgt.ofpState_result_E = src.ofpState_result_E,
    tgt.ofpState_result_reachedWave = src.ofpState_result_reachedWave,
    tgt.ofpState_result_bossDefeated = src.ofpState_result_bossDefeated,
    tgt.ofpState_result_playerResults = src.ofpState_result_playerResults,
    tgt.ofpState_result_reachedWaveAchievementRate = src.ofpState_result_reachedWaveAchievementRate
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
    onpInfo_behaviours,
    onpInfo_behaviours_target,
    onpInfo_dataVersion,
    onpInfo_isRankedParty,
    onpInfo_partyCategory,
    players,
    players_onpInfo,
    players_onpInfo_avatarParams,
    questId,
    version,
    laevedAt,
    ofpState_uses,
    ofpState_result_rate,
    ofpState_result_score,
    ofpState_players,
    ofpState_fieldLevel,
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
    ofpState_dataVersion,
    fieldId,
    onpInfo_fieldLevel,
    onpInfo_isTransform,
    leavedAt,
    created_at,
    ofpState,
    ofpState_result_exp,
    ofpState_result_A,
    ofpState_result_B,
    ofpState_result_C,
    ofpState_result_D,
    ofpState_result_E,
    ofpState_result_reachedWave,
    ofpState_result_bossDefeated,
    ofpState_result_playerResults,
    ofpState_result_reachedWaveAchievementRate
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
    src.onpInfo_behaviours,
    src.onpInfo_behaviours_target,
    src.onpInfo_dataVersion,
    src.onpInfo_isRankedParty,
    src.onpInfo_partyCategory,
    src.players,
    src.players_onpInfo,
    src.players_onpInfo_avatarParams,
    src.questId,
    src.version,
    src.laevedAt,
    src.ofpState_uses,
    src.ofpState_result_rate,
    src.ofpState_result_score,
    src.ofpState_players,
    src.ofpState_fieldLevel,
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
    src.ofpState_dataVersion,
    src.fieldId,
    src.onpInfo_fieldLevel,
    src.onpInfo_isTransform,
    src.leavedAt,
    src.created_at,
    src.ofpState,
    src.ofpState_result_exp,
    src.ofpState_result_A,
    src.ofpState_result_B,
    src.ofpState_result_C,
    src.ofpState_result_D,
    src.ofpState_result_E,
    src.ofpState_result_reachedWave,
    src.ofpState_result_bossDefeated,
    src.ofpState_result_playerResults,
    src.ofpState_result_reachedWaveAchievementRate
);

--Task再開
ALTER TASK OFP_PLAYRESULTS_TASK RESUME;

--Task実行
EXECUTE TASK OFP_PLAYRESULTS_TASK;

-- Query for LOG_TYPE = party:create

CREATE OR REPLACE STREAM LOG_PARTY_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_PARTY_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_PARTY_NAME_CREATE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
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
    tgt.created_at = src.created_at,
    tgt.party_name = src.party_name,
    tgt.party_tts_name = src.party_tts_name
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    created_at,
    party_name,
    party_tts_name
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_PARTY_NAME_EDIT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"party_name" AS STRING) AS party_name,
        CAST(PARSE_JSON(LOG):"party_tts_name" AS STRING) AS party_tts_name,
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
    tgt.created_at = src.created_at,
    tgt.party_name = src.party_name,
    tgt.party_tts_name = src.party_tts_name,
    tgt.before_party_name = src.before_party_name,
    tgt.before_party_tts_name = src.before_party_tts_name
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    created_at,
    party_name,
    party_tts_name,
    before_party_name,
    before_party_tts_name
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.created_at,
    src.party_name,
    src.party_tts_name,
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
        CAST(PARSE_JSON(LOG):"paid.coupon_info.coupon_group_id" AS NUMBER) AS paid_coupon_info_coupon_group_id,
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
    tgt.gacha_gacha_id = src.gacha_gacha_id,
    tgt.paid_coupon_info_coupon_id = src.paid_coupon_info_coupon_id,
    tgt.paid_coupon_info_coupon_code = src.paid_coupon_info_coupon_code,
    tgt.paid_coupon_info_coupon_effect_type = src.paid_coupon_info_coupon_effect_type,
    tgt.paid_coupon_info_coupon_effect_amount = src.paid_coupon_info_coupon_effect_amount,
    tgt.paid_coupon_info_code_type = src.paid_coupon_info_code_type,
    tgt.paid_coupon_info_user_coupon_id = src.paid_coupon_info_user_coupon_id,
    tgt.paid_coupon_info_coupon_group_id = src.paid_coupon_info_coupon_group_id
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
    gacha_gacha_id,
    paid_coupon_info_coupon_id,
    paid_coupon_info_coupon_code,
    paid_coupon_info_coupon_effect_type,
    paid_coupon_info_coupon_effect_amount,
    paid_coupon_info_code_type,
    paid_coupon_info_user_coupon_id,
    paid_coupon_info_coupon_group_id
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
    src.gacha_gacha_id,
    src.paid_coupon_info_coupon_id,
    src.paid_coupon_info_coupon_code,
    src.paid_coupon_info_coupon_effect_type,
    src.paid_coupon_info_coupon_effect_amount,
    src.paid_coupon_info_code_type,
    src.paid_coupon_info_user_coupon_id,
    src.paid_coupon_info_coupon_group_id
);

--Task再開
ALTER TASK LOG_PAYMENT_PURCHASE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_PAYMENT_PURCHASE_TASK;

-- Query for LOG_TYPE = payment:q5_pay

CREATE OR REPLACE STREAM LOG_PAYMENT_Q5_PAY_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_PAYMENT_Q5_PAY_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
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

-- Query for LOG_TYPE = reservation:create

CREATE OR REPLACE STREAM LOG_RESERVATION_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_RESERVATION_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_RESERVATION_CREATE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"persons" AS NUMBER) AS persons,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"reservation_id" AS NUMBER) AS reservation_id,
        CAST(PARSE_JSON(LOG):"reservation_date" AS DATE) AS reservation_date,
        CAST(PARSE_JSON(LOG):"reservation_time" AS STRING) AS reservation_time,
        CAST(PARSE_JSON(LOG):"reservation_type" AS NUMBER) AS reservation_type,
        metadata$action
    FROM LOG_RESERVATION_CREATE_STERAM
    WHERE LOG_TYPE = 'reservation:create'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.persons = src.persons,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at,
    tgt.reservation_id = src.reservation_id,
    tgt.reservation_date = src.reservation_date,
    tgt.reservation_time = src.reservation_time,
    tgt.reservation_type = src.reservation_type
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    persons,
    user_id,
    created_at,
    reservation_id,
    reservation_date,
    reservation_time,
    reservation_type
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.persons,
    src.user_id,
    src.created_at,
    src.reservation_id,
    src.reservation_date,
    src.reservation_time,
    src.reservation_type
);

--Task再開
ALTER TASK LOG_RESERVATION_CREATE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_RESERVATION_CREATE_TASK;

-- Query for LOG_TYPE = reservation:use

CREATE OR REPLACE STREAM LOG_RESERVATION_USE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_RESERVATION_USE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_RESERVATION_USE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"reservation_id" AS NUMBER) AS reservation_id,
        metadata$action
    FROM LOG_RESERVATION_USE_STERAM
    WHERE LOG_TYPE = 'reservation:use'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.created_at = src.created_at,
    tgt.reservation_id = src.reservation_id
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    created_at,
    reservation_id
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.created_at,
    src.reservation_id
);

--Task再開
ALTER TASK LOG_RESERVATION_USE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_RESERVATION_USE_TASK;

-- Query for LOG_TYPE = session:checkin

CREATE OR REPLACE STREAM LOG_SESSION_CHECKIN_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SESSION_CHECKIN_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_SESSION_CHECKIN AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"response.rfid" AS STRING) AS response_rfid,
        CAST(PARSE_JSON(LOG):"response.type" AS STRING) AS response_type,
        CAST(PARSE_JSON(LOG):"response.playerId" AS NUMBER) AS response_playerId,
        CAST(PARSE_JSON(LOG):"response.accountId" AS STRING) AS response_accountId,
        CAST(PARSE_JSON(LOG):"response.createdAt" AS NUMBER) AS response_createdAt,
        CAST(PARSE_JSON(LOG):"response.sessionId" AS NUMBER) AS response_sessionId,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_SESSION_CHECKIN_STERAM
    WHERE LOG_TYPE = 'session:checkin'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.response_rfid = src.response_rfid,
    tgt.response_type = src.response_type,
    tgt.response_playerId = src.response_playerId,
    tgt.response_accountId = src.response_accountId,
    tgt.response_createdAt = src.response_createdAt,
    tgt.response_sessionId = src.response_sessionId,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    response_rfid,
    response_type,
    response_playerId,
    response_accountId,
    response_createdAt,
    response_sessionId,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.response_rfid,
    src.response_type,
    src.response_playerId,
    src.response_accountId,
    src.response_createdAt,
    src.response_sessionId,
    src.created_at
);

--Task再開
ALTER TASK LOG_SESSION_CHECKIN_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SESSION_CHECKIN_TASK;

-- Query for LOG_TYPE = session:checkout

CREATE OR REPLACE STREAM LOG_SESSION_CHECKOUT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SESSION_CHECKOUT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_SESSION_CHECKOUT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"response.rfid" AS STRING) AS response_rfid,
        CAST(PARSE_JSON(LOG):"response.type" AS STRING) AS response_type,
        CAST(PARSE_JSON(LOG):"response.playerId" AS NUMBER) AS response_playerId,
        CAST(PARSE_JSON(LOG):"response.accountId" AS STRING) AS response_accountId,
        CAST(PARSE_JSON(LOG):"response.createdAt" AS NUMBER) AS response_createdAt,
        CAST(PARSE_JSON(LOG):"response.sessionId" AS NUMBER) AS response_sessionId,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_SESSION_CHECKOUT_STERAM
    WHERE LOG_TYPE = 'session:checkout'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.response_rfid = src.response_rfid,
    tgt.response_type = src.response_type,
    tgt.response_playerId = src.response_playerId,
    tgt.response_accountId = src.response_accountId,
    tgt.response_createdAt = src.response_createdAt,
    tgt.response_sessionId = src.response_sessionId,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    response_rfid,
    response_type,
    response_playerId,
    response_accountId,
    response_createdAt,
    response_sessionId,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.response_rfid,
    src.response_type,
    src.response_playerId,
    src.response_accountId,
    src.response_createdAt,
    src.response_sessionId,
    src.created_at
);

--Task再開
ALTER TASK LOG_SESSION_CHECKOUT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SESSION_CHECKOUT_TASK;

-- Query for LOG_TYPE = session:create

CREATE OR REPLACE STREAM LOG_SESSION_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SESSION_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_SESSION_CREATE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
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
        CAST(PARSE_JSON(LOG):"terminal_id" AS NUMBER) AS terminal_id,
        metadata$action
    FROM LOG_SESSION_CREATE_STERAM
    WHERE LOG_TYPE = 'session:create'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.request_items = src.request_items,
    tgt.request_players = src.request_players,
    tgt.user_id = src.user_id,
    tgt.response_data_players = src.response_data_players,
    tgt.response_data_sessionId = src.response_data_sessionId,
    tgt.created_at = src.created_at,
    tgt.user_reservation_id = src.user_reservation_id,
    tgt.request_items_onpInfo_effects = src.request_items_onpInfo_effects,
    tgt.request_party_name = src.request_party_name,
    tgt.request_party_onpId = src.request_party_onpId,
    tgt.request_party_onpInfo_language = src.request_party_onpInfo_language,
    tgt.request_party_onpInfo_leaderId = src.request_party_onpInfo_leaderId,
    tgt.request_party_onpInfo_partyCode = src.request_party_onpInfo_partyCode,
    tgt.request_party_onpInfo_requestId = src.request_party_onpInfo_requestId,
    tgt.request_party_onpInfo_dataVersion = src.request_party_onpInfo_dataVersion,
    tgt.request_party_onpInfo_isTransform = src.request_party_onpInfo_isTransform,
    tgt.request_party_onpInfo_isRankedParty = src.request_party_onpInfo_isRankedParty,
    tgt.request_party_onpInfo_partyCategory = src.request_party_onpInfo_partyCategory,
    tgt.request_party_onpInfo_hasNextSession = src.request_party_onpInfo_hasNextSession,
    tgt.request_party_ttsName = src.request_party_ttsName,
    tgt.request_party_rankedPartyId = src.request_party_rankedPartyId,
    tgt.request_gameId = src.request_gameId,
    tgt.request_fieldId = src.request_fieldId,
    tgt.request_players_onpInfo = src.request_players_onpInfo,
    tgt.request_players_onpInfo_avatarParams = src.request_players_onpInfo_avatarParams,
    tgt.request_version = src.request_version,
    tgt.request_isReserved = src.request_isReserved,
    tgt.request_sourceSessionId = src.request_sourceSessionId,
    tgt.response_data_entryNumber = src.response_data_entryNumber,
    tgt.response_meta_status = src.response_meta_status,
    tgt.terminal_id = src.terminal_id
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    request_items,
    request_players,
    user_id,
    response_data_players,
    response_data_sessionId,
    created_at,
    user_reservation_id,
    request_items_onpInfo_effects,
    request_party_name,
    request_party_onpId,
    request_party_onpInfo_language,
    request_party_onpInfo_leaderId,
    request_party_onpInfo_partyCode,
    request_party_onpInfo_requestId,
    request_party_onpInfo_dataVersion,
    request_party_onpInfo_isTransform,
    request_party_onpInfo_isRankedParty,
    request_party_onpInfo_partyCategory,
    request_party_onpInfo_hasNextSession,
    request_party_ttsName,
    request_party_rankedPartyId,
    request_gameId,
    request_fieldId,
    request_players_onpInfo,
    request_players_onpInfo_avatarParams,
    request_version,
    request_isReserved,
    request_sourceSessionId,
    response_data_entryNumber,
    response_meta_status,
    terminal_id
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.request_items,
    src.request_players,
    src.user_id,
    src.response_data_players,
    src.response_data_sessionId,
    src.created_at,
    src.user_reservation_id,
    src.request_items_onpInfo_effects,
    src.request_party_name,
    src.request_party_onpId,
    src.request_party_onpInfo_language,
    src.request_party_onpInfo_leaderId,
    src.request_party_onpInfo_partyCode,
    src.request_party_onpInfo_requestId,
    src.request_party_onpInfo_dataVersion,
    src.request_party_onpInfo_isTransform,
    src.request_party_onpInfo_isRankedParty,
    src.request_party_onpInfo_partyCategory,
    src.request_party_onpInfo_hasNextSession,
    src.request_party_ttsName,
    src.request_party_rankedPartyId,
    src.request_gameId,
    src.request_fieldId,
    src.request_players_onpInfo,
    src.request_players_onpInfo_avatarParams,
    src.request_version,
    src.request_isReserved,
    src.request_sourceSessionId,
    src.response_data_entryNumber,
    src.response_meta_status,
    src.terminal_id
);

--Task再開
ALTER TASK LOG_SESSION_CREATE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SESSION_CREATE_TASK;

-- Query for LOG_TYPE = session:end

CREATE OR REPLACE STREAM LOG_SESSION_END_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SESSION_END_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_SESSION_END AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
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
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
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
    tgt.response_party_onpInfo_fieldLevel = src.response_party_onpInfo_fieldLevel,
    tgt.response_party_onpInfo_isTransform = src.response_party_onpInfo_isTransform,
    tgt.response_quests = src.response_quests,
    tgt.response_quests_ofpState_uses = src.response_quests_ofpState_uses,
    tgt.response_quests_ofpState_result = src.response_quests_ofpState_result,
    tgt.response_quests_ofpState_players = src.response_quests_ofpState_players,
    tgt.response_raeson = src.response_raeson,
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
    response_party_onpInfo_fieldLevel,
    response_party_onpInfo_isTransform,
    response_quests,
    response_quests_ofpState_uses,
    response_quests_ofpState_result,
    response_quests_ofpState_players,
    response_raeson,
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
    src.response_party_onpInfo_fieldLevel,
    src.response_party_onpInfo_isTransform,
    src.response_quests,
    src.response_quests_ofpState_uses,
    src.response_quests_ofpState_result,
    src.response_quests_ofpState_players,
    src.response_raeson,
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_SESSION_ENTRY AS tgt
USING (
    SELECT DISTINCT 
        id,
        response_players.index AS idx,
        request_id,
        created,
        user_id AS LOGs_userId,
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
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
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
    tgt.response_players_accountId = src.response_players_accountId,
    tgt.response_partName = src.response_partName,
    tgt.response_createdAt = src.response_createdAt,
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
    response_players_accountId,
    response_partName,
    response_createdAt,
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
    src.response_players_accountId,
    src.response_partName,
    src.response_createdAt,
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_SESSION_EXIT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"response.type" AS STRING) AS response_type,
        CAST(PARSE_JSON(LOG):"response.partyId" AS STRING) AS response_partyId,
        CAST(PARSE_JSON(LOG):"response.createdAt" AS NUMBER) AS response_createdAt,
        CAST(PARSE_JSON(LOG):"response.sessionId" AS NUMBER) AS response_sessionId,
        CAST(PARSE_JSON(LOG):"response.entryNumber" AS NUMBER) AS response_entryNumber,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_SESSION_START AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"response.type" AS STRING) AS response_type,
        CAST(PARSE_JSON(LOG):"response.raeson" AS STRING) AS response_raeson,
        CAST(PARSE_JSON(LOG):"response.partyId" AS STRING) AS response_partyId,
        CAST(PARSE_JSON(LOG):"response.copyCount" AS NUMBER) AS response_copyCount,
        CAST(PARSE_JSON(LOG):"response.createdAt" AS NUMBER) AS response_createdAt,
        CAST(PARSE_JSON(LOG):"response.sessionId" AS NUMBER) AS response_sessionId,
        CAST(PARSE_JSON(LOG):"response.entryNumber" AS NUMBER) AS response_entryNumber,
        CAST(PARSE_JSON(LOG):"response.sourceSessionId" AS NUMBER) AS response_sourceSessionId,
        CAST(PARSE_JSON(LOG):"response.originalSessionId" AS NUMBER) AS response_originalSessionId,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
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
    tgt.response_raeson = src.response_raeson,
    tgt.response_partyId = src.response_partyId,
    tgt.response_copyCount = src.response_copyCount,
    tgt.response_createdAt = src.response_createdAt,
    tgt.response_sessionId = src.response_sessionId,
    tgt.response_entryNumber = src.response_entryNumber,
    tgt.response_sourceSessionId = src.response_sourceSessionId,
    tgt.response_originalSessionId = src.response_originalSessionId,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    response_type,
    response_raeson,
    response_partyId,
    response_copyCount,
    response_createdAt,
    response_sessionId,
    response_entryNumber,
    response_sourceSessionId,
    response_originalSessionId,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.response_type,
    src.response_raeson,
    src.response_partyId,
    src.response_copyCount,
    src.response_createdAt,
    src.response_sessionId,
    src.response_entryNumber,
    src.response_sourceSessionId,
    src.response_originalSessionId,
    src.created_at
);

--Task再開
ALTER TASK LOG_SESSION_START_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SESSION_START_TASK;

-- Query for LOG_TYPE = smaregi:pos_transactions_temporaries_create

CREATE OR REPLACE STREAM LOG_SMAREGI_POS_TRANSACTIONS_TEMPORARIES_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SMAREGI_POS_TRANSACTIONS_TEMPORARIES_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_SMAREGI_POS_TRANSACTIONS_TEMPORARIES_CREATE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
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
        CAST(PARSE_JSON(LOG):"coupon_group_id" AS NUMBER) AS coupon_group_id,
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
    tgt.request_memo_party_id = src.request_memo_party_id,
    tgt.request_memo_party_code = src.request_memo_party_code,
    tgt.request_memo_party_name = src.request_memo_party_name,
    tgt.request_memo_entryNumber = src.request_memo_entryNumber,
    tgt.request_preRegistrationName = src.request_preRegistrationName,
    tgt.party_id = src.party_id,
    tgt.response_barcode = src.response_barcode,
    tgt.response_transactionHeadId = src.response_transactionHeadId,
    tgt.response_transactionDateTime = src.response_transactionDateTime,
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
    request_memo_party_id,
    request_memo_party_code,
    request_memo_party_name,
    request_memo_entryNumber,
    request_preRegistrationName,
    party_id,
    response_barcode,
    response_transactionHeadId,
    response_transactionDateTime,
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
    src.request_memo_party_id,
    src.request_memo_party_code,
    src.request_memo_party_name,
    src.request_memo_entryNumber,
    src.request_preRegistrationName,
    src.party_id,
    src.response_barcode,
    src.response_transactionHeadId,
    src.response_transactionDateTime,
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

-- Query for LOG_TYPE = smaregi:webhook_pos_adjustments

CREATE OR REPLACE STREAM LOG_SMAREGI_WEBHOOK_POS_ADJUSTMENTS_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SMAREGI_WEBHOOK_POS_ADJUSTMENTS_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_SMAREGI_WEBHOOK_POS_ADJUSTMENTS AS tgt
USING (
    SELECT DISTINCT 
        id,
        response_ids.index AS idx,
        request_id,
        created,
        user_id AS LOGs_userId,
        response_ids.value:"storeId"::STRING AS response_ids_storeId,
        response_ids.value:"terminalId"::STRING AS response_ids_terminalId,
        response_ids.value:"adjustmentDateTime"::STRING AS response_ids_adjustmentDateTime,
        CAST(PARSE_JSON(LOG):"response.event" AS STRING) AS response_event,
        CAST(PARSE_JSON(LOG):"response.action" AS STRING) AS response_action,
        CAST(PARSE_JSON(LOG):"response.contractId" AS STRING) AS response_contractId,
        CAST(PARSE_JSON(LOG):"response.cashAdjustment" AS BOOLEAN) AS response_cashAdjustment,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_SMAREGI_WEBHOOK_POS_ADJUSTMENTS_STERAM,
        LATERAL FLATTEN(input => PARSE_JSON(LOG):"response.ids") response_ids
    WHERE LOG_TYPE = 'smaregi:webhook_pos_adjustments'
) src
ON tgt.id = src.id
   AND tgt.idx = src.idx
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.response_ids_storeId = src.response_ids_storeId,
    tgt.response_ids_terminalId = src.response_ids_terminalId,
    tgt.response_ids_adjustmentDateTime = src.response_ids_adjustmentDateTime,
    tgt.response_event = src.response_event,
    tgt.response_action = src.response_action,
    tgt.response_contractId = src.response_contractId,
    tgt.response_cashAdjustment = src.response_cashAdjustment,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    idx,
    request_id,
    created,
    LOGs_userId,
    response_ids_storeId,
    response_ids_terminalId,
    response_ids_adjustmentDateTime,
    response_event,
    response_action,
    response_contractId,
    response_cashAdjustment,
    created_at
) VALUES (
    src.id,
    src.idx,
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.response_ids_storeId,
    src.response_ids_terminalId,
    src.response_ids_adjustmentDateTime,
    src.response_event,
    src.response_action,
    src.response_contractId,
    src.response_cashAdjustment,
    src.created_at
);

--Task再開
ALTER TASK LOG_SMAREGI_WEBHOOK_POS_ADJUSTMENTS_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SMAREGI_WEBHOOK_POS_ADJUSTMENTS_TASK;

-- Query for LOG_TYPE = spirit:get

CREATE OR REPLACE STREAM LOG_SPIRIT_GET_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SPIRIT_GET_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_SPIRIT_GET AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"spirit_id" AS NUMBER) AS spirit_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"user_spirit_id" AS NUMBER) AS user_spirit_id,
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
    tgt.created_at = src.created_at,
    tgt.user_spirit_id = src.user_spirit_id
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    spirit_id,
    created_at,
    user_spirit_id
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.spirit_id,
    src.created_at,
    src.user_spirit_id
);

--Task再開
ALTER TASK LOG_SPIRIT_GET_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SPIRIT_GET_TASK;

-- Query for LOG_TYPE = spirit:use

CREATE OR REPLACE STREAM LOG_SPIRIT_USE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_SPIRIT_USE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_SPIRIT_USE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"spirit_id" AS NUMBER) AS spirit_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"user_spirit_id" AS NUMBER) AS user_spirit_id,
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
    tgt.created_at = src.created_at,
    tgt.user_spirit_id = src.user_spirit_id
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    spirit_id,
    created_at,
    user_spirit_id
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.spirit_id,
    src.created_at,
    src.user_spirit_id
);

--Task再開
ALTER TASK LOG_SPIRIT_USE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_SPIRIT_USE_TASK;

-- Query for LOG_TYPE = title:get

CREATE OR REPLACE STREAM LOG_TITLE_GET_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_TITLE_GET_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_TITLE_GET AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"title_id" AS NUMBER) AS title_id,
        CAST(PARSE_JSON(LOG):"modified_at" AS NUMBER) AS modified_at,
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_ADULT_CHILD_SELECT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"selected" AS NUMBER) AS selected,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
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

-- Query for LOG_TYPE = user:ban

CREATE OR REPLACE STREAM LOG_USER_BAN_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_BAN_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_BAN AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"modified_at" AS NUMBER) AS modified_at,
        metadata$action
    FROM LOG_USER_BAN_STERAM
    WHERE LOG_TYPE = 'user:ban'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.modified_at = src.modified_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    modified_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.modified_at
);

--Task再開
ALTER TASK LOG_USER_BAN_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_BAN_TASK;

-- Query for LOG_TYPE = user:ban_cancel

CREATE OR REPLACE STREAM LOG_USER_BAN_CANCEL_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_BAN_CANCEL_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_BAN_CANCEL AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"modified_at" AS NUMBER) AS modified_at,
        metadata$action
    FROM LOG_USER_BAN_CANCEL_STERAM
    WHERE LOG_TYPE = 'user:ban_cancel'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.modified_at = src.modified_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    modified_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.modified_at
);

--Task再開
ALTER TASK LOG_USER_BAN_CANCEL_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_BAN_CANCEL_TASK;

-- Query for LOG_TYPE = user:create

CREATE OR REPLACE STREAM LOG_USER_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_CREATE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"auth_code" AS STRING) AS auth_code,
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
    tgt.created_at = src.created_at,
    tgt.auth_code = src.auth_code
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    created_at,
    auth_code
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.created_at,
    src.auth_code
);

--Task再開
ALTER TASK LOG_USER_CREATE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_CREATE_TASK;

-- Query for LOG_TYPE = user:delete

CREATE OR REPLACE STREAM LOG_USER_DELETE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_DELETE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_DELETE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"auth_code" AS STRING) AS auth_code,
        CAST(PARSE_JSON(LOG):"deleted_at" AS NUMBER) AS deleted_at,
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
    tgt.auth_code = src.auth_code,
    tgt.deleted_at = src.deleted_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    auth_code,
    deleted_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.auth_code,
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_FLAG AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"user_flag" AS NUMBER) AS user_flag,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
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
    tgt.user_id = src.user_id,
    tgt.user_flag = src.user_flag,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    user_flag,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.user_flag,
    src.created_at
);

--Task再開
ALTER TASK LOG_USER_FLAG_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_FLAG_TASK;

-- Query for LOG_TYPE = user:item

CREATE OR REPLACE STREAM LOG_USER_ITEM_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_ITEM_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_ITEM AS tgt
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
    FROM LOG_USER_ITEM_STERAM,
        LATERAL FLATTEN(input => PARSE_JSON(LOG):"items") items
    WHERE LOG_TYPE = 'user:item'
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
ALTER TASK LOG_USER_ITEM_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_ITEM_TASK;

-- Query for LOG_TYPE = user:language_select

CREATE OR REPLACE STREAM LOG_USER_LANGUAGE_SELECT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_LANGUAGE_SELECT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_LANGUAGE_SELECT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"language" AS NUMBER) AS language,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
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
    tgt.user_id = src.user_id,
    tgt.language = src.language,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    language,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.language,
    src.created_at
);

--Task再開
ALTER TASK LOG_USER_LANGUAGE_SELECT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_LANGUAGE_SELECT_TASK;

-- Query for LOG_TYPE = user:login

CREATE OR REPLACE STREAM LOG_USER_LOGIN_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_LOGIN_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_LOGIN AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"auth_code" AS STRING) AS auth_code,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
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
    tgt.auth_code = src.auth_code,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    auth_code,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.auth_code,
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_LOGIN_FIRST AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"auth_code" AS STRING) AS auth_code,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"continue_day" AS NUMBER) AS continue_day,
        CAST(PARSE_JSON(LOG):"day_since_last" AS NUMBER) AS day_since_last,
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
    tgt.user_id = src.user_id,
    tgt.auth_code = src.auth_code,
    tgt.created_at = src.created_at,
    tgt.continue_day = src.continue_day,
    tgt.day_since_last = src.day_since_last
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    auth_code,
    created_at,
    continue_day,
    day_since_last
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.auth_code,
    src.created_at,
    src.continue_day,
    src.day_since_last
);

--Task再開
ALTER TASK LOG_USER_LOGIN_FIRST_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_LOGIN_FIRST_TASK;

-- Query for LOG_TYPE = user:logout

CREATE OR REPLACE STREAM LOG_USER_LOGOUT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_LOGOUT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_LOGOUT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"auth_code" AS STRING) AS auth_code,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        metadata$action
    FROM LOG_USER_LOGOUT_STERAM
    WHERE LOG_TYPE = 'user:logout'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.user_id = src.user_id,
    tgt.auth_code = src.auth_code,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    user_id,
    auth_code,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.user_id,
    src.auth_code,
    src.created_at
);

--Task再開
ALTER TASK LOG_USER_LOGOUT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_LOGOUT_TASK;

-- Query for LOG_TYPE = user:name_create

CREATE OR REPLACE STREAM LOG_USER_NAME_CREATE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_NAME_CREATE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_NAME_CREATE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"name" AS STRING) AS name,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"tts_name" AS STRING) AS tts_name,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
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
    tgt.tts_name = src.tts_name,
    tgt.created_at = src.created_at
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    name,
    user_id,
    tts_name,
    created_at
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.name,
    src.user_id,
    src.tts_name,
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
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_NAME_EDIT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"name" AS STRING) AS name,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"tts_name" AS STRING) AS tts_name,
        CAST(PARSE_JSON(LOG):"created_at" AS NUMBER) AS created_at,
        CAST(PARSE_JSON(LOG):"before_name" AS STRING) AS before_name,
        CAST(PARSE_JSON(LOG):"before_tts_name" AS STRING) AS before_tts_name,
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
    tgt.tts_name = src.tts_name,
    tgt.created_at = src.created_at,
    tgt.before_name = src.before_name,
    tgt.before_tts_name = src.before_tts_name
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    name,
    user_id,
    tts_name,
    created_at,
    before_name,
    before_tts_name
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.name,
    src.user_id,
    src.tts_name,
    src.created_at,
    src.before_name,
    src.before_tts_name
);

--Task再開
ALTER TASK LOG_USER_NAME_EDIT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_NAME_EDIT_TASK;

-- Query for LOG_TYPE = user:profile_edit

CREATE OR REPLACE STREAM LOG_USER_PROFILE_EDIT_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_PROFILE_EDIT_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_PROFILE_EDIT AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
        CAST(PARSE_JSON(LOG):"url" AS STRING) AS url,
        CAST(PARSE_JSON(LOG):"name" AS STRING) AS name,
        CAST(PARSE_JSON(LOG):"message" AS STRING) AS message,
        CAST(PARSE_JSON(LOG):"user_id" AS STRING) AS user_id,
        CAST(PARSE_JSON(LOG):"tts_name" AS STRING) AS tts_name,
        CAST(PARSE_JSON(LOG):"party_name" AS STRING) AS party_name,
        CAST(PARSE_JSON(LOG):"modified_at" AS NUMBER) AS modified_at,
        CAST(PARSE_JSON(LOG):"party_tts_name" AS STRING) AS party_tts_name,
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
    tgt.tts_name = src.tts_name,
    tgt.party_name = src.party_name,
    tgt.modified_at = src.modified_at,
    tgt.party_tts_name = src.party_tts_name
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    url,
    name,
    message,
    user_id,
    tts_name,
    party_name,
    modified_at,
    party_tts_name
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.url,
    src.name,
    src.message,
    src.user_id,
    src.tts_name,
    src.party_name,
    src.modified_at,
    src.party_tts_name
);

--Task再開
ALTER TASK LOG_USER_PROFILE_EDIT_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_PROFILE_EDIT_TASK;

-- Query for LOG_TYPE = user:score

CREATE OR REPLACE STREAM LOG_USER_SCORE_STERAM on TABLE DBLOG;

CREATE OR REPLACE TASK LOG_USER_SCORE_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = dblog_prod_int
SCHEDULE = '180 MINUTE'
AS 
MERGE INTO LOG_USER_SCORE AS tgt
USING (
    SELECT DISTINCT 
        id,
        
        request_id,
        created,
        user_id AS LOGs_userId,
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
        CAST(PARSE_JSON(LOG):"q5_b_reached_num" AS NUMBER) AS q5_b_reached_num,
        metadata$action
    FROM LOG_USER_SCORE_STERAM
    WHERE LOG_TYPE = 'user:score'
) src
ON tgt.id = src.id
   
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    tgt.level = src.level,
    tgt.user_id = src.user_id,
    tgt.total_exp = src.total_exp,
    tgt.created_at = src.created_at,
    tgt.total_gold = src.total_gold,
    tgt.q1_play_num = src.q1_play_num,
    tgt.q4_play_num = src.q4_play_num,
    tgt.total_score = src.total_score,
    tgt.fq1_play_num = src.fq1_play_num,
    tgt.fq2_play_num = src.fq2_play_num,
    tgt.q2_a_play_num = src.q2_a_play_num,
    tgt.q2_b_play_num = src.q2_b_play_num,
    tgt.q2_c_play_num = src.q2_c_play_num,
    tgt.q3_a_play_num = src.q3_a_play_num,
    tgt.q3_b_play_num = src.q3_b_play_num,
    tgt.q3_c_play_num = src.q3_c_play_num,
    tgt.q3_d_play_num = src.q3_d_play_num,
    tgt.q3_e_play_num = src.q3_e_play_num,
    tgt.q3_f_play_num = src.q3_f_play_num,
    tgt.q3_g_play_num = src.q3_g_play_num,
    tgt.q3_h_play_num = src.q3_h_play_num,
    tgt.q3_i_play_num = src.q3_i_play_num,
    tgt.q5_a_play_num = src.q5_a_play_num,
    tgt.q5_b_play_num = src.q5_b_play_num,
    tgt.fq1_reached_num = src.fq1_reached_num,
    tgt.fq2_reached_num = src.fq2_reached_num,
    tgt.q5_a_reached_num = src.q5_a_reached_num,
    tgt.q5_b_reached_num = src.q5_b_reached_num
WHEN NOT MATCHED THEN INSERT (
    id,
    
    request_id,
    created,
    LOGs_userId,
    level,
    user_id,
    total_exp,
    created_at,
    total_gold,
    q1_play_num,
    q4_play_num,
    total_score,
    fq1_play_num,
    fq2_play_num,
    q2_a_play_num,
    q2_b_play_num,
    q2_c_play_num,
    q3_a_play_num,
    q3_b_play_num,
    q3_c_play_num,
    q3_d_play_num,
    q3_e_play_num,
    q3_f_play_num,
    q3_g_play_num,
    q3_h_play_num,
    q3_i_play_num,
    q5_a_play_num,
    q5_b_play_num,
    fq1_reached_num,
    fq2_reached_num,
    q5_a_reached_num,
    q5_b_reached_num
) VALUES (
    src.id,
    
    src.request_id,
    src.created,
    src.LOGs_userId,
    src.level,
    src.user_id,
    src.total_exp,
    src.created_at,
    src.total_gold,
    src.q1_play_num,
    src.q4_play_num,
    src.total_score,
    src.fq1_play_num,
    src.fq2_play_num,
    src.q2_a_play_num,
    src.q2_b_play_num,
    src.q2_c_play_num,
    src.q3_a_play_num,
    src.q3_b_play_num,
    src.q3_c_play_num,
    src.q3_d_play_num,
    src.q3_e_play_num,
    src.q3_f_play_num,
    src.q3_g_play_num,
    src.q3_h_play_num,
    src.q3_i_play_num,
    src.q5_a_play_num,
    src.q5_b_play_num,
    src.fq1_reached_num,
    src.fq2_reached_num,
    src.q5_a_reached_num,
    src.q5_b_reached_num
);

--Task再開
ALTER TASK LOG_USER_SCORE_TASK RESUME;

--Task実行
EXECUTE TASK LOG_USER_SCORE_TASK;

