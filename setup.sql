-- ============================================
-- Cortex Analyst HOL用 セットアップスクリプト
-- ============================================

-- ============================================
-- 1. アカウント設定
-- ============================================

USE ROLE ACCOUNTADMIN;

-- Cortex LLM のクロスリージョン利用を有効化
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';


-- ============================================
-- 2. ウェアハウスの作成
-- ============================================

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE      = 'SMALL'
    WAREHOUSE_TYPE      = 'STANDARD'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'Warehouse for HOL';


-- ============================================
-- 3. データベース・スキーマの作成
-- ============================================

CREATE DATABASE IF NOT EXISTS AIRLINE_DEMO;
USE DATABASE AIRLINE_DEMO;
USE SCHEMA PUBLIC;


-- ============================================
-- 4. ステージの作成
-- ============================================
-- CSVファイルアップロード用の内部ステージ

CREATE OR REPLACE STAGE AIRLINE_DEMO.PUBLIC.DEMO_STAGE
    DIRECTORY  = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT    = 'Stage for HandsOn';


-- ============================================
-- 5. Git連携の設定
-- ============================================
-- ハンズオン用アセットをGitHubから取得

CREATE OR REPLACE API INTEGRATION git_api_integration
    API_PROVIDER        = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/snow-jp-handson-org/')
    ENABLED             = TRUE;

CREATE OR REPLACE GIT REPOSITORY GIT_INTEGRATION_FOR_ANALYZE_HANDSON
    API_INTEGRATION = git_api_integration
    ORIGIN          = 'https://github.com/snow-jp-handson-org/HOL_CortexAnalyst_for_JAL.git';

-- Gitリポジトリの内容を確認
LIST @GIT_INTEGRATION_FOR_ANALYZE_HANDSON/branches/main;

-- CSVファイルをステージにコピー
COPY FILES INTO @AIRLINE_DEMO.PUBLIC.DEMO_STAGE/Data/
    FROM @GIT_INTEGRATION_FOR_ANALYZE_HANDSON/branches/main/Data/
    PATTERN = '.*\\.csv';


-- ============================================
-- 6. テーブル作成
-- ============================================

-- --------------------------------------------
-- 6-1. 顧客マスタ (CUSTOMER)
-- --------------------------------------------
-- 顧客の基本情報とJMB会員ステータスを管理

CREATE OR REPLACE TABLE AIRLINE_DEMO.PUBLIC.CUSTOMER (
    CUSTOMER_ID         NUMBER,                 -- 顧客ID（主キー）
    CUSTOMER_NAME_JP    VARCHAR,                -- 顧客名（日本語）
    JMB_TIER            VARCHAR,                -- JMB会員ランク
    COUNTRY_JP          VARCHAR,                -- 国籍（日本語）
    AGE_GROUP           VARCHAR,                -- 年齢層
    GENDER_JP           VARCHAR,                -- 性別（日本語）
    JMB_MEMBER_SINCE    DATE                    -- JMB入会日
);

-- --------------------------------------------
-- 6-2. スタッフマスタ (STAFF)
-- --------------------------------------------
-- 客室乗務員・地上スタッフの情報を管理

CREATE OR REPLACE TABLE AIRLINE_DEMO.PUBLIC.STAFF (
    STAFF_CODE          VARCHAR(16777216),      -- スタッフコード（主キー）
    STAFF_NAME_JP       VARCHAR(16777216),      -- スタッフ名（日本語）
    EMPLOYMENT_TYPE     VARCHAR(16777216),      -- 雇用形態
    HIRE_DATE           DATE,                   -- 入社日
    BIRTH_YEAR          NUMBER(38,0),           -- 生年
    GENDER_JP           VARCHAR(16777216),      -- 性別（日本語）
    AGE_GROUP           VARCHAR(16777216)       -- 年齢層
);

-- --------------------------------------------
-- 6-3. 空港マスタ (AIRPORT)
-- --------------------------------------------
-- 空港の基本情報を管理

CREATE OR REPLACE TABLE AIRLINE_DEMO.PUBLIC.AIRPORT (
    AIRPORT_ID          NUMBER(38,0),           -- 空港ID（主キー）
    AIRPORT_CODE        VARCHAR(16777216),      -- 空港コード（IATA）
    AIRPORT_NAME_JP     VARCHAR(16777216),      -- 空港名（日本語）
    COUNTRY_JP          VARCHAR(16777216),      -- 国名（日本語）
    REGION_GROUP        VARCHAR(16777216)       -- 地域グループ
);

-- --------------------------------------------
-- 6-4. 商品マスタ (PRODUCT)
-- --------------------------------------------
-- 機内販売商品の情報を管理

CREATE OR REPLACE TABLE AIRLINE_DEMO.PUBLIC.PRODUCT (
    PRODUCT_CODE        VARCHAR,                -- 商品コード（主キー）
    PRODUCT_NAME_JP     VARCHAR,                -- 商品名（日本語）
    PRODUCT_CATEGORY    VARCHAR,                -- 商品カテゴリ
    UNIT_PRICE_JPY      NUMBER                  -- 単価（日本円）
);

-- --------------------------------------------
-- 6-5. フライト運航 (FLIGHT)
-- --------------------------------------------
-- フライトの運航実績データを管理

CREATE OR REPLACE TABLE AIRLINE_DEMO.PUBLIC.FLIGHT (
    FLIGHT_ID                       NUMBER,     -- フライトID（主キー）
    FLIGHT_NUMBER                   VARCHAR,    -- 便名
    SERVICE_DATE                    DATE,       -- 運航日
    ORIGIN_AIRPORT_CODE             VARCHAR,    -- 出発空港コード
    DEST_AIRPORT_CODE               VARCHAR,    -- 到着空港コード
    SCHED_DEP_DATETIME_LOCAL        TIMESTAMP,  -- 定刻出発時刻（現地時間）
    ACTUAL_DEP_DATETIME_LOCAL       TIMESTAMP,  -- 実際出発時刻（現地時間）
    SCHED_ARR_DATETIME_LOCAL        TIMESTAMP,  -- 定刻到着時刻（現地時間）
    ACTUAL_ARR_DATETIME_LOCAL       TIMESTAMP,  -- 実際到着時刻（現地時間）
    AIRCRAFT_CODE                   VARCHAR,    -- 機材コード
    SEATS_OFFERED                   NUMBER,     -- 提供座席数
    PAX_BOARDED                     NUMBER,     -- 搭乗旅客数
    CABIN_PAX_Y                     NUMBER,     -- エコノミークラス搭乗数
    CABIN_PAX_J                     NUMBER,     -- ビジネスクラス搭乗数
    CABIN_PAX_F                     NUMBER,     -- ファーストクラス搭乗数
    CANCELLATION_FLAG               VARCHAR,    -- 欠航フラグ
    CANCELLATION_REASON_CATEGORY    VARCHAR,    -- 欠航理由カテゴリ
    ARRIVAL_DELAY_MIN               NUMBER,     -- 到着遅延（分）
    ON_TIME_ARRIVAL_FLAG            VARCHAR     -- 定時到着フラグ
);

-- --------------------------------------------
-- 6-6. 機内販売 (PURCHASE)
-- --------------------------------------------
-- 機内販売の取引データを管理

CREATE OR REPLACE TABLE AIRLINE_DEMO.PUBLIC.PURCHASE (
    SALE_ID             NUMBER,                 -- 販売ID（主キー）
    SALE_DATETIME_LOCAL TIMESTAMP,              -- 販売日時（現地時間）
    FLIGHT_ID           NUMBER,                 -- フライトID（外部キー → FLIGHT）
    CUSTOMER_ID         NUMBER,                 -- 顧客ID（外部キー → CUSTOMER）
    SELLER_STAFF_CODE   VARCHAR,                -- 販売スタッフコード（外部キー → STAFF）
    CABIN_CLASS         VARCHAR,                -- キャビンクラス（Y/J/F）
    PRODUCT_CODE        VARCHAR,                -- 商品コード（外部キー → PRODUCT）
    QUANTITY            NUMBER,                 -- 数量
    UNIT_PRICE_JPY      NUMBER,                 -- 単価（日本円）
    TOTAL_AMOUNT_JPY    NUMBER,                 -- 合計金額（日本円）
    CUSTOMER_SEGMENT    VARCHAR                 -- 顧客セグメント
);

-- --------------------------------------------
-- 6-7. 搭乗実績 (BOARDING)
-- --------------------------------------------
-- 顧客の搭乗実績データを管理

CREATE OR REPLACE TABLE AIRLINE_DEMO.PUBLIC.BOARDING (
    FLIGHT_ID           NUMBER(38,0),           -- フライトID（外部キー → FLIGHT）
    CUSTOMER_ID         NUMBER(38,0),           -- 顧客ID（外部キー → CUSTOMER）
    BOARDING_FLAG       VARCHAR(16777216),      -- 搭乗フラグ
    CABIN_CLASS         VARCHAR(16777216),      -- キャビンクラス（Y/J/F）
    TICKET_REVENUE_JPY  NUMBER(38,0)            -- チケット収入（日本円）
);

-- --------------------------------------------
-- 6-8. 乗務員アサイン (ASSIGNMENT)
-- --------------------------------------------
-- フライトへの乗務員配置を管理

CREATE OR REPLACE TABLE AIRLINE_DEMO.PUBLIC.ASSIGNMENT (
    ASSIGNMENT_ID       NUMBER(38,0),           -- アサインID（主キー）
    FLIGHT_ID           NUMBER(38,0),           -- フライトID（外部キー → FLIGHT）
    STAFF_CODE          VARCHAR(16777216),      -- スタッフコード（外部キー → STAFF）
    DUTY_ROLE           VARCHAR(16777216)       -- 役割（CP/CA等）
);


-- ============================================
-- 7. データロード
-- ============================================
-- ステージからテーブルへCSVデータをロード

COPY INTO CUSTOMER
    FROM @demo_stage/Data/customer.csv
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO STAFF
    FROM @demo_stage/Data/staff.csv
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO AIRPORT
    FROM @demo_stage/Data/airport.csv
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO PRODUCT
    FROM @demo_stage/Data/product.csv
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO FLIGHT
    FROM @demo_stage/Data/flight.csv
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO PURCHASE
    FROM @demo_stage/Data/purchase.csv
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO BOARDING
    FROM @demo_stage/Data/boarding.csv
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO ASSIGNMENT
    FROM @demo_stage/Data/assignment.csv
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


-- ============================================
-- 8. セマンティックビュー用権限設定
-- ============================================
-- PUBLICスキーマでセマンティックビューを作成・利用するための権限

-- セマンティックビューの作成権限
GRANT CREATE SEMANTIC VIEW ON SCHEMA AIRLINE_DEMO.PUBLIC TO ROLE ACCOUNTADMIN;

-- テーブルへのSELECT権限（セマンティックビューが参照するため）
GRANT SELECT ON ALL TABLES IN SCHEMA AIRLINE_DEMO.PUBLIC TO ROLE ACCOUNTADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA AIRLINE_DEMO.PUBLIC TO ROLE ACCOUNTADMIN;



-- ============================================
-- 9. Sample Appの作成
-- ============================================
-- Streamlit in Snowflakeの作成
CREATE OR REPLACE STREAMLIT sis_snowretail_analysis_dev
    FROM @GIT_INTEGRATION_FOR_ANALYZE_HANDSON/branches/main/App
    MAIN_FILE = 'Cortex Analyst Sample App.py'
    QUERY_WAREHOUSE = COMPUTE_WH;


-- ============================================
-- 10. データ確認
-- ============================================
-- 各テーブルのレコード件数を確認

SELECT 'CUSTOMER'   AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM CUSTOMER
UNION ALL
SELECT 'STAFF',                    COUNT(*)              FROM STAFF
UNION ALL
SELECT 'AIRPORT',                  COUNT(*)              FROM AIRPORT
UNION ALL
SELECT 'PRODUCT',                  COUNT(*)              FROM PRODUCT
UNION ALL
SELECT 'FLIGHT',                   COUNT(*)              FROM FLIGHT
UNION ALL
SELECT 'PURCHASE',                 COUNT(*)              FROM PURCHASE
UNION ALL
SELECT 'BOARDING',                 COUNT(*)              FROM BOARDING
UNION ALL
SELECT 'ASSIGNMENT',               COUNT(*)              FROM ASSIGNMENT;
