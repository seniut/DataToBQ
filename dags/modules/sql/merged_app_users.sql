-- DROP TABLE IF EXISTS `{{ project_id }}.analytics.merged_app_users`;
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.merged_app_users` (
  hash_key STRING,
  user_id STRING,
  hash_date_campaign_country STRING,
  -- mobile_brand_name STRING,
  -- mobile_marketing_name STRING,
  -- source STRING,
  order_id STRING,
  -- sku STRING,
  --app_version_code FLOAT64,
  star_rating INTEGER,
  country_code STRING,
  -- date TIMESTAMP,
  -- install_timestamp TIMESTAMP,
  -- purchase_timestamp TIMESTAMP
  sessions FLOAT64,
  avg_session_duration FLOAT64,
  dau FLOAT64,
  wau FLOAT64,
  mau FLOAT64,
  etl_timestamp TIMESTAMP,
  FOREIGN KEY (hash_key) REFERENCES `{{ project_id }}.analytics.app_user_mapping_hash_key`(hash_key) NOT ENFORCED,
  FOREIGN KEY (hash_date_campaign_country) REFERENCES `{{ project_id }}.analytics.date_campaign_country_mapping_hash_key`(hash_key) NOT ENFORCED
);

-- TRUNCATE TABLE  `{{ project_id }}.analytics.merged_app_users`;
INSERT INTO `{{ project_id }}.analytics.merged_app_users` (
  hash_key,
  user_id,
  hash_date_campaign_country,
  --mobile_brand_name,
  --mobile_marketing_name,
  --source,
  order_id,
  --sku,
  --install_timestamp,
  --purchase_timestamp,
  --app_version_code,
  star_rating,
  --date AS app_rating_date,
  country_code,
  sessions,
  avg_session_duration,
  dau,
  wau,
  mau,
  etl_timestamp
)
WITH user_subscription_trials_by_day AS (
  SELECT DISTINCT
    uu.hash_key,
    uu.user_id,
    uu.order_id,
    --uu.sku
  FROM `{{ project_id }}.analytics.user_subscription_trials` AS uu
),
app_rating_by_day AS (
  SELECT DISTINCT
    a.hash_key,
    --a.app_version_code,
    a.star_rating
  FROM `{{ project_id }}.analytics.app_rating` AS a
)
SELECT DISTINCT
  COALESCE(u.hash_key, uu.hash_key, a.hash_key, a.hash_key) AS hash_key,
  COALESCE(u.user_id, uu.user_id) AS user_id,
  -- u.hash_key,
  -- u.user_id,
  u.hash_date_campaign_country,
  -- u.mobile_brand_name,
  -- u.mobile_marketing_name,
  --u.source,
  uu.order_id,
  --uu.sku,
  --uu.install_timestamp,
  --uu.purchase_timestamp,
  --a.app_version_code,
  a.star_rating,
  --a.date AS app_rating_date,
  aa.country_code,
  aa.sessions,
  aa.avg_session_duration,
  aa.dau,
  aa.wau,
  aa.mau,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', '{{ etl_timestamp }}') AS etl_timestamp
FROM `{{ project_id }}.analytics.user_installs` AS u
INNER JOIN user_subscription_trials_by_day AS uu ON u.hash_key = uu.hash_key AND u.user_id = uu.user_id
INNER JOIN app_rating_by_day AS a ON a.hash_key = COALESCE(u.hash_key, uu.hash_key)
INNER JOIN `{{ project_id }}.analytics.app_engagement_metrics` AS aa ON a.hash_key = COALESCE(aa.hash_key, u.hash_key, uu.hash_key)
;
