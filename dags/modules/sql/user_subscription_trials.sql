--DROP TABLE IF EXISTS `{{ project_id }}.analytics.user_subscription_trials`;
CREATE OR REPLACE TABLE`{{ project_id }}.analytics.user_subscription_trials` (
  hash_key STRING,
  user_id STRING,
  sku STRING,
  order_id STRING,
  install_timestamp TIMESTAMP,
  purchase_timestamp TIMESTAMP,
  etl_timestamp TIMESTAMP,
  FOREIGN KEY (hash_key) REFERENCES `{{ project_id }}.analytics.app_user_mapping_hash_key`(hash_key) NOT ENFORCED,
);

-- TRUNCATE TABLE `{{ project_id }}.analytics.user_subscription_trials`;
INSERT INTO `{{ project_id }}.analytics.user_subscription_trials` (
  hash_key,
  user_id,
  sku,
  order_id,
  install_timestamp,
  purchase_timestamp,
  etl_timestamp
)
SELECT DISTINCT
  bb.hash_key,
  b.userID AS user_id,
  b.sku, 
  b.orderID AS order_id, 
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S %Z', b.installTimestamp) AS install_timestamp,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S %Z', b.purchaseTimestamp) AS purchase_timestamp,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', '{{ etl_timestamp }}') AS etl_timestamp
FROM `{{ project_id }}.data.user_subscription_trials` AS b 
INNER JOIN `{{ project_id }}.analytics.app_user_mapping_hash_key` bb
  ON TO_HEX(MD5(CONCAT(TRIM(REGEXP_REPLACE(LOWER(IFNULL(b.package, '')), r'\s+', ' ')), IFNULL(FORMAT_DATE('%Y-%m-%d', DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', b.installTimestamp))), ''), IFNULL(b.version, '')))) = bb.hash_key
;
