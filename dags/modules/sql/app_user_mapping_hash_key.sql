--DROP TABLE IF EXISTS `{{ project_id }}.analytics.app_user_mapping_hash_key`;
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.app_user_mapping_hash_key` (
  hash_key STRING,
  package STRING,
  date TIMESTAMP,
  version STRING,
  etl_timestamp TIMESTAMP,
  PRIMARY KEY (hash_key) NOT ENFORCED
);

--TRUNCATE TABLE `{{ project_id }}.analytics.app_user_mapping_hash_key`;
INSERT INTO `{{ project_id }}.analytics.app_user_mapping_hash_key` (
  hash_key,
  package,
  date,
  version,
  etl_timestamp
)
SELECT DISTINCT
  TO_HEX(MD5(CONCAT(TRIM(REGEXP_REPLACE(LOWER(IFNULL(a.package, '')), r'\s+', ' ')), IFNULL(FORMAT_DATE('%Y-%m-%d', (PARSE_DATE('%Y-%m-%d', a.date))), ''), IFNULL(a.version, '')))) AS hash_key,
  a.package,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', a.date || ' 00:00:00 UTC') AS date,
  a.version,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', '{{ etl_timestamp }}') AS etl_timestamp
FROM `{{ project_id }}.data.package_engagement_metrics` AS a
UNION DISTINCT
SELECT
  TO_HEX(MD5(CONCAT(TRIM(REGEXP_REPLACE(LOWER(IFNULL(b.package, '')), r'\s+', ' ')), IFNULL(FORMAT_DATE('%Y-%m-%d', DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', b.date))), ''), IFNULL(b.app_version_name, '')))) AS hash_key,
  package,
  TIMESTAMP(DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', b.date))) AS date,
  b.app_version_name AS version,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', '{{ etl_timestamp }}') AS etl_timestamp
FROM `{{ project_id }}.data.app_ratings` AS b
UNION DISTINCT
SELECT
  TO_HEX(MD5(CONCAT(TRIM(REGEXP_REPLACE(LOWER(IFNULL(a.package, '')), r'\s+', ' ')), IFNULL(FORMAT_DATE('%Y-%m-%d', (PARSE_DATE('%Y-%m-%d', a.date))), ''), IFNULL(a.version, '')))) AS hash_key,
  a.package,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', a.date || ' 00:00:00 UTC') AS date,
  a.version,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', '{{ etl_timestamp }}') AS etl_timestamp
FROM `{{ project_id }}.data.user_installs` AS a
UNION DISTINCT
SELECT
  TO_HEX(MD5(CONCAT(TRIM(REGEXP_REPLACE(LOWER(IFNULL(b.package, '')), r'\s+', ' ')), IFNULL(FORMAT_DATE('%Y-%m-%d', DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', b.installTimestamp))), ''), IFNULL(b.version, '')))) AS hash_key,
  package,
  TIMESTAMP(DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', b.installTimestamp))) AS date,
  b.version AS version,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', '{{ etl_timestamp }}') AS etl_timestamp
FROM `{{ project_id }}.data.user_subscription_trials` AS b
;
