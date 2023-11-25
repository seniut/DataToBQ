--DROP TABLE IF EXISTS `{{ project_id }}.analytics.app_engagement_metrics`;
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.app_engagement_metrics` (
  hash_key STRING,
  country_code STRING,
  sessions FLOAT64,
  avg_session_duration FLOAT64,
  dau FLOAT64,
  wau FLOAT64,
  mau FLOAT64,
  etl_timestamp TIMESTAMP,
  FOREIGN KEY (hash_key) REFERENCES `{{ project_id }}.analytics.app_user_mapping_hash_key`(hash_key) NOT ENFORCED
);

-- TRUNCATE TABLE `{{ project_id }}.analytics.app_engagement_metrics`;
INSERT INTO `{{ project_id }}.analytics.app_engagement_metrics` (
  hash_key,
  country_code,
  sessions,
  avg_session_duration,
  dau,
  wau,
  mau,
  etl_timestamp
)
SELECT DISTINCT
  aa.hash_key,
  a.country_code,
  a.sessions,
  a.avg_session_duration,
  a.dau,
  a.wau,
  a.mau,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', '{{ etl_timestamp }}') AS etl_timestamp
FROM `{{ project_id }}.data.package_engagement_metrics` a
INNER JOIN `{{ project_id }}.analytics.app_user_mapping_hash_key` aa
  ON TO_HEX(MD5(CONCAT(TRIM(REGEXP_REPLACE(LOWER(IFNULL(a.package, '')), r'\s+', ' ')), IFNULL(FORMAT_DATE('%Y-%m-%d', (PARSE_DATE('%Y-%m-%d', a.date))), ''), IFNULL(a.version, '')))) = aa.hash_key
;