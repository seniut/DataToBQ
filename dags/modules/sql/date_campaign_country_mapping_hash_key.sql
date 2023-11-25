--DROP TABLE IF EXISTS `{{ project_id }}.analytics.date_campaign_country_mapping_hash_key`;
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.date_campaign_country_mapping_hash_key` (
  hash_key STRING,
  date TIMESTAMP,
  campaign STRING,
  country STRING,
  etl_timestamp TIMESTAMP,
  PRIMARY KEY (hash_key) NOT ENFORCED
);

--TRUNCATE TABLE `{{ project_id }}.analytics.date_campaign_country_mapping_hash_key`;
INSERT INTO `{{ project_id }}.analytics.date_campaign_country_mapping_hash_key` (
  hash_key,
  date,
  campaign,
  country,
  etl_timestamp
)
SELECT DISTINCT
  TO_HEX(MD5(CONCAT(IFNULL(FORMAT_DATE('%Y-%m-%d', (PARSE_DATE('%Y-%m-%d', a.date))), ''), TRIM(REGEXP_REPLACE(LOWER(IFNULL(a.campaign, '')), r'\s+', ' ')), TRIM(REGEXP_REPLACE(LOWER(IFNULL(a.country, '')), r'\s+', ' '))))) AS hash_key,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', a.date || ' 00:00:00 UTC') AS date,
  a.campaign,
  a.country,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', '{{ etl_timestamp }}') AS etl_timestamp
FROM `{{ project_id }}.data.user_installs` a
UNION DISTINCT
SELECT
  TO_HEX(MD5(CONCAT(IFNULL(FORMAT_DATE('%Y-%m-%d', (PARSE_DATE('%Y-%m-%d', b.date))), ''), TRIM(REGEXP_REPLACE(LOWER(IFNULL(b.campaign, '')), r'\s+', ' ')), TRIM(REGEXP_REPLACE(LOWER(IFNULL(b.country, '')), r'\s+', ' '))))) AS hash_key,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', b.date || ' 00:00:00 UTC') AS date,
  b.campaign,
  b.country,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', '{{ etl_timestamp }}') AS etl_timestamp
FROM `{{ project_id }}.data.ua_campaign_spendings` b
;
