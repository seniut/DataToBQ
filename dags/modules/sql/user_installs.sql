--DROP TABLE IF EXISTS `{{ project_id }}.analytics.user_installs`;
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.user_installs` (
  hash_key STRING,
  user_id STRING,
  mobile_brand_name STRING,
  mobile_marketing_name STRING,
  hash_date_campaign_country STRING,
  -- country STRING,
  -- campaign STRING,
  source STRING,
  etl_timestamp TIMESTAMP,
  FOREIGN KEY (hash_key) REFERENCES `{{ project_id }}.analytics.app_user_mapping_hash_key`(hash_key) NOT ENFORCED,
  FOREIGN KEY (hash_date_campaign_country) REFERENCES `{{ project_id }}.analytics.date_campaign_country_mapping_hash_key`(hash_key) NOT ENFORCED
);

--TRUNCATE TABLE `{{ project_id }}.analytics.user_installs`;
INSERT INTO `{{ project_id }}.analytics.user_installs` (
  hash_key,
  user_id,
  mobile_brand_name,
  mobile_marketing_name,
  hash_date_campaign_country,
  -- country,
  -- campaign,
  source,
  etl_timestamp
)
SELECT DISTINCT
  aa.hash_key,
  a.user_id,
  a.mobile_brand_name, 
  a.mobile_marketing_name, 
  aaa.hash_key AS hash_date_campaign_country,
  a.source,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', '{{ etl_timestamp }}') AS etl_timestamp
FROM `{{ project_id }}.data.user_installs` AS a
INNER JOIN `{{ project_id }}.analytics.app_user_mapping_hash_key` AS aa 
    ON TO_HEX(MD5(CONCAT(TRIM(REGEXP_REPLACE(LOWER(IFNULL(a.package, '')), r'\s+', ' ')), IFNULL(FORMAT_DATE('%Y-%m-%d', (PARSE_DATE('%Y-%m-%d', a.date))), ''), IFNULL(a.version, '')))) = aa.hash_key
INNER JOIN `{{ project_id }}.analytics.date_campaign_country_mapping_hash_key` AS aaa
    ON TO_HEX(MD5(CONCAT(IFNULL(FORMAT_DATE('%Y-%m-%d', (PARSE_DATE('%Y-%m-%d', a.date))), ''), TRIM(REGEXP_REPLACE(LOWER(IFNULL(a.campaign, '')), r'\s+', ' ')), TRIM(REGEXP_REPLACE(LOWER(IFNULL(a.country, '')), r'\s+', ' '))))) = aaa.hash_key
;