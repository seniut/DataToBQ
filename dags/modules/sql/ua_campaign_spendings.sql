-- DROP TABLE IF EXISTS `{{ project_id }}.analytics.ua_campaign_spendings`;
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.ua_campaign_spendings` (
  hash_key STRING,
  cost_ils FLOAT64 ,
  etl_timestamp TIMESTAMP,
  FOREIGN KEY (hash_key) REFERENCES `{{ project_id }}.analytics.date_campaign_country_mapping_hash_key`(hash_key) NOT ENFORCED
);

-- TRUNCATE TABLE `{{ project_id }}.analytics.ua_campaign_spendings`;
INSERT INTO `{{ project_id }}.analytics.ua_campaign_spendings` (
  hash_key,
  cost_ils,
  etl_timestamp
)
SELECT DISTINCT
  bb.hash_key,
  b.costILS AS cost_ils,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', '{{ etl_timestamp }}') AS etl_timestamp
FROM `{{ project_id }}.data.ua_campaign_spendings` b
INNER JOIN `{{ project_id }}.analytics.date_campaign_country_mapping_hash_key` bb
        ON TO_HEX(MD5(CONCAT(IFNULL(FORMAT_DATE('%Y-%m-%d', (PARSE_DATE('%Y-%m-%d', b.date))), ''), TRIM(REGEXP_REPLACE(LOWER(IFNULL(b.campaign, '')), r'\s+', ' ')), TRIM(REGEXP_REPLACE(LOWER(IFNULL(b.country, '')), r'\s+', ' '))))) = bb.hash_key
;
