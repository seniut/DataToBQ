--DROP TABLE IF EXISTS `{{ project_id }}.analytics.app_rating`;
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.app_rating` (
  hash_key STRING,
  date TIMESTAMP,
  app_version_code FLOAT64,
  star_rating INTEGER,
  etl_timestamp TIMESTAMP,
  FOREIGN KEY (hash_key) REFERENCES `{{ project_id }}.analytics.app_user_mapping_hash_key`(hash_key) NOT ENFORCED
);

-- TRUNCATE TABLE `{{ project_id }}.analytics.app_rating`;
INSERT INTO `{{ project_id }}.analytics.app_rating` (
  hash_key,
  date,
  app_version_code,
  star_rating,
  etl_timestamp
)
SELECT DISTINCT
  bb.hash_key,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', b.date) AS date,
  b.app_version_code,
  b.star_rating,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', '{{ etl_timestamp }}') AS etl_timestamp
FROM `{{ project_id }}.data.app_ratings` b
INNER JOIN `{{ project_id }}.analytics.app_user_mapping_hash_key` bb 
  ON TO_HEX(MD5(CONCAT(TRIM(REGEXP_REPLACE(LOWER(IFNULL(b.package, '')), r'\s+', ' ')), IFNULL(FORMAT_DATE('%Y-%m-%d', DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', b.date))), ''), IFNULL(b.app_version_name, '')))) = bb.hash_key
;