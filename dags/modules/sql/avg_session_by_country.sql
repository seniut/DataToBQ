-- DROP TABLE IF EXISTS `{{ project_id }}.analytics.avg_session_by_country`;
CREATE OR REPLACE TABLE `{{ project_id }}.analytics.avg_session_by_country` (
  country STRING,
  avg_session_duration_by_country FLOAT64,
  avg_session_by_country FLOAT64,
  avg_dau_by_country FLOAT64,
  avg_wau_by_country FLOAT64,
  avg_mau_by_country FLOAT64,
  etl_timestamp TIMESTAMP
);

-- TRUNCATE TABLE `{{ project_id }}.analytics.avg_session_by_country`;
INSERT INTO `{{ project_id }}.analytics.avg_session_by_country` (
  country,
  avg_session_duration_by_country,
  avg_session_by_country,
  avg_dau_by_country,
  avg_wau_by_country,
  avg_mau_by_country,
  etl_timestamp
)
SELECT --*
  b.country,
  ROUND(COALESCE(AVG(a.avg_session_duration), 0), 2) AS avg_session_duration_by_country,
  ROUND(COALESCE(AVG(a.sessions), 0), 2) AS avg_session_by_country,
  ROUND(COALESCE(AVG(a.dau), 0), 2) AS avg_dau_by_country,
  ROUND(COALESCE(AVG(a.wau), 0), 2) AS avg_wau_by_country,
  ROUND(COALESCE(AVG(a.mau), 0), 2) AS avg_mau_by_country,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', '{{ etl_timestamp }}') AS etl_timestamp
FROM `{{ project_id }}.analytics.app_engagement_metrics` AS a
LEFT JOIN `{{ project_id }}.data.country_code_to_name` AS b ON a.country_code = b.country_code
GROUP BY 1
;
