-- Enrich raw_job_posting with minimum salary by title and month (assumes posted_date is DATE)
-- Produces preprocessed_job_posting table

CREATE OR REPLACE TABLE `your-project.dataset.preprocessed_job_posting` AS
SELECT
  r.job_id,
  r.title,
  r.posted_date,
  FORMAT_DATE('%Y-%m', PARSE_DATE('%Y-%m-%d', r.posted_date)) AS year_month,
  COALESCE(m.min_salary, r.min_salary) AS min_salary,
  r.max_salary,
  r.company,
  r.location,
  r.description,
  r.ingestion_time
FROM
  `your-project.dataset.raw_job_posting` r
LEFT JOIN (
  SELECT title, year_month, MIN(min_salary) AS min_salary
  FROM `your-project.dataset.min_wage`
  GROUP BY title, year_month
) m
ON m.title = r.title
AND m.year_month = FORMAT_DATE('%Y-%m', PARSE_DATE('%Y-%m-%d', r.posted_date));
