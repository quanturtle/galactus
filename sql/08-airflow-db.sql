-- Create a separate metadata database for Airflow on the same Postgres instance.
-- Runs only on first `docker compose up` (when the pgdata volume is empty).
-- For existing volumes, run manually:
--   docker compose exec db psql -U the_scraper -c "CREATE DATABASE airflow;"
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO the_scraper;
