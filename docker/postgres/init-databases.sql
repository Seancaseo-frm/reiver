-- Create the shared database for all Reiver services.
-- This script runs automatically on first Postgres start via docker-entrypoint-initdb.d.
-- The default "postgres" database is created by the POSTGRES_DB env var;
-- this is the single application database used by Watch, Flow, Pond, and Website.

CREATE DATABASE reiver;
