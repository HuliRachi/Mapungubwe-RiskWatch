-- =========================================================================
-- MAPUNGUBWE RISKWATCH: MOCK TRANSACTINAL SOURCE (OLTP)
-- This script runs inside the MySQL container to simulate Cloud SQL logs.
-- =========================================================================

CREATE DATABASE IF NOT EXISTS mapungubwe_db;
USE mapungubwe_db;

-- 1. Create Source Transactional Tables
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY,
    date VARCHAR(30) NOT NULL,
    month INT NOT NULL,
    season VARCHAR(20) NOT NULL,
    is_weekend VARCHAR(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_ranger (
    ranger_id INT PRIMARY KEY,
    ranger_name VARCHAR(100) NOT NULL,
    years_of_experience INT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_zone (
    zone_id INT PRIMARY KEY,
    zone_name VARCHAR(100) NOT NULL,
    habitat_type VARCHAR(100) NOT NULL,
    distance_from_gate INT NOT NULL,
    historical_poaching_count INT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_incidents (
    incident_id INT PRIMARY KEY,
    date_id INT NOT NULL,
    date VARCHAR(30) NOT NULL,
    zone_id INT NOT NULL,
    incident_type VARCHAR(50) NOT NULL,
    animals_involved INT NOT NULL,
    outcome VARCHAR(30) NOT NULL,
    animals VARCHAR(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_patrol (
    patrol_id INT PRIMARY KEY,
    date_id INT NOT NULL,
    date VARCHAR(30) NOT NULL,
    zone_id INT NOT NULL,
    ranger_id INT NOT NULL,
    hours_patrolled INT NOT NULL,
    sightings_count INT NOT NULL,
    risk_score DECIMAL(10,2) NOT NULL
);

-- 2. Seed Mock Records from your Kaggle Datasets
INSERT INTO dim_date (date_id, date, month, season, is_weekend) VALUES
(1, '2023-01-01', 1, 'Summer', 'True'),
(2, '2023-01-02', 1, 'Summer', 'False'),
(766, '2025-02-04', 2, 'Summer', 'False'),
(861, '2025-05-10', 5, 'Winter', 'True');

INSERT INTO dim_ranger (ranger_id, ranger_name, years_of_experience) VALUES
(1, 'Lufuno Baloyi', 7),
(2, 'Thabo Ndlovu', 4),
(34, 'Sipho Mudau', 6);

INSERT INTO dim_zone (zone_id, zone_name, habitat_type, distance_from_gate, historical_poaching_count) VALUES
(1, 'Confluence Point', 'Mopane Shrubland', 5, 2),
(2, 'Tree Top Walkway', 'Rocky Outcrop', 1, 6),
(3, 'Leokwe Camp', 'Savannah', 12, 3);

INSERT INTO fact_incidents (incident_id, date_id, date, zone_id, incident_type, animals_involved, outcome, animals) VALUES
(1, 766, '2025-02-04', 3, 'Trespassing', 1, 'Investigating', 'Elephant'),
(2, 861, '2025-05-10', 1, 'Poaching Attempt', 1, 'Neutralized', 'None');

INSERT INTO fact_patrol (patrol_id, date_id, date, zone_id, ranger_id, hours_patrolled, sightings_count, risk_score) VALUES
(1, 861, '2025-05-10', 2, 34, 7, 3, 0.38),
(2, 2, '2023-01-02', 1, 1, 3, 1, 0.58);
