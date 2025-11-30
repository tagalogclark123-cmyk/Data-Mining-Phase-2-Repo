
-- -- Main table matching your "Breakdown of Data Types"
-- CREATE TABLE drug_shortages (
--     shortage_id VARCHAR(255) PRIMARY KEY, -- Unique ID to link to products
--     drugName TEXT,                        -- Was generic_name
--     manufacturer TEXT,                    -- Was company_name
--     status TEXT,
--     shortageReason TEXT,
--     availabilityScore FLOAT,              -- New field (will be NULL initially)
--     regionAffected TEXT DEFAULT 'USA',    -- New field (Default to USA)
--     lastUpdate DATE
-- );

-- -- Child table for the specific package details (Normalization)
-- CREATE TABLE affected_products (
--     id SERIAL PRIMARY KEY, -- Changed from AUTOINCREMENT to SERIAL for PostgreSQL
--     shortage_id VARCHAR(255),
--     ndc VARCHAR(50),
--     presentation TEXT,
--     status TEXT,
--     FOREIGN KEY (shortage_id) REFERENCES drug_shortages(shortage_id)
-- );

-- -- Optimization: Index the columns you will filter by in Phase 4
-- CREATE INDEX idx_status ON drug_shortages(status);
-- CREATE INDEX idx_drug_name ON drug_shortages(drugName);


-- SELECT * FROM drug_shortages


-- =============================================
-- PHASE 3: Data Pipelining and Preprocessing
-- System: PostgreSQL
-- Description: Cleaning, Scoring, and Automation
-- =============================================

-- ---------------------------------------------
-- 1. HANDLING MISSING VALUES (Data Cleaning)
-- ---------------------------------------------

-- Requirement: "Handle missing values"
-- We ensured Python caught most nulls, but let's enforce a database-level default
-- for 'shortagereason' just in case raw data slipped through.

-- UPDATE drug_shortages 
-- SET shortagereason = 'Not Specified' 
-- WHERE shortagereason IS NULL OR shortagereason = '';

-- -- Let's make the 'regionaffected' more descriptive. 
-- -- Since FDA is federal, 'USA' implies 'Nationwide'.
-- UPDATE drug_shortages
-- SET regionaffected = 'Nationwide'
-- WHERE regionaffected = 'USA';


-- -- ---------------------------------------------
-- -- 2. FEATURE ENGINEERING (Calculating the Score)
-- -- ---------------------------------------------

-- -- Requirement: "Preprocess data... apply logic"
-- -- We have an empty column 'availabilityscore'. 
-- -- LOGIC: The severity of a shortage can be estimated by how many 
-- -- distinct product packages (NDCs) are affected. 
-- -- More packages down = Higher Score.

-- -- We use a correlated subquery to calculate this dynamically.
-- UPDATE drug_shortages ds
-- SET availabilityscore = (
--     SELECT COUNT(*)
--     FROM affected_products ap
--     WHERE ap.shortage_id = ds.shortage_id
-- );

-- -- Handle cases where no products were listed (set score to 0 instead of NULL)
-- UPDATE drug_shortages
-- SET availabilityscore = 0
-- WHERE availabilityscore IS NULL;


-- -- ---------------------------------------------
-- -- 3. AUTOMATION (Stored Procedure)
-- -- ---------------------------------------------

-- -- Requirement: "Create stored procedures for automation"
-- -- This procedure allows you to re-run the scoring logic automatically
-- -- whenever new data is added in the future.

-- CREATE OR REPLACE PROCEDURE refresh_shortage_scores()
-- LANGUAGE plpgsql
-- AS $$
-- BEGIN
--     -- Re-calculate scores based on current data
--     UPDATE drug_shortages ds
--     SET availabilityscore = (
--         SELECT COUNT(*)
--         FROM affected_products ap
--         WHERE ap.shortage_id = ds.shortage_id
--     );
    
--     RAISE NOTICE 'Availability scores have been refreshed.';
-- END;
-- $$;

-- -- HOW TO RUN THIS PROCEDURE:
-- -- CALL refresh_shortage_scores();


-- -- ---------------------------------------------
-- -- 4. DATASET MERGING (Creating Views)
-- -- ---------------------------------------------

-- -- Requirement: "Create views... dataset merging"
-- -- Instead of writing complex JOINs every time we want to analyze data,
-- -- we create a "Virtual Table" (View) that merges our 2NF tables back together.

-- CREATE OR REPLACE VIEW v_full_shortage_report AS
-- SELECT 
--     ds.drugname,
--     ds.manufacturer,
--     ds.status AS shortage_status,
--     ds.availabilityscore AS impact_score,
--     ap.ndc,
--     ap.presentation,
--     ap.status AS product_status,
--     ds.lastupdate
-- FROM drug_shortages ds
-- JOIN affected_products ap ON ds.shortage_id = ap.shortage_id;

-- -- Requirement: "Normalization/Aggregation"
-- -- Let's create a summary view for your "Data Analysis" phase (Phase 4).
-- -- This shows which manufacturers are having the most trouble.

-- CREATE OR REPLACE VIEW v_manufacturer_impact AS
-- SELECT 
--     manufacturer,
--     COUNT(shortage_id) AS total_shortages,
--     SUM(availabilityscore) AS total_products_affected
-- FROM drug_shortages
-- GROUP BY manufacturer
-- ORDER BY total_products_affected DESC;

SELECT drugname, availabilityscore, regionaffected 
FROM drug_shortages 
WHERE availabilityscore > 0
ORDER BY availabilityscore DESC 
LIMIT 10;

SELECT * FROM v_manufacturer_impact LIMIT 10;