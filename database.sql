-- Main table matching your "Breakdown of Data Types"
CREATE TABLE drug_shortages (
    shortage_id VARCHAR(255) PRIMARY KEY, -- Unique ID to link to products
    drugName TEXT,                        -- Was generic_name
    manufacturer TEXT,                    -- Was company_name
    status TEXT,
    shortageReason TEXT,
    availabilityScore FLOAT,              -- New field (will be NULL initially)
    regionAffected TEXT DEFAULT 'USA',    -- New field (Default to USA)
    lastUpdate DATE
);

-- Child table for the specific package details (Normalization)
CREATE TABLE affected_products (
    id SERIAL PRIMARY KEY, -- Changed from AUTOINCREMENT to SERIAL for PostgreSQL
    shortage_id VARCHAR(255),
    ndc VARCHAR(50),
    presentation TEXT,
    status TEXT,
    FOREIGN KEY (shortage_id) REFERENCES drug_shortages(shortage_id)
);

-- Optimization: Index the columns you will filter by in Phase 4
CREATE INDEX idx_status ON drug_shortages(status);
CREATE INDEX idx_drug_name ON drug_shortages(drugName);


SELECT * FROM affected_products