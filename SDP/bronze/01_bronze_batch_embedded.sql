-- Databricks Delta Live Tables (DLT) Pipeline
-- Layer: Bronze (Alternative - Static/Batch Load)
-- Purpose: For loading sample data when streaming not available
-- Author: Marcus (Data Engineer)

-- =============================================================================
-- BRONZE LAYER: Static Data Load (Alternative to Streaming)
-- Use this when source data is not in cloud files location
-- EXPANDED: 60+ claims across 6 origin years (2018-2023) for all loss types
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Bronze Table: Raw Claims (Batch)
-- Use this for batch/static data loads
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE bronze_raw_claims_batch
COMMENT "Raw claims data - Bronze Layer (Batch Load)"
TBLPROPERTIES ("quality" = "bronze")
AS 
WITH sample_claims AS (
    -- ===================== 2018 CLAIMS (10 claims) =====================
    SELECT 'CLM-2018-001' as claim_id, 'POL-12345' as policy_id, 
        DATE'2018-03-15' as loss_date, DATE'2018-03-16' as report_date,
        'John Smith' as claimant_name, 'Water Damage' as loss_type,
        'Pipe burst causing flooding' as loss_description,
        'Water damage to flooring and walls. Mold risk present.' as adjuster_notes,
        'Closed' as claim_status, 'Miami-Dade' as county, 'FL' as state, '33101' as zip_code,
        NULL as catastrophe_code
    UNION ALL SELECT 'CLM-2018-002', 'POL-12346', DATE'2018-05-20', DATE'2018-05-22',
        'Maria Garcia', 'Fire', 'Kitchen fire from stove',
        'Fire damage contained to kitchen. Smoke damage throughout.',
        'Closed', 'Broward', 'FL', '33301', NULL
    UNION ALL SELECT 'CLM-2018-003', 'POL-12347', DATE'2018-07-10', DATE'2018-07-12',
        'Robert Johnson', 'Wind', 'Storm damage to roof',
        'Roof shingles torn off. Minor interior water damage.',
        'Closed', 'Palm Beach', 'FL', '33401', NULL
    UNION ALL SELECT 'CLM-2018-004', 'POL-12348', DATE'2018-09-15', DATE'2018-09-17',
        'Sarah Williams', 'Water Damage', 'Hurricane flooding',
        'Significant water damage from storm surge. Mold developing.',
        'Closed', 'Miami-Dade', 'FL', '33102', 'CAT-2018-MICHAEL'
    UNION ALL SELECT 'CLM-2018-005', 'POL-12349', DATE'2018-09-15', DATE'2018-09-20',
        'Michael Brown', 'Wind', 'Hurricane wind damage',
        'Severe wind damage to exterior walls and roof.',
        'Closed', 'Broward', 'FL', '33302', 'CAT-2018-MICHAEL'
    UNION ALL SELECT 'CLM-2018-006', 'POL-12350', DATE'2018-04-01', DATE'2018-04-03',
        'Emily Davis', 'Theft', 'Home burglary',
        'Break-in through window. Electronics stolen.',
        'Closed', 'Orange', 'FL', '32801', NULL
    UNION ALL SELECT 'CLM-2018-007', 'POL-12351', DATE'2018-06-15', DATE'2018-06-18',
        'James Wilson', 'Liability', 'Guest injury on property',
        'Visitor fell on wet floor. Medical expenses claimed.',
        'Closed', 'Hillsborough', 'FL', '33601', NULL
    UNION ALL SELECT 'CLM-2018-008', 'POL-12352', DATE'2018-08-20', DATE'2018-08-22',
        'Patricia Moore', 'Fire', 'Electrical fire',
        'Fire from faulty wiring. Bedroom and hallway damaged.',
        'Closed', 'Duval', 'FL', '32099', NULL
    UNION ALL SELECT 'CLM-2018-009', 'POL-12353', DATE'2018-10-05', DATE'2018-10-08',
        'David Taylor', 'Water Damage', 'Roof leak',
        'Persistent roof leak causing ceiling damage.',
        'Closed', 'Miami-Dade', 'FL', '33103', NULL
    UNION ALL SELECT 'CLM-2018-010', 'POL-12354', DATE'2018-11-12', DATE'2018-11-15',
        'Jennifer Anderson', 'Theft', 'Vehicle break-in',
        'Car broken into on property. Personal items stolen.',
        'Closed', 'Broward', 'FL', '33303', NULL
    
    -- ===================== 2019 CLAIMS (10 claims) =====================
    UNION ALL SELECT 'CLM-2019-001', 'POL-22345', DATE'2019-02-10', DATE'2019-02-12',
        'Thomas White', 'Fire', 'Garage fire',
        'Fire started in garage. Vehicle and structure damaged.',
        'Closed', 'Palm Beach', 'FL', '33402', NULL
    UNION ALL SELECT 'CLM-2019-002', 'POL-22346', DATE'2019-04-15', DATE'2019-04-17',
        'Nancy Harris', 'Water Damage', 'Appliance leak',
        'Washing machine overflow. Floor and cabinet damage.',
        'Closed', 'Miami-Dade', 'FL', '33104', NULL
    UNION ALL SELECT 'CLM-2019-003', 'POL-22347', DATE'2019-06-20', DATE'2019-06-22',
        'Christopher Martin', 'Wind', 'Tropical storm damage',
        'Wind damage to fence and patio cover.',
        'Closed', 'Broward', 'FL', '33304', NULL
    UNION ALL SELECT 'CLM-2019-004', 'POL-22348', DATE'2019-07-08', DATE'2019-07-10',
        'Susan Thompson', 'Liability', 'Dog bite incident',
        'Neighbor bitten by insured dog. Lawsuit pending.',
        'Open', 'Orange', 'FL', '32802', NULL
    UNION ALL SELECT 'CLM-2019-005', 'POL-22349', DATE'2019-08-25', DATE'2019-08-27',
        'Daniel Garcia', 'Theft', 'Package theft',
        'Multiple packages stolen from porch.',
        'Closed', 'Hillsborough', 'FL', '33602', NULL
    UNION ALL SELECT 'CLM-2019-006', 'POL-22350', DATE'2019-09-10', DATE'2019-09-12',
        'Margaret Robinson', 'Fire', 'Lightning strike',
        'Lightning caused roof fire. Extensive damage.',
        'Closed', 'Duval', 'FL', '32100', NULL
    UNION ALL SELECT 'CLM-2019-007', 'POL-22351', DATE'2019-10-18', DATE'2019-10-20',
        'Paul Clark', 'Water Damage', 'Sewer backup',
        'Sewer line backup. Basement flooded.',
        'Closed', 'Miami-Dade', 'FL', '33105', NULL
    UNION ALL SELECT 'CLM-2019-008', 'POL-22352', DATE'2019-11-05', DATE'2019-11-08',
        'Linda Lewis', 'Wind', 'Fallen tree damage',
        'Neighbor tree fell on roof during storm.',
        'Closed', 'Palm Beach', 'FL', '33403', NULL
    UNION ALL SELECT 'CLM-2019-009', 'POL-22353', DATE'2019-11-22', DATE'2019-11-25',
        'Mark Walker', 'Liability', 'Pool accident',
        'Child injured in pool. Medical bills substantial.',
        'Open', 'Broward', 'FL', '33305', NULL
    UNION ALL SELECT 'CLM-2019-010', 'POL-22354', DATE'2019-12-15', DATE'2019-12-18',
        'Barbara Hall', 'Theft', 'Holiday burglary',
        'Home burglarized during vacation. Jewelry and electronics.',
        'Closed', 'Orange', 'FL', '32803', NULL
    
    -- ===================== 2020 CLAIMS (10 claims) =====================
    UNION ALL SELECT 'CLM-2020-001', 'POL-32345', DATE'2020-01-20', DATE'2020-01-22',
        'Steven Allen', 'Water Damage', 'Frozen pipe burst',
        'Cold snap caused pipe burst. Significant flooding.',
        'Closed', 'Hillsborough', 'FL', '33603', NULL
    UNION ALL SELECT 'CLM-2020-002', 'POL-32346', DATE'2020-03-10', DATE'2020-03-12',
        'Dorothy Young', 'Fire', 'Cooking fire',
        'Grease fire in kitchen. Fire extinguisher used.',
        'Closed', 'Duval', 'FL', '32101', NULL
    UNION ALL SELECT 'CLM-2020-003', 'POL-32347', DATE'2020-05-15', DATE'2020-05-17',
        'Andrew King', 'Wind', 'Severe thunderstorm',
        'High winds damaged roof and siding.',
        'Closed', 'Miami-Dade', 'FL', '33106', NULL
    UNION ALL SELECT 'CLM-2020-004', 'POL-32348', DATE'2020-06-22', DATE'2020-06-25',
        'Elizabeth Wright', 'Liability', 'Slip and fall',
        'Delivery person fell on icy walkway. Lawsuit filed.',
        'Open', 'Palm Beach', 'FL', '33404', NULL
    UNION ALL SELECT 'CLM-2020-005', 'POL-32349', DATE'2020-07-18', DATE'2020-07-20',
        'Joshua Scott', 'Theft', 'Shed break-in',
        'Tools and equipment stolen from shed.',
        'Closed', 'Broward', 'FL', '33306', NULL
    UNION ALL SELECT 'CLM-2020-006', 'POL-32350', DATE'2020-08-30', DATE'2020-09-02',
        'Karen Green', 'Water Damage', 'AC unit leak',
        'Air conditioning overflow damaged ceiling.',
        'Closed', 'Orange', 'FL', '32804', NULL
    UNION ALL SELECT 'CLM-2020-007', 'POL-32351', DATE'2020-09-16', DATE'2020-09-18',
        'Brian Adams', 'Fire', 'Dryer fire',
        'Lint buildup caused dryer fire. Laundry room damaged.',
        'Closed', 'Hillsborough', 'FL', '33604', NULL
    UNION ALL SELECT 'CLM-2020-008', 'POL-32352', DATE'2020-10-10', DATE'2020-10-12',
        'Michelle Nelson', 'Wind', 'Hurricane Sally',
        'Category 2 hurricane damage to property.',
        'Closed', 'Duval', 'FL', '32102', 'CAT-2020-SALLY'
    UNION ALL SELECT 'CLM-2020-009', 'POL-32353', DATE'2020-11-05', DATE'2020-11-08',
        'Kevin Carter', 'Liability', 'Property damage',
        'Tree from property fell on neighbor car.',
        'Closed', 'Miami-Dade', 'FL', '33107', NULL
    UNION ALL SELECT 'CLM-2020-010', 'POL-32354', DATE'2020-12-20', DATE'2020-12-23',
        'Sandra Mitchell', 'Theft', 'Identity theft',
        'Personal documents stolen. Fraud occurred.',
        'Closed', 'Palm Beach', 'FL', '33405', NULL
    
    -- ===================== 2021 CLAIMS (10 claims) =====================
    UNION ALL SELECT 'CLM-2021-001', 'POL-42345', DATE'2021-02-15', DATE'2021-02-17',
        'Ronald Perez', 'Water Damage', 'Bathroom flood',
        'Toilet overflow caused water damage to multiple rooms.',
        'Closed', 'Broward', 'FL', '33307', NULL
    UNION ALL SELECT 'CLM-2021-002', 'POL-42346', DATE'2021-04-08', DATE'2021-04-10',
        'Lisa Roberts', 'Fire', 'Candle fire',
        'Unattended candle started bedroom fire.',
        'Closed', 'Orange', 'FL', '32805', NULL
    UNION ALL SELECT 'CLM-2021-003', 'POL-42347', DATE'2021-05-22', DATE'2021-05-24',
        'Jason Turner', 'Wind', 'Tornado damage',
        'EF1 tornado damaged roof and garage.',
        'Closed', 'Hillsborough', 'FL', '33605', NULL
    UNION ALL SELECT 'CLM-2021-004', 'POL-42348', DATE'2021-06-30', DATE'2021-07-02',
        'Betty Phillips', 'Liability', 'Trampoline injury',
        'Neighbor child injured on trampoline.',
        'Open', 'Duval', 'FL', '32103', NULL
    UNION ALL SELECT 'CLM-2021-005', 'POL-42349', DATE'2021-08-14', DATE'2021-08-16',
        'Timothy Campbell', 'Theft', 'Car theft',
        'Vehicle stolen from driveway.',
        'Closed', 'Miami-Dade', 'FL', '33108', NULL
    UNION ALL SELECT 'CLM-2021-006', 'POL-42350', DATE'2021-09-01', DATE'2021-09-03',
        'Amanda Martinez', 'Wind', 'Hurricane Ida damage',
        'Roof and siding damage from Hurricane Ida.',
        'Open', 'Palm Beach', 'FL', '33406', 'CAT-2021-IDA'
    UNION ALL SELECT 'CLM-2021-007', 'POL-42351', DATE'2021-10-10', DATE'2021-10-12',
        'Jerry Parker', 'Water Damage', 'Storm water intrusion',
        'Heavy rain caused basement flooding.',
        'Closed', 'Broward', 'FL', '33308', NULL
    UNION ALL SELECT 'CLM-2021-008', 'POL-42352', DATE'2021-11-05', DATE'2021-11-08',
        'Rebecca Evans', 'Fire', 'Space heater fire',
        'Portable heater ignited curtains.',
        'Closed', 'Orange', 'FL', '32806', NULL
    UNION ALL SELECT 'CLM-2021-009', 'POL-42353', DATE'2021-11-20', DATE'2021-11-23',
        'Gary Edwards', 'Liability', 'Assault claim',
        'Altercation on property resulted in injury.',
        'Open', 'Hillsborough', 'FL', '33606', NULL
    UNION ALL SELECT 'CLM-2021-010', 'POL-42354', DATE'2021-12-10', DATE'2021-12-13',
        'Deborah Collins', 'Theft', 'Package theft ring',
        'Serial package thefts over holiday season.',
        'Closed', 'Duval', 'FL', '32104', NULL
    
    -- ===================== 2022 CLAIMS (10 claims) =====================
    UNION ALL SELECT 'CLM-2022-001', 'POL-52345', DATE'2022-01-25', DATE'2022-01-27',
        'Frank Stewart', 'Water Damage', 'Ice dam damage',
        'Ice dam caused roof leak and interior damage.',
        'Closed', 'Miami-Dade', 'FL', '33109', NULL
    UNION ALL SELECT 'CLM-2022-002', 'POL-52346', DATE'2022-03-15', DATE'2022-03-17',
        'Helen Sanchez', 'Fire', 'HVAC fire',
        'Furnace malfunction caused fire.',
        'Closed', 'Palm Beach', 'FL', '33407', NULL
    UNION ALL SELECT 'CLM-2022-003', 'POL-52347', DATE'2022-05-10', DATE'2022-05-12',
        'Raymond Morris', 'Wind', 'Straight-line winds',
        'Derecho caused widespread damage.',
        'Closed', 'Broward', 'FL', '33309', NULL
    UNION ALL SELECT 'CLM-2022-004', 'POL-52348', DATE'2022-06-28', DATE'2022-07-01',
        'Marie Rogers', 'Liability', 'Construction defect',
        'Hired contractor caused damage to neighbor.',
        'Open', 'Orange', 'FL', '32807', NULL
    UNION ALL SELECT 'CLM-2022-005', 'POL-52349', DATE'2022-08-05', DATE'2022-08-08',
        'Eugene Reed', 'Theft', 'Copper theft',
        'AC copper piping stolen.',
        'Closed', 'Hillsborough', 'FL', '33607', NULL
    UNION ALL SELECT 'CLM-2022-006', 'POL-52350', DATE'2022-09-28', DATE'2022-09-30',
        'Janet Cook', 'Wind', 'Hurricane Ian damage',
        'Major hurricane caused severe damage.',
        'Open', 'Duval', 'FL', '32105', 'CAT-2022-IAN'
    UNION ALL SELECT 'CLM-2022-007', 'POL-52351', DATE'2022-09-28', DATE'2022-10-01',
        'Henry Morgan', 'Water Damage', 'Hurricane Ian flooding',
        'Storm surge and flooding from Hurricane Ian.',
        'Open', 'Miami-Dade', 'FL', '33110', 'CAT-2022-IAN'
    UNION ALL SELECT 'CLM-2022-008', 'POL-52352', DATE'2022-10-20', DATE'2022-10-22',
        'Virginia Bell', 'Fire', 'Chimney fire',
        'Creosote buildup caused chimney fire.',
        'Closed', 'Palm Beach', 'FL', '33408', NULL
    UNION ALL SELECT 'CLM-2022-009', 'POL-52353', DATE'2022-11-15', DATE'2022-11-18',
        'Arthur Murphy', 'Liability', 'Snow removal injury',
        'Contractor injured clearing snow.',
        'Closed', 'Broward', 'FL', '33310', NULL
    UNION ALL SELECT 'CLM-2022-010', 'POL-52354', DATE'2022-12-08', DATE'2022-12-10',
        'Frances Bailey', 'Theft', 'Jewelry theft',
        'Valuable jewelry stolen during party.',
        'Closed', 'Orange', 'FL', '32808', NULL
    
    -- ===================== 2023 CLAIMS (10 claims) =====================
    UNION ALL SELECT 'CLM-2023-001', 'POL-62345', DATE'2023-02-10', DATE'2023-02-12',
        'Carl Rivera', 'Water Damage', 'Supply line failure',
        'Under-sink supply line burst.',
        'Open', 'Hillsborough', 'FL', '33608', NULL
    UNION ALL SELECT 'CLM-2023-002', 'POL-62346', DATE'2023-03-22', DATE'2023-03-24',
        'Ruth Cooper', 'Fire', 'Electrical short',
        'Old wiring caused electrical fire.',
        'Open', 'Duval', 'FL', '32106', NULL
    UNION ALL SELECT 'CLM-2023-003', 'POL-62347', DATE'2023-05-05', DATE'2023-05-07',
        'Lawrence Richardson', 'Wind', 'Microburst damage',
        'Sudden microburst damaged multiple structures.',
        'Open', 'Miami-Dade', 'FL', '33111', NULL
    UNION ALL SELECT 'CLM-2023-004', 'POL-62348', DATE'2023-06-18', DATE'2023-06-20',
        'Gloria Cox', 'Liability', 'Guest injury stairs',
        'Guest fell down stairs at party.',
        'Open', 'Palm Beach', 'FL', '33409', NULL
    UNION ALL SELECT 'CLM-2023-005', 'POL-62349', DATE'2023-07-30', DATE'2023-08-01',
        'Wayne Howard', 'Theft', 'Garage break-in',
        'Power tools and bike stolen from garage.',
        'Open', 'Broward', 'FL', '33311', NULL
    UNION ALL SELECT 'CLM-2023-006', 'POL-62350', DATE'2023-08-15', DATE'2023-08-17',
        'Joan Ward', 'Water Damage', 'Dishwasher leak',
        'Dishwasher connection failed causing flood.',
        'Open', 'Orange', 'FL', '32809', NULL
    UNION ALL SELECT 'CLM-2023-007', 'POL-62351', DATE'2023-09-10', DATE'2023-09-12',
        'Roy Torres', 'Fire', 'Grill fire spread',
        'BBQ grill fire spread to deck.',
        'Open', 'Hillsborough', 'FL', '33609', NULL
    UNION ALL SELECT 'CLM-2023-008', 'POL-62352', DATE'2023-10-05', DATE'2023-10-07',
        'Judith Peterson', 'Wind', 'Late season hurricane',
        'Hurricane caused moderate wind damage.',
        'Open', 'Duval', 'FL', '32107', NULL
    UNION ALL SELECT 'CLM-2023-009', 'POL-62353', DATE'2023-11-01', DATE'2023-11-03',
        'Philip Gray', 'Liability', 'Tree limb injury',
        'Falling limb injured passerby.',
        'Open', 'Miami-Dade', 'FL', '33112', NULL
    UNION ALL SELECT 'CLM-2023-010', 'POL-62354', DATE'2023-12-05', DATE'2023-12-07',
        'Carolyn Ramirez', 'Theft', 'Home invasion',
        'Armed robbery at residence.',
        'Open', 'Palm Beach', 'FL', '33410', NULL
)
SELECT 
    *,
    current_timestamp() AS _ingested_at,
    'sample_data' AS _source_file
FROM sample_claims;


-- -----------------------------------------------------------------------------
-- Bronze Table: Raw Payments (Batch)
-- EXPANDED: Payments for all 60 claims with realistic development patterns
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE bronze_raw_payments_batch
COMMENT "Raw payment transactions - Bronze Layer (Batch Load)"
TBLPROPERTIES ("quality" = "bronze")
AS 
WITH sample_payments AS (
    -- ===================== 2018 CLAIM PAYMENTS =====================
    -- CLM-2018-001 (Water Damage) - 4 payments over 3 years
    SELECT 'PAY-2018-001-1' as payment_id, 'CLM-2018-001' as claim_id, DATE'2018-04-15' as payment_date,
           'Indemnity' as payment_type, 15000.0 as payment_amount, 15000.0 as cumulative_paid,
           35000.0 as reserve_amount, 'Cleared' as payment_status, 'CHK-10001' as check_number,
           'John Smith' as payee_name, 'Loss Payment' as expense_type
    UNION ALL SELECT 'PAY-2018-001-2', 'CLM-2018-001', DATE'2018-08-20', 'Indemnity', 12000.0, 27000.0, 23000.0, 'Cleared', 'CHK-10015', 'John Smith', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-001-3', 'CLM-2018-001', DATE'2019-02-10', 'Indemnity', 8000.0, 35000.0, 15000.0, 'Cleared', 'CHK-10089', 'John Smith', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-001-4', 'CLM-2018-001', DATE'2020-01-15', 'Indemnity', 5000.0, 40000.0, 0.0, 'Cleared', 'CHK-10234', 'John Smith', 'Loss Payment'
    
    -- CLM-2018-002 (Fire) - 3 payments
    UNION ALL SELECT 'PAY-2018-002-1', 'CLM-2018-002', DATE'2018-06-25', 'Indemnity', 45000.0, 45000.0, 35000.0, 'Cleared', 'CHK-10020', 'Maria Garcia', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-002-2', 'CLM-2018-002', DATE'2018-10-15', 'Indemnity', 25000.0, 70000.0, 10000.0, 'Cleared', 'CHK-10045', 'Maria Garcia', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-002-3', 'CLM-2018-002', DATE'2019-03-20', 'Indemnity', 10000.0, 80000.0, 0.0, 'Cleared', 'CHK-10112', 'Maria Garcia', 'Loss Payment'
    
    -- CLM-2018-003 (Wind) - 2 payments
    UNION ALL SELECT 'PAY-2018-003-1', 'CLM-2018-003', DATE'2018-08-15', 'Indemnity', 20000.0, 20000.0, 15000.0, 'Cleared', 'CHK-10025', 'Robert Johnson', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-003-2', 'CLM-2018-003', DATE'2019-01-10', 'Indemnity', 15000.0, 35000.0, 0.0, 'Cleared', 'CHK-10067', 'Robert Johnson', 'Loss Payment'
    
    -- CLM-2018-004 (Water Damage) - 5 payments (hurricane claim with development)
    UNION ALL SELECT 'PAY-2018-004-1', 'CLM-2018-004', DATE'2018-10-20', 'Indemnity', 25000.0, 25000.0, 75000.0, 'Cleared', 'CHK-10030', 'Sarah Williams', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-004-2', 'CLM-2018-004', DATE'2019-02-15', 'Indemnity', 30000.0, 55000.0, 45000.0, 'Cleared', 'CHK-10098', 'Sarah Williams', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-004-3', 'CLM-2018-004', DATE'2019-08-10', 'Expense', 20000.0, 75000.0, 50000.0, 'Cleared', 'CHK-10156', 'Mold Remediation Co', 'Remediation'
    UNION ALL SELECT 'PAY-2018-004-4', 'CLM-2018-004', DATE'2020-04-20', 'Indemnity', 15000.0, 90000.0, 20000.0, 'Cleared', 'CHK-10298', 'Sarah Williams', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-004-5', 'CLM-2018-004', DATE'2021-02-10', 'Indemnity', 10000.0, 100000.0, 0.0, 'Cleared', 'CHK-10445', 'Sarah Williams', 'Loss Payment'
    
    -- CLM-2018-005 (Wind) - 3 payments
    UNION ALL SELECT 'PAY-2018-005-1', 'CLM-2018-005', DATE'2018-10-25', 'Indemnity', 35000.0, 35000.0, 25000.0, 'Cleared', 'CHK-10035', 'Michael Brown', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-005-2', 'CLM-2018-005', DATE'2019-04-15', 'Indemnity', 20000.0, 55000.0, 5000.0, 'Cleared', 'CHK-10134', 'Michael Brown', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-005-3', 'CLM-2018-005', DATE'2019-10-01', 'Indemnity', 5000.0, 60000.0, 0.0, 'Cleared', 'CHK-10201', 'Michael Brown', 'Loss Payment'
    
    -- CLM-2018-006 (Theft) - 2 payments
    UNION ALL SELECT 'PAY-2018-006-1', 'CLM-2018-006', DATE'2018-05-10', 'Indemnity', 8000.0, 8000.0, 4000.0, 'Cleared', 'CHK-10040', 'Emily Davis', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-006-2', 'CLM-2018-006', DATE'2018-07-20', 'Indemnity', 4000.0, 12000.0, 0.0, 'Cleared', 'CHK-10078', 'Emily Davis', 'Loss Payment'
    
    -- CLM-2018-007 (Liability) - 4 payments (lawsuit settlement)
    UNION ALL SELECT 'PAY-2018-007-1', 'CLM-2018-007', DATE'2018-08-01', 'Expense', 5000.0, 5000.0, 95000.0, 'Cleared', 'CHK-10055', 'Medical Center', 'Medical'
    UNION ALL SELECT 'PAY-2018-007-2', 'CLM-2018-007', DATE'2019-01-15', 'Expense', 15000.0, 20000.0, 80000.0, 'Cleared', 'CHK-10089', 'Legal Defense LLC', 'Legal Defense'
    UNION ALL SELECT 'PAY-2018-007-3', 'CLM-2018-007', DATE'2019-09-20', 'Indemnity', 45000.0, 65000.0, 35000.0, 'Cleared', 'CHK-10198', 'James Wilson', 'Settlement'
    UNION ALL SELECT 'PAY-2018-007-4', 'CLM-2018-007', DATE'2020-06-10', 'Indemnity', 35000.0, 100000.0, 0.0, 'Cleared', 'CHK-10356', 'James Wilson', 'Settlement'
    
    -- CLM-2018-008 (Fire) - 3 payments
    UNION ALL SELECT 'PAY-2018-008-1', 'CLM-2018-008', DATE'2018-09-25', 'Indemnity', 55000.0, 55000.0, 45000.0, 'Cleared', 'CHK-10065', 'Patricia Moore', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-008-2', 'CLM-2018-008', DATE'2019-02-20', 'Indemnity', 35000.0, 90000.0, 10000.0, 'Cleared', 'CHK-10115', 'Patricia Moore', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-008-3', 'CLM-2018-008', DATE'2019-07-15', 'Indemnity', 10000.0, 100000.0, 0.0, 'Cleared', 'CHK-10178', 'Patricia Moore', 'Loss Payment'
    
    -- CLM-2018-009 (Water Damage) - 2 payments
    UNION ALL SELECT 'PAY-2018-009-1', 'CLM-2018-009', DATE'2018-11-15', 'Indemnity', 18000.0, 18000.0, 12000.0, 'Cleared', 'CHK-10075', 'David Taylor', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-009-2', 'CLM-2018-009', DATE'2019-03-10', 'Indemnity', 12000.0, 30000.0, 0.0, 'Cleared', 'CHK-10125', 'David Taylor', 'Loss Payment'
    
    -- CLM-2018-010 (Theft) - 1 payment
    UNION ALL SELECT 'PAY-2018-010-1', 'CLM-2018-010', DATE'2018-12-20', 'Indemnity', 5000.0, 5000.0, 0.0, 'Cleared', 'CHK-10085', 'Jennifer Anderson', 'Loss Payment'
    
    -- ===================== 2019 CLAIM PAYMENTS =====================
    -- CLM-2019-001 (Fire) - 3 payments
    UNION ALL SELECT 'PAY-2019-001-1', 'CLM-2019-001', DATE'2019-03-15', 'Indemnity', 40000.0, 40000.0, 40000.0, 'Cleared', 'CHK-11001', 'Thomas White', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-001-2', 'CLM-2019-001', DATE'2019-07-20', 'Indemnity', 30000.0, 70000.0, 10000.0, 'Cleared', 'CHK-11056', 'Thomas White', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-001-3', 'CLM-2019-001', DATE'2020-01-10', 'Indemnity', 10000.0, 80000.0, 0.0, 'Cleared', 'CHK-11145', 'Thomas White', 'Loss Payment'
    
    -- CLM-2019-002 (Water Damage) - 2 payments
    UNION ALL SELECT 'PAY-2019-002-1', 'CLM-2019-002', DATE'2019-05-20', 'Indemnity', 12000.0, 12000.0, 8000.0, 'Cleared', 'CHK-11015', 'Nancy Harris', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-002-2', 'CLM-2019-002', DATE'2019-09-15', 'Indemnity', 8000.0, 20000.0, 0.0, 'Cleared', 'CHK-11078', 'Nancy Harris', 'Loss Payment'
    
    -- CLM-2019-003 (Wind) - 2 payments
    UNION ALL SELECT 'PAY-2019-003-1', 'CLM-2019-003', DATE'2019-07-25', 'Indemnity', 15000.0, 15000.0, 10000.0, 'Cleared', 'CHK-11030', 'Christopher Martin', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-003-2', 'CLM-2019-003', DATE'2019-11-10', 'Indemnity', 10000.0, 25000.0, 0.0, 'Cleared', 'CHK-11098', 'Christopher Martin', 'Loss Payment'
    
    -- CLM-2019-004 (Liability) - 3 payments (ongoing lawsuit)
    UNION ALL SELECT 'PAY-2019-004-1', 'CLM-2019-004', DATE'2019-08-15', 'Expense', 8000.0, 8000.0, 92000.0, 'Cleared', 'CHK-11045', 'Veterinary Hospital', 'Medical'
    UNION ALL SELECT 'PAY-2019-004-2', 'CLM-2019-004', DATE'2020-03-20', 'Expense', 20000.0, 28000.0, 72000.0, 'Cleared', 'CHK-11189', 'Legal Defense LLC', 'Legal Defense'
    UNION ALL SELECT 'PAY-2019-004-3', 'CLM-2019-004', DATE'2021-01-15', 'Indemnity', 50000.0, 78000.0, 22000.0, 'Cleared', 'CHK-11356', 'Susan Thompson', 'Settlement'
    
    -- CLM-2019-005 (Theft) - 1 payment
    UNION ALL SELECT 'PAY-2019-005-1', 'CLM-2019-005', DATE'2019-09-30', 'Indemnity', 2500.0, 2500.0, 0.0, 'Cleared', 'CHK-11065', 'Daniel Garcia', 'Loss Payment'
    
    -- CLM-2019-006 (Fire) - 4 payments
    UNION ALL SELECT 'PAY-2019-006-1', 'CLM-2019-006', DATE'2019-10-20', 'Indemnity', 50000.0, 50000.0, 70000.0, 'Cleared', 'CHK-11085', 'Margaret Robinson', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-006-2', 'CLM-2019-006', DATE'2020-02-15', 'Indemnity', 40000.0, 90000.0, 30000.0, 'Cleared', 'CHK-11167', 'Margaret Robinson', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-006-3', 'CLM-2019-006', DATE'2020-08-10', 'Indemnity', 20000.0, 110000.0, 10000.0, 'Cleared', 'CHK-11278', 'Margaret Robinson', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-006-4', 'CLM-2019-006', DATE'2021-03-05', 'Indemnity', 10000.0, 120000.0, 0.0, 'Cleared', 'CHK-11398', 'Margaret Robinson', 'Loss Payment'
    
    -- CLM-2019-007 (Water Damage) - 3 payments
    UNION ALL SELECT 'PAY-2019-007-1', 'CLM-2019-007', DATE'2019-11-25', 'Indemnity', 22000.0, 22000.0, 28000.0, 'Cleared', 'CHK-11102', 'Paul Clark', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-007-2', 'CLM-2019-007', DATE'2020-04-20', 'Indemnity', 18000.0, 40000.0, 10000.0, 'Cleared', 'CHK-11212', 'Paul Clark', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-007-3', 'CLM-2019-007', DATE'2020-10-15', 'Indemnity', 10000.0, 50000.0, 0.0, 'Cleared', 'CHK-11312', 'Paul Clark', 'Loss Payment'
    
    -- CLM-2019-008 (Wind) - 2 payments
    UNION ALL SELECT 'PAY-2019-008-1', 'CLM-2019-008', DATE'2019-12-10', 'Indemnity', 28000.0, 28000.0, 22000.0, 'Cleared', 'CHK-11118', 'Linda Lewis', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-008-2', 'CLM-2019-008', DATE'2020-05-25', 'Indemnity', 22000.0, 50000.0, 0.0, 'Cleared', 'CHK-11245', 'Linda Lewis', 'Loss Payment'
    
    -- CLM-2019-009 (Liability) - 3 payments
    UNION ALL SELECT 'PAY-2019-009-1', 'CLM-2019-009', DATE'2020-01-20', 'Expense', 25000.0, 25000.0, 125000.0, 'Cleared', 'CHK-11135', 'Hospital', 'Medical'
    UNION ALL SELECT 'PAY-2019-009-2', 'CLM-2019-009', DATE'2020-09-15', 'Expense', 30000.0, 55000.0, 95000.0, 'Cleared', 'CHK-11298', 'Legal Team', 'Legal Defense'
    UNION ALL SELECT 'PAY-2019-009-3', 'CLM-2019-009', DATE'2021-06-10', 'Indemnity', 75000.0, 130000.0, 20000.0, 'Cleared', 'CHK-11456', 'Mark Walker', 'Settlement'
    
    -- CLM-2019-010 (Theft) - 2 payments
    UNION ALL SELECT 'PAY-2019-010-1', 'CLM-2019-010', DATE'2020-01-20', 'Indemnity', 15000.0, 15000.0, 10000.0, 'Cleared', 'CHK-11145', 'Barbara Hall', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-010-2', 'CLM-2019-010', DATE'2020-06-15', 'Indemnity', 10000.0, 25000.0, 0.0, 'Cleared', 'CHK-11267', 'Barbara Hall', 'Loss Payment'
    
    -- ===================== 2020 CLAIM PAYMENTS =====================
    -- CLM-2020-001 (Water Damage) - 3 payments
    UNION ALL SELECT 'PAY-2020-001-1', 'CLM-2020-001', DATE'2020-02-25', 'Indemnity', 18000.0, 18000.0, 22000.0, 'Cleared', 'CHK-12001', 'Steven Allen', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-001-2', 'CLM-2020-001', DATE'2020-06-20', 'Indemnity', 15000.0, 33000.0, 7000.0, 'Cleared', 'CHK-12078', 'Steven Allen', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-001-3', 'CLM-2020-001', DATE'2021-01-10', 'Indemnity', 7000.0, 40000.0, 0.0, 'Cleared', 'CHK-12189', 'Steven Allen', 'Loss Payment'
    
    -- CLM-2020-002 (Fire) - 3 payments
    UNION ALL SELECT 'PAY-2020-002-1', 'CLM-2020-002', DATE'2020-04-15', 'Indemnity', 25000.0, 25000.0, 25000.0, 'Cleared', 'CHK-12015', 'Dorothy Young', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-002-2', 'CLM-2020-002', DATE'2020-08-20', 'Indemnity', 18000.0, 43000.0, 7000.0, 'Cleared', 'CHK-12098', 'Dorothy Young', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-002-3', 'CLM-2020-002', DATE'2021-02-15', 'Indemnity', 7000.0, 50000.0, 0.0, 'Cleared', 'CHK-12212', 'Dorothy Young', 'Loss Payment'
    
    -- CLM-2020-003 (Wind) - 2 payments
    UNION ALL SELECT 'PAY-2020-003-1', 'CLM-2020-003', DATE'2020-06-20', 'Indemnity', 22000.0, 22000.0, 18000.0, 'Cleared', 'CHK-12035', 'Andrew King', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-003-2', 'CLM-2020-003', DATE'2020-11-15', 'Indemnity', 18000.0, 40000.0, 0.0, 'Cleared', 'CHK-12145', 'Andrew King', 'Loss Payment'
    
    -- CLM-2020-004 (Liability) - 2 payments (ongoing)
    UNION ALL SELECT 'PAY-2020-004-1', 'CLM-2020-004', DATE'2020-08-15', 'Expense', 15000.0, 15000.0, 85000.0, 'Cleared', 'CHK-12065', 'Legal Firm', 'Legal Defense'
    UNION ALL SELECT 'PAY-2020-004-2', 'CLM-2020-004', DATE'2021-04-20', 'Expense', 25000.0, 40000.0, 60000.0, 'Cleared', 'CHK-12278', 'Legal Firm', 'Legal Defense'
    
    -- CLM-2020-005 (Theft) - 2 payments
    UNION ALL SELECT 'PAY-2020-005-1', 'CLM-2020-005', DATE'2020-08-25', 'Indemnity', 6000.0, 6000.0, 4000.0, 'Cleared', 'CHK-12075', 'Joshua Scott', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-005-2', 'CLM-2020-005', DATE'2020-12-10', 'Indemnity', 4000.0, 10000.0, 0.0, 'Cleared', 'CHK-12165', 'Joshua Scott', 'Loss Payment'
    
    -- CLM-2020-006 (Water Damage) - 2 payments
    UNION ALL SELECT 'PAY-2020-006-1', 'CLM-2020-006', DATE'2020-10-05', 'Indemnity', 10000.0, 10000.0, 8000.0, 'Cleared', 'CHK-12095', 'Karen Green', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-006-2', 'CLM-2020-006', DATE'2021-03-15', 'Indemnity', 8000.0, 18000.0, 0.0, 'Cleared', 'CHK-12245', 'Karen Green', 'Loss Payment'
    
    -- CLM-2020-007 (Fire) - 2 payments
    UNION ALL SELECT 'PAY-2020-007-1', 'CLM-2020-007', DATE'2020-10-25', 'Indemnity', 20000.0, 20000.0, 15000.0, 'Cleared', 'CHK-12110', 'Brian Adams', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-007-2', 'CLM-2020-007', DATE'2021-04-10', 'Indemnity', 15000.0, 35000.0, 0.0, 'Cleared', 'CHK-12267', 'Brian Adams', 'Loss Payment'
    
    -- CLM-2020-008 (Wind) - 3 payments (hurricane)
    UNION ALL SELECT 'PAY-2020-008-1', 'CLM-2020-008', DATE'2020-11-15', 'Indemnity', 35000.0, 35000.0, 45000.0, 'Cleared', 'CHK-12125', 'Michelle Nelson', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-008-2', 'CLM-2020-008', DATE'2021-05-20', 'Indemnity', 30000.0, 65000.0, 15000.0, 'Cleared', 'CHK-12312', 'Michelle Nelson', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-008-3', 'CLM-2020-008', DATE'2022-01-10', 'Indemnity', 15000.0, 80000.0, 0.0, 'Cleared', 'CHK-12445', 'Michelle Nelson', 'Loss Payment'
    
    -- CLM-2020-009 (Liability) - 2 payments
    UNION ALL SELECT 'PAY-2020-009-1', 'CLM-2020-009', DATE'2020-12-10', 'Indemnity', 8000.0, 8000.0, 7000.0, 'Cleared', 'CHK-12155', 'Kevin Carter', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-009-2', 'CLM-2020-009', DATE'2021-06-15', 'Indemnity', 7000.0, 15000.0, 0.0, 'Cleared', 'CHK-12345', 'Kevin Carter', 'Loss Payment'
    
    -- CLM-2020-010 (Theft) - 2 payments
    UNION ALL SELECT 'PAY-2020-010-1', 'CLM-2020-010', DATE'2021-01-25', 'Indemnity', 5000.0, 5000.0, 5000.0, 'Cleared', 'CHK-12175', 'Sandra Mitchell', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-010-2', 'CLM-2020-010', DATE'2021-07-10', 'Indemnity', 5000.0, 10000.0, 0.0, 'Cleared', 'CHK-12378', 'Sandra Mitchell', 'Loss Payment'
    
    -- ===================== 2021 CLAIM PAYMENTS =====================
    -- CLM-2021-001 (Water Damage) - 2 payments
    UNION ALL SELECT 'PAY-2021-001-1', 'CLM-2021-001', DATE'2021-03-20', 'Indemnity', 15000.0, 15000.0, 15000.0, 'Cleared', 'CHK-13001', 'Ronald Perez', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-001-2', 'CLM-2021-001', DATE'2021-08-15', 'Indemnity', 12000.0, 27000.0, 3000.0, 'Cleared', 'CHK-13089', 'Ronald Perez', 'Loss Payment'
    
    -- CLM-2021-002 (Fire) - 2 payments
    UNION ALL SELECT 'PAY-2021-002-1', 'CLM-2021-002', DATE'2021-05-15', 'Indemnity', 30000.0, 30000.0, 25000.0, 'Cleared', 'CHK-13020', 'Lisa Roberts', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-002-2', 'CLM-2021-002', DATE'2021-10-20', 'Indemnity', 22000.0, 52000.0, 3000.0, 'Cleared', 'CHK-13125', 'Lisa Roberts', 'Loss Payment'
    
    -- CLM-2021-003 (Wind) - 2 payments
    UNION ALL SELECT 'PAY-2021-003-1', 'CLM-2021-003', DATE'2021-06-25', 'Indemnity', 40000.0, 40000.0, 30000.0, 'Cleared', 'CHK-13035', 'Jason Turner', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-003-2', 'CLM-2021-003', DATE'2022-01-15', 'Indemnity', 25000.0, 65000.0, 5000.0, 'Cleared', 'CHK-13198', 'Jason Turner', 'Loss Payment'
    
    -- CLM-2021-004 (Liability) - 2 payments
    UNION ALL SELECT 'PAY-2021-004-1', 'CLM-2021-004', DATE'2021-08-10', 'Expense', 12000.0, 12000.0, 68000.0, 'Cleared', 'CHK-13055', 'Medical Center', 'Medical'
    UNION ALL SELECT 'PAY-2021-004-2', 'CLM-2021-004', DATE'2022-03-20', 'Expense', 18000.0, 30000.0, 50000.0, 'Cleared', 'CHK-13267', 'Legal Team', 'Legal Defense'
    
    -- CLM-2021-005 (Theft) - 1 payment
    UNION ALL SELECT 'PAY-2021-005-1', 'CLM-2021-005', DATE'2021-09-20', 'Indemnity', 18000.0, 18000.0, 0.0, 'Cleared', 'CHK-13075', 'Timothy Campbell', 'Loss Payment'
    
    -- CLM-2021-006 (Wind) - 2 payments (hurricane)
    UNION ALL SELECT 'PAY-2021-006-1', 'CLM-2021-006', DATE'2021-10-20', 'Indemnity', 25000.0, 25000.0, 55000.0, 'Cleared', 'CHK-13095', 'Amanda Martinez', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-006-2', 'CLM-2021-006', DATE'2022-04-15', 'Indemnity', 35000.0, 60000.0, 20000.0, 'Cleared', 'CHK-13298', 'Amanda Martinez', 'Loss Payment'
    
    -- CLM-2021-007 (Water Damage) - 2 payments
    UNION ALL SELECT 'PAY-2021-007-1', 'CLM-2021-007', DATE'2021-11-15', 'Indemnity', 16000.0, 16000.0, 14000.0, 'Cleared', 'CHK-13110', 'Jerry Parker', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-007-2', 'CLM-2021-007', DATE'2022-05-10', 'Indemnity', 12000.0, 28000.0, 2000.0, 'Cleared', 'CHK-13312', 'Jerry Parker', 'Loss Payment'
    
    -- CLM-2021-008 (Fire) - 2 payments
    UNION ALL SELECT 'PAY-2021-008-1', 'CLM-2021-008', DATE'2021-12-10', 'Indemnity', 22000.0, 22000.0, 18000.0, 'Cleared', 'CHK-13125', 'Rebecca Evans', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-008-2', 'CLM-2021-008', DATE'2022-06-15', 'Indemnity', 15000.0, 37000.0, 3000.0, 'Cleared', 'CHK-13356', 'Rebecca Evans', 'Loss Payment'
    
    -- CLM-2021-009 (Liability) - 1 payment (ongoing)
    UNION ALL SELECT 'PAY-2021-009-1', 'CLM-2021-009', DATE'2022-02-20', 'Expense', 20000.0, 20000.0, 80000.0, 'Cleared', 'CHK-13178', 'Legal Defense', 'Legal Defense'
    
    -- CLM-2021-010 (Theft) - 1 payment
    UNION ALL SELECT 'PAY-2021-010-1', 'CLM-2021-010', DATE'2022-01-15', 'Indemnity', 3500.0, 3500.0, 0.0, 'Cleared', 'CHK-13145', 'Deborah Collins', 'Loss Payment'
    
    -- ===================== 2022 CLAIM PAYMENTS =====================
    -- CLM-2022-001 (Water Damage) - 2 payments
    UNION ALL SELECT 'PAY-2022-001-1', 'CLM-2022-001', DATE'2022-03-01', 'Indemnity', 14000.0, 14000.0, 16000.0, 'Cleared', 'CHK-14001', 'Frank Stewart', 'Loss Payment'
    UNION ALL SELECT 'PAY-2022-001-2', 'CLM-2022-001', DATE'2022-08-15', 'Indemnity', 11000.0, 25000.0, 5000.0, 'Cleared', 'CHK-14098', 'Frank Stewart', 'Loss Payment'
    
    -- CLM-2022-002 (Fire) - 2 payments
    UNION ALL SELECT 'PAY-2022-002-1', 'CLM-2022-002', DATE'2022-04-20', 'Indemnity', 45000.0, 45000.0, 35000.0, 'Cleared', 'CHK-14025', 'Helen Sanchez', 'Loss Payment'
    UNION ALL SELECT 'PAY-2022-002-2', 'CLM-2022-002', DATE'2022-10-10', 'Indemnity', 28000.0, 73000.0, 7000.0, 'Cleared', 'CHK-14145', 'Helen Sanchez', 'Loss Payment'
    
    -- CLM-2022-003 (Wind) - 2 payments
    UNION ALL SELECT 'PAY-2022-003-1', 'CLM-2022-003', DATE'2022-06-15', 'Indemnity', 20000.0, 20000.0, 20000.0, 'Cleared', 'CHK-14045', 'Raymond Morris', 'Loss Payment'
    UNION ALL SELECT 'PAY-2022-003-2', 'CLM-2022-003', DATE'2022-12-01', 'Indemnity', 18000.0, 38000.0, 2000.0, 'Cleared', 'CHK-14178', 'Raymond Morris', 'Loss Payment'
    
    -- CLM-2022-004 (Liability) - 1 payment (ongoing)
    UNION ALL SELECT 'PAY-2022-004-1', 'CLM-2022-004', DATE'2022-09-10', 'Expense', 15000.0, 15000.0, 85000.0, 'Cleared', 'CHK-14078', 'Legal Services', 'Legal Defense'
    
    -- CLM-2022-005 (Theft) - 1 payment
    UNION ALL SELECT 'PAY-2022-005-1', 'CLM-2022-005', DATE'2022-09-15', 'Indemnity', 4500.0, 4500.0, 0.0, 'Cleared', 'CHK-14085', 'Eugene Reed', 'Loss Payment'
    
    -- CLM-2022-006 (Wind) - 2 payments (hurricane)
    UNION ALL SELECT 'PAY-2022-006-1', 'CLM-2022-006', DATE'2022-11-10', 'Indemnity', 55000.0, 55000.0, 65000.0, 'Cleared', 'CHK-14125', 'Janet Cook', 'Loss Payment'
    UNION ALL SELECT 'PAY-2022-006-2', 'CLM-2022-006', DATE'2023-05-20', 'Indemnity', 40000.0, 95000.0, 25000.0, 'Cleared', 'CHK-14312', 'Janet Cook', 'Loss Payment'
    
    -- CLM-2022-007 (Water Damage) - 2 payments (hurricane)
    UNION ALL SELECT 'PAY-2022-007-1', 'CLM-2022-007', DATE'2022-11-15', 'Indemnity', 35000.0, 35000.0, 65000.0, 'Cleared', 'CHK-14135', 'Henry Morgan', 'Loss Payment'
    UNION ALL SELECT 'PAY-2022-007-2', 'CLM-2022-007', DATE'2023-06-10', 'Indemnity', 40000.0, 75000.0, 25000.0, 'Cleared', 'CHK-14356', 'Henry Morgan', 'Loss Payment'
    
    -- CLM-2022-008 (Fire) - 1 payment
    UNION ALL SELECT 'PAY-2022-008-1', 'CLM-2022-008', DATE'2022-12-01', 'Indemnity', 32000.0, 32000.0, 18000.0, 'Cleared', 'CHK-14155', 'Virginia Bell', 'Loss Payment'
    
    -- CLM-2022-009 (Liability) - 1 payment
    UNION ALL SELECT 'PAY-2022-009-1', 'CLM-2022-009', DATE'2022-12-20', 'Indemnity', 12000.0, 12000.0, 0.0, 'Cleared', 'CHK-14175', 'Arthur Murphy', 'Loss Payment'
    
    -- CLM-2022-010 (Theft) - 1 payment
    UNION ALL SELECT 'PAY-2022-010-1', 'CLM-2022-010', DATE'2023-01-15', 'Indemnity', 8000.0, 8000.0, 0.0, 'Cleared', 'CHK-14195', 'Frances Bailey', 'Loss Payment'
    
    -- ===================== 2023 CLAIM PAYMENTS =====================
    -- CLM-2023-001 (Water Damage) - 1 payment (recent)
    UNION ALL SELECT 'PAY-2023-001-1', 'CLM-2023-001', DATE'2023-03-15', 'Indemnity', 12000.0, 12000.0, 18000.0, 'Cleared', 'CHK-15001', 'Carl Rivera', 'Loss Payment'
    
    -- CLM-2023-002 (Fire) - 1 payment (recent)
    UNION ALL SELECT 'PAY-2023-002-1', 'CLM-2023-002', DATE'2023-05-01', 'Indemnity', 35000.0, 35000.0, 45000.0, 'Cleared', 'CHK-15015', 'Ruth Cooper', 'Loss Payment'
    
    -- CLM-2023-003 (Wind) - 1 payment (recent)
    UNION ALL SELECT 'PAY-2023-003-1', 'CLM-2023-003', DATE'2023-06-10', 'Indemnity', 28000.0, 28000.0, 32000.0, 'Cleared', 'CHK-15030', 'Lawrence Richardson', 'Loss Payment'
    
    -- CLM-2023-004 (Liability) - 1 payment (recent)
    UNION ALL SELECT 'PAY-2023-004-1', 'CLM-2023-004', DATE'2023-08-01', 'Expense', 10000.0, 10000.0, 70000.0, 'Cleared', 'CHK-15045', 'Medical Provider', 'Medical'
    
    -- CLM-2023-005 (Theft) - 1 payment (recent)
    UNION ALL SELECT 'PAY-2023-005-1', 'CLM-2023-005', DATE'2023-09-10', 'Indemnity', 5500.0, 5500.0, 2500.0, 'Cleared', 'CHK-15055', 'Wayne Howard', 'Loss Payment'
    
    -- CLM-2023-006 (Water Damage) - 1 payment (recent)
    UNION ALL SELECT 'PAY-2023-006-1', 'CLM-2023-006', DATE'2023-09-20', 'Indemnity', 9000.0, 9000.0, 11000.0, 'Cleared', 'CHK-15065', 'Joan Ward', 'Loss Payment'
    
    -- CLM-2023-007 (Fire) - 1 payment (recent)
    UNION ALL SELECT 'PAY-2023-007-1', 'CLM-2023-007', DATE'2023-10-15', 'Indemnity', 18000.0, 18000.0, 22000.0, 'Cleared', 'CHK-15075', 'Roy Torres', 'Loss Payment'
    
    -- CLM-2023-008 (Wind) - 1 payment (recent)
    UNION ALL SELECT 'PAY-2023-008-1', 'CLM-2023-008', DATE'2023-11-10', 'Indemnity', 22000.0, 22000.0, 28000.0, 'Cleared', 'CHK-15085', 'Judith Peterson', 'Loss Payment'
    
    -- CLM-2023-009 (Liability) - 1 payment (recent, ongoing)
    UNION ALL SELECT 'PAY-2023-009-1', 'CLM-2023-009', DATE'2023-12-05', 'Expense', 8000.0, 8000.0, 72000.0, 'Cleared', 'CHK-15095', 'Legal Team', 'Legal Defense'
    
    -- CLM-2023-010 (Theft) - 1 payment (recent)
    UNION ALL SELECT 'PAY-2023-010-1', 'CLM-2023-010', DATE'2024-01-10', 'Indemnity', 15000.0, 15000.0, 10000.0, 'Cleared', 'CHK-15105', 'Carolyn Ramirez', 'Loss Payment'
    
    -- ===================== ADDITIONAL DEVELOPMENT PAYMENTS =====================
    -- Adding more payments to extend development patterns for all loss types
    -- This creates fuller triangles with payments in development periods 2, 3, 4, 5
    
    -- === ADDITIONAL THEFT PAYMENTS (extending development) ===
    -- 2018 Theft claims - additional late payments
    UNION ALL SELECT 'PAY-2018-006-3', 'CLM-2018-006', DATE'2019-04-15', 'Indemnity', 3000.0, 15000.0, 0.0, 'Cleared', 'CHK-10456', 'Emily Davis', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-010-2', 'CLM-2018-010', DATE'2019-06-20', 'Indemnity', 3500.0, 8500.0, 0.0, 'Cleared', 'CHK-10567', 'Jennifer Anderson', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-010-3', 'CLM-2018-010', DATE'2020-03-10', 'Indemnity', 2000.0, 10500.0, 0.0, 'Cleared', 'CHK-10678', 'Jennifer Anderson', 'Loss Payment'
    
    -- 2019 Theft claims - additional payments
    UNION ALL SELECT 'PAY-2019-005-2', 'CLM-2019-005', DATE'2020-08-15', 'Indemnity', 1500.0, 4000.0, 0.0, 'Cleared', 'CHK-11456', 'Daniel Garcia', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-010-3', 'CLM-2019-010', DATE'2021-04-20', 'Indemnity', 5000.0, 30000.0, 0.0, 'Cleared', 'CHK-11567', 'Barbara Hall', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-010-4', 'CLM-2019-010', DATE'2022-02-15', 'Indemnity', 3000.0, 33000.0, 0.0, 'Cleared', 'CHK-11678', 'Barbara Hall', 'Loss Payment'
    
    -- 2020 Theft claims - additional payments  
    UNION ALL SELECT 'PAY-2020-005-3', 'CLM-2020-005', DATE'2021-06-25', 'Indemnity', 2500.0, 12500.0, 0.0, 'Cleared', 'CHK-12456', 'Joshua Scott', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-005-4', 'CLM-2020-005', DATE'2022-04-10', 'Indemnity', 1500.0, 14000.0, 0.0, 'Cleared', 'CHK-12567', 'Joshua Scott', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-010-3', 'CLM-2020-010', DATE'2022-05-20', 'Indemnity', 4000.0, 14000.0, 0.0, 'Cleared', 'CHK-12678', 'Sandra Mitchell', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-010-4', 'CLM-2020-010', DATE'2023-03-15', 'Indemnity', 2500.0, 16500.0, 0.0, 'Cleared', 'CHK-12789', 'Sandra Mitchell', 'Loss Payment'
    
    -- 2021 Theft claims - additional payments
    UNION ALL SELECT 'PAY-2021-005-2', 'CLM-2021-005', DATE'2022-08-10', 'Indemnity', 4000.0, 22000.0, 0.0, 'Cleared', 'CHK-13456', 'Timothy Campbell', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-005-3', 'CLM-2021-005', DATE'2023-06-20', 'Indemnity', 2500.0, 24500.0, 0.0, 'Cleared', 'CHK-13567', 'Timothy Campbell', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-010-2', 'CLM-2021-010', DATE'2023-01-25', 'Indemnity', 2000.0, 5500.0, 0.0, 'Cleared', 'CHK-13678', 'Deborah Collins', 'Loss Payment'
    
    -- 2022 Theft claims - additional payments
    UNION ALL SELECT 'PAY-2022-005-2', 'CLM-2022-005', DATE'2023-07-15', 'Indemnity', 2500.0, 7000.0, 0.0, 'Cleared', 'CHK-14456', 'Eugene Reed', 'Loss Payment'
    UNION ALL SELECT 'PAY-2022-010-2', 'CLM-2022-010', DATE'2023-10-20', 'Indemnity', 4000.0, 12000.0, 0.0, 'Cleared', 'CHK-14567', 'Frances Bailey', 'Loss Payment'
    
    -- === ADDITIONAL LIABILITY PAYMENTS (extending development - lawsuits take years) ===
    -- 2018 Liability - more settlement payments
    UNION ALL SELECT 'PAY-2018-007-5', 'CLM-2018-007', DATE'2021-03-15', 'Expense', 15000.0, 115000.0, 0.0, 'Cleared', 'CHK-10789', 'Legal Defense LLC', 'Legal Defense'
    UNION ALL SELECT 'PAY-2018-007-6', 'CLM-2018-007', DATE'2022-01-20', 'Indemnity', 10000.0, 125000.0, 0.0, 'Cleared', 'CHK-10890', 'James Wilson', 'Settlement'
    
    -- 2019 Liability - extended litigation
    UNION ALL SELECT 'PAY-2019-004-4', 'CLM-2019-004', DATE'2022-06-15', 'Indemnity', 20000.0, 98000.0, 0.0, 'Cleared', 'CHK-11789', 'Susan Thompson', 'Settlement'
    UNION ALL SELECT 'PAY-2019-004-5', 'CLM-2019-004', DATE'2023-04-10', 'Expense', 8000.0, 106000.0, 0.0, 'Cleared', 'CHK-11890', 'Legal Defense LLC', 'Legal Defense'
    UNION ALL SELECT 'PAY-2019-009-4', 'CLM-2019-009', DATE'2022-04-20', 'Indemnity', 25000.0, 155000.0, 0.0, 'Cleared', 'CHK-11901', 'Mark Walker', 'Settlement'
    UNION ALL SELECT 'PAY-2019-009-5', 'CLM-2019-009', DATE'2023-02-15', 'Expense', 10000.0, 165000.0, 0.0, 'Cleared', 'CHK-11912', 'Legal Team', 'Legal Defense'
    
    -- 2020 Liability - ongoing litigation development
    UNION ALL SELECT 'PAY-2020-004-3', 'CLM-2020-004', DATE'2022-02-10', 'Expense', 20000.0, 60000.0, 40000.0, 'Cleared', 'CHK-12789', 'Legal Firm', 'Legal Defense'
    UNION ALL SELECT 'PAY-2020-004-4', 'CLM-2020-004', DATE'2023-01-15', 'Indemnity', 35000.0, 95000.0, 5000.0, 'Cleared', 'CHK-12890', 'Elizabeth Wright', 'Settlement'
    UNION ALL SELECT 'PAY-2020-009-3', 'CLM-2020-009', DATE'2022-04-25', 'Indemnity', 5000.0, 20000.0, 0.0, 'Cleared', 'CHK-12901', 'Kevin Carter', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-009-4', 'CLM-2020-009', DATE'2023-02-20', 'Indemnity', 3000.0, 23000.0, 0.0, 'Cleared', 'CHK-12912', 'Kevin Carter', 'Loss Payment'
    
    -- 2021 Liability - litigation continues
    UNION ALL SELECT 'PAY-2021-004-3', 'CLM-2021-004', DATE'2023-01-10', 'Indemnity', 30000.0, 60000.0, 20000.0, 'Cleared', 'CHK-13789', 'Betty Phillips', 'Settlement'
    UNION ALL SELECT 'PAY-2021-009-2', 'CLM-2021-009', DATE'2023-01-25', 'Expense', 25000.0, 45000.0, 55000.0, 'Cleared', 'CHK-13890', 'Legal Defense', 'Legal Defense'
    
    -- 2022 Liability - ongoing
    UNION ALL SELECT 'PAY-2022-004-2', 'CLM-2022-004', DATE'2023-06-20', 'Expense', 22000.0, 37000.0, 63000.0, 'Cleared', 'CHK-14678', 'Legal Services', 'Legal Defense'
    UNION ALL SELECT 'PAY-2022-009-2', 'CLM-2022-009', DATE'2023-09-15', 'Indemnity', 8000.0, 20000.0, 0.0, 'Cleared', 'CHK-14789', 'Arthur Murphy', 'Loss Payment'
    
    -- === ADDITIONAL FIRE PAYMENTS (extending development) ===
    -- 2018 Fire - late reconstruction payments
    UNION ALL SELECT 'PAY-2018-002-4', 'CLM-2018-002', DATE'2019-10-15', 'Indemnity', 8000.0, 88000.0, 0.0, 'Cleared', 'CHK-10345', 'Maria Garcia', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-002-5', 'CLM-2018-002', DATE'2020-06-20', 'Indemnity', 5000.0, 93000.0, 0.0, 'Cleared', 'CHK-10456', 'Maria Garcia', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-008-4', 'CLM-2018-008', DATE'2020-04-10', 'Indemnity', 8000.0, 108000.0, 0.0, 'Cleared', 'CHK-10567', 'Patricia Moore', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-008-5', 'CLM-2018-008', DATE'2021-02-15', 'Indemnity', 5000.0, 113000.0, 0.0, 'Cleared', 'CHK-10678', 'Patricia Moore', 'Loss Payment'
    
    -- 2019 Fire - additional development
    UNION ALL SELECT 'PAY-2019-001-4', 'CLM-2019-001', DATE'2020-08-20', 'Indemnity', 8000.0, 88000.0, 0.0, 'Cleared', 'CHK-11234', 'Thomas White', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-001-5', 'CLM-2019-001', DATE'2021-05-15', 'Indemnity', 5000.0, 93000.0, 0.0, 'Cleared', 'CHK-11345', 'Thomas White', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-006-5', 'CLM-2019-006', DATE'2021-11-10', 'Indemnity', 8000.0, 128000.0, 0.0, 'Cleared', 'CHK-11456', 'Margaret Robinson', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-006-6', 'CLM-2019-006', DATE'2022-08-20', 'Indemnity', 5000.0, 133000.0, 0.0, 'Cleared', 'CHK-11567', 'Margaret Robinson', 'Loss Payment'
    
    -- 2020 Fire - additional development
    UNION ALL SELECT 'PAY-2020-002-4', 'CLM-2020-002', DATE'2021-10-15', 'Indemnity', 5000.0, 55000.0, 0.0, 'Cleared', 'CHK-12345', 'Dorothy Young', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-002-5', 'CLM-2020-002', DATE'2022-07-20', 'Indemnity', 3000.0, 58000.0, 0.0, 'Cleared', 'CHK-12456', 'Dorothy Young', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-007-3', 'CLM-2020-007', DATE'2022-02-15', 'Indemnity', 8000.0, 43000.0, 0.0, 'Cleared', 'CHK-12567', 'Brian Adams', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-007-4', 'CLM-2020-007', DATE'2023-01-10', 'Indemnity', 5000.0, 48000.0, 0.0, 'Cleared', 'CHK-12678', 'Brian Adams', 'Loss Payment'
    
    -- 2021 Fire - additional development  
    UNION ALL SELECT 'PAY-2021-002-3', 'CLM-2021-002', DATE'2022-05-20', 'Indemnity', 8000.0, 60000.0, 0.0, 'Cleared', 'CHK-13234', 'Lisa Roberts', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-002-4', 'CLM-2021-002', DATE'2023-03-15', 'Indemnity', 5000.0, 65000.0, 0.0, 'Cleared', 'CHK-13345', 'Lisa Roberts', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-008-3', 'CLM-2021-008', DATE'2023-04-20', 'Indemnity', 6000.0, 43000.0, 0.0, 'Cleared', 'CHK-13456', 'Rebecca Evans', 'Loss Payment'
    
    -- 2022 Fire - additional development
    UNION ALL SELECT 'PAY-2022-002-3', 'CLM-2022-002', DATE'2023-05-15', 'Indemnity', 10000.0, 83000.0, 0.0, 'Cleared', 'CHK-14234', 'Helen Sanchez', 'Loss Payment'
    UNION ALL SELECT 'PAY-2022-008-2', 'CLM-2022-008', DATE'2023-08-20', 'Indemnity', 12000.0, 44000.0, 0.0, 'Cleared', 'CHK-14345', 'Virginia Bell', 'Loss Payment'
    
    -- === ADDITIONAL WIND PAYMENTS (extending development) ===
    -- 2018 Wind - late storm damage payments
    UNION ALL SELECT 'PAY-2018-003-3', 'CLM-2018-003', DATE'2019-08-15', 'Indemnity', 8000.0, 43000.0, 0.0, 'Cleared', 'CHK-10234', 'Robert Johnson', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-003-4', 'CLM-2018-003', DATE'2020-05-20', 'Indemnity', 5000.0, 48000.0, 0.0, 'Cleared', 'CHK-10345', 'Robert Johnson', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-005-4', 'CLM-2018-005', DATE'2020-07-15', 'Indemnity', 8000.0, 68000.0, 0.0, 'Cleared', 'CHK-10456', 'Michael Brown', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-005-5', 'CLM-2018-005', DATE'2021-04-20', 'Indemnity', 5000.0, 73000.0, 0.0, 'Cleared', 'CHK-10567', 'Michael Brown', 'Loss Payment'
    
    -- 2019 Wind - additional development
    UNION ALL SELECT 'PAY-2019-003-3', 'CLM-2019-003', DATE'2020-08-15', 'Indemnity', 6000.0, 31000.0, 0.0, 'Cleared', 'CHK-11234', 'Christopher Martin', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-003-4', 'CLM-2019-003', DATE'2021-06-20', 'Indemnity', 4000.0, 35000.0, 0.0, 'Cleared', 'CHK-11345', 'Christopher Martin', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-008-3', 'CLM-2019-008', DATE'2021-03-10', 'Indemnity', 8000.0, 58000.0, 0.0, 'Cleared', 'CHK-11456', 'Linda Lewis', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-008-4', 'CLM-2019-008', DATE'2022-01-15', 'Indemnity', 5000.0, 63000.0, 0.0, 'Cleared', 'CHK-11567', 'Linda Lewis', 'Loss Payment'
    
    -- 2020 Wind - additional development
    UNION ALL SELECT 'PAY-2020-003-3', 'CLM-2020-003', DATE'2021-08-20', 'Indemnity', 8000.0, 48000.0, 0.0, 'Cleared', 'CHK-12234', 'Andrew King', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-003-4', 'CLM-2020-003', DATE'2022-06-15', 'Indemnity', 5000.0, 53000.0, 0.0, 'Cleared', 'CHK-12345', 'Andrew King', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-008-4', 'CLM-2020-008', DATE'2022-10-20', 'Indemnity', 10000.0, 90000.0, 0.0, 'Cleared', 'CHK-12567', 'Michelle Nelson', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-008-5', 'CLM-2020-008', DATE'2023-07-15', 'Indemnity', 5000.0, 95000.0, 0.0, 'Cleared', 'CHK-12678', 'Michelle Nelson', 'Loss Payment'
    
    -- 2021 Wind - additional development
    UNION ALL SELECT 'PAY-2021-003-3', 'CLM-2021-003', DATE'2022-09-10', 'Indemnity', 8000.0, 73000.0, 0.0, 'Cleared', 'CHK-13234', 'Jason Turner', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-003-4', 'CLM-2021-003', DATE'2023-06-20', 'Indemnity', 5000.0, 78000.0, 0.0, 'Cleared', 'CHK-13345', 'Jason Turner', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-006-3', 'CLM-2021-006', DATE'2023-02-15', 'Indemnity', 15000.0, 75000.0, 5000.0, 'Cleared', 'CHK-13456', 'Amanda Martinez', 'Loss Payment'
    
    -- 2022 Wind - additional development  
    UNION ALL SELECT 'PAY-2022-003-3', 'CLM-2022-003', DATE'2023-08-20', 'Indemnity', 8000.0, 46000.0, 0.0, 'Cleared', 'CHK-14234', 'Raymond Morris', 'Loss Payment'
    UNION ALL SELECT 'PAY-2022-006-3', 'CLM-2022-006', DATE'2024-01-15', 'Indemnity', 15000.0, 110000.0, 10000.0, 'Cleared', 'CHK-14345', 'Janet Cook', 'Loss Payment'
    
    -- === ADDITIONAL WATER DAMAGE PAYMENTS (extending development) ===
    -- 2018 Water Damage - mold remediation takes years
    UNION ALL SELECT 'PAY-2018-001-5', 'CLM-2018-001', DATE'2021-03-15', 'Expense', 4000.0, 44000.0, 0.0, 'Cleared', 'CHK-10345', 'Mold Specialists', 'Remediation'
    UNION ALL SELECT 'PAY-2018-001-6', 'CLM-2018-001', DATE'2022-02-20', 'Indemnity', 3000.0, 47000.0, 0.0, 'Cleared', 'CHK-10456', 'John Smith', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-004-6', 'CLM-2018-004', DATE'2022-01-15', 'Expense', 8000.0, 108000.0, 0.0, 'Cleared', 'CHK-10567', 'Mold Remediation Co', 'Remediation'
    UNION ALL SELECT 'PAY-2018-004-7', 'CLM-2018-004', DATE'2023-01-10', 'Indemnity', 5000.0, 113000.0, 0.0, 'Cleared', 'CHK-10678', 'Sarah Williams', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-009-3', 'CLM-2018-009', DATE'2020-02-20', 'Indemnity', 6000.0, 36000.0, 0.0, 'Cleared', 'CHK-10234', 'David Taylor', 'Loss Payment'
    UNION ALL SELECT 'PAY-2018-009-4', 'CLM-2018-009', DATE'2021-01-15', 'Indemnity', 4000.0, 40000.0, 0.0, 'Cleared', 'CHK-10345', 'David Taylor', 'Loss Payment'
    
    -- 2019 Water Damage - additional development
    UNION ALL SELECT 'PAY-2019-002-3', 'CLM-2019-002', DATE'2020-06-15', 'Indemnity', 5000.0, 25000.0, 0.0, 'Cleared', 'CHK-11234', 'Nancy Harris', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-002-4', 'CLM-2019-002', DATE'2021-04-20', 'Indemnity', 3000.0, 28000.0, 0.0, 'Cleared', 'CHK-11345', 'Nancy Harris', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-007-4', 'CLM-2019-007', DATE'2021-08-15', 'Indemnity', 6000.0, 56000.0, 0.0, 'Cleared', 'CHK-11456', 'Paul Clark', 'Loss Payment'
    UNION ALL SELECT 'PAY-2019-007-5', 'CLM-2019-007', DATE'2022-06-20', 'Indemnity', 4000.0, 60000.0, 0.0, 'Cleared', 'CHK-11567', 'Paul Clark', 'Loss Payment'
    
    -- 2020 Water Damage - additional development
    UNION ALL SELECT 'PAY-2020-001-4', 'CLM-2020-001', DATE'2021-10-20', 'Indemnity', 5000.0, 45000.0, 0.0, 'Cleared', 'CHK-12234', 'Steven Allen', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-001-5', 'CLM-2020-001', DATE'2022-08-15', 'Indemnity', 3000.0, 48000.0, 0.0, 'Cleared', 'CHK-12345', 'Steven Allen', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-006-3', 'CLM-2020-006', DATE'2022-01-20', 'Indemnity', 5000.0, 23000.0, 0.0, 'Cleared', 'CHK-12456', 'Karen Green', 'Loss Payment'
    UNION ALL SELECT 'PAY-2020-006-4', 'CLM-2020-006', DATE'2023-01-15', 'Indemnity', 3000.0, 26000.0, 0.0, 'Cleared', 'CHK-12567', 'Karen Green', 'Loss Payment'
    
    -- 2021 Water Damage - additional development
    UNION ALL SELECT 'PAY-2021-001-3', 'CLM-2021-001', DATE'2022-04-15', 'Indemnity', 5000.0, 32000.0, 0.0, 'Cleared', 'CHK-13234', 'Ronald Perez', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-001-4', 'CLM-2021-001', DATE'2023-02-20', 'Indemnity', 3000.0, 35000.0, 0.0, 'Cleared', 'CHK-13345', 'Ronald Perez', 'Loss Payment'
    UNION ALL SELECT 'PAY-2021-007-3', 'CLM-2021-007', DATE'2023-03-15', 'Indemnity', 4000.0, 32000.0, 0.0, 'Cleared', 'CHK-13456', 'Jerry Parker', 'Loss Payment'
    
    -- 2022 Water Damage - additional development
    UNION ALL SELECT 'PAY-2022-001-3', 'CLM-2022-001', DATE'2023-04-20', 'Indemnity', 6000.0, 31000.0, 0.0, 'Cleared', 'CHK-14234', 'Frank Stewart', 'Loss Payment'
    UNION ALL SELECT 'PAY-2022-007-3', 'CLM-2022-007', DATE'2024-01-20', 'Indemnity', 12000.0, 87000.0, 13000.0, 'Cleared', 'CHK-14456', 'Henry Morgan', 'Loss Payment'
)
SELECT 
    *,
    current_timestamp() AS _ingested_at,
    'sample_data' AS _source_file
FROM sample_payments;

