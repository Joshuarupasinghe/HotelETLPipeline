CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS hotel_bookings CASCADE;
DROP TABLE IF EXISTS rejected_records_audit CASCADE;

CREATE TABLE hotel_bookings (
    booking_id VARCHAR(64) PRIMARY KEY,
    hotel VARCHAR(50) NOT NULL,
    is_canceled SMALLINT NOT NULL CHECK (is_canceled IN (0, 1)),
    lead_time INTEGER NOT NULL CHECK (lead_time >= 0),
    arrival_date DATE NOT NULL,
    stays_in_weekend_nights INTEGER NOT NULL CHECK (stays_in_weekend_nights >= 0),
    stays_in_week_nights INTEGER NOT NULL CHECK (stays_in_week_nights >= 0),
    total_stay_nights INTEGER GENERATED ALWAYS AS (stays_in_weekend_nights + stays_in_week_nights) STORED,
    adults INTEGER NOT NULL CHECK (adults >= 0),
    children INTEGER NOT NULL DEFAULT 0 CHECK (children >= 0),
    babies INTEGER NOT NULL DEFAULT 0 CHECK (babies >= 0),
    total_guests INTEGER GENERATED ALWAYS AS (adults + children + babies) STORED,
    meal VARCHAR(20) NOT NULL DEFAULT 'SC',
    country VARCHAR(3) NOT NULL DEFAULT 'UNK',
    market_segment VARCHAR(50) NOT NULL,
    distribution_channel VARCHAR(50) NOT NULL,
    is_repeated_guest SMALLINT NOT NULL CHECK (is_repeated_guest IN (0, 1)),
    previous_cancellations INTEGER NOT NULL DEFAULT 0,
    previous_bookings_not_canceled INTEGER NOT NULL DEFAULT 0,
    reserved_room_type VARCHAR(10) NOT NULL,
    assigned_room_type VARCHAR(10) NOT NULL,
    booking_changes INTEGER NOT NULL DEFAULT 0,
    deposit_type VARCHAR(50) NOT NULL DEFAULT 'No Deposit',
    agent_id VARCHAR(20),
    company_id VARCHAR(20),
    customer_type VARCHAR(50) NOT NULL,
    adr NUMERIC(10, 2) NOT NULL CHECK (adr >= 0.00), -- Average Daily Rate (Price)
    required_car_parking_spaces INTEGER NOT NULL DEFAULT 0,
    total_of_special_requests INTEGER NOT NULL DEFAULT 0,
    reservation_status VARCHAR(50) NOT NULL,
    reservation_status_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE rejected_records_audit (
    rejection_id SERIAL PRIMARY KEY,
    raw_payload JSONB NOT NULL,
    rejection_reason TEXT NOT NULL,
    rejected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Performance Indexes
-- Index on country for geographical aggregation
CREATE INDEX idx_bookings_country ON hotel_bookings (country);

-- Index on arrival_date for temporal & monthly growth filtering
CREATE INDEX idx_bookings_arrival_date ON hotel_bookings (arrival_date);

-- Composite Index on market_segment and is_canceled for revenue queries
CREATE INDEX idx_bookings_segment_canceled ON hotel_bookings (market_segment, is_canceled);