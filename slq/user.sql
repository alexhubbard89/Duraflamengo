-- Enable extension for UUID generation if it's not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create the user table
CREATE TABLE users (
    user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    display_name TEXT NOT NULL
);

-- Insert seed data
INSERT INTO users (display_name) VALUES ('Alex');
INSERT INTO users (display_name) VALUES ('Kim');
