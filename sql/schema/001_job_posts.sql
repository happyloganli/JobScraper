-- +goose Up
CREATE TABLE job_posts (
                     id UUID PRIMARY KEY,
                     detail_url VARCHAR(1024) NOT NULL,
                     title VARCHAR(255) NOT NULL,
                     company VARCHAR(255) NOT NULL,
                     location VARCHAR(255) NOT NULL,
                     level VARCHAR(255) NOT NULL,
                     apply_url VARCHAR(1024) NOT NULL,
                     minimum_qualifications TEXT[],
                     preferred_qualifications TEXT[],
                     about_job TEXT[],
                     responsibilities TEXT[],
                     created_at TIMESTAMP NOT NULL
);
-- +goose Down
DROP TABLE job_posts;