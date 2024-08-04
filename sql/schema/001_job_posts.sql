-- +goose Up
CREATE TABLE job_posts (
                     id UUID PRIMARY KEY,
                     title VARCHAR(255) NOT NULL,
                     company VARCHAR(255) NOT NULL,
                     location VARCHAR(255) NOT NULL,
                     level VARCHAR(255) NOT NULL,
                     minimum_qualifications TEXT[],
                     detail_url VARCHAR(255) NOT NULL,
                     created_at TIMESTAMP NOT NULL
);
-- +goose Down
DROP TABLE job_posts;