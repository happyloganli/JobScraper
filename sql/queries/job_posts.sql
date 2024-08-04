-- name: CreateJobPost :one
INSERT INTO job_posts (id, title, company, location, level, minimum_qualifications, detail_url, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    RETURNING *;