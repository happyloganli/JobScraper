-- name: CreateJobPost :one
INSERT INTO job_posts (id, detail_url, title, company, location, level, apply_url, minimum_qualifications, preferred_qualifications, about_job, responsibilities, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    RETURNING *;