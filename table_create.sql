CREATE TABLE public.file_copy_job (
	id bigserial NOT NULL,
	source_path text NOT NULL,
	destination_path text NOT NULL,
	status text DEFAULT 'pending'::text NOT NULL,
	last_error text NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT file_copy_job_pkey PRIMARY KEY (id)
);