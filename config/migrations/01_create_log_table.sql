create schema if not exists payments;

create table if not exists payments.log (
    id uuid primary key,
    amount decimal not null,
    requested_at timestamptz not null,
    processed_by text
);
