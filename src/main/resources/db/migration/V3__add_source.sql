create type pm.source_status as enum ('AUTHORIZED', 'UNAUTHORIZED');

create table if not exists pm.source
(
    id
    bigserial
    not
    null,
    source_id
    varchar
    not
    null,
    status
    pm
    .
    source_status
    not
    null,
    currency_code
    varchar,
    constraint
    source_id_pkey
    primary
    key
(
    id
),
    constraint source_source_id_ukey unique
(
    source_id
)
    );
