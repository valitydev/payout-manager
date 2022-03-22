create type pm.payout_tool_info as enum ('RUSSIAN_BANK_ACCOUNT', 'INTERNATIONAL_BANK_ACCOUNT', 'WALLET_INFO', 'PAYMENT_INSTITUTION_ACCOUNT');
alter table pm.payout
    add column payout_tool_info pm.payout_tool_info;
alter table pm.payout
    add column wallet_id character varying;
alter type pm.payout_status RENAME to payout_status_old;
create type pm.payout_status as enum ('UNPAID', 'CONFIRMED', 'CANCELLED');
alter table pm.payout alter column status TYPE pm.payout_status USING status::text::pm.payout_status;
