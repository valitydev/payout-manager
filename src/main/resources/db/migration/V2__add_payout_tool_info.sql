create type pm.payout_tool_info as enum ('RUSSIAN_BANK_ACCOUNT', 'INTERNATIONAL_BANK_ACCOUNT', 'WALLET_INFO', 'PAYMENT_INSTITUTION_ACCOUNT');
alter table pm.payout
    add column payout_tool_info pm.payout_tool_info;
alter table pm.payout
    add column wallet_id character varying;
create type pm.payout_status_new as enum ('UNPAID', 'CONFIRMED', 'CANCELLED', 'FAILED');
alter table pm.payout
    alter column status type pm.payout_status_new using (status::text::pm.payout_status_new);
drop type pm.payout_status;
alter type pm.payout_status_new rename to payout_status;
