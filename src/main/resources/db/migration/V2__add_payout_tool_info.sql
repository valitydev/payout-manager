create type pm.payout_tool_info as enum ('russian_bank_account', 'international_bank_account', 'wallet_info', 'payment_institution_account');
alter table pm.payout
    add column payout_tool_info pm.payout_tool_info;
alter table pm.payout
    add column wallet_id character varying;

