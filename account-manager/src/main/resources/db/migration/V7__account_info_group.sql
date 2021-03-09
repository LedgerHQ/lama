ALTER TABLE account_info
  ADD COLUMN group_col VARCHAR NOT NULL DEFAULT 'Serge',
  -- Uniqueness of the fields used to compute account uuid
  DROP CONSTRAINT account_info_key_coin_family_coin_key,
  ADD CONSTRAINT account_unique_id_seed_key UNIQUE (key, coin_family, coin, group_col);

CREATE OR REPLACE VIEW account_sync_status AS (
    SELECT DISTINCT ON (account_id)
        account_id,
        "key",
        coin_family,
        coin,
        sync_frequency,
        sync_id,
        status,
        "cursor",
        error,
        updated,
        label,
        group_col
    FROM account_info JOIN account_sync_event USING (account_id)
    ORDER BY account_id, updated DESC
);
