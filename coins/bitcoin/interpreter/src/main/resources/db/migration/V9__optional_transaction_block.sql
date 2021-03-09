ALTER TABLE "transaction" ALTER COLUMN block_hash DROP NOT NULL;
ALTER TABLE "transaction" ALTER COLUMN block_height DROP NOT NULL;
ALTER TABLE "transaction" ALTER COLUMN block_time DROP NOT NULL;

ALTER TABLE operation ADD
    FOREIGN KEY (account_id, hash) REFERENCES transaction (account_id, hash) ON DELETE CASCADE;

DROP TABLE unconfirmed_transaction_view;
DROP TABLE unconfirmed_transaction_cache;
