-- Run once if `watchlist` already exists without `source`:
ALTER TABLE watchlist ADD COLUMN source VARCHAR(32) NULL;
