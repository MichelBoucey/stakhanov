module Database.PostgreSQL.Bijou.Types where

import           Data.Aeson.Types
import           Data.Text        as T
-- import           Numeric.Natural
import Data.Int
import Data.Time

-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/types.md

-- data Message = Message { unMessage :: !Object }
-- newtype Queue = Queue { name :: T.Text }

-- https://github.com/pgmq/pgmq/blob/main/pgmq-extension/sql/pgmq.sql

{-
CREATE TYPE pgmq.message_record AS (
    msg_id BIGINT,
    read_ct INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    vt TIMESTAMP WITH TIME ZONE,
    message JSONB,
    headers JSONB
);
-}

data Message = Message { messageId :: Int64, readCount ::Int32, enqueuedAt :: UTCTime, visibilityTimeout :: UTCTime, jsonMessage :: !Object, jsonHeaders :: !Object }

