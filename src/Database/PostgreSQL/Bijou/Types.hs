module Database.PostgreSQL.Bijou.Types where

import           Data.Aeson.Types
import           Data.Int
import           Data.Time

-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/types.md
-- https://github.com/pgmq/pgmq/blob/main/pgmq-extension/sql/pgmq.sql

data Message =
  Message
    { messageId         :: Int64
    , readCount         ::Int32
    , enqueuedAt        :: UTCTime
    , visibilityTimeout :: UTCTime
    , jsonMessage       :: !Object
    , jsonHeaders       :: !Object }

