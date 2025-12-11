module Database.PostgreSQL.Stakhanov.Types where

import           Data.Aeson.Types
import           Data.Int
import qualified Data.Text        as T
import           Data.Time
import           Data.Vector

-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/types.md
-- https://github.com/pgmq/pgmq/blob/main/pgmq-extension/sql/pgmq.sql

newtype Queue =
  Queue { queueName :: T.Text }
  deriving (Show)

type MsgId = Int64

data Message =
  Message
    { msgId             :: MsgId
    , readCount         :: Int32
    , enqueuedAt        :: UTCTime
    , visibilityTimeout :: UTCTime
    , message           :: !Value
    , headers           :: !(Maybe Value)
    } deriving (Show)

newtype Messages =
  Messages { unMessages :: Vector Message }
  deriving (Show)

