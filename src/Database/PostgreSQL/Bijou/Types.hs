module Database.PostgreSQL.Bijou.Types where

import           Data.Aeson.Types
import           Data.Int
import qualified Data.Text        as T
import           Data.Time

-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/types.md
-- https://github.com/pgmq/pgmq/blob/main/pgmq-extension/sql/pgmq.sql

data Queue = Queue { queueName :: T.Text } deriving (Show)

data Message =
  Message
    { messageId         :: Int64
    , readCount         :: Int32
    , enqueuedAt        :: UTCTime
    , visibilityTimeout :: UTCTime
    , message           :: !Object
    , headers           :: !(Maybe Object) }

-- data Msg = Msg { messageId :: Int64, messsage :: !Object }

data Messages = Vector Message

