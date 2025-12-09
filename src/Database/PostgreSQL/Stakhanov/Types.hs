module Database.PostgreSQL.Stakhanov.Types where

import           Data.Aeson.Types
import           Data.Int
import qualified Data.Text        as T
import           Data.Time
import           Data.Vector

-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/types.md
-- https://github.com/pgmq/pgmq/blob/main/pgmq-extension/sql/pgmq.sql

data Queue = Queue { queueName :: T.Text } deriving (Show)

data Message =
  Message
    { msgId             :: Int64
    , readCount         :: Int32
    , enqueuedAt        :: UTCTime
    , visibilityTimeout :: UTCTime
    , message           :: !Value
    , headers           :: !(Maybe Value) }

-- data Msg = Msg { messageId :: Int64, messsage :: !Object }

type Messages = Vector Message

tupleToMessage :: (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value) -> Message
tupleToMessage (e1,e2,e3,e4,e5,e6) =
  Message
    { msgId             = e1
    , readCount         = e2
    , enqueuedAt        = e3
    , visibilityTimeout = e4
    , message           = e5
    , headers           = e6 }
