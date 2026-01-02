module Database.PostgreSQL.Stakhanov.Types where

import           Data.Aeson.Types
import           Data.Int
import qualified Data.Text        as T
import           Data.Time
import           Data.Vector

type VT           = Int32
type Qty          = Int32
type Seconds      = Int32
type Milliseconds = Int32
type MsgId        = Int64
type MsgIds       = Vector MsgId
type Messages     = Vector Message

data Queue =
  Queue
    { queueName    :: T.Text
    , queueDetails :: Maybe Details
    , queueMetrics :: Maybe Metrics
    } deriving (Show)

instance Eq Queue where
 Queue n _ _ == Queue n' _ _ = n == n'

data Details =
  Details
    {
      createdAt     :: UTCTime
    , isPartitioned :: Bool
    , isUnlogged    :: Bool
    } deriving (Show)

data Message =
  Message
    { msgId             :: MsgId
    , readCount         :: Int32
    , enqueuedAt        :: UTCTime
    , visibilityTimeout :: UTCTime
    , message           :: !Value
    , headers           :: !(Maybe Value)
    } deriving (Show)

instance Eq Message where
  Message i _ _ _ _ _ == Message i' _ _ _ _ _ = i == i'

data Metrics =
  Metrics
    { queueLength        :: Int64
    , newestMsgAge       :: Maybe Seconds
    , oldestMsgAge       :: Maybe Seconds
    , totalMessages      :: Int64
    , scrapeTime         :: UTCTime
    , queueVisibleLength :: Int64
    } deriving (Show)

data Delay = InSeconds Int32 | WithTimestamp UTCTime

