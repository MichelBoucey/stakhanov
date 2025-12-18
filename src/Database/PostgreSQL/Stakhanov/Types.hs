module Database.PostgreSQL.Stakhanov.Types where

import           Data.Aeson.Types
import           Data.Int
import qualified Data.Text        as T
import           Data.Time
import           Data.Vector

data Queue =
  Queue
    { queueName    :: T.Text
    , queueMetrics :: Maybe Metrics
    } deriving (Show)

type MsgId = Int64

type VT = Int32
type Qty = Int32
type Delay = Int32

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

data Metrics =
  Metrics
    { queueLength        :: Int64
    , newestMsgAge       :: Int32
    , oldestMsgAge       :: Int32
    , totalMessages      :: Int64
    , scrapeTime         :: UTCTime
    , queueVisibleLength :: Int64
    } deriving (Show)

