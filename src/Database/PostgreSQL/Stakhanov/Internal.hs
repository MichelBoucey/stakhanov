module Database.PostgreSQL.Stakhanov.Internal where

import           Data.Aeson.Types
import           Data.Int
import           Data.Time
import           Data.Vector                         as V
import           Database.PostgreSQL.Stakhanov.Types

mMsgs
  :: Vector (MsgId, Int32, UTCTime, UTCTime, Value, Maybe Value)
  -> Maybe Messages
mMsgs vts =
  if V.null vts
    then Nothing
    else Just $ Messages $ msgTupleToMsg <$> vts

msgTupleToMsg
  :: (MsgId, Int32, UTCTime, UTCTime, Value, Maybe Value)
  -> Message
msgTupleToMsg (e1,e2,e3,e4,e5,e6) =
  Message
    { msgId             = e1
    , readCount         = e2
    , enqueuedAt        = e3
    , visibilityTimeout = e4
    , message           = e5
    , headers           = e6 }

