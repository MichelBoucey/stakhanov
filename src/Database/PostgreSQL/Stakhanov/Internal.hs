module Database.PostgreSQL.Stakhanov.Internal where

import           Data.Aeson.Types
import           Data.Int
import           Data.List                           (intersperse)
import qualified Data.Monoid                         as M
import qualified Data.Text                           as T
import           Data.Time
import           Data.Vector                         as V
import           Database.PostgreSQL.Stakhanov.Types
import qualified Hasql.Connection                    as C
import qualified Hasql.DynamicStatements.Snippet     as S
import qualified Hasql.Encoders                      as E

pureMap
  :: (Applicative f1, Functor f2)
  => (a -> b) -> f2 a -> f1 (f2 b)
pureMap f e = pure $ f <$> e

isJSON :: Value -> Bool
isJSON (Object _) = True
isJSON _          = False

allJSON :: Vector Value -> Bool
allJSON = V.all isJSON

maybeMessages
  :: Vector (Int64, Int32, UTCTime, UTCTime, UTCTime, Value, Maybe Value)
  -> Maybe Messages
maybeMessages v =
  if V.null v
    then Nothing
    else Just $ tupleToMessage <$> v

tupleToDetails :: (UTCTime,Bool,Bool) -> Details
tupleToDetails (e1,e2,e3) =
  Details
    { createdAt     = e1
    , isPartitioned = e2
    , isUnlogged    = e3
    }

tupleToQueueWithMetrics
  :: C.Connection
  -> (T.Text, Int64, Maybe Int32, Maybe Int32, Int64, UTCTime, Int64)
  -> Queue
tupleToQueueWithMetrics c (e1,e2,e3,e4,e5,e6,e7) =
  Queue
   { qName    = e1
   , qPGConn  = HasqlConn c
   , qDetails = Nothing
   , qMetrics = Just $ tupleToMetrics (e2,e3,e4,e5,e6,e7)
   }

tupleToMetrics
 :: (Int64, Maybe Int32, Maybe Int32, Int64, UTCTime, Int64)
 -> Metrics
tupleToMetrics (e1,e2,e3,e4,e5,e6) =
  Metrics
    { queueLength        = e1
    , newestMsgAge       = e2
    , oldestMsgAge       = e3
    , totalMessages      = e4
    , scrapeTime         = e5
    , queueVisibleLength = e6 }

tupleToMessage
  :: (Int64, Int32, UTCTime, UTCTime, UTCTime, Value, Maybe Value)
  -> Message
tupleToMessage (e1,e2,e3,e4,e5,e6,e7) =
  Message
    { msgId             = e1
    , readCount         = e2
    , enqueuedAt        = e3
    , lastReadAt        = e4
    , visibilityTimeout = e5
    , message           = e6
    , headers           = e7 }

maybeHeaders :: Maybe Value -> S.Snippet
maybeHeaders (Just v) = "," <> S.encoderAndParam (E.nonNullable E.json) v <> "::jsonb"
maybeHeaders Nothing  = mempty

maybeDelay :: Maybe Delay -> S.Snippet
maybeDelay (Just (InSeconds s))     = "," <> S.encoderAndParam (E.nonNullable E.int4) s
maybeDelay (Just (WithTimestamp t)) = "," <> S.encoderAndParam (E.nonNullable E.timestamptz) t
maybeDelay Nothing                  = mempty

jsonbArrayEncoder :: V.Vector Value -> S.Snippet
jsonbArrayEncoder v =
  "ARRAY[" <> M.mconcat (intersperse (S.sql ",") $ V.toList $ S.encoderAndParam (E.nonNullable E.json) <$> v) <> "]::jsonb[]"

bigintArrayEncoder :: V.Vector Int64 -> S.Snippet
bigintArrayEncoder v =
  "ARRAY[" <> M.mconcat (intersperse (S.sql ",") $ V.toList $ S.encoderAndParam (E.nonNullable E.int8) <$> v) <> "]::bigint[]"

