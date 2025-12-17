module Database.PostgreSQL.Stakhanov.Internal where

import           Data.Aeson.Types
import           Data.Int
import           Data.List                           (intersperse)
import qualified Data.Monoid                         as M
import           Data.Time
import           Data.Vector                         as V
import           Database.PostgreSQL.Stakhanov.Types
import qualified Hasql.DynamicStatements.Snippet     as S
import qualified Hasql.Encoders                      as E

maybeMessages
  :: Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value)
  -> Maybe Messages
maybeMessages v =
    if V.null v
      then Nothing
      else Just $ Messages $ tupleToMessage <$> v

tupleToMetrics
 :: (Int64, Int32, Int32, Int64, UTCTime, Int64)
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
  :: (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value)
  -> Message
tupleToMessage (e1,e2,e3,e4,e5,e6) =
  Message
    { msgId             = e1
    , readCount         = e2
    , enqueuedAt        = e3
    , visibilityTimeout = e4
    , message           = e5
    , headers           = e6 }

jsonbArrayEncoder :: V.Vector Value -> S.Snippet
jsonbArrayEncoder v =
  "ARRAY[" <> M.mconcat (intersperse (S.sql ",") $ V.toList $ S.encoderAndParam (E.nonNullable E.json) <$> v) <> "]::jsonb[]"

int8ArrayEncoder :: V.Vector Int64 -> S.Snippet
int8ArrayEncoder v =
  "ARRAY[" <> M.mconcat (intersperse (S.sql ",") $ V.toList $ S.encoderAndParam (E.nonNullable E.int8) <$> v) <> "]::bigint[]"

