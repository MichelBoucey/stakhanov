module Database.PostgreSQL.Stakhanov.Metrics.Statements where

import           Data.Int
import qualified Data.Text       as T
import           Data.Time
import qualified Data.Vector     as V
import qualified Hasql.Decoders  as D
import qualified Hasql.Encoders  as E
import           Hasql.Statement
import           Prelude         hiding (pi)

getMetrics :: Statement T.Text (Int64, Maybe Int32, Maybe Int32, Int64, UTCTime, Int64)
getMetrics =
  preparable sql encoder decoder
  where
    sql = "select " <> columnsMetrics <> " from pgmq.metrics($1)"
    encoder = E.param (E.nonNullable E.text)
    decoder =
      D.singleRow $
        (,,,,,) <$>
          D.column (D.nonNullable D.int8) <*>
          D.column (D.nullable D.int4) <*>
          D.column (D.nullable D.int4) <*>
          D.column (D.nonNullable D.int8) <*>
          D.column (D.nonNullable D.timestamptz) <*>
          D.column (D.nonNullable D.int8)

getAllMetrics :: Statement () (V.Vector (T.Text, Int64, Maybe Int32, Maybe Int32, Int64, UTCTime, Int64))
getAllMetrics =
  preparable sql E.noParams decoder
  where
    sql = "select queue_name," <> columnsMetrics <> " from pgmq.metrics_all()"
    decoder =
      D.rowVector $
        (,,,,,,) <$>
          D.column (D.nonNullable D.text) <*>
          D.column (D.nonNullable D.int8) <*>
          D.column (D.nullable D.int4) <*>
          D.column (D.nullable D.int4) <*>
          D.column (D.nonNullable D.int8) <*>
          D.column (D.nonNullable D.timestamptz) <*>
          D.column (D.nonNullable D.int8)

columnsMetrics :: T.Text
columnsMetrics = "queue_length,newest_msg_age_sec,oldest_msg_age_sec,total_messages,scrape_time,queue_visible_length"

