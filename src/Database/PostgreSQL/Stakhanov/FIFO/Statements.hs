module Database.PostgreSQL.Stakhanov.FIFO.Statements where

import           Data.Aeson
import           Data.Int
import qualified Data.List                              as L
import qualified Data.Text                              as T
import           Data.Time
import qualified Data.Vector                            as V
import           Database.PostgreSQL.Stakhanov.Internal
import qualified Hasql.DynamicStatements.Snippet        as S
import           Hasql.Statement
import qualified Hasql.TH                               as TH
import           Prelude                                hiding (pi)

readGroupedMessages :: Statement (T.Text,Int32,Int32) (V.Vector (Int64, Int32, UTCTime, Maybe UTCTime, UTCTime, Value, Maybe Value))
readGroupedMessages =
  preparable sql readTupleEncoder tupleMessageDecoder
  where
    sql = "select " <> columnsMessage <> " from pgmq.read_grouped($1,$2,$3)"

readGroupedHeadMessages :: Statement (T.Text,Int32,Int32) (V.Vector (Int64, Int32, UTCTime, Maybe UTCTime, UTCTime, Value, Maybe Value))
readGroupedHeadMessages =
  preparable sql readTupleEncoder tupleMessageDecoder
  where
    sql = "select "<> columnsMessage <> " from pgmq.read_grouped_head($1,$2,$3)"

readGroupedMessagesWithPoll
  :: T.Text
  -> Int32
  -> Int32
  -> Maybe Int32
  -> Maybe Int32
  -> Statement () (V.Vector (Int64, Int32, UTCTime, Maybe UTCTime, UTCTime, Value, Maybe Value))
readGroupedMessagesWithPoll q vt qty mmp mpi =
  let mp = maybe 5 id mmp
      pi = maybe 100 id mpi
      snippet = "select " <> S.sql columnsMessage <> " from pgmq.read_grouped_with_poll(" <>
                mconcat (L.intersperse "," [S.param q, S.param vt, S.param qty, S.param mp, S.param pi]) <> ")"
  in S.toStatement snippet tupleMessageDecoder

readGroupedRRMessages :: Statement (T.Text,Int32,Int32) (V.Vector (Int64, Int32, UTCTime, Maybe UTCTime, UTCTime, Value, Maybe Value))
readGroupedRRMessages =
  preparable sql readTupleEncoder tupleMessageDecoder
  where
    sql = "select " <> columnsMessage <> " from pgmq.read_grouped_rr($1,$2,$3)"

readGroupedRRMessagesWithPoll
  :: T.Text
  -> Int32
  -> Int32
  -> Maybe Int32
  -> Maybe Int32
  -> Statement () (V.Vector (Int64, Int32, UTCTime, Maybe UTCTime, UTCTime, Value, Maybe Value))
readGroupedRRMessagesWithPoll q vt qty mmp mpi =
  let mp = maybe 5 id mmp
      pi = maybe 100 id mpi
      snippet = "select " <> S.sql columnsMessage <> " from pgmq.read_grouped_rr_with_poll(" <>
                mconcat (L.intersperse "," [S.param q, S.param vt, S.param qty, S.param mp, S.param pi]) <> ")"
  in S.toStatement snippet tupleMessageDecoder

createFIFOIndexQueue :: Statement T.Text ()
createFIFOIndexQueue = [TH.resultlessStatement|select from pgmq.create_fifo_index($1::text)|]

createFIFOIndexesAllQueues :: Statement () ()
createFIFOIndexesAllQueues = [TH.resultlessStatement|select from pgmq.create_fifo_indexes_all()|]

