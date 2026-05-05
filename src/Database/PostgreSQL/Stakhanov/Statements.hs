module Database.PostgreSQL.Stakhanov.Statements where

import           Contravariant.Extras.Contrazip         (contrazip2, contrazip3)
import           Data.Aeson
import           Data.Int
import qualified Data.List                              as L
import           Data.Maybe                             (fromMaybe)
import qualified Data.Text                              as T
import           Data.Time
import qualified Data.Vector                            as V
import           Database.PostgreSQL.Stakhanov.Internal
import           Database.PostgreSQL.Stakhanov.Types
import qualified Hasql.Decoders                         as D
import qualified Hasql.DynamicStatements.Snippet        as S
import qualified Hasql.Encoders                         as E
import           Hasql.Statement
import qualified Hasql.TH                               as TH
import           Prelude                                hiding (pi)

createQueue :: Statement T.Text ()
createQueue = [TH.resultlessStatement|select from pgmq.create($1::text)|]

createUnloggedQueue :: Statement T.Text ()
createUnloggedQueue = [TH.resultlessStatement|select from pgmq.create_unlogged($1::text)|]

createFIFOIndexQueue :: Statement T.Text ()
createFIFOIndexQueue = [TH.resultlessStatement|select from pgmq.create_fifo_index($1::text)|]

createFIFOIndexesAllQueues :: Statement () ()
createFIFOIndexesAllQueues = [TH.resultlessStatement|select from pgmq.create_fifo_indexes_all()|]

getQueuesDetails :: Statement () (V.Vector (T.Text, (UTCTime, Bool, Bool)))
getQueuesDetails =
  preparable sql E.noParams decoder
  where
    sql = "select queue_name::text,created_at,is_partitioned,is_unlogged from pgmq.list_queues()"
    decoder =
      D.rowVector $
        (,) <$>
          D.column (D.nonNullable D.text) <*>
            ((,,) <$>
              D.column (D.nonNullable D.timestamptz) <*>
              D.column (D.nonNullable D.bool) <*>
              D.column (D.nonNullable D.bool))

purgeQueue :: Statement T.Text Int64
purgeQueue = [TH.singletonStatement|select pgmq.purge_queue($1::text)::int8|]

dropQueue :: Statement T.Text Bool
dropQueue = [TH.singletonStatement|select pgmq.drop_queue($1::text)::bool|]

sendMessage :: Statement (T.Text,Value) Int64
sendMessage =
  preparable sql encoder decoder
  where
    sql = "select * from pgmq.send($1::text,$2::jsonb)"
    encoder =
      contrazip2
        (E.param $ E.nonNullable E.text)
        (E.param $ E.nonNullable E.jsonb)
    decoder = D.singleRow $ D.column $ D.nonNullable D.int8

sendMessage' :: T.Text -> Value -> Maybe Value -> Maybe Delay -> Statement () Int64
sendMessage' q v mv mi =
  let snippet =
        "select * from pgmq.send(" <> S.param q <> ","
        <> S.param v <> maybeHeaders mv <> maybeDelay mi <> ")"
      decoder = D.singleRow $ D.column $ D.nonNullable D.int8
  in S.toStatement snippet decoder

sendMessages :: T.Text -> (V.Vector Value) -> Statement () (V.Vector Int64)
sendMessages q msgs =
  let snippet = "select * from pgmq.send_batch(" <> S.param q <> "," <> jsonbArrayEncoder msgs <> ")"
      decoder = D.rowVector $ D.column $ D.nonNullable D.int8
  in S.toStatement snippet decoder

sendMessages' :: T.Text -> (V.Vector Value) -> Maybe (V.Vector Value) -> Maybe Delay -> Statement () (V.Vector Int64)
sendMessages' q vv mvv md =
  let snippet =
        "select * from pgmq.send_batch(" <> S.param q <> "," <> jsonbArrayEncoder vv
        <> (fromMaybe mempty $ (mappend "," . jsonbArrayEncoder) <$> mvv) <> maybeDelay md <> ")"
      decoder = D.rowVector $ D.column $ D.nonNullable D.int8
  in S.toStatement snippet decoder

readMessagesWithPoll :: T.Text -> Int32 -> Int32 -> Maybe Int32 -> Maybe Int32 -> Statement () (V.Vector (Int64, Int32, UTCTime, Maybe UTCTime, UTCTime, Value, Maybe Value))
readMessagesWithPoll q vt qty mmp mpi =
  let mp = maybe 5 id mmp
      pi = maybe 100 id mpi
      snippet = "select " <> S.sql columnsMessage <> " from pgmq.read_with_poll(" <>
                mconcat (L.intersperse "," [S.param q, S.param vt, S.param qty, S.param mp, S.param pi]) <> ")"
  in S.toStatement snippet tupleMessageDecoder

readMessages :: Statement (T.Text,Int32,Int32) (V.Vector (Int64, Int32, UTCTime, Maybe UTCTime, UTCTime, Value, Maybe Value))
readMessages =
  preparable sql readTupleEncoder tupleMessageDecoder
  where
    sql = "select " <> columnsMessage <> " from pgmq.read($1,$2,$3)"

readGroupedMessages :: Statement (T.Text,Int32,Int32) (V.Vector (Int64, Int32, UTCTime, Maybe UTCTime, UTCTime, Value, Maybe Value))
readGroupedMessages =
  preparable sql readTupleEncoder tupleMessageDecoder
  where
    sql = "select " <> columnsMessage <> " from pgmq.read_grouped($1,$2,$3)"

readGroupedMessagesWithPoll :: T.Text -> Int32 -> Int32 -> Maybe Int32 -> Maybe Int32 -> Statement () (V.Vector (Int64, Int32, UTCTime, Maybe UTCTime, UTCTime, Value, Maybe Value))
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

readGroupedRRMessagesWithPoll :: T.Text -> Int32 -> Int32 -> Maybe Int32 -> Maybe Int32 -> Statement () (V.Vector (Int64, Int32, UTCTime, Maybe UTCTime, UTCTime, Value, Maybe Value))
readGroupedRRMessagesWithPoll q vt qty mmp mpi =
  let mp = maybe 5 id mmp
      pi = maybe 100 id mpi
      snippet = "select " <> S.sql columnsMessage <> " from pgmq.read_grouped_rr_with_poll(" <>
                mconcat (L.intersperse "," [S.param q, S.param vt, S.param qty, S.param mp, S.param pi]) <> ")"
  in S.toStatement snippet tupleMessageDecoder

popMessages :: Statement (T.Text,Int32) (V.Vector (Int64, Int32, UTCTime, Maybe UTCTime, UTCTime, Value, Maybe Value))
popMessages =
  preparable sql encoder tupleMessageDecoder
  where
    sql = "select " <> columnsMessage <> " from pgmq.pop($1,$2)"
    encoder =
      contrazip2
        (E.param $ E.nonNullable E.text)
        (E.param $ E.nonNullable E.int4)

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

archiveMessage :: Statement (T.Text,Int64) Bool
archiveMessage = [TH.singletonStatement|select pgmq.archive($1::text,$2::int8)::bool|]

archiveMessages :: T.Text -> (V.Vector Int64) -> Statement () (V.Vector Int64)
archiveMessages q v =
  let snippet = "select * from pgmq.archive(" <> S.param q <> "," <> bigintArrayEncoder v <> ")"
      decoder = D.rowVector (D.column (D.nonNullable D.int8))
  in S.toStatement snippet decoder

deleteMessage :: Statement (T.Text,Int64) Bool
deleteMessage = [TH.singletonStatement|select pgmq.delete($1::text,$2::int8)::bool|]

deleteMessages :: T.Text -> (V.Vector Int64) -> Statement () (V.Vector Int64)
deleteMessages q v =
  let snippet = "select * from pgmq.delete(" <> S.param q <> "," <> bigintArrayEncoder v <> ")"
      decoder = D.rowVector (D.column (D.nonNullable D.int8))
  in S.toStatement snippet decoder

setMessagesVT :: T.Text -> V.Vector Int64 -> Int32 -> Statement () (V.Vector (Int64, Int32, UTCTime, Maybe UTCTime, UTCTime, Value, Maybe Value))
setMessagesVT q v s =
  let snippet = "select * from pgmq.set_vt(" <> S.param q <> ","
                <> bigintArrayEncoder v <> "," <> S.param s <> ")"
  in S.toStatement snippet tupleMessageDecoder

readTupleEncoder :: E.Params (T.Text, Int32, Int32)
readTupleEncoder =
  contrazip3
    (E.param $ E.nonNullable E.text)
    (E.param $ E.nonNullable E.int4)
    (E.param $ E.nonNullable E.int4)

tupleMessageDecoder :: D.Result (V.Vector (Int64, Int32, UTCTime, Maybe UTCTime, UTCTime, Value, Maybe Value))
tupleMessageDecoder =
  D.rowVector $
    (,,,,,,) <$>
      D.column (D.nonNullable D.int8) <*>
      D.column (D.nonNullable D.int4) <*>
      D.column (D.nonNullable D.timestamptz) <*>
      D.column (D.nullable D.timestamptz) <*>
      D.column (D.nonNullable D.timestamptz) <*>
      D.column (D.nonNullable D.jsonb) <*>
      D.column (D.nullable D.jsonb)

columnsMessage :: T.Text
columnsMessage = "msg_id,read_ct,enqueued_at,last_read_at,vt,message,headers"

columnsMetrics :: T.Text
columnsMetrics = "queue_length,newest_msg_age_sec,oldest_msg_age_sec,total_messages,scrape_time,queue_visible_length"

