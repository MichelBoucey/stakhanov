module Database.PostgreSQL.Stakhanov.Statements where

import           Contravariant.Extras.Contrazip         (contrazip2, contrazip3)
import           Data.Aeson
import           Data.ByteString
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
import           Hasql.DynamicStatements.Statement
import qualified Hasql.Encoders                         as E
import           Hasql.Statement
import qualified Hasql.TH                               as TH
import           Prelude                                hiding (pi)

createQueue :: Statement T.Text ()
createQueue = [TH.resultlessStatement|select from pgmq.create($1::text)|]

createUnloggedQueue :: Statement T.Text ()
createUnloggedQueue = [TH.resultlessStatement|select from pgmq.create_unlogged($1::text)|]

getQueuesDetails :: Statement () (V.Vector (T.Text, (UTCTime, Bool, Bool)))
getQueuesDetails =
  Statement sql E.noParams decoder True
  where
    sql = "select queue_name,created_at,is_partitioned,is_unlogged from pgmq.list_queues()"
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
  Statement sql encoder decoder True
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
  in dynamicallyParameterized snippet decoder True

sendMessages :: T.Text -> (V.Vector Value) -> Statement () (V.Vector Int64)
sendMessages q msgs =
  let snippet = "select * from pgmq.send_batch(" <> S.param q <> "," <> jsonbArrayEncoder msgs <> ")"
      decoder = D.rowVector $ D.column $ D.nonNullable D.int8
  in dynamicallyParameterized snippet decoder True

sendMessages' :: T.Text -> (V.Vector Value) -> Maybe (V.Vector Value) -> Maybe Delay -> Statement () (V.Vector Int64)
sendMessages' q vv mvv md =
  let snippet =
        "select * from pgmq.send_batch(" <> S.param q <> "," <> jsonbArrayEncoder vv
        <> (fromMaybe mempty $ (mappend "," . jsonbArrayEncoder) <$> mvv) <> maybeDelay md <> ")"
      decoder = D.rowVector $ D.column $ D.nonNullable D.int8
  in dynamicallyParameterized snippet decoder True

readMessagesWithPoll :: T.Text -> Int32 -> Int32 -> Maybe Int32 -> Maybe Int32 -> Statement () (V.Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value))
readMessagesWithPoll q vt qty mmp mpi =
  let mp = maybe 5 id mmp
      pi = maybe 100 id mpi
      snippet = "select " <> S.sql columnsMessage <> " from pgmq.read_with_poll(" <>
                mconcat (L.intersperse "," [S.param q, S.param vt, S.param qty, S.param mp, S.param pi]) <> ")"
  in dynamicallyParameterized snippet tupleMessageDecoder True

readMessages :: Statement (T.Text,Int32,Int32) (V.Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value))
readMessages =
  Statement sql encoder tupleMessageDecoder True
  where
    sql = "select " <> columnsMessage <> " from pgmq.read($1,$2,$3)"
    encoder =
      contrazip3
        (E.param $ E.nonNullable E.text)
        (E.param $ E.nonNullable E.int4)
        (E.param $ E.nonNullable E.int4)

popMessages :: Statement (T.Text,Int32) (V.Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value))
popMessages =
  Statement sql encoder tupleMessageDecoder True
  where
    sql = "select " <> columnsMessage <> " from pgmq.pop($1,$2)"
    encoder =
      contrazip2
        (E.param $ E.nonNullable E.text)
        (E.param $ E.nonNullable E.int4)

getMetrics :: Statement T.Text (Int64, Maybe Int32, Maybe Int32, Int64, UTCTime, Int64)
getMetrics =
  Statement sql encoder decoder True
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
  Statement sql E.noParams decoder True
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
  in dynamicallyParameterized snippet decoder True

deleteMessage :: Statement (T.Text,Int64) Bool
deleteMessage = [TH.singletonStatement|select pgmq.delete($1::text,$2::int8)::bool|]

deleteMessages :: T.Text -> (V.Vector Int64) -> Statement () (V.Vector Int64)
deleteMessages q v =
  let snippet = "select * from pgmq.delete(" <> S.param q <> "," <> bigintArrayEncoder v <> ")"
      decoder = D.rowVector (D.column (D.nonNullable D.int8))
  in dynamicallyParameterized snippet decoder True

setMessagesVT :: T.Text -> V.Vector Int64 -> Int32 -> Statement () (V.Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value))
setMessagesVT q v s =
  let snippet = "select * from pgmq.set_vt(" <> S.param q <> ","
                <> bigintArrayEncoder v <> "," <> S.param s <> ")"
  in dynamicallyParameterized snippet tupleMessageDecoder True

tupleMessageDecoder :: D.Result (V.Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value))
tupleMessageDecoder =
  D.rowVector $
    (,,,,,) <$>
      D.column (D.nonNullable D.int8) <*>
      D.column (D.nonNullable D.int4) <*>
      D.column (D.nonNullable D.timestamptz) <*>
      D.column (D.nonNullable D.timestamptz) <*>
      D.column (D.nonNullable D.jsonb) <*>
      D.column (D.nullable D.jsonb)

columnsMessage :: ByteString
columnsMessage = "msg_id,read_ct,enqueued_at,vt,message,headers"

columnsMetrics :: ByteString
columnsMetrics = "queue_length,newest_msg_age_sec,oldest_msg_age_sec,total_messages,scrape_time,queue_visible_length"

