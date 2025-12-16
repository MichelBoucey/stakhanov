module Database.PostgreSQL.Stakhanov.Statements where

import           Contravariant.Extras.Contrazip         (contrazip2, contrazip3)
import           Data.Aeson
import           Data.Int
import qualified Data.Text                              as T
import           Data.Time
import qualified Data.Vector                            as V
import           Database.PostgreSQL.Stakhanov.Internal
import qualified Hasql.Decoders                         as D
import qualified Hasql.DynamicStatements.Snippet        as S
import           Hasql.DynamicStatements.Statement
import qualified Hasql.Encoders                         as E
import           Hasql.Statement
import qualified Hasql.TH                               as TH

createQueue :: Statement T.Text ()
createQueue = [TH.resultlessStatement|select from pgmq.create($1::text)|]

purgeQueue :: Statement T.Text Int64
purgeQueue = [TH.singletonStatement|select pgmq.purge_queue($1::text)::int8|]

dropQueue :: Statement T.Text Bool
dropQueue = [TH.singletonStatement|select pgmq.drop_queue($1::text)::bool|]

sendMessage :: Statement (T.Text,Value) Int64
sendMessage =
  Statement snippet encoder decoder True
    where
      snippet = "select * from pgmq.send($1::text,$2::jsonb)"
      encoder =
        contrazip2
          (E.param (E.nonNullable E.text))
          (E.param (E.nonNullable E.jsonb))
      decoder = D.singleRow $ D.column $ D.nonNullable D.int8

sendMessages :: T.Text -> (V.Vector Value) -> Statement () (V.Vector Int64)
sendMessages q msgs =
  let snippet =
        "select * from pgmq.send_batch(" <> S.param q <> "," <> jsonArrayEncoder msgs <> ")"
      decoder = D.rowVector (D.column (D.nonNullable D.int8))
  in dynamicallyParameterized snippet decoder True

readMessages :: Statement (T.Text,Int32,Int32) (V.Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value))
readMessages =
  Statement snippet encoder messageDecoder True
    where
      snippet = "select msg_id,read_ct,enqueued_at,vt,message,headers from pgmq.read($1,$2,$3)"
      encoder =
        contrazip3
          (E.param (E.nonNullable E.text))
          (E.param (E.nonNullable E.int4))
          (E.param (E.nonNullable E.int4))

popMessages :: Statement (T.Text,Int32) (V.Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value))
popMessages =
  Statement snippet encoder messageDecoder True
    where
      snippet = "select msg_id,read_ct,enqueued_at,vt,message,headers from pgmq.pop($1,$2)"
      encoder =
        contrazip2
          (E.param (E.nonNullable E.text))
          (E.param (E.nonNullable E.int4))

{-
 Metrics
    { queueLength        :: Int64
    , newestMsgAge       :: Int32
    , oldestMsgAge       :: Int32
    , totalMessages      :: Int64
    , scrapeTime         :: UTCTime
    , queueVisibleLength :: Int64
-}
-- getMetrics :: Statement T.Text (Int64, Int32, Int32, Int64, UTCTime, Int64)
-- getMetrics q =
--   let snippet = "select * from pgmq.metrics('my_queue')"

archiveMessage :: Statement (T.Text,Int64) Bool
archiveMessage = [TH.singletonStatement|select pgmq.archive($1::text,$2::int8)::bool|]

archiveMessages :: T.Text -> (V.Vector Int64) -> Statement () (V.Vector Int64)
archiveMessages q v =
  let snippet =
        "select * from pgmq.archive(" <> S.param q <> "," <> jsonArrayEncoder' v <> ")"
      decoder = D.rowVector (D.column (D.nonNullable D.int8))
  in dynamicallyParameterized snippet decoder True

deleteMessage :: Statement (T.Text,Int64) Bool
deleteMessage = [TH.singletonStatement|select pgmq.delete($1::text,$2::int8)::bool|]

deleteMessages :: T.Text -> (V.Vector Int64) -> Statement () (V.Vector Int64)
deleteMessages q v =
  let snippet =
        "select * from pgmq.delete(" <> S.param q <> "," <> jsonArrayEncoder' v <> ")"
      decoder = D.rowVector (D.column (D.nonNullable D.int8))
  in dynamicallyParameterized snippet decoder True

messageDecoder :: D.Result (V.Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value))
messageDecoder =
  D.rowVector $
    (,,,,,) <$>
      D.column (D.nonNullable D.int8) <*>
      D.column (D.nonNullable D.int4) <*>
      D.column (D.nonNullable D.timestamptz) <*>
      D.column (D.nonNullable D.timestamptz) <*>
      D.column (D.nonNullable D.jsonb) <*>
      D.column (D.nullable D.jsonb)

