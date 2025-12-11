module Database.PostgreSQL.Stakhanov.Statements where

import           Contravariant.Extras.Contrazip  (contrazip2,contrazip3)
import           Data.Aeson
import           Data.Int
import qualified Data.Text                       as T
import           Data.Time
import           Data.Vector
import qualified Hasql.Decoders                  as D
import qualified Hasql.Encoders                  as E
import           Hasql.Statement
import qualified Hasql.TH                        as TH
-- Hasql.DynamicStatements.Statement

-- https://hackage.haskell.org/package/hasql-th-0.4.0.23/docs/Hasql-TH.html
-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md

createQueue :: Statement T.Text ()
createQueue = [TH.resultlessStatement|select from pgmq.create($1::text)|]

dropQueue :: Statement T.Text Bool
dropQueue = [TH.singletonStatement|select pgmq.drop_queue($1::text)::bool|]

sendMessage :: Statement (T.Text,Value) Int64
sendMessage =
  Statement sql encoder decoder True
    where
      sql = "select * from pgmq.send($1::text,$2::jsonb)"
      encoder = 
        contrazip2
          (E.param (E.nonNullable E.text))
          (E.param (E.nonNullable E.jsonb))
      decoder = D.singleRow $ D.column $ D.nonNullable D.int8

-- selectSubstring :: Text -> Maybe Int32 -> Maybe Int32 -> Statement () Text
-- selectSubstring string from to = let
--   snippet =
--     "select substring(" <> Snippet.param string <>
--     foldMap (mappend " from " . Snippet.param) from <>
--     foldMap (mappend " for " . Snippet.param) to <>
--     ")"
--   decoder = Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.text))
--   in dynamicallyParameterized snippet decoder True

batchSendStmt :: Statement (T.Text,Vector Value, Maybe (Vector Value), Maybe Int32, Maybe UTCTime) (Vector Int64)
batchSendStmt = undefined

readMessages :: Statement (T.Text,Int32,Int32) (Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value))
readMessages =
  Statement sql encoder messageDecoder True
    where
      sql = "select msg_id,read_ct,enqueued_at,vt,message,headers from pgmq.read($1,$2,$3)"
      encoder =
        contrazip3
          (E.param (E.nonNullable E.text))
          (E.param (E.nonNullable E.int4))
          (E.param (E.nonNullable E.int4))

popMessages :: Statement (T.Text,Int32) (Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value))
popMessages =
  Statement sql encoder messageDecoder True
    where
      sql = "select msg_id,read_ct,enqueued_at,vt,message,headers from pgmq.pop($1,$2)"
      encoder =
        contrazip2
          (E.param (E.nonNullable E.text))
          (E.param (E.nonNullable E.int4))

archiveMessage :: Statement (T.Text,Int64) Bool
archiveMessage = [TH.singletonStatement|select pgmq.archive($1::text,$2::int8)::bool|]

deleteMessage :: Statement (T.Text,Int64) Bool
deleteMessage = [TH.singletonStatement|select pgmq.delete($1::text,$2::int8)::bool|]

messageDecoder :: D.Result (Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value))
messageDecoder =
  D.rowVector $
    (,,,,,) <$>
      D.column (D.nonNullable D.int8) <*>
      D.column (D.nonNullable D.int4) <*>
      D.column (D.nonNullable D.timestamptz) <*>
      D.column (D.nonNullable D.timestamptz) <*>
      D.column (D.nonNullable D.jsonb) <*>
      D.column (D.nullable D.jsonb)

