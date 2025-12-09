module Database.PostgreSQL.Bijou.Statements where

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

-- https://hackage.haskell.org/package/hasql-th-0.4.0.23/docs/Hasql-TH.html
-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md

createQueue :: Statement T.Text ()
createQueue = [TH.resultlessStatement|select from pgmq.create($1::text)|]

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

dropQueue :: Statement T.Text Bool
dropQueue = [TH.singletonStatement|select pgmq.drop_queue($1::text)::bool|]

{-
SELECT * FROM pgmq.read(
  queue_name => 'my_queue',
  vt         => 30,
  qty        => 1
);

data Message = Message { messageId :: Int64, readCount ::Int32, enqueuedAt :: UTCTime, visibilityTimeout :: UTCTime, jsonMessage :: !Object, jsonHeaders :: !Object }

-}
readMessages :: Statement (T.Text,Int32,Int32) (Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value))
readMessages =
  Statement sql encoder decoder True
    where
      sql = "select msg_id,read_ct,enqueued_at,vt,message,headers from pgmq.read($1,$2,$3)"
      encoder =
        contrazip3
          (E.param (E.nonNullable E.text))
          (E.param (E.nonNullable E.int4))
          (E.param (E.nonNullable E.int4))
      decoder =
        D.rowVector $
          (,,,,,) <$>
            D.column (D.nonNullable D.int8) <*>
            D.column (D.nonNullable D.int4) <*>
            D.column (D.nonNullable D.timestamptz) <*>
            D.column (D.nonNullable D.timestamptz) <*>
            D.column (D.nonNullable D.jsonb) <*>
            D.column (D.nullable D.jsonb)

