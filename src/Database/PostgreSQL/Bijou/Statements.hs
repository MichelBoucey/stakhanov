{-# LANGUAGE QuasiQuotes #-}

module Database.PostgreSQL.Bijou.Statements where

import           Data.Aeson
import           Data.Int
import qualified Data.Text       as T
import           Hasql.Statement
import qualified Hasql.TH        as TH

-- https://hackage.haskell.org/package/hasql-th-0.4.0.23/docs/Hasql-TH.html
-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md

createQueue :: Statement T.Text ()
createQueue = [TH.resultlessStatement|select from pgmq.create($1::text)|]

sendMessage :: Statement (T.Text,Value) Int64
sendMessage = [TH.singletonStatement|select msg_id::int8 from pgmq.send($1::text,$2::jsonb)|]

dropQueue :: Statement T.Text Bool
dropQueue = [TH.singletonStatement|select pgmq.drop_queue($1::text)::bool|]

-- data Message = Message { messageId :: Int64, readCount ::Int32, enqueuedAt :: UTCTime, visibilityTimeout :: UTCTime, jsonMessage :: !Object, jsonHeaders :: !Object }

{-
SELECT * FROM pgmq.read(
  queue_name => 'my_queue',
  vt         => 30,
  qty        => 1
);
-}
