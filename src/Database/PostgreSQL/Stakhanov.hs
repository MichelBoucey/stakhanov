module Database.PostgreSQL.Stakhanov
 ( conn
 , create
 , drop
 , send
 , read
 , delete
 , archive
 ) where

import           Data.Aeson.Types
import           Data.Int
import           Data.Time
import           Data.Vector                              as V hiding (create, drop)
import           Database.PostgreSQL.Stakhanov.Connection
import           Database.PostgreSQL.Stakhanov.Statements
import           Database.PostgreSQL.Stakhanov.Types
import qualified Hasql.Connection                         as C
import qualified Hasql.Session                            as S
import           Prelude                                  hiding (drop, read)

-- https://hackage.haskell.org/package/hasql
-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md

-- Queue management

create :: C.Connection -> Queue -> IO (Either S.SessionError ())
create c Queue{..} = S.run (S.statement queueName createQueue) c

purgeQueue ::a
purgeQueue = undefined

drop :: C.Connection -> Queue -> IO (Either S.SessionError Bool)
drop c Queue{..} = S.run (S.statement queueName dropQueue) c

-- Sending Messages

-- TODO : Replacing Value per Object
send :: C.Connection -> Queue -> Value -> IO (Either S.SessionError Int64)
send c Queue{..} v = S.run (S.statement (queueName,v) sendMessage) c

sendBatch :: a
sendBatch = undefined

-- Reading Messages

-- TODO : WIP
read :: C.Connection -> Queue -> Int32 -> Int32 -> IO (Either S.SessionError (Maybe Messages))
read c Queue{..} v q = do
  Right vts <- S.run (S.statement (queueName,v,q) readMessages) c
  pure $ Right $ Just (V.map tupleToMessage vts :: Messages)

-- TODO : readWithPoll

pop :: a
pop = undefined

-- Deleting/Archiving Messages

archive :: C.Connection -> Queue -> Int64 -> IO (Either S.SessionError Bool)
archive c Queue{..} i = S.run (S.statement (queueName,i) archiveMessage) c

archiveBatch = undefined

delete :: C.Connection -> Queue -> Int64 -> IO (Either S.SessionError Bool)
delete c Queue{..} i = S.run (S.statement (queueName,i) deleteMessage) c

deleteBatch ::a
deleteBatch =undefined

