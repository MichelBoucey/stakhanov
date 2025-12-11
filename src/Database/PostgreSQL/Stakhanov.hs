module Database.PostgreSQL.Stakhanov
 ( conn

 -- * Queue management
 , create
 , drop

 -- * Sending Messages
 , send

 -- * Reading Messages
 , read
 , pop

-- Deleting/Archiving Messages
 , archive
 , delete

 ) where

import           Data.Aeson.Types
import           Data.Int
import           Data.Vector                              as V hiding (create,
                                                                drop)
import           Database.PostgreSQL.Stakhanov.Connection
import           Database.PostgreSQL.Stakhanov.Statements
import           Database.PostgreSQL.Stakhanov.Types
import qualified Hasql.Connection                         as C
import qualified Hasql.Session                            as S
import           Prelude                                  hiding (drop, read)
-- import Data.Either

-- https://hackage.haskell.org/package/hasql
-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md

create
  :: C.Connection
  -> Queue
  -> IO (Either S.SessionError ())
create c Queue{..} = S.run (S.statement queueName createQueue) c

-- | Permanently deletes all messages in a queue.
-- Returns the number of messages that were deleted.
purge ::a
purge = undefined

drop
  :: C.Connection
  -> Queue
  -> IO (Either S.SessionError Bool)
drop c Queue{..} = S.run (S.statement queueName dropQueue) c

send
  :: C.Connection
  -> Queue
  -> Value
  -> IO (Either S.SessionError Int64)
send c Queue{..} v = S.run (S.statement (queueName,v) sendMessage) c

batchSend :: a
batchSend = undefined

-- | Read one or more messages from a queue. The VT specifies the amount of time
-- in seconds that the message will be invisible to other consumers after reading
read :: C.Connection
  -> Queue
  -> Int32
  -> Int32
  -> IO (Either S.SessionError (Maybe Messages))
read c Queue{..} v q =
  S.run (S.statement (queueName,v,q) readMessages) c >>= \e -> pure $ mMsgs <$> e

-- TODO : readWithPoll

-- | Reads one or more messages from a queue and deletes them upon read.
--
-- Note: utilization of pop() results in at-most-once delivery semantics
-- if the consuming application does not guarantee processing of the message.
--
pop
  :: C.Connection
  -> Queue
  -> Int32
  -> IO (Either S.SessionError (Maybe Messages))
pop c Queue{..} q =
  S.run (S.statement (queueName,q) popMessages) c >>= \e -> pure $ mMsgs <$> e

archive
  :: C.Connection
  -> Queue
  -> Int64
  -> IO (Either S.SessionError Bool)
archive c Queue{..} i = S.run (S.statement (queueName,i) archiveMessage) c

batchArchive :: a
batchArchive = undefined

-- | Deletes a single message from a queue.
delete
  :: C.Connection
  -> Queue
  -> Int64
  -> IO (Either S.SessionError Bool)
delete c Queue{..} i = S.run (S.statement (queueName,i) deleteMessage) c

-- | Delete one or many messages from a queue.
batchDelete ::a
batchDelete =undefined

-- mMsgs
--   :: Vector (Int64, Int32, UTCTime, UTCTime, Value, Maybe Value)
--   -> Maybe Messages
mMsgs vts =
  if V.null vts
    then Nothing
    else Just $ Messages $ msgTupleToMsg <$> vts

