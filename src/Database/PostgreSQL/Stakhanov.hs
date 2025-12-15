module Database.PostgreSQL.Stakhanov
 ( conn

 -- * Queue management
 , create
 , drop

 -- * Sending Messages
 , send
 , batchSend

 -- * Reading Messages
 , read
 , pop

-- Deleting/Archiving Messages
 , archive
 , delete
 -- , batchDelete

 ) where

import           Data.Aeson.Types
import           Data.Int
import qualified Data.Vector                              as V
import           Database.PostgreSQL.Stakhanov.Connection
import           Database.PostgreSQL.Stakhanov.Internal
import           Database.PostgreSQL.Stakhanov.Statements
import           Database.PostgreSQL.Stakhanov.Types
import qualified Hasql.Connection                         as C
import qualified Hasql.Session                            as S
import           Prelude                                  hiding (drop, read)

-- https://hackage.haskell.org/package/hasql
-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md

create
  :: C.Connection
  -> Queue
  -> IO (Either S.SessionError ())
create c Queue{..} = S.run (S.statement queueName createQueue) c

-- | Permanently deletes all messages in a queue.
-- Returns the number of messages that were deleted.
-- TODO : purge

drop
  :: C.Connection
  -> Queue
  -> IO (Either S.SessionError Bool)
drop c Queue{..} = S.run (S.statement queueName dropQueue) c

send
  :: C.Connection
  -> Queue
  -> Value
  -> IO (Either S.SessionError MsgId)
send c Queue{..} v = S.run (S.statement (queueName,v) sendMessage) c

batchSend
  :: C.Connection
  -> Queue
  -> (V.Vector Value)
  -> IO (Either S.SessionError (V.Vector MsgId))
batchSend c Queue{..} v = S.run (S.statement () $ sendMessages queueName v) c

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
  -> MsgId
  -> IO (Either S.SessionError Bool)
archive c Queue{..} i = S.run (S.statement (queueName,i) archiveMessage) c

-- TODO : batchArchive

-- | Deletes a single message from a queue.
delete
  :: C.Connection
  -> Queue
  -> MsgId
  -> IO (Either S.SessionError Bool)
delete c Queue{..} i = S.run (S.statement (queueName,i) deleteMessage) c

-- | Delete one or many messages from a queue.
-- batchDelete
--   :: C.Connection
--   -> Queue
--   -> (V.Vector MsgId)
--   -> IO (Either S.SessionError (V.Vector MsgId))
-- batchDelete c Queue{..} v = S.run (S.statement () $ sendMessages queueName v) c

