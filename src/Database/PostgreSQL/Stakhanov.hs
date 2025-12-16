module Database.PostgreSQL.Stakhanov
 ( conn

 -- * Queue management
 , create
 , declare
 , purge
 , drop

 -- * Sending Messages
 , send
 , batchSend

 -- * Reading Messages
 , read
 , pop

 -- * Deleting/Archiving Messages
 , archive
 , batchArchive
 , delete
 , batchDelete

 -- -- * Utilities
 -- , metrics
 ) where

import           Data.Aeson.Types
import           Data.Int
import           Data.Text                                as T hiding (drop)
import qualified Data.Vector                              as V
import           Database.PostgreSQL.Stakhanov.Connection
import           Database.PostgreSQL.Stakhanov.Internal
import           Database.PostgreSQL.Stakhanov.Statements
import           Database.PostgreSQL.Stakhanov.Types
import qualified Hasql.Connection                         as C
import qualified Hasql.Session                            as S
import           Prelude                                  hiding (drop, read)

-- | Create a new queue.
create
  :: C.Connection
  -> T.Text
  -> IO (Either S.SessionError Queue)
create c t =
  S.run (S.statement t createQueue) c >>=
    \case
      Right () -> pure $ Right $ Queue t Nothing
      Left r   -> pure $ Left r

-- | Declare an already existing queue
declare :: T.Text -> Queue
declare t = Queue t Nothing

-- metrics :: Queue -> (Either S.SessionError Queue)

-- | Permanently deletes all messages in a queue.
-- Returns the number of messages that were deleted.
purge
  :: C.Connection
  -> Queue
  -> IO (Either S.SessionError Int64)
purge c Queue{..} = S.run (S.statement queueName purgeQueue) c

-- | Deletes a queue and its archives.
drop
  :: C.Connection
  -> Queue
  -> IO (Either S.SessionError Bool)
drop c Queue{..} = S.run (S.statement queueName dropQueue) c

-- | Send a single message to a queue.
send
  :: C.Connection
  -> Queue
  -> Value
  -> IO (Either S.SessionError MsgId)
send c Queue{..} v = S.run (S.statement (queueName,v) sendMessage) c

-- | Send on or more messages to a queue.
batchSend
  :: C.Connection
  -> Queue
  -> V.Vector Value
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
pop
  :: C.Connection
  -> Queue
  -> Int32
  -> IO (Either S.SessionError (Maybe Messages))
pop c Queue{..} q =
  S.run (S.statement (queueName,q) popMessages) c >>= \e -> pure $ mMsgs <$> e

-- | Removes a single requested message from the specified queue
-- and inserts it into the queue's archive.
archive
  :: C.Connection
  -> Queue
  -> MsgId
  -> IO (Either S.SessionError Bool)
archive c Queue{..} i = S.run (S.statement (queueName,i) archiveMessage) c

-- | Deletes a batch of requested messages from the specified queue and inserts them into the queue's archive.
-- Returns a Vector of MsgId that were successfully archived.
batchArchive
  :: C.Connection
  -> Queue
  -> V.Vector MsgId
  -> IO (Either S.SessionError (V.Vector MsgId))
batchArchive c Queue{..} v = S.run (S.statement () $ archiveMessages queueName v) c

-- | Deletes a single message from a queue.
delete
  :: C.Connection
  -> Queue
  -> MsgId
  -> IO (Either S.SessionError Bool)
delete c Queue{..} i = S.run (S.statement (queueName,i) deleteMessage) c

-- | Delete one or many messages from a queue.
batchDelete
  :: C.Connection
  -> Queue
  -> V.Vector MsgId
  -> IO (Either S.SessionError (V.Vector MsgId))
batchDelete c Queue{..} v = S.run (S.statement () $ deleteMessages queueName v) c

