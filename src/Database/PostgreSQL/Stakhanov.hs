module Database.PostgreSQL.Stakhanov
 ( conn

 -- * Queue management
 , create
 , declare
 , metrics
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
 , delete
 , batchArchive
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

-- | Create a new `Queue`.
--
-- > λ: create co "mq"
-- > Right (Queue {queueName = "mq", queueMetrics = Nothing})
--
create
  :: C.Connection
  -> T.Text
  -> IO (Either S.SessionError Queue)
create c t =
  S.run (S.statement t createQueue) c >>=
    \case
      Right () -> pure $ Right $ Queue t Nothing
      Left r   -> pure $ Left r

-- | Declare an already existing `Queue`.
declare :: T.Text -> Queue
declare = flip Queue Nothing

-- | Get `Queue`'s `Metrics`.
--
-- > λ: metrics co mq
-- > Right (Queue {queueName = "mq", queueMetrics = Just (Metrics {queueLength = 4, newestMsgAge = 272336, oldestMsgAge = 798677, totalMessages = 4, scrapeTime = 2025-12-18 14:23:41.714705 UTC, queueVisibleLength = 4})})
--
metrics :: C.Connection -> Queue -> IO (Either S.SessionError Queue)
metrics c q@Queue{..} =
  S.run (S.statement queueName getMetrics) c >>= \e -> pure $ addMetrics <$> e
   where
     addMetrics m = q { queueMetrics = Just $ tupleToMetrics m }

-- | Permanently deletes all `Messages` in a `Queue`.
-- Returns the number of `Messages` that were deleted.
purge
  :: C.Connection
  -> Queue
  -> IO (Either S.SessionError Int64)
purge c Queue{..} = S.run (S.statement queueName purgeQueue) c

-- | Deletes a `Queue` and its archive.
drop
  :: C.Connection
  -> Queue
  -> IO (Either S.SessionError Bool)
drop c Queue{..} = S.run (S.statement queueName dropQueue) c

-- | Send a single `Message` to a `Queue`.
send
  :: C.Connection
  -> Queue
  -> Value
  -> IO (Either S.SessionError MsgId)
send c Queue{..} v = S.run (S.statement (queueName,v) sendMessage) c

-- | Send on or more `Messages` to a `Queue`.
batchSend
  :: C.Connection
  -> Queue
  -> V.Vector Value
  -> IO (Either S.SessionError (V.Vector MsgId))
batchSend c Queue{..} v = S.run (S.statement () $ sendMessages queueName v) c

-- | Read one or more `Messages` from a `Queue`. The VT specifies the amount of time
-- in seconds that the `Message` will be invisible to other consumers after reading.
read :: C.Connection
  -> Queue
  -> Int32
  -> Int32
  -> IO (Either S.SessionError (Maybe Messages))
read c Queue{..} v q =
  S.run (S.statement (queueName,v,q) readMessages) c >>= \e -> pure $ maybeMessages <$> e

-- TODO : readWithPoll

-- | Reads one or more `Messages` from a `Queue` and /deletes them upon read/.
pop
  :: C.Connection
  -> Queue
  -> Int32
  -> IO (Either S.SessionError (Maybe Messages))
pop c Queue{..} q =
  S.run (S.statement (queueName,q) popMessages) c >>= \e -> pure $ maybeMessages <$> e

-- | Removes a single requested `Message` from the specified `Queue`
-- and inserts it into the `Queue`'s archive.
archive
  :: C.Connection
  -> Queue
  -> MsgId
  -> IO (Either S.SessionError Bool)
archive c Queue{..} i = S.run (S.statement (queueName,i) archiveMessage) c

-- | Deletes a batch of requested `Messages` from the specified `Queue` and inserts them into the `Queue`'s archive.
-- Returns a `Vector` of `MsgId` that were successfully archived.
batchArchive
  :: C.Connection
  -> Queue
  -> V.Vector MsgId
  -> IO (Either S.SessionError (V.Vector MsgId))
batchArchive c Queue{..} v = S.run (S.statement () $ archiveMessages queueName v) c

-- | Deletes a single `Message` from a `Queue`.
delete
  :: C.Connection
  -> Queue
  -> MsgId
  -> IO (Either S.SessionError Bool)
delete c Queue{..} i = S.run (S.statement (queueName,i) deleteMessage) c

-- | Delete one or many `Messages` from a `Queue`.
batchDelete
  :: C.Connection
  -> Queue
  -> V.Vector MsgId
  -> IO (Either S.SessionError (V.Vector MsgId))
batchDelete c Queue{..} v = S.run (S.statement () $ deleteMessages queueName v) c

