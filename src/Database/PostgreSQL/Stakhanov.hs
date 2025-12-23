module Database.PostgreSQL.Stakhanov
 (

 -- * Queue management
   create
 , declare
 , metrics
 , allMetrics
 , purge
 , drop

 -- * Sending Messages
 , send
 , send'
 , batchSend

 -- * Reading Messages
 , read
 , pop

 -- * Deleting/Archiving Messages
 , archive
 , delete
 , batchArchive
 , batchDelete

 ) where

import           Data.Aeson.Types
import           Data.Int
import           Data.Text                                as T hiding (drop)
import qualified Data.Vector                              as V
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
  :: C.Connection -- ^ The connection to PostgreSQL
  -> T.Text       -- ^ The name of the queue to create
  -> IO (Either S.SessionError Queue)
create c t =
  S.run (S.statement t createQueue) c >>=
    \case
      Right () -> pure $ Right $ Queue t Nothing
      Left r   -> pure $ Left r

-- | Declare an already existing `Queue`.
declare
  :: T.Text -- ^ The name of the queue to declare
  -> Queue
declare = flip Queue Nothing

-- | Get `Queue`'s `Metrics`.
--
-- > λ: metrics co mq
-- > Right (Queue {queueName = "mq", queueMetrics = Just (Metrics {queueLength = 4, newestMsgAge = 272336, oldestMsgAge = 798677, totalMessages = 4, scrapeTime = 2025-12-18 14:23:41.714705 UTC, queueVisibleLength = 4})})
--
metrics
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The name of the queue
  -> IO (Either S.SessionError Queue)
metrics c q@Queue{..} =
  S.run (S.statement queueName getMetrics) c >>= \e -> pure $ addMetrics <$> e
   where
     addMetrics m = q { queueMetrics = Just $ tupleToMetrics m }

-- | Get `Metrics` of all created `Queue`s
allMetrics :: C.Connection -> IO (Either S.SessionError (V.Vector Queue))
allMetrics c =
  S.run (S.statement () getAllMetrics) c >>= \e -> pure $ (tupleToQueueWithMetrics <$>) <$> e

-- | Permanently deletes all `Messages` in a `Queue`.
-- Returns the number of `Messages` that were deleted.
purge
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> IO (Either S.SessionError Int64)
purge c Queue{..} = S.run (S.statement queueName purgeQueue) c

-- | Deletes a `Queue` and its archive.
drop
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> IO (Either S.SessionError Bool)
drop c Queue{..} = S.run (S.statement queueName dropQueue) c

-- | Send a single `Message` to a `Queue`.
-- Returns the `MsgId` of the just created `Message`.
send
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> Value        -- ^ The message to send to the queue (JSON)
  -> IO (Either S.SessionError MsgId)
send c Queue{..} v@(Object _) = S.run (S.statement (queueName,v) sendMessage) c
send _ _         _            = fail "The Aeson Value must be an Object, i.e. a JSON"

-- | Send a single `Message` to a `Queue` with optional metadata (a JSON named headers)
-- and an optional `Delay`. Returns the `MsgId` of the just created `Message`.
send'
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> Value        -- ^ The message to send to the queue (JSON)
  -> Maybe Value  -- ^ Optional message headers/metadata (JSON)
  -> Maybe Delay  -- ^ Optional time before the message becomes visible
  -> IO (Either S.SessionError MsgId)
send' c Queue{..} v@(Object _) mv@(Just (Object _)) md =
  S.run (S.statement () $ sendMessage' queueName v mv md) c
send' _ _         _            _                    _  =
  fail "The Aeson Values must be Objects, i.e. JSON"

-- | Send on or more `Messages` to a `Queue`.
batchSend
  :: C.Connection   -- ^ The connection to PostgreSQL
  -> Queue          -- ^ The queue to work with
  -> V.Vector Value -- ^ A Vector of messages to send to the queue
  -> IO (Either S.SessionError (V.Vector MsgId))
batchSend c Queue{..} v =
  if allJSON v
    then S.run (S.statement () $ sendMessages queueName v) c
    else fail "All Aeson Values of the Vector must be Objects, i.e. all JSON"

-- TODO : batchSend'

-- | Read one or more `Messages` from a `Queue`. The visibility timeout (`VT`) specifies the amount of time
-- in seconds that the `Message` will be invisible to other consumers after reading.
read
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> VT           -- ^ The Visibility Timeout : the time in seconds that message(s) become invisible after reading
  -> Qty          -- ^ The number of messages to read from the queue
  -> IO (Either S.SessionError (Maybe Messages))
read c Queue{..} v q =
  S.run (S.statement (queueName,v,q) readMessages) c >>= \e -> pure $ maybeMessages <$> e

-- TODO : readWithPoll

-- | Reads one or more `Messages` from a `Queue` and /deletes them upon read/.
pop
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> Qty          -- ^ The number of messages to pop from the queue (defaults to 1)
  -> IO (Either S.SessionError (Maybe Messages))
pop c Queue{..} q =
  S.run (S.statement (queueName,q) popMessages) c >>= \e -> pure $ maybeMessages <$> e

-- | Removes a single requested `Message` from the specified `Queue`
-- and inserts it into the `Queue`'s archive.
archive
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> MsgId        -- ^ Message ID of the message to archive
  -> IO (Either S.SessionError Bool)
archive c Queue{..} i = S.run (S.statement (queueName,i) archiveMessage) c

-- | Deletes a batch of requested `Messages` from the specified `Queue` and inserts them into the `Queue`'s archive.
-- Returns a `Vector` of `MsgId` that were successfully archived.
batchArchive
  :: C.Connection   -- ^ The connection to PostgreSQL
  -> Queue          -- ^ The queue to work with
  -> V.Vector MsgId -- ^ A Vector of message IDs to archive
  -> IO (Either S.SessionError (V.Vector MsgId))
batchArchive c Queue{..} v = S.run (S.statement () $ archiveMessages queueName v) c

-- | Deletes a single `Message` from a `Queue`.
delete
  :: C.Connection   -- ^ The connection to PostgreSQL
  -> Queue          -- ^ The queue to work with
  -> MsgId          -- ^ The message ID to delete
  -> IO (Either S.SessionError Bool)
delete c Queue{..} i = S.run (S.statement (queueName,i) deleteMessage) c

-- | Delete one or many `Messages` from a `Queue`.
batchDelete
  :: C.Connection   -- ^ The connection to PostgreSQL
  -> Queue          -- ^ The queue to work with
  -> V.Vector MsgId -- ^ A Vector of message IDs to delete
  -> IO (Either S.SessionError (V.Vector MsgId))
batchDelete c Queue{..} v = S.run (S.statement () $ deleteMessages queueName v) c

