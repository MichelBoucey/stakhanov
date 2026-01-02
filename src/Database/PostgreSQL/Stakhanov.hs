module Database.PostgreSQL.Stakhanov
 (

 -- * Queue management
   create
 , createUnlogged
 , declare
 , purge
 , drop

 -- * Sending Messages
 , send
 , send'
 , batchSend
 , batchSend'

 -- * Reading Messages
 , read
 , readWithPoll
 , pop

 -- * Deleting/Archiving Messages
 , archive
 , delete
 , batchArchive
 , batchDelete

 -- * Utilities
 , listQueues
 , batchSetVT

 ) where

import           Data.Aeson.Types
import           Data.Int
import           Data.Maybe
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
    \e -> pure $ (\_ -> Queue t Nothing Nothing) <$> e

-- | Create an unlogged new `Queue`. This is useful
-- when write throughput is more important that durability.
-- See [PostgreSQL documentation about unlogged tables](https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-UNLOGGED).
createUnlogged
  :: C.Connection -- ^ The connection to PostgreSQL
  -> T.Text       -- ^ The name of the queue to create
  -> IO (Either S.SessionError Queue)
createUnlogged c t =
  S.run (S.statement t createUnloggedQueue) c >>=
    \e -> pure $ (\_ -> Queue t Nothing Nothing) <$> e

-- | Declare an already existing `Queue`.
declare
  :: T.Text -- ^ The name of the queue to declare
  -> Queue
declare t = Queue t Nothing Nothing

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
  -> Value        -- ^ The message to send to the queue (a JSON object)
  -> IO (Either S.SessionError MsgId)
send c Queue{..} v@(Object _) = S.run (S.statement (queueName,v) sendMessage) c
send _ _         _            = fail "The Aeson Value must be an Object"

-- | Send a single `Message` to a `Queue` with optional metadata (a JSON object named headers)
-- and an optional `Delay`. Returns the `MsgId` of the just created `Message`.
send'
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> Value        -- ^ The message to send to the queue (a JSON object)
  -> Maybe Value  -- ^ Optional message headers/metadata (a JSON object)
  -> Maybe Delay  -- ^ Optional time before the message becomes visible
  -> IO (Either S.SessionError MsgId)
send' c Queue{..} v@(Object _) mv@(Just (Object _)) md =
  S.run (S.statement () $ sendMessage' queueName v mv md) c
send' _ _         _            _                    _  =
  fail "The Aeson Values must be Objects"

-- | Send on or more `Messages` to a `Queue`.
batchSend
  :: C.Connection   -- ^ The connection to PostgreSQL
  -> Queue          -- ^ The queue to work with
  -> V.Vector Value -- ^ A Vector of messages (JSON objects) to send to the queue
  -> IO (Either S.SessionError (V.Vector MsgId))
batchSend c Queue{..} v =
  if allJSON v
    then S.run (S.statement () $ sendMessages queueName v) c
    else fail "All Aeson Values of the Vector must be Objects"

-- | Send on or more `Messages` to a `Queue` with optional headers (a JSON object of metadata)
-- and an optional `Delay`. Returns `MsgId`s of just created `Messages`.
batchSend'
  :: C.Connection           -- ^ The connection to PostgreSQL
  -> Queue                  -- ^ The queue to work with
  -> V.Vector Value         -- ^ A vector of messages (JSON objects) to send to the queue
  -> Maybe (V.Vector Value) -- ^ Optional vector of headers/metadata (JSON objects). Its length must be the same as the vector of messages
  -> Maybe Delay            -- ^ Optional time before messages becomes visible
  -> IO (Either S.SessionError (V.Vector MsgId))
batchSend' c Queue{..} vv mvv md =
  if allJSON vv && maybe True allJSON mvv
    then
      if isNothing mvv || isJust mvv && V.length vv == V.length (fromJust mvv)
        then S.run (S.statement () $ sendMessages' queueName vv mvv md) c
        else fail "The vector of headers must be equal to the vector of messages"
    else fail "All Aeson Values of Vectors must be Objects"

-- | Read one or more `Messages` from a `Queue`. The visibility timeout (`VT`) specifies the amount of time
-- in seconds that the `Message` will be invisible to other consumers after reading.
read
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> VT           -- ^ The Visibility Timeout : the time in seconds that message(s) become invisible after reading
  -> Qty          -- ^ The number of messages to read from the queue
  -> IO (Either S.SessionError (Maybe Messages))
read c Queue{..} v q =
  S.run (S.statement (queueName,v,q) readMessages) c >>= pureMap maybeMessages

-- | Same as `read`. Also provides convenient long-poll functionality. When there are no `Messages` in the `Queue`,
-- the function call will wait for max_poll_seconds in duration before returning. If messages reach the queue
-- during that duration, they will be read and returned immediately.
readWithPoll
  :: C.Connection        -- ^ The connection to PostgreSQL
  -> Queue               -- ^ The queue to work with
  -> VT                  -- ^ The Visibility Timeout : the time in seconds that message(s) become invisible after reading
  -> Qty                 -- ^ The number of messages to read from the queue
  -> Maybe Seconds       -- ^ The max_poll_seconds : the time in seconds to wait for new messages to reach the queue. Defaults to 5
  -> Maybe Milliseconds  -- ^ Milliseconds between the internal poll operations. Defaults to 100
  -> IO (Either S.SessionError (Maybe Messages))
readWithPoll c Queue{..} v q mmp mpi =
  S.run (S.statement () $ readMessagesWithPoll queueName v q mmp mpi) c >>= pureMap maybeMessages

-- | Reads one or more `Messages` from a `Queue` and /deletes them upon read/.
pop
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> Qty          -- ^ The number of messages to pop from the queue (defaults to 1)
  -> IO (Either S.SessionError (Maybe Messages))
pop c Queue{..} q =
  S.run (S.statement (queueName,q) popMessages) c >>= pureMap maybeMessages

-- | Removes a single requested `Message` from the specified `Queue`
-- and inserts it into the `Queue`'s archive.
archive
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> MsgId        -- ^ The message ID of the message to archive
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
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> MsgId        -- ^ The message ID to delete
  -> IO (Either S.SessionError Bool)
delete c Queue{..} i = S.run (S.statement (queueName,i) deleteMessage) c

-- | Delete one or many `Messages` from a `Queue`.
batchDelete
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> MsgIds       -- ^ A Vector of message IDs to delete
  -> IO (Either S.SessionError (V.Vector MsgId))
batchDelete c Queue{..} v = S.run (S.statement () $ deleteMessages queueName v) c

listQueues
  :: C.Connection -- ^ The connection to PostgreSQL
  -> IO (Either S.SessionError (V.Vector Queue))
listQueues c =
  S.run (S.statement () getQueuesDetails) c >>= pureMap (fmap toQueue)
  where
    toQueue r =
      Queue
        { queueName = fst r
        , queueDetails = Just (tupleToDetails $ snd r)
        , queueMetrics = Nothing }

-- | Sets the Visibility Timeout of one or many messages to a specified time duration
-- in the future. Returns the `Messages` that were updated.
batchSetVT
  :: C.Connection -- ^ The connection to PostgreSQL
  -> Queue        -- ^ The queue to work with
  -> MsgIds       -- ^ A vector of message IDs to set visibility time
  -> Seconds      -- ^ Duration from now, in seconds, that the messages VT should be set to
  -> IO (Either S.SessionError Messages)
batchSetVT c Queue{..} v s =
  S.run (S.statement () $ setMessagesVT queueName v s) c >>= pureMap (fmap tupleToMessage)

