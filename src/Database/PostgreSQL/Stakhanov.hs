-- | This Haskell library, based upon [Hasql](https://hackage.haskell.org/package/hasql)'s ecosystem and [Vector](https://hackage.haskell.org/package/vector), implements the most of [PGMQ](https://github.com/pgmq/pgmq) [API functions](https://pgmq.github.io/pgmq/api/sql/functions/) and should be used qualified.

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
 , batchSetVT
 , listQueues
 , listQueues'
 , details

 -- * Queue details getters
 , getQName
 , getCreatedAt
 , getIsPartitioned
 , getIsUnlogged

 ) where
import           Control.Monad
import           Data.Aeson.Types
import           Data.Int
import           Data.Maybe
import           Data.Text                                as T hiding (drop)
import           Data.Time
import qualified Data.Vector                              as V
import           Database.PostgreSQL.Stakhanov.Internal
import           Database.PostgreSQL.Stakhanov.Statements
import           Database.PostgreSQL.Stakhanov.Types
import qualified Hasql.Connection                         as C
import qualified Hasql.Session                            as S
import           Prelude                                  hiding (drop, read)

-- | Create a new `Queue`.
--
-- > λ: create "MyQueue" conn
-- > Right (Queue {qName = "MyQueue", qPGConn = "a Hasql connection", qDetails = Nothing, qMetrics = Nothing})
--
create
  :: T.Text       -- ^ The name of the queue to create
  -> C.Connection -- ^ The PostgreSQL connection to use
  -> IO (Either S.SessionError Queue)
create t c =
  S.run (S.statement t createQueue) c >>=
    pureMap (\_ -> Queue t (HasqlConn c) Nothing Nothing)

-- | Create an unlogged new `Queue`. This is useful
-- when write throughput is more important that durability.
-- See [PostgreSQL documentation about unlogged tables](https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-UNLOGGED).
--
-- > λ: createUnlogged "MyQueue" conn
-- > Right (Queue {qName = "MyQueue", qPGConn = "a Hasql connection", qDetails = Nothing, qMetrics = Nothing})
--
createUnlogged
  :: T.Text       -- ^ The name of the queue to create
  -> C.Connection -- ^ The PostgreSQL connection to use
  -> IO (Either S.SessionError Queue)
createUnlogged t c =
  S.run (S.statement t createUnloggedQueue) c >>=
    pureMap (\_ -> Queue t (HasqlConn c) Nothing Nothing)

-- | Declare an already existing `Queue`.
--
-- > λ: declare "MyQueue" conn
-- > Queue {qName = "MyQueue", qPGConn = "a Hasql connection", qDetails = Nothing, qMetrics = Nothing}
--
declare
  :: T.Text       -- ^ The name of the queue to declare
  -> C.Connection -- ^ The PostgreSQL connection to use
  -> Queue
declare t c = Queue t (HasqlConn c) Nothing Nothing

-- | Permanently deletes all `Messages` in the given `Queue`.
-- Returns the number of `Messages` that were deleted.
purge
  :: Queue                            -- ^ The queue to work with
  -> IO (Either S.SessionError Int64) -- ^ Returns the number of messages that were deleted
purge Queue{..} =
  S.run (S.statement qName purgeQueue) (unHasqlConn qPGConn)

-- | Deletes a `Queue` and its archive.
drop
  :: Queue                           -- ^ The queue to work with
  -> IO (Either S.SessionError Bool) -- ^ Returns `True` in case of deletion
drop Queue{..} =
  S.run (S.statement qName dropQueue) (unHasqlConn qPGConn)

-- | Send a single `Message` to a `Queue`.
-- Returns the `MsgId` of the just created `Message`.
send
  :: Queue                            -- ^ The queue to work with
  -> Value                            -- ^ A JSON object to send as a message to the queue
  -> IO (Either S.SessionError MsgId) -- ^ Returns the message ID of the just created message
send Queue{..} v@(Object _) =
  S.run (S.statement (qName,v) sendMessage) (unHasqlConn qPGConn)
send _         _            = fail "The Aeson Value must be an Object"

-- | Send a single `Message` to a `Queue` with optional metadata (a JSON object named headers)
-- and an optional `Delay`. Returns the `MsgId` of the just created `Message`.
send'
  :: Queue                            -- ^ The queue to work with
  -> Value                            -- ^ A JSON object sent as a message to the queue
  -> Maybe Value                      -- ^ Maybe a JSON object sent as headers/metadata to the queue
  -> Maybe Delay                      -- ^ Maybe a time before which the message becomes visible
  -> IO (Either S.SessionError MsgId) -- ^ Returns the message ID of the just created message
send' Queue{..} v@(Object _) mv@(Just (Object _)) md =
  S.run (S.statement () $ sendMessage' qName v mv md) (unHasqlConn qPGConn)
send' _ _ _ _  = fail "The Aeson Values must be Objects"

-- | Send on or more `Messages` to a `Queue`. Returns the `MsgId` of just created `Message`.
batchSend
  :: Queue                                       -- ^ The queue to work with
  -> V.Vector Value                              -- ^ A vector of JSON objects sent as messages to the queue
  -> IO (Either S.SessionError MsgIds) -- ^ Returns a vector of message IDs of just created messages
batchSend Queue{..} v =
  if allJSON v
    then S.run (S.statement () $ sendMessages qName v) (unHasqlConn qPGConn)
    else fail "All Aeson Values of the Vector must be Objects"

-- | Send on or more `Messages` to a `Queue` with optional headers (a JSON object of metadata)
-- and an optional `Delay`. Returns `MsgId`s of just created `Messages`.
batchSend'
  :: Queue                                       -- ^ The queue to work with
  -> V.Vector Value                              -- ^ A vector of JSON objects sent as messages to the queue
  -> Maybe (V.Vector Value)                      -- ^ Maybe a vector of JSON objects sent as headers/metadata. Its length must be the same as the vector of messages
  -> Maybe Delay                                 -- ^ Maybe a time before which messages becomes visible
  -> IO (Either S.SessionError MsgIds) -- ^ Returns a vector of message IDs of just created messages
batchSend' Queue{..} vv mvv md =
  if allJSON vv && maybe True allJSON mvv
    then
      if isNothing mvv || isJust mvv && V.length vv == V.length (fromJust mvv)
        then S.run (S.statement () $ sendMessages' qName vv mvv md) (unHasqlConn qPGConn)
        else fail "The vector of headers must be equal to the vector of messages"
    else fail "All Aeson Values of Vectors must be Objects"

-- | Read one or more `Messages` from a `Queue`. The /Visibility Timeout/ (`VT`) specifies the amount of time
-- in seconds that the `Message` will be invisible to other consumers after reading.
read
  :: Queue -- ^ The queue to work with
  -> VT    -- ^ The Visibility Timeout : the time in seconds that message(s) become invisible after reading
  -> Qty   -- ^ The number of messages to read from the queue
  -> IO (Either S.SessionError (Maybe Messages))
read Queue{..} v q =
  S.run (S.statement (qName,v,q) readMessages) (unHasqlConn qPGConn) >>= pureMap maybeMessages

-- | Same as `read`. Also provides convenient long-poll functionality. When there are no `Messages` in the `Queue`,
-- the function call will wait for /max_poll_seconds/ in duration before returning. If messages reach the queue
-- during that duration, they will be read and returned immediately.
readWithPoll
  :: Queue               -- ^ The queue to work with
  -> VT                  -- ^ The Visibility Timeout : the time in seconds that message(s) become invisible after reading
  -> Qty                 -- ^ The number of messages to read from the queue
  -> Maybe Seconds       -- ^ The max_poll_seconds : the time in seconds to wait for new messages to reach the queue. Defaults to 5
  -> Maybe Milliseconds  -- ^ The milliseconds between the internal poll operations. Defaults to 100
  -> IO (Either S.SessionError (Maybe Messages))
readWithPoll Queue{..} v q mmp mpi =
  S.run (S.statement () $ readMessagesWithPoll qName v q mmp mpi) (unHasqlConn qPGConn) >>= pureMap maybeMessages

-- | Reads one or more `Messages` from a `Queue` and /deletes them upon read/.
--
-- > λ: pop MyQueue 2
-- > Right (Just [Message {msgId = 2, readCount = 13, enqueuedAt = 2025-12-09 09:51:50.259464 UTC, visibilityTimeout = 2025-12-15 10:46:41.096843 UTC, message = Object (fromList [("Action",String "hug"),("Quantity",Number 3)]), headers = Nothing},Message {msgId = 3, readCount = 2, enqueuedAt = 2025-12-15 10:44:45.612983 UTC, visibilityTimeout = 2025-12-29 18:04:32.938332 UTC, message = Object (fromList [("Action",String "hug"),("Quantity",Number 5)]), headers = Object (fromList [("Reason",String empathy"")])}])
--
pop
  :: Queue -- ^ The queue to work with
  -> Qty   -- ^ The number of messages to pop from the queue (defaults to 1)
  -> IO (Either S.SessionError (Maybe Messages))
pop Queue{..} y =
  S.run (S.statement (qName,y) popMessages) (unHasqlConn qPGConn) >>= pureMap maybeMessages

-- | Removes a single requested `Message` from the specified `Queue`
-- and inserts it into the `Queue`'s archive.
archive
  :: Queue -- ^ The queue to work with
  -> MsgId -- ^ The message ID of the message to archive
  -> IO (Either S.SessionError Bool)
archive Queue{..} i =
  S.run (S.statement (qName,i) archiveMessage) (unHasqlConn qPGConn)

-- | Deletes a batch of requested `Messages` from the specified `Queue` and inserts them into the `Queue`'s archive.
-- Returns a `V.Vector` of `MsgId` that were successfully archived.
batchArchive
  :: Queue          -- ^ The queue to work with
  -> V.Vector MsgId -- ^ A vector of message IDs to archive
  -> IO (Either S.SessionError MsgIds)
batchArchive Queue{..} v =
  S.run (S.statement () $ archiveMessages qName v) (unHasqlConn qPGConn)

-- | Deletes a single `Message` from a `Queue`.
delete
  :: Queue -- ^ The queue to work with
  -> MsgId -- ^ The message ID to delete
  -> IO (Either S.SessionError Bool)
delete Queue{..} i = S.run (S.statement (qName,i) deleteMessage) (unHasqlConn qPGConn)

-- | Delete one or many `Messages` from a `Queue`.
batchDelete
  :: Queue  -- ^ The queue to work with
  -> MsgIds -- ^ The vector of message IDs to delete
  -> IO (Either S.SessionError MsgIds)
batchDelete Queue{..} v =
  S.run (S.statement () $ deleteMessages qName v) (unHasqlConn qPGConn)

-- | List all the `Queue`s that currently exist, with a raw Hasql `C.Connection` as parameter.
--
-- > λ: listQueues conn
-- > Right [Queue {qName = "test", qPGConn = "a Hasql connection", qDetails = Just (Details {createdAt = 2025-12-18 14:33:41.563365 UTC, isPartitioned = False, isUnlogged = False}), qMetrics = Nothing},Queue {qName = "MyQueue", qPGConn = "a Hasql connection", qDetails = Just (Details {createdAt = 2026-01-09 19:05:24.976526 UTC, isPartitioned = False, isUnlogged = False}), qMetrics = Nothing}]
--
listQueues
  :: C.Connection -- ^ A Hasql connection
  -> IO (Either S.SessionError Queues)
listQueues c =
  S.run (S.statement () getQueuesDetails) c >>= pureMap (toQueue <$>)
  where
    toQueue r =
      Queue
        { qName    = fst r
        , qPGConn  = HasqlConn c
        , qDetails = Just (tupleToDetails $ snd r)
        , qMetrics = Nothing }

-- | Same as `listQueues`, with a `Queue` as parameter.
listQueues'
  :: Queue -- ^ A queue to use its connection to reach PostgreSQL and add this connection to each queue collected
  -> IO (Either S.SessionError Queues)
listQueues' Queue{..} = listQueues (unHasqlConn qPGConn)

-- | Add `Details` information, collected with `listQueues`, to a `Queue` record.
--
-- > λ: Right list <- listQueues conn
-- > λ: details MyQueue list
-- > Just (Queue {qName = "test", qPGConn = "a Hasql connection", qDetails = Just (Details {createdAt = 2025-12-18 14:33:41.563365 UTC, isPartitioned = False, isUnlogged = False}), qMetrics = Nothing})
--
details
  :: Queue       -- ^ The queue to get details for
  -> Queues      -- ^ A vector of queues obtained from one of the listQueues functions
  -> Maybe Queue -- ^ The queue with details added
details q vq =
  case get q vq of
    Nothing -> Nothing
    Just q' -> Just $ q { qDetails = qDetails q' }
  where
    get a b = do
      let c = V.uncons b
      case c of
        Nothing -> Nothing
        Just t  ->
          if qName a == qName (fst t)
            then Just (fst t)
            else get a (snd t)

getQName :: Queue -> T.Text
getQName Queue{..} = qName

getCreatedAt :: Queue -> Maybe UTCTime
getCreatedAt (Queue _ _ (Just Details{..}) _) = Just createdAt
getCreatedAt (Queue _ _ Nothing _)            = Nothing

getIsPartitioned :: Queue -> Maybe Bool
getIsPartitioned (Queue _ _ (Just Details{..}) _) = Just isPartitioned
getIsPartitioned (Queue _ _ Nothing _)            = Nothing

getIsUnlogged :: Queue -> Maybe Bool
getIsUnlogged (Queue _ _ (Just Details{..}) _) = Just isUnlogged
getIsUnlogged (Queue _ _ Nothing _)            = Nothing

-- | Sets the /Visibility Timeout/ (`VT`) of one or many `Messages` to a specified time duration
-- in the future. Returns the `Messages` that were updated.
batchSetVT
  :: Queue   -- ^ The queue to work with
  -> MsgIds  -- ^ The vector of message IDs to set visibility time
  -> Seconds -- ^ Duration from now, in seconds, that the messages VT should be set to
  -> IO (Either S.SessionError Messages)
batchSetVT Queue{..} v s =
  S.run (S.statement () $ setMessagesVT qName v s) (unHasqlConn qPGConn) >>= pureMap (tupleToMessage <$>)

