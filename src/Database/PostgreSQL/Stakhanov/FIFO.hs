-- | [Full PGMQ FIFO documentation](https://pgmq.github.io/pgmq/latest/fifo-queues/#fifo-queues).

module Database.PostgreSQL.Stakhanov.FIFO
  (

  -- * Reading FIFO Messages
    readGrouped
  , readGroupedWithPoll
  , readGroupedRR
  , readGroupedRRWithPoll
  , readGroupedHead

  -- * Utils
  , createFIFOIndex
  , createFIFOIndexesAll

  ) where

import           Database.PostgreSQL.Stakhanov.Internal
import           Database.PostgreSQL.Stakhanov.Statements
import           Database.PostgreSQL.Stakhanov.Types
import           Hasql.Connection
import           Hasql.Errors
import           Hasql.Session

-- https://pgmq.github.io/pgmq/latest/api/sql/functions/#read_grouped
readGrouped
  :: Queue
  -> VT
  -> Qty
  -> IO (Either SessionError (Maybe Messages))
readGrouped Queue{..} v q =
  use (unHasqlConn qPGConn) (statement (qName,v,q) readGroupedMessages) >>= pureMap maybeMessages

-- https://pgmq.github.io/pgmq/latest/api/sql/functions/#read_grouped_with_poll
readGroupedWithPoll
  :: Queue              -- ^ The queue to work with
  -> VT                 -- ^ The Visibility Timeout : the time in seconds that message(s) become invisible after reading
  -> Qty                -- ^ The number of messages to read from the queue
  -> Maybe Seconds      -- ^ The max_poll_seconds : the time in seconds to wait for new messages to reach the queue. Defaults to 5
  -> Maybe Milliseconds -- ^ The milliseconds between the internal poll operations. Defaults to 100
  -> IO (Either SessionError (Maybe Messages))
readGroupedWithPoll Queue{..} v q mmp mpi =
  use (unHasqlConn qPGConn) (statement () $ readGroupedMessagesWithPoll qName v q mmp mpi) >>= pureMap maybeMessages

-- https://pgmq.github.io/pgmq/latest/api/sql/functions/#read_grouped_rr
readGroupedRR
 :: Queue
 -> VT
 -> Qty
 -> IO (Either SessionError (Maybe Messages))
readGroupedRR = undefined

-- pgmq.read_grouped_rr_with_poll(queue_name, vt, qty, max_poll_seconds, poll_interval_ms)
readGroupedRRWithPoll
  :: Queue              -- ^ The queue to work with
  -> VT                 -- ^ The Visibility Timeout : the time in seconds that message(s) become invisible after reading
  -> Qty                -- ^ The number of messages to read from the queue
  -> Maybe Seconds      -- ^ The max_poll_seconds : the time in seconds to wait for new messages to reach the queue. Defaults to 5
  -> Maybe Milliseconds -- ^ The milliseconds between the internal poll operations. Defaults to 100
  -> IO (Either SessionError (Maybe Messages))
readGroupedRRWithPoll = undefined

-- https://pgmq.github.io/pgmq/latest/fifo-queues/#pgmqread_grouped_headqueue_name-vt-qty
readGroupedHead
 :: Queue
 -> VT
 -> Qty
 -> IO (Either SessionError (Maybe Messages))
readGroupedHead = undefined

-- https://pgmq.github.io/pgmq/latest/api/sql/functions/#create_fifo_index
createFIFOIndex :: Queue -> IO ()
createFIFOIndex = undefined

-- https://pgmq.github.io/pgmq/latest/api/sql/functions/#create_fifo_indexes_all
createFIFOIndexesAll :: IO ()
createFIFOIndexesAll = undefined

