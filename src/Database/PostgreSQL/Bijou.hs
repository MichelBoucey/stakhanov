module Database.PostgreSQL.Bijou where

-- https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md
import Hasql.Session -- (Session)
import qualified Hasql.Connection as C
import qualified Hasql.Connection.Setting as S
import qualified Hasql.Connection.Setting.Connection as SC
import Data.Text as T
import Database.PostgreSQL.Bijou.Statements
-- import Data.Int

conn :: SC.Connection
conn = SC.string "postgres://postgres:pgmq@0.0.0.0:5432/postgres"

create :: T.Text -> IO (Either SessionError ()) 
create q = do
  Right co <- C.acquire [(S.connection conn)]
  run (statement q createQueue) co

drop :: T.Text -> IO (Either SessionError Bool) 
drop q = do
  Right co <- C.acquire [(S.connection conn)]
  run (statement q dropQueue) co

-- Sending Messages

send :: a
send = undefined

sendBatch :: a
sendBatch = undefined

-- Reading Messages

read :: a
read = undefined

read_with_poll :: a
read_with_poll = undefined

pop :: a
pop = undefined

-- Deleting/Archiving Messages

delete :: a
delete = undefined

deleteBatch ::a
deleteBatch =undefined

purgeQueue ::a
purgeQueue = undefined

