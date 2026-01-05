
-- | This module is just here for a quick start with Stakhanov and Hasql, otherwise use the module [Hasql.Connection](https://hackage.haskell.org/package/hasql/docs/Hasql-Connection.html) and its subsequent modules to create full-blown Hasql connections.

module Database.PostgreSQL.Stakhanov.Connection where

import qualified Data.Text                           as T
import           Hasql.Connection
import           Hasql.Connection.Setting            (connection)
import           Hasql.Connection.Setting.Connection (string)

-- | Get a local PostgreSQL connection to a Docker container of PGMQ, with the default PostgreSQL connection string "__postgres:\/\/postgres:postgres@0.0.0.0:5432/postgres__".
acquireLocalPGConn :: IO (Either ConnectionError Connection)
acquireLocalPGConn = acquire [connection $ string "postgres://postgres:postgres@0.0.0.0:5432/postgres"]

-- | Get a PostgreSQL connection configured with the given PostgreSQL connection string.
acquirePGConn :: T.Text -> IO (Either ConnectionError Connection)
acquirePGConn t = acquire [connection $ string t]

