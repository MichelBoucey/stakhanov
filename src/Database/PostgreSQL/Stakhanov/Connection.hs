module Database.PostgreSQL.Stakhanov.Connection where

import           Hasql.Connection
import           Hasql.Connection.Setting            (connection)
import           Hasql.Connection.Setting.Connection (string)

acquirePgConn :: IO (Either ConnectionError Connection)
acquirePgConn = acquire [connection $ string "postgres://postgres:postgres@0.0.0.0:5432/postgres"]

