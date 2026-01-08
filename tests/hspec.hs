{-# LANGUAGE OverloadedStrings #-}

import           Data.Aeson
import qualified Data.Aeson.KeyMap                        as K
import           Data.Int
import qualified Data.Vector                              as V
import qualified Database.PostgreSQL.Stakhanov            as S
import           Database.PostgreSQL.Stakhanov.Connection
import           Database.PostgreSQL.Stakhanov.Types
import           Test.Hspec

main :: IO ()
main = hspec $ do

  describe "Create a Hspec test queue" $
      it "Return the record of the created queue" $ do
        Right c <- acquireLocalPGConn
        Right q <- S.create "HspecTestQueue" c
        q `shouldBe` (q :: Queue)

  describe "Send a message" $
      it "Return the ID of message created" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            j = Object (K.fromList [("Item", String "Banana"),("Qty", Number 3)])
        S.send q j `shouldReturn` Right 1

  describe "Send a message with options (send')" $
      it "Return the ID of message created" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            j1 = Object (K.fromList [("Item", String "Apple"),("Qty", Number 6)])
            j2 = Object (K.fromList [("Metadata", Object (K.fromList [("Checksum", Bool True), ("Lenght", Number 37)]))])
        S.send' q j1 (Just j2) Nothing `shouldReturn` Right 2

  describe "Send a batch of messages" $
      it "Return the IDs of messages created" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            o1 = Object (K.fromList [("Item", String "Pineapple"),("Qty", Number 4)])
            o2 = Object (K.fromList [("Item", String "Tomato"),("Qty", Number 9)])
            o3 = Object (K.fromList [("Item", String "strawnberry"),("Qty", Number 23)])
            v  = V.fromList[o1,o2,o3]
        S.batchSend q v `shouldReturn` Right (V.fromList[3,4,5])

  describe "Send a batch of messages with options (batchSend')" $
      it "Return the IDs of messages created" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            o1 = Object (K.fromList [("Item", String "Potato"),("Qty", Number 16)])
            o2 = Object (K.fromList [("Item", String "Carrot"),("Qty", Number 21)])
            o3 = Object (K.fromList [("Item", String "strawnberry"),("Qty", Number 23)])
            v  = V.fromList[o1,o2,o3]
        S.batchSend' q v Nothing (Just (InSeconds 1)) `shouldReturn` Right (V.fromList[6,7,8])

  describe "Read a message" $
      it "Maybe returns Messages (Vector Message)" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        Right vm <- S.read q 30 1
        vm  `shouldBe` (vm :: Maybe Messages)

  describe "Archive a message" $
      it "Return True" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        S.archive q 1 `shouldReturn` Right True

  describe "Archive messages" $
      it "Return the IDs of archived messages" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            v = V.fromList[3,4,5]
        S.batchArchive q v `shouldReturn` Right v

  describe "Delete a message" $
      it "Return True" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        S.delete q 2 `shouldReturn` Right True

  describe "Delete messages" $
      it "Return the IDs of deleted messages" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
            v = V.fromList[6,7,8]
        S.batchDelete q v `shouldReturn` Right v

  describe "Purge the Hspec test queue" $
      it "Return the number of deleted messages" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        Right n <- S.purge q
        n `shouldBe` (n :: Int64)

  describe "Get the list of queues" $
      it "Return (Vector Queue)" $ do
        Right c <- acquireLocalPGConn
        Right v <- S.listQueues c
        v `shouldBe` (v :: V.Vector Queue)

  describe "Delete the Hspec test queue" $
      it "Return True" $ do
        Right c <- acquireLocalPGConn
        let q = S.declare "HspecTestQueue" c
        S.drop q `shouldReturn` Right True

