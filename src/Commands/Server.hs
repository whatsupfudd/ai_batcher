module Commands.Server where

import Control.Exception (bracket)
import Control.Monad.Cont (runContT, ContT (..))    -- liftIO

import System.Posix (installHandler)

import Network.Wai.Handler.Warp as Wrp

import Api.Handlers (setupWai)
import DB.Connect (startPg)
import Api.Serve (launchServant)
import Options.Runtime as Rto

serverCmd :: Rto.RunOptions -> IO ()
serverCmd rtOpts = do
  putStrLn "@[serverCmd] going to start mainAction."
  runContT (startPg rtOpts.pgDbConf) mainAction
  where
  mainAction pgPool = do
    putStrLn $ "@[mainAction] starting (host: " <> show rtOpts.serverHost <> ", port: "
          <> show rtOpts.serverPort <> ")."
    let 
      settings = setupWai rtOpts.serverPort rtOpts.serverHost globalShutdownHandler
    webHandler <- launchServant rtOpts pgPool
    putStrLn $ "@[mainAction] webHandler starting..."
    Wrp.runSettings settings webHandler
    putStrLn $ "@[mainAction] webHandler ended."
    pure ()
  globalShutdownHandler = do
    putStrLn "@[globalShutdownHandler] done."


mainBgTask :: ContT r IO ()
mainBgTask =
  let
    defaultStart = putStrLn "@[mainBgTask] Starting default bg thread."
    defaultFinish _ = putStrLn "@[mainBgTask] finishing default bg thread."
  in do
  ContT $ bracket defaultStart defaultFinish

