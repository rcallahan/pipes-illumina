{-# LANGUAGE OverloadedStrings #-}

module Pipes.NextSeq where

import Control.Applicative
import Control.Monad
import System.IO
import qualified Data.ByteString as B
import Data.ByteString (ByteString)
import Data.ByteString.Unsafe
import Data.List
import Pipes
import qualified Pipes.Prelude as P
import Pipes.Illumina
import System.FilePath
import System.Directory

data Cluster = Cluster
    { sq :: !ByteString
    , qual :: !ByteString
    , xCoord :: !Float
    , yCoord :: !Float
    , passedFilter :: !Bool
    , lane :: !Int
    , tile :: !Int }

nextSeqProducer :: FilePath -> Producer Cluster IO ()
nextSeqProducer rundir = forM_ [1..4] proclane where
    proclane ln = do
        let lanestr = "L00" ++ show ln
            bcldir = joinPath [rundir, "Data", "Intensities", "BaseCalls", lanestr]
        allconts <- liftIO $ getDirectoryContents bcldir
        let bcls = sort $ filter (\q -> takeExtension q == ".bgzf") allconts
        locshdl <- liftIO $ openFile (joinPath [rundir, "Data", "Intensities", lanestr, "s_" ++ show ln ++ ".locs"]) ReadMode
        filthdl <- liftIO $ openFile (joinPath [bcldir, "s_" ++ show ln ++ ".filter"]) ReadMode
        bcihdl <- liftIO $ openFile (joinPath [bcldir, "s_" ++ show ln ++ ".bci"]) ReadMode
        bclhdls <- liftIO $ mapM (\r -> openFile (joinPath [bcldir, r]) ReadMode) bcls
        let bclprod = bclBgzfProducer bclhdls
            tileprod = for (bciProducer bcihdl) $ \(tnum, cnt) -> replicateM_ cnt $ yield tnum
            filtprod = filterProducer filthdl
            locsprod = locsProducer locshdl
            zipd = P.zip tileprod $ P.zip filtprod $ P.zip locsprod bclprod
        for zipd $ \(tnum, (pf, ((x,y), (sq, qu)))) ->
            yield $ Cluster sq qu x y pf ln tnum
        liftIO $ hClose locshdl
        liftIO $ hClose filthdl
        liftIO $ hClose bcihdl
        liftIO $ mapM_ hClose bclhdls
