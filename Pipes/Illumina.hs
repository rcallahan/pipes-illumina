{-# LANGUAGE BangPatterns #-}
module Pipes.Illumina where

import Data.Bits
import Data.Word
import Data.Int
import Foreign.Ptr
import Foreign.Storable
import Foreign.ForeignPtr.Unsafe
import Foreign.ForeignPtr.Safe
import Foreign.Marshal.Alloc
import Data.ByteString.Internal
import Control.Applicative
import Control.Monad
import System.IO
import qualified Data.ByteString as B
import Data.ByteString.Unsafe
import Pipes
import Pipes.Bgzf

bclBgzfProducer :: MonadIO m => [Handle] -> Producer (ByteString, ByteString) m ()
bclBgzfProducer [] = return ()
bclBgzfProducer hdls = start where
    start = do
        mblocks <- getBlocks
        case mblocks of
            Nothing -> return ()
            Just bs -> do
                b2fPipe $ map (B.drop 4) bs
                go
    go = do
        mblocks <- getBlocks
        case mblocks of
            Nothing -> return ()
            Just bs -> do
                b2fPipe bs
                go
    getBlocks = do
        mbchunks <- forM hdls $ \h -> do
            header <- liftIO $ B.hGet h 18
            case parseHeader header of
                28 -> return Nothing
                blocklen -> do
                    chunk <- liftIO $ B.hGet h (blocklen - 18)
                    return $ Just chunk
        return $ map inflateBlock <$> sequence mbchunks
    cycs = length hdls
    b2fPipe chunks = do
        let l = B.length $ head chunks
        forM_ [0..l-1] $ \i -> do
            r <- liftIO $ do
                seqfp <- mallocByteString cycs
                qualfp <- mallocByteString cycs
                let seqp :: Ptr Word8
                    seqp = unsafeForeignPtrToPtr seqfp
                    qualp = unsafeForeignPtrToPtr qualfp
                forM_ (zip chunks [0..]) $ \(c,j) -> do
                    case unsafeIndex c i of
                        0 -> do
                            pokeElemOff seqp j 78 
                            pokeElemOff qualp j 35
                        w -> do
                            pokeElemOff seqp j $ case w .&. 3 of
                                0 -> 65
                                1 -> 67
                                2 -> 71
                                3 -> 84
                            pokeElemOff qualp j $ 33 + w `shiftR` 2
                touchForeignPtr seqfp
                touchForeignPtr qualfp
                let !sq = PS seqfp 0 cycs
                    !qual = PS qualfp 0 cycs
                return $! (sq, qual)
            yield r

filterProducer :: Handle -> Producer Bool IO ()
filterProducer hdl = go where
    numclusters :: IO Int32
    numclusters = do
        buf <- mallocBytes 12
        12 <- hGetBufSome hdl buf 12
        cnt <- peek (buf `plusPtr` 8)
        free buf
        return cnt
    go = do
        clusts <- liftIO $ fromIntegral <$> numclusters
        buf <- liftIO $ mallocBytes 1
        replicateM_ clusts $ do
            1 <- liftIO $ hGetBufSome hdl buf 1
            b <- liftIO $ peek buf
            yield b
        liftIO $ free buf

locsProducer :: Handle -> Producer (Float, Float) IO ()
locsProducer hdl = go where
    numclusters :: IO Int32
    numclusters = do
        buf <- mallocBytes 12
        12 <- hGetBufSome hdl buf 12
        cnt <- peek (buf `plusPtr` 8)
        free buf
        return cnt
    go = do
        clusts <- liftIO $ fromIntegral <$> numclusters
        buf <- liftIO $ mallocBytes 8
        replicateM_ clusts $ do
            8 <- liftIO $ hGetBuf hdl buf 8
            x <- liftIO $ peek buf
            y <- liftIO $ peekElemOff buf 1
            yield (x, y)
        liftIO $ free buf

bciProducer :: Handle -> Producer (Int, Int) IO ()
bciProducer hdl = go where
    go = do
        buf <- liftIO $ (mallocBytes 8 :: IO (Ptr Int32))
        let go' = do
                r <- liftIO $ hGetBuf hdl buf 8
                case r of
                    8 -> do
                        a <- liftIO $ peek buf
                        b <- liftIO $ peekElemOff buf 1
                        yield (fromIntegral a, fromIntegral b)
                        go'
                    0 -> liftIO $ free buf
        go'
