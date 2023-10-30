package simpledb.buffer;
import simpledb.file.*;
import simpledb.log.LogMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 *
 */
public class BufferMgr {
    private Buffer[] bufferpool;
    private int numAvailable;
    private static final long MAX_TIME = 10000; // 10 seconds
    private String strategy = "naive";
    private int time = 0;
    private int lastReplacement = 0; // used by the Clock strategy

    /**
     * Creates a buffer manager having the specified number
     * of buffer slots.
     * This constructor depends on a {@link FileMgr} and
     * {@link simpledb.log.LogMgr LogMgr} object.
     * 
     * @param numbuffs the number of buffer slots to allocate
     */
    public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
        bufferpool = new Buffer[numbuffs];
        numAvailable = numbuffs;
        for (int i = 0; i < numbuffs; i++)
            bufferpool[i] = new Buffer(fm, lm);
    }

    /**
     * Returns the number of available (i.e. unpinned) buffers.
     * 
     * @return the number of available buffers
     */
    public synchronized int available() {
        return numAvailable;
    }

    /**
     * Flushes the dirty buffers modified by the specified transaction.
     * 
     * @param txnum the transaction's id number
     */
    public synchronized void flushAll(int txnum) {
        for (Buffer buff : bufferpool)
            if (buff.modifyingTx() == txnum)
                buff.flush();
    }

    /**
     * Unpins the specified data buffer. If its pin count
     * goes to zero, then notify any waiting threads.
     * 
     * @param buff the buffer to be unpinned
     */
    public synchronized void unpin(Buffer buff) {
        // TODO: unpin time must be updated
        buff.unpin();
        time++;
        buff.setTimeUnpinned(time);
        lastReplacement = (lastReplacement + 1) % bufferpool.length;
        if (!buff.isPinned()) {
            numAvailable++;
            notifyAll();
        }
    }

    /**
     * Pins a buffer to the specified block, potentially
     * waiting until a buffer becomes available.
     * If no buffer becomes available within a fixed
     * time period, then a {@link BufferAbortException} is thrown.
     * 
     * @param blk a reference to a disk block
     * @return the buffer pinned to that block
     */
    public synchronized Buffer pin(BlockId blk) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buff = tryToPin(blk);
            while (buff == null && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
                buff = tryToPin(blk);
            }
            if (buff == null)
                throw new BufferAbortException();
            return buff;
        } catch (InterruptedException e) {
            throw new BufferAbortException();
        }
    }

    private boolean waitingTooLong(long starttime) {
        return System.currentTimeMillis() - starttime > MAX_TIME;
    }

    /**
     * Tries to pin a buffer to the specified block.
     * If there is already a buffer assigned to that block
     * then that buffer is used;
     * otherwise, an unpinned buffer from the pool is chosen.
     * Returns a null value if there are no available buffers.
     * 
     * @param blk a reference to a disk block
     * @return the pinned buffer
     */
    private Buffer tryToPin(BlockId blk) {
        // TODO: pin time and replacement buffer must be updated
        Buffer buff = findExistingBuffer(blk);
        if (buff == null) {
            buff = chooseUnpinnedBuffer();
            if (buff == null)
                return null;
            buff.assignToBlock(blk);
        }
        if (!buff.isPinned())
            numAvailable--;
        buff.pin();
        time++;
        buff.setTimePinned(time);
        lastReplacement = (lastReplacement + 1) % bufferpool.length;
        return buff;
    }

    private Buffer findExistingBuffer(BlockId blk) {
        for (Buffer buff : bufferpool) {
            BlockId b = buff.block();
            if (b != null && b.equals(blk))
                return buff;
        }
        return null;
    }

    /**
     * Get buffer selection strategy
     *
     * @return buffer selection strategy
     */
    public String getStrategy() {
        return this.strategy;
    }

    /**
     * Set buffer selection strategy
     *
     * @param strategy (Naive, FIFO, LRU, Clock)
     */
    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    private Buffer chooseUnpinnedBuffer() {
        for (Buffer b : bufferpool) {
            if (b.block() == null) {
                return b;
            }
        }
        switch (this.strategy) {
            case "naive":
                return useNaiveStrategy();
            case "fifo":
                return useFIFOStrategy();
            case "lru":
                return useLRUStrategy();
            case "clock":
                return useClockStrategy();
            default:
                return null;
        }
    }

    /**
     * @return Allocated buffers
     */
    public Buffer[] getBuffers() {
        return this.bufferpool;
    }

    /**
     * Naive buffer selection strategy
     *
     * @return chosen buffer
     */
    private Buffer useNaiveStrategy() {
        // TODO: Implement Naive strategy
        for (Buffer b : bufferpool) {
            if (!b.isPinned() || b == null) {
                return b;
            }
        }
        return null;
    }

    /**
     * FIFO buffer selection strategy
     *
     * @return chosen buffer
     */
    private Buffer useFIFOStrategy() {
        Buffer chosenBuffer = null;

        for (Buffer b : bufferpool) {
            if (b == null) {
                return b;
            }
            if (chosenBuffer == null) {
                chosenBuffer = b;
            }
            if (b.getTimePinned() < chosenBuffer.getTimePinned()) {
                chosenBuffer = b;
            }
        }
        return chosenBuffer;
    }

    /**
     * LRU buffer selection strategy
     *
     * @return chosen buffer
     */
    private Buffer useLRUStrategy() {
        Buffer chosenBuffer = null;

        for (Buffer b : bufferpool) {
            if (b == null) {
                return b;
            }
            if (chosenBuffer == null) {
                chosenBuffer = b;
            }
            if (!b.isPinned() && b.getTimeUnpinned() < chosenBuffer.getTimeUnpinned()) {
                chosenBuffer = b;
            }
        }

        return chosenBuffer;

    }

    /**
     * Clock buffer selection strategy
     *
     * @return chosen buffer
     * Sources used:
     * https://www.geeksforgeeks.org/java-do-while-loop-with-examples/
     */
    private Buffer useClockStrategy() {
        // TODO: Implement Clock strategy
        int startClock = lastReplacement;
        int currentClock = startClock;

        do {
            Buffer currentBuffer = bufferpool[currentClock];
    
            if (currentBuffer == null || !currentBuffer.isPinned()) {
                lastReplacement = currentClock;
                return currentBuffer;
            }
    
            currentBuffer.unpin();
            currentClock = (currentClock + 1) % bufferpool.length;
        } while (currentClock != startClock);
    
        return null;
    
    }

}
