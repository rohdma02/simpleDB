package simpledb.buffer;

import java.util.ArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import simpledb.file.BlockId;
import simpledb.server.SimpleDB;

/**
 * @author Roman Yasinovskyy
 */
public class BufferMgrUnitTest {

    private SimpleDB db4 = new SimpleDB("testdb", 400, 4);
    private SimpleDB db5 = new SimpleDB("testdb", 400, 5);

    public BufferMgrUnitTest() {
    }

    @BeforeAll
    public static void setUpClass() {
    }

    @AfterAll
    public static void tearDownClass() {
    }

    @BeforeEach
    public void setUp() {
        Buffer buff10, buff20, buff30, buff40, buff50;

        buff10 = buff20 = buff30 = buff40 = buff50 = null;
        buff10 = db4.bufferMgr().pin(new BlockId("tempbuffer", 10));
        buff20 = db4.bufferMgr().pin(new BlockId("tempbuffer", 20));
        buff30 = db4.bufferMgr().pin(new BlockId("tempbuffer", 30));
        buff40 = db4.bufferMgr().pin(new BlockId("tempbuffer", 40));
        db4.bufferMgr().unpin(buff20);
        buff50 = db4.bufferMgr().pin(new BlockId("tempbuffer", 50));
        db4.bufferMgr().unpin(buff40);
        db4.bufferMgr().unpin(buff10);
        db4.bufferMgr().unpin(buff30);
        db4.bufferMgr().unpin(buff50);

        buff10 = buff20 = buff30 = buff40 = buff50 = null;
        buff10 = db5.bufferMgr().pin(new BlockId("tempbuffer", 10));
        buff20 = db5.bufferMgr().pin(new BlockId("tempbuffer", 20));
        buff30 = db5.bufferMgr().pin(new BlockId("tempbuffer", 30));
        buff40 = db5.bufferMgr().pin(new BlockId("tempbuffer", 40));
        db5.bufferMgr().unpin(buff20);
        buff50 = db5.bufferMgr().pin(new BlockId("tempbuffer", 50));
        db5.bufferMgr().unpin(buff40);
        db5.bufferMgr().unpin(buff10);
        db5.bufferMgr().unpin(buff30);
        db5.bufferMgr().unpin(buff50);
    }

    @AfterEach
    public void tearDown() {
    }

    /**
     * Test of Naive Buffer selection strategy, of class BufferMgr.
     */
    @Test
    public void testNaiveStrategy4() {
        System.out.println("Naive Strategy (4 buffers)");
        SimpleDB db = this.db4;
        Integer[] expected = new Integer[] { 60, 70, 30, 40 };
        ArrayList<Integer> result = new ArrayList<Integer>();

        db.bufferMgr().setStrategy("naive");
        db.bufferMgr().pin(new BlockId("tempbuffer", 60));
        db.bufferMgr().pin(new BlockId("tempbuffer", 70));

        for (Buffer b : db.bufferMgr().getBuffers()) {
            result.add(b.block().number());
        }
        assertArrayEquals(expected, result.toArray(), "Naive strategy failed");
    }

    /**
     * Test of Naive Buffer selection strategy, of class BufferMgr.
     */
    @Test
    public void testNaiveStrategy5() {
        System.out.println("Naive Strategy (5 buffers)");
        SimpleDB db = this.db5;
        Integer[] expected = new Integer[] { 60, 70, 30, 40, 50 };
        ArrayList<Integer> result = new ArrayList<Integer>();

        db.bufferMgr().setStrategy("naive");
        db.bufferMgr().pin(new BlockId("tempbuffer", 60));
        db.bufferMgr().pin(new BlockId("tempbuffer", 70));

        for (Buffer b : db.bufferMgr().getBuffers()) {
            result.add(b.block().number());
        }
        assertArrayEquals(expected, result.toArray(), "Naive strategy failed");
    }

    /**
     * Test of FIFO Buffer selection strategy, of class BufferMgr.
     */
    @Test
    public void testFIFOStrategy4() {
        System.out.println("FIFO Strategy (4 buffers)");
        SimpleDB db = this.db4;
        Integer[] expected = new Integer[] { 60, 50, 70, 40 };
        ArrayList<Integer> result = new ArrayList<Integer>();

        db.bufferMgr().setStrategy("fifo");
        db.bufferMgr().pin(new BlockId("tempbuffer", 60));
        db.bufferMgr().pin(new BlockId("tempbuffer", 70));

        for (Buffer b : db.bufferMgr().getBuffers()) {
            result.add(b.block().number());
        }
        assertArrayEquals(expected, result.toArray(), "FIFO strategy failed");
    }

    /**
     * Test of FIFO Buffer selection strategy, of class BufferMgr.
     */
    @Test
    public void testFIFOStrategy5() {
        System.out.println("FIFO Strategy (5 buffers)");
        SimpleDB db = this.db5;
        Integer[] expected = new Integer[] { 60, 70, 30, 40, 50 };
        ArrayList<Integer> result = new ArrayList<Integer>();

        db.bufferMgr().setStrategy("fifo");
        db.bufferMgr().pin(new BlockId("tempbuffer", 60));
        db.bufferMgr().pin(new BlockId("tempbuffer", 70));

        for (Buffer b : db.bufferMgr().getBuffers()) {
            result.add(b.block().number());
        }
        assertArrayEquals(expected, result.toArray(), "FIFO strategy failed");
    }

    /**
     * Test of LRU Buffer selection strategy, of class BufferMgr.
     */
    @Test
    public void testLRUStrategy4() {
        System.out.println("LRU Strategy (4 buffers)");
        SimpleDB db = this.db4;
        Integer[] expected = new Integer[] { 70, 50, 30, 60 };
        ArrayList<Integer> result = new ArrayList<Integer>();

        db.bufferMgr().setStrategy("lru");
        db.bufferMgr().pin(new BlockId("tempbuffer", 60));
        db.bufferMgr().pin(new BlockId("tempbuffer", 70));

        for (Buffer b : db.bufferMgr().getBuffers()) {
            result.add(b.block().number());
        }
        assertArrayEquals(expected, result.toArray(), "LRU strategy failed");
    }

    /**
     * Test of LRU Buffer selection strategy, of class BufferMgr.
     */
    @Test
    public void testLRUStrategy5() {
        System.out.println("LRU Strategy (5 buffers)");
        SimpleDB db = this.db5;
        Integer[] expected = new Integer[] { 10, 60, 30, 70, 50 };
        ArrayList<Integer> result = new ArrayList<Integer>();

        db.bufferMgr().setStrategy("lru");
        db.bufferMgr().pin(new BlockId("tempbuffer", 60));
        db.bufferMgr().pin(new BlockId("tempbuffer", 70));

        for (Buffer b : db.bufferMgr().getBuffers()) {
            result.add(b.block().number());
        }
        assertArrayEquals(expected, result.toArray(), "LRU strategy failed");
    }

    /**
     * Test of Clock Buffer selection strategy, of class BufferMgr.
     */
    @Test
    public void testClockStrategy4() {
        System.out.println("Clock Strategy (4 buffers)");
        SimpleDB db = this.db4;
        Integer[] expected = new Integer[] { 10, 50, 60, 70 };
        ArrayList<Integer> result = new ArrayList<Integer>();

        db.bufferMgr().setStrategy("clock");
        db.bufferMgr().pin(new BlockId("tempbuffer", 60));
        db.bufferMgr().pin(new BlockId("tempbuffer", 70));

        for (Buffer b : db.bufferMgr().getBuffers()) {
            result.add(b.block().number());
        }
        assertArrayEquals(expected, result.toArray(), "Clock strategy failed");
    }

    /**
     * Test of Clock Buffer selection strategy, of class BufferMgr.
     */
    @Test
    public void testClockStrategy5() {
        System.out.println("Clock Strategy (5 buffers)");
        SimpleDB db = this.db5;
        Integer[] expected = new Integer[] { 60, 70, 30, 40, 50 };
        ArrayList<Integer> result = new ArrayList<Integer>();

        db.bufferMgr().setStrategy("clock");
        db.bufferMgr().pin(new BlockId("tempbuffer", 60));
        db.bufferMgr().pin(new BlockId("tempbuffer", 70));

        for (Buffer b : db.bufferMgr().getBuffers()) {
            result.add(b.block().number());
        }
        assertArrayEquals(expected, result.toArray(), "Clock strategy failed");
    }
}
