package simpledb.query;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.plan.ExtendPlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author roman
 */
public class ExtendScanTest {
    private SimpleDB db = new SimpleDB("collegedb");
    private MetadataMgr mdm = db.mdMgr();

    public ExtendScanTest() {
    }

    @BeforeAll
    public static void setUpClass() {
    }

    @AfterAll
    public static void tearDownClass() {
    }

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void tearDown() {
    }

    @Test
    public void testExtendScan() {
        System.out.println("EXTEND");
        Transaction tx = db.newTx();
        Plan studentTblPlan = new TablePlan(tx, "student", mdm);
        tx.commit();
        Plan extendPlan = new ExtendPlan(studentTblPlan, "gradclass", INTEGER, 4);
        Scan extendScan = extendPlan.open();
        assertEquals(true, extendScan.hasField("gradclass"));
        Plan extendPlan2 = new ExtendPlan(studentTblPlan, "initial", VARCHAR, 1);
        Scan extendScan2 = extendPlan2.open();
        assertEquals(true, extendScan2.hasField("initial"));
    }

}