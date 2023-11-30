package simpledb.query;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.ExtendPlan;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class ExtendScanUnitTest {

    private SimpleDB db = new SimpleDB("collegedb");
    private MetadataMgr mdm = db.mdMgr();

    public ExtendScanUnitTest() {
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

        Expression gradYearExpr = new Expression("gradYear");
        Expression senior = new Expression(gradYearExpr.toString() + " <= 2024");
        Plan extendPlan = new ExtendPlan(studentTblPlan, "Senior", senior);
        Scan extendScan = extendPlan.open();
        System.out.println(extendPlan.schema().fields());

        assertEquals(true, extendScan.hasField("Senior"));
        assertEquals(false, extendScan.hasField("nonexistentField"));
    }
}
