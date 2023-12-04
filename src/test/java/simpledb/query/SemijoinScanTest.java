package simpledb.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.SemijoinPlan;
import simpledb.plan.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author roman
 */
public class SemijoinScanTest {
  private SimpleDB db = new SimpleDB("collegedb");
  private MetadataMgr mdm = db.mdMgr();

  public SemijoinScanTest() {
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
  public void testSemiJoinScan() {
    System.out.println("SEMIJOIN");
    Transaction tx = db.newTx();
    Plan studentTblPlan = new TablePlan(tx, "student", mdm);
    Plan deptTblPlan = new TablePlan(tx, "dept", mdm);
    tx.commit();
    Plan plan = new SemijoinPlan(deptTblPlan, studentTblPlan,
        new Predicate(
            new Term(
                new Expression("did"),
                new Expression("majorid"))));
    assertEquals(Arrays.asList("did", "dname"), plan.schema().fields());
    assertEquals(3, plan.recordsOutput());
  }

  @Test
  public void testSemiJoinScan2() {
    System.out.println("SEMIJOIN");
    Transaction tx = db.newTx();
    Plan studentTblPlan = new TablePlan(tx, "student", mdm);
    Plan enrollTblPlan = new TablePlan(tx, "enroll", mdm);
    tx.commit();
    Plan plan = new SemijoinPlan(studentTblPlan, enrollTblPlan,
        new Predicate(
            new Term(
                new Expression("sid"),
                new Expression("studentid"))));
    assertEquals(Arrays.asList("sid", "sname", "majorid", "gradyear"), plan.schema().fields());
    assertEquals(4, plan.recordsOutput());
  }
}