package simpledb.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.JoinPlan;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author roman
 */
public class JoinScanTest {
  private SimpleDB db = new SimpleDB("collegedb");
  private MetadataMgr mdm = db.mdMgr();

  public JoinScanTest() {
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
  public void testJoinScan() {
    System.out.println("JOIN");
    Transaction tx = db.newTx();
    Plan studentTblPlan = new TablePlan(tx, "student", mdm);
    Plan deptTblPlan = new TablePlan(tx, "dept", mdm);
    tx.commit();
    Plan joinPlan = new JoinPlan(studentTblPlan, deptTblPlan,
        new Predicate(
            new Term(
                new Expression("majorid"),
                new Expression("did"))));
    assertEquals(9, joinPlan.recordsOutput());
  }
}