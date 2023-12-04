
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
import simpledb.plan.TablePlan;
import simpledb.plan.UnionPlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author roman
 */
public class UnionScanTest {
  private SimpleDB db = new SimpleDB("collegedb");
  private MetadataMgr mdm = db.mdMgr();

  public UnionScanTest() {
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
  public void testUnionScan() {
    System.out.println("UNION");
    Transaction tx = db.newTx();
    Plan studentTblPlan1 = new TablePlan(tx, "student", mdm);
    Plan studentTblPlan2 = new TablePlan(tx, "student", mdm);
    tx.commit();
    Plan plan = new UnionPlan(studentTblPlan1, studentTblPlan2);
    assertEquals(Arrays.asList("sid", "sname", "majorid", "gradyear"), plan.schema().fields());
    assertEquals(18, plan.recordsOutput());
  }

}
