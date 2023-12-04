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
import simpledb.plan.ProductPlan;
import simpledb.plan.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author roman
 */
public class ProductScanTest {
  private SimpleDB db = new SimpleDB("collegedb");
  private MetadataMgr mdm = db.mdMgr();

  public ProductScanTest() {
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
  public void testProductScan() {
    System.out.println("PRODUCT");
    Transaction tx = db.newTx();
    Plan studentTblPlan = new TablePlan(tx, "student", mdm);
    Plan deptTblPlan = new TablePlan(tx, "dept", mdm);
    tx.commit();
    Plan plan = new ProductPlan(studentTblPlan, deptTblPlan);
    assertEquals(Arrays.asList("sid", "sname", "majorid", "gradyear", "did", "dname"), plan.schema().fields());
    assertEquals(27, plan.recordsOutput());
  }
}