package simpledb.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import simpledb.plan.Plan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author roman
 */
public class SelectScanTest {

  private SimpleDB db = new SimpleDB("collegedb");

  public SelectScanTest() {
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
  public void testSelectAllScan() {
    System.out.println("SELECT ALL");
    Transaction tx = db.newTx();
    String qry = "select sid from student";
    Plan plan = db.planner().createQueryPlan(qry, tx);
    tx.commit();
    assertEquals(Arrays.asList("sid"), plan.schema().fields());
    assertEquals(9, plan.recordsOutput());
  }

  @Test
  public void testSelectByConstantScan() {
    System.out.println("SELECT BY CONSTANT");
    Transaction tx = db.newTx();
    String qry = "select sname from student where MajorId=10";
    Plan plan = db.planner().createQueryPlan(qry, tx);
    tx.commit();
    assertEquals(Arrays.asList("sname"), plan.schema().fields());
    assertEquals(2, plan.recordsOutput());
  }

}
