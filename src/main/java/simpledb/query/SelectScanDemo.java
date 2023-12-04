package simpledb.query;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.SelectPlan;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * Demo of the scan class corresponding to the <i>select</i> relational
 * algebra operator.
 * 
 * @author roman
 */
public class SelectScanDemo {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("collegedb");
        MetadataMgr mdm = db.mdMgr();
        System.out.println("SELECT");
        Transaction tx = db.newTx();
        Plan studentTblPlan = new TablePlan(tx, "student", mdm);
        tx.commit();
        Plan plan = new SelectPlan(studentTblPlan,
                new Predicate(
                        new Term(
                                new Expression("majorid"),
                                new Expression(new Constant(10)))));
        Scan scan = plan.open();
        for (String field : plan.schema().fields()) {
            System.out.printf("%10s", field);
        }
        System.out.println();
        while (scan.next()) {
            for (String field : plan.schema().fields()) {
                System.out.printf("%10s", scan.getVal(field).toString());
            }
            System.out.println();
        }
    }
}
