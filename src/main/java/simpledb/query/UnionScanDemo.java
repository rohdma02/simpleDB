package simpledb.query;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.UnionPlan;
import simpledb.plan.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * Demo of the scan class corresponding to the <i>union</i> relational
 * algebra operator.
 * 
 * @author roman
 */
public class UnionScanDemo {
    public static void main(String[] args) {
        System.out.println("UNION");
        SimpleDB db = new SimpleDB("collegedb");
        MetadataMgr mdm = db.mdMgr();
        Transaction tx = db.newTx();
        Plan studentTblPlan1 = new TablePlan(tx, "student", mdm);
        Plan studentTblPlan2 = new TablePlan(tx, "student", mdm);
        tx.commit();
        Plan plan = new UnionPlan(studentTblPlan1, studentTblPlan2);
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
