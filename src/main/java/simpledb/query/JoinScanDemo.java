package simpledb.query;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.plan.UnionPlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class JoinScanDemo {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("collegedb");

        MetadataMgr mdm = db.mdMgr();

        System.out.println("JOIN");
        Transaction tx = db.newTx();
        Plan studentTblPlan = new TablePlan(tx, "student", mdm);
        Plan deptTblPlan = new TablePlan(tx, "dept", mdm);
        tx.commit();
        Plan joinPlan = new UnionPlan(studentTblPlan, deptTblPlan,
                new Predicate(
                        new Term(
                                new Expression("majorid"),
                                new Expression("did"))));
        Scan joinScan = joinPlan.open();
        while (joinScan.next()) {
            for (String field : joinPlan.schema().fields()) {
                System.out.printf("%10s", joinScan.getVal(field).toString());
            }
            System.out.println();
        }
    }
}
