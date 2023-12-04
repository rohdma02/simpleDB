// JoinScan.java
package simpledb.query;

public class JoinScan implements Scan {
    private Scan leftScan;
    private Scan rightScan;
    private Predicate joinPredicate;
    private boolean joined;

    public JoinScan(Scan leftScan, Scan rightScan, Predicate joinPredicate) {
        this.leftScan = leftScan;
        this.rightScan = rightScan;
        this.joinPredicate = joinPredicate;
        this.joined = false;
        beforeFirst();
    }

    public void beforeFirst() {
        leftScan.beforeFirst();
        rightScan.beforeFirst();
        joined = false;
    }

    public boolean next() {
        while (leftScan.next() || !joined) {
            while (rightScan.next()) {
                if (joinPredicate.isSatisfied(this)) {
                    joined = true;
                    return true;
                }
            }
            rightScan.beforeFirst();
            joined = false;
        }
        return false;
    }

    public int getInt(String fldname) {
        return joined ? rightScan.getInt(fldname) : leftScan.getInt(fldname);
    }

    public String getString(String fldname) {
        return joined ? rightScan.getString(fldname) : leftScan.getString(fldname);
    }

    public Constant getVal(String fldname) {
        return joined ? rightScan.getVal(fldname) : leftScan.getVal(fldname);
    }

    public boolean hasField(String fldname) {
        return leftScan.hasField(fldname) || rightScan.hasField(fldname);
    }

    public void close() {
        leftScan.close();
        rightScan.close();
    }
}
