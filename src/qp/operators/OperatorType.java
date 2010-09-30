/**
 * 
 */
package qp.operators;

/**
 * @author Yann-Loup
 */
public enum OperatorType {

    SCAN(0), SELECT(1), PROJECT(2), JOIN(3), SORT(4);

    /**
     * 
     */
    private final int opId;

    /**
     * @param opId
     */
    private OperatorType(int opId) {
	this.opId = opId;
    }

    /**
     * @return
     */
    public int getOpId() {
	return this.opId;
    }
}
