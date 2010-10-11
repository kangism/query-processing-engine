package qp.utils;

import java.util.Comparator;
import java.util.Vector;

/**
 * @author Yann-Loup
 */
public class TupleComparator implements Comparator<Tuple> {

    Vector<AttributeOption> attrOps;

    public TupleComparator(Vector<AttributeOption> attrOps) {
	this.attrOps = attrOps;
    }

    /*
     * (non-Javadoc)
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    @Override
    public int compare(Tuple o1, Tuple o2) {
	if (Tuple.goodOrder(o1, o2, attrOps)) {
	    // o1 is before o2.
	    return -1;
	} else if (Tuple.goodOrder(o2, o1, attrOps)) {
	    // o1 is after o2.
	    return 1;
	} else {
	    // o1 and o2 has the same value for the sort attributes.
	    return 0;
	}
    }
}
