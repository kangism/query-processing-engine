package qp.operators;

import java.util.ArrayList;
import java.util.List;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.OrderByOption;
import qp.utils.Tuple;

/**
 * Sort merge join as an additional item of our project.
 * 
 * @author Yann-Loup
 */
public class SortMergeJoin extends Join {
    /**
     * number of tuples per out batch
     */
    int batchsize;
    /**
     * index of the join attribute in left table
     */
    int leftindex;
    /**
     * index of the join attribute in right table
     */
    int rightindex;

    Batch leftbatch = null;
    Batch rightbatch = null;
    Batch outbatch = null;
    Batch outbatchOverflow = null;

    Tuple leftPointer = null;
    Tuple rightPointer = null;

    OrderByOption orderByOption = OrderByOption.ASC;

    List<Tuple> rightTuplesEqualsForJoin;
    List<Tuple> leftTuplesEqualsForJoin;

    public SortMergeJoin(Join jn) {
	super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOperatorType());
	schema = jn.getSchema();
	jointype = jn.getJoinType();
	numBuff = jn.getNumBuff();
    }

    public boolean open() {
	int tuplesize = schema.getTupleSize();
	batchsize = Batch.getPageSize() / tuplesize;

	Attribute leftattr = con.getLhs();
	Attribute rightattr = (Attribute) con.getRhs();
	leftindex = left.getSchema().indexOf(leftattr);
	rightindex = right.getSchema().indexOf(rightattr);

	if (!left.open())
	    return false;
	if (!right.open())
	    return false;

	leftbatch = left.next();
	rightbatch = right.next();

	rightTuplesEqualsForJoin = new ArrayList<Tuple>();
	leftTuplesEqualsForJoin = new ArrayList<Tuple>();

	leftPointer = pollOutNextTuple(leftbatch, left);
	rightPointer = pollOutNextTuple(rightbatch, right);

	outbatchOverflow = new Batch(batchsize);

	return true;
    }

    public Batch next() {
	// Join is completed
	if (joinIsCompleted()) {
	    return null;
	}

	// System.out.println("new page");
	outbatch = new Batch(batchsize);

	// add the one of the previous work that would have been overflowed
	while (!outbatch.isFull() && !outbatchOverflow.isEmpty()) {
	    outbatch.add(outbatchOverflow.elementAt(0));
	    outbatchOverflow.remove(0);
	}

	// We continue the join
	while (!outbatch.isFull() && !joinIsCompleted()) {

	    int compare = compareTuplesForJoin(leftPointer, rightPointer);

	    if (compare == 0) {
		leftTuplesEqualsForJoin.add(leftPointer);
		rightTuplesEqualsForJoin.add(rightPointer);

		// get all the tuple from the right that are equal
		rightPointer = pollOutNextRightTuple();
		while (rightPointer != null && compareTuplesForJoin(leftTuplesEqualsForJoin.get(0), rightPointer) == 0) {
		    rightTuplesEqualsForJoin.add(rightPointer);
		    rightPointer = pollOutNextRightTuple();
		}

		// get all the tuple from the left that are equal
		leftPointer = pollOutNextLeftTuple();
		while (leftPointer != null && compareTuplesForJoin(leftPointer, rightTuplesEqualsForJoin.get(0)) == 0) {
		    leftTuplesEqualsForJoin.add(leftPointer);
		    leftPointer = pollOutNextLeftTuple();
		}

		// make all association
		// System.out.println("addSize: " + leftTuplesEqualsForJoin.size() + " x " +
		// rightTuplesEqualsForJoin.size());
		for (Tuple leftTuple : leftTuplesEqualsForJoin) {
		    for (Tuple rightTuple : rightTuplesEqualsForJoin) {
			if (!outbatch.isFull()) {
			    // System.out.println("add: " + leftTuple.dataAt(leftindex) + "=" +
			    // rightTuple.dataAt(rightindex));
			    outbatch.add(leftTuple.joinWith(rightTuple));
			} else {
			    // System.out.println("addT: " + leftTuple.dataAt(leftindex) + "=" +
			    // rightTuple.dataAt(rightindex));
			    outbatchOverflow.add(leftTuple.joinWith(rightTuple));
			}
		    }
		}
		rightTuplesEqualsForJoin = new ArrayList<Tuple>();
		leftTuplesEqualsForJoin = new ArrayList<Tuple>();

	    } else if (compare < 0) {
		leftPointer = pollOutNextLeftTuple();
	    } else if (compare > 0) {
		rightPointer = pollOutNextRightTuple();
	    }

	}
	return outbatch;
    }

    public boolean close() {
	if (left.close() && right.close()) {

	    return true;
	} else
	    return false;
    }

    /**
     * To know when we should stop the operator.
     * 
     * @return
     */
    private boolean joinIsCompleted() {
	return leftbatch == null || rightbatch == null;
    }

    /**
     * To know from which table the next tuple should be taken.
     * 
     * @throws Exception
     */
    private int compareTuplesForJoin(Tuple leftTuple, Tuple rightTuple) throws RuntimeException {
	// System.out.println(leftTuple.dataAt(leftindex) + "?=" + rightTuple.dataAt(rightindex));
	int compareTuples = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);
	switch (orderByOption) {
	    case ASC:
		return compareTuples;
	    case DESC:
		return -compareTuples;
	    default:
		throw new RuntimeException("No orderByOption");
	}
    }

    private Tuple pollOutNextTuple(Batch batch, Operator node) {
	if (batch != null) {
	    Tuple tuple = batch.getTuples().get(0);
	    batch.getTuples().remove(0);
	    return tuple;
	} else {
	    return null;
	}
    }

    private Tuple pollOutNextLeftTuple() {
	if (leftbatch.isEmpty()) {
	    // System.out.println("new left presort");
	    leftbatch = left.next();
	}
	return pollOutNextTuple(leftbatch, left);
    }

    private Tuple pollOutNextRightTuple() {
	if (rightbatch.isEmpty()) {
	    // System.out.println("new right presort");
	    rightbatch = right.next();
	}
	return pollOutNextTuple(rightbatch, right);
    }

    /**
     * @param orderByOption
     *        the orderByOption to set
     */
    public void setOrderByOption(OrderByOption orderByOption) {
	this.orderByOption = orderByOption;
    }
}