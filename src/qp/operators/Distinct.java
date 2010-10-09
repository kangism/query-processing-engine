package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;

public class Distinct extends Operator {

	Operator base; // base operator
	int batchsize; // no. of tuples per page
	Batch inbatch = null; // input buffer
	Batch outbatch = null; // output buffer
	boolean eos = false; // end of stream
	int cursor = 0; // cursor of inbatch
	Tuple preTuple = null; // previous tuple, used to compare with next new
							// tuple
	int numCol = 0; // no. of columns of the base table

	public Distinct(Operator base, OperatorType type) {
		super(type);
		this.base = base;
		numCol = base.getSchema().getNumCols();
	}

	/**
	 * Opens the connection to the base operator
	 **/
	public boolean open() {
		// set number of tuples per page
		int tuplesize = schema.getTupleSize();
		batchsize = Batch.getPageSize() / tuplesize;

		if (base.open())
			return true;
		else
			return false;
	}

	public Batch next() {
		int i = 0;

		// finish all the batches
		if (eos) {
			this.close();
			return null;
		}

		// An output buffer is initiated
		outbatch = new Batch(batchsize);

		// keep on checking the incoming pages until the output buffer is full
		while (!outbatch.isFull()) {
			// read in another new batch
			if (cursor == 0) {
				inbatch = base.next();
				// There is no more incoming pages from base operator
				if (inbatch == null) {
					eos = true;
					return outbatch;
				}
			}

			// Continue this for loop until this page is fully checked or the
			// output buffer is full
			for (i = cursor; i < inbatch.size() && (!outbatch.isFull()); i++) {
				Tuple present = inbatch.elementAt(i);
				if (preTuple != null && isTupleEqual(preTuple, present)) {
					continue;
				}
				outbatch.add(present);
				preTuple = present;
			}

			// Modify the cursor to the position required when the base operator
			// is called next time
			if (i == inbatch.size())
				cursor = 0;
			else
				cursor = i;
		}
		return outbatch;
	}

	public boolean close() {
		if (base.close())
			return true;
		else
			return false;
	}

	public boolean isTupleEqual(Tuple t1, Tuple t2) {
		for (int i = 0; i < numCol; i++) {
			if (Tuple.compareTuples(t1, t2, i, i) != 0)
				return false;
		}
		return true;
	}

	public void setBase(Operator base) {
		this.base = base;
	}

	public Operator getBase() {
		return base;
	}

	public Object clone() {
		Operator newbase = (Operator) base.clone();
		Distinct newDis = new Distinct(newbase, operatorType);
		newDis.setSchema(newbase.getSchema());
		return newDis;
	}
}
