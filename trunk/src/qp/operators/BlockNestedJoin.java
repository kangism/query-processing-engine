package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

/**
 * Block Nested Join algorithm
 * 
 * @author Yann-Loup
 */
public class BlockNestedJoin extends Join {

    /**
     * Number of tuples per out batch
     */
    int batchsize;

    /*
     * The following fields are useful during execution of the BlockNestedJoin operation
     */

    /**
     * Number of batches (pages) per block
     */
    int blockSize;

    /**
     * Index of the join attribute in left table.
     */
    int leftindex;
    /**
     * Index of the join attribute in right table.
     */
    int rightindex;

    /**
     * The file name where the right table is materialize.
     */
    String rfname;

    /**
     * To get unique filenum for this operation.
     */
    static int filenum = 0;

    /**
     * Output buffer.
     */
    Batch outbatch;
    /**
     * Buffer for left input stream.
     */
    List<Batch> leftbatches;
    /**
     * Buffer for left input stream.
     * 
     * @deprecated
     */
    Batch leftbatch;
    /**
     * Buffer for right input stream.
     */
    Batch rightbatch;
    /**
     * File pointer to the right hand materialized file.
     */
    ObjectInputStream in;

    /**
     * Cursor for left side buffer.
     */
    int lcurs;
    /**
     * Cursor for right side buffer.
     */
    int rcurs;
    /**
     * Whether end of stream (left table) is reached.
     */
    boolean eosl;
    /**
     * End of stream (right table).
     */
    boolean eosr;

    /**
     * Construction of a BlockNestedJoin from the given <b>jn</b> Join.
     * 
     * @param jn
     */
    public BlockNestedJoin(Join jn) {
	super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
	schema = jn.getSchema();
	jointype = jn.getJoinType();
	numBuff = jn.getNumBuff();

	// one page for the inner buffer and another for the output buffer
	// TODO blockSize could be a parameter??
	blockSize = numBuff - 2;
    }

    /*
     * During open finds the index of the join attributes. Also materializes the right hand side
     * into a file and opens the connections.
     */

    /*
     * (non-Javadoc)
     * @see qp.operators.Operator#open()
     */
    public boolean open() {

	/**
	 * select number of tuples per batch.
	 */
	int tuplesize = schema.getTupleSize();
	batchsize = Batch.getPageSize() / tuplesize;

	Attribute leftattr = con.getLhs();
	Attribute rightattr = (Attribute) con.getRhs();
	leftindex = left.getSchema().indexOf(leftattr);
	rightindex = right.getSchema().indexOf(rightattr);
	Batch rightpage;

	/* initialize the cursors of input buffers */
	lcurs = 0;
	rcurs = 0;
	eosl = false;
	/**
	 * because right stream is to be repetitively scanned if it reached end, we have to start
	 * new scan
	 **/
	eosr = true;

	/*
	 * Right hand side table is to be materialized for the Nested join to perform
	 */
	if (!right.open()) {
	    return false;
	} else {
	    /*
	     * If the right operator is not a base table then Materialize the intermediate result
	     * from right into a file
	     */

	    // if(right.getOpType() != OpType.SCAN){
	    filenum++;
	    rfname = "BNJtemp-" + String.valueOf(filenum);
	    try {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
		while ((rightpage = right.next()) != null) {
		    out.writeObject(rightpage);
		}
		out.close();
	    } catch (IOException io) {
		System.out.println("BlockNestedJoin:writing the temporay file error");
		return false;
	    }
	    // }
	    if (!right.close())
		return false;
	}
	if (left.open())
	    return true;
	else
	    return false;
    }

    /*
     * From input buffers selects the tuples satisfying join condition. And returns a page of output
     * tuples
     */

    /*
     * (non-Javadoc)
     * @see qp.operators.Operator#next()
     */
    public Batch next() {

	// System.out.print("BlockNestedJoin:--------------------------in next----------------");
	// Debug.PPrint(con);
	// System.out.println();
	int i, j;
	if (eosl) {
	    close();
	    return null;
	}
	outbatch = new Batch(batchsize);

	while (!outbatch.isFull()) {

	    if (lcurs == 0 && eosr == true) {
		// /* new left page is to be fetched */
		// leftbatch = (Batch) left.next();

		// new block of left pages is to be fetched
		leftbatches = new ArrayList<Batch>(blockSize);
		for (i = 0; i < blockSize; i++) {
		    leftbatches.add((Batch) left.next());
		}

		// if (leftbatch == null) {
		// eosl = true;
		// return outbatch;
		// }

		if (leftbatches.isEmpty()) {
		    eosl = true;
		    return outbatch;
		}

		// Whenver a new block of left pages came ,
		// we have to start the scanning of right table

		try {

		    in = new ObjectInputStream(new FileInputStream(rfname));
		    eosr = false;
		} catch (IOException io) {
		    System.err.println("BlockNestedJoin:error in reading the file");
		    System.exit(1);
		}

	    }

	    while (eosr == false) {

		try {
		    if (rcurs == 0 && lcurs == 0) {
			rightbatch = (Batch) in.readObject();
		    }

		    // I merge all the page in the buffer of the outer table
		    // in order to make the scan easier
		    // (specially for the management of the cursor)
		    List<Tuple> leftTuplesInCurrentBlock = new ArrayList<Tuple>();
		    for (Batch page : leftbatches) {
			for (i = lcurs; i < page.size(); i++) {
			    leftTuplesInCurrentBlock.add(page.elementAt(i));
			}
		    }

		    // FOR each block of the outer table
		    // I hold k tuple in the memory and I check all the tuple of the inner table
		    for (i = lcurs; i < leftTuplesInCurrentBlock.size(); i++) {

			for (j = rcurs; j < rightbatch.size(); j++) {
			    Tuple lefttuple = leftTuplesInCurrentBlock.get(i);
			    Tuple righttuple = rightbatch.elementAt(j);
			    if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
				Tuple outtuple = lefttuple.joinWith(righttuple);

				// Debug.PPrint(outtuple);
				// System.out.println();
				outbatch.add(outtuple);
				if (outbatch.isFull()) {
				    if (i == leftTuplesInCurrentBlock.size() - 1 && j == rightbatch.size() - 1) {// case
					// 1
					lcurs = 0;
					rcurs = 0;
				    } else if (i != leftTuplesInCurrentBlock.size() - 1 && j == rightbatch.size() - 1) {// case
					// 2
					lcurs = i + 1;
					rcurs = 0;
				    } else if (i == leftTuplesInCurrentBlock.size() - 1 && j != rightbatch.size() - 1) {// case
					// 3
					lcurs = i;
					rcurs = j + 1;
				    } else {
					lcurs = i;
					rcurs = j + 1;
				    }
				    return outbatch;
				}
			    }
			}
			rcurs = 0;
		    }
		    lcurs = 0;
		} catch (EOFException e) {
		    try {
			in.close();
		    } catch (IOException io) {
			System.out.println("NestedJoin:Error in temporary file reading");
		    }
		    eosr = true;
		} catch (ClassNotFoundException c) {
		    System.out.println("BlockNestedJoin:Some error in deserialization ");
		    System.exit(1);
		} catch (IOException io) {
		    System.out.println("BlockNestedJoin:temporary file reading error");
		    System.exit(1);
		}
	    }
	}
	return outbatch;
    }

    /* Close the operator */

    /*
     * (non-Javadoc)
     * @see qp.operators.Operator#close()
     */
    public boolean close() {

	File f = new File(rfname);
	f.delete();
	return true;

    }

}