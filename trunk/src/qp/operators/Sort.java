/**
 * 
 */
package qp.operators;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Vector;

import qp.optimizer.BufferManager;
import qp.utils.AttributeOption;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;
import qp.utils.TupleComparator;

/**
 * Content the Sort Algorithm which is not completed yet. Do only the phase ONE of the external
 * sort. Can Sort several given attributes. DESC not implemented yet
 * 
 * @author Yann-Loup
 */
public class Sort extends Operator {

    /**
     * Number of buffers available.
     */
    int numBuff;

    Operator base;
    Vector<AttributeOption> attrSet;

    /**
     * number of tuples per outbatch
     */
    int batchsize;

    /**
     * The following fields are required during execution of the Sort Operator
     **/

    Batch next;
    Batch outbatch;

    List<Tuple> tuplesInMem;

    /**
     * For the phase TWO. here we read the different temporary files in order to merge the sort
     * runs.
     */
    List<Batch> pagesInMem;

    /**
     * The file num of the temp file.
     */
    int filenum = 0;

    Queue<String> tempFiles = new LinkedList<String>();

    /**
     * The current temp file name.
     */
    String tempFile = "";

    public Sort(Operator base, Vector<AttributeOption> as, OperatorType type) {
	super(type);
	this.attrSet = as;
	this.base = base;
    }

    public void setBase(Operator base) {
	this.base = base;
    }

    public Operator getBase() {
	return base;
    }

    public Vector<AttributeOption> getSortAttr() {
	return attrSet;
    }

    public void setNumBuff(int num) {
	this.numBuff = num;
    }

    public int getNumBuff() {
	return numBuff;
    }

    /**
     * Opens the connection to the base operator Also figures out what are the columns to be
     * projected from the base operator
     **/

    public boolean open() {
	/** setnumber of tuples per batch **/
	int tuplesize = schema.getTupleSize();
	batchsize = Batch.getPageSize() / tuplesize;
	this.numBuff = BufferManager.getNumBuffer();

	Schema baseSchema = base.getSchema();
	AttributeOption attr;
	int index;
	for (int i = 0; i < attrSet.size(); i++) {
	    attr = attrSet.elementAt(i);
	    index = baseSchema.indexOf(attr.getAttribute());
	    attr.setAttributeIndexInSchema(index);
	}

	// //////// Phase ONE of External Sort \\\\\\\\\

	return base.open();
    }

    /** Read next tuple from operator */

    public Batch next() {
	outbatch = new Batch(batchsize);

	tuplesInMem = new ArrayList<Tuple>();

	// all the tuples in the different page of the
	// memory are in one list to make it easier
	for (int i = 0; i < numBuff - 1; i++) {
	    next = base.next();
	    if (next != null) {
		tuplesInMem.addAll(next.getTuples());
	    }
	}

	if (tuplesInMem.isEmpty()) { // ///////PHASE TWO \\\\\\\\\\\\
	    if (tempFiles.isEmpty()) {
		return null;
	    }
	    pagesInMem = new ArrayList<Batch>(numBuff - 1);
	    ObjectInputStream in = null;
	    Batch input = null;

	    for (int i = 0; i < numBuff - 1; i++) {
		if (!tempFiles.isEmpty()) {
		    tempFile = tempFiles.poll();
		    try {
			in = new ObjectInputStream(new FileInputStream(tempFile));
		    } catch (FileNotFoundException e) {
			e.printStackTrace();
		    } catch (IOException e) {
			e.printStackTrace();
		    }
		    try {
			input = (Batch) in.readObject();
		    } catch (IOException e) {
			e.printStackTrace();
		    } catch (ClassNotFoundException e) {
			e.printStackTrace();
		    }
		    pagesInMem.add(input);
		}
	    }

	    Tuple minTuple = null;
	    Batch minTupleFromBatch = null;
	    while (!pagesInMem.isEmpty()) {
		// System.out.println(pagesInMem.size());
		minTuple = null;
		minTupleFromBatch = null;
		// Looking for the minimum
		for (Batch batch : pagesInMem) {
		    if (minTuple == null) {
			minTuple = batch.elementAt(0);
			minTupleFromBatch = batch;
		    } else {
			if (Tuple.goodOrder(batch.elementAt(0),minTuple, attrSet)) {
			    minTuple = batch.elementAt(0);
			    minTupleFromBatch = batch;
			}
		    }
		}
		// Inserting the element in the output and removing in from the pile
		outbatch.add(minTuple);
		minTupleFromBatch.remove(0);
		if (minTupleFromBatch.isEmpty()) {
		    pagesInMem.remove(minTupleFromBatch);
		}
	    }
	    if (tempFiles.isEmpty()) {
		// System.out.println("RETURN");
		return outbatch;
	    } else {
		filenum++;
		tempFile = "SortTemp-" + String.valueOf(filenum);
		try {
		    ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tempFile));
		    out.writeObject(outbatch);
		    out.close();
		    tempFiles.add(tempFile);
		    // System.out.println("P2 :" + tempFile);
		} catch (IOException io) {
		    System.out.println("Sort:writing the temporay file error");
		}
		return new Batch(batchsize);
	    }

	} else {
	    // Phase ONE
	    Collections.sort(tuplesInMem, new TupleComparator(attrSet));
	    for (int i = 0; i < tuplesInMem.size(); i++) {
		outbatch.add(tuplesInMem.get(i));
	    }

	    // for(int i = 0; i < tuplesInMem.size(); i++) {
	    // findGoodPlace(tuplesInMem.get(i), outbatch);
	    // }

	    filenum++;
	    tempFile = "SortTemp-" + String.valueOf(filenum);
	    try {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tempFile));
		out.writeObject(outbatch);
		tempFiles.add(tempFile);
		// System.out.println("P1: " + tempFile);
		out.close();
	    } catch (IOException io) {
		System.out.println("Sort:writing the temporay file error");
	    }

	    // XXX PROBLEM HERE THE OUTBATCH IS TOO LARGE
	    return new Batch(batchsize);// outbatch;
	}
    }

    // private void findGoodPlace(Tuple tuple, Batch outbatch) {
    // /**
    // * Flag to know if we have found the right place.
    // */
    // boolean found = false;
    //
    // // We add each tuples one by one and we try to find the good position!
    //
    // // we add the first one
    // if (outbatch.isEmpty()) {
    // outbatch.add(tuple);
    // found = true;
    // }
    //
    // // case: before than the first one
    // if (!found && Tuple.goodOrder(tuple, outbatch.elementAt(0), attrSet)) {
    // // The tuples is before than the first attribute
    // outbatch.insertElementAt(tuple, 0);
    // found = true;
    // }
    //
    // // case: after than the last one
    // if (!found && Tuple.goodOrder(outbatch.elementAt(outbatch.size() - 1),tuple, attrSet)) {
    // // The tuples is after than the last attribute
    // outbatch.add(tuple);
    // found = true;
    // }
    // int j = 1;
    // while(!found && j<outbatch.size()){
    // if (Tuple.goodOrder(outbatch.elementAt(j - 1),tuple, attrSet)
    // && Tuple.goodOrder(tuple, outbatch.elementAt(j), attrSet)) {
    // // the tuple is between the element j-1 and j
    // outbatch.insertElementAt(tuple, j);
    // found = true;
    // }
    // j++;
    // }
    // if(!found){ // just in case... but should be never hit
    // System.err.println("Should Be never hit!!");
    // outbatch.insertElementAt(tuple, j);
    // found = true;
    // }
    // }

    /** Close the operator */
    public boolean close() {
	return base.close();
    }

    public Object clone() {
	Operator newbase = (Operator) base.clone();
	Vector<AttributeOption> newattr = new Vector<AttributeOption>();
	for (int i = 0; i < attrSet.size(); i++)
	    newattr.add(attrSet.elementAt(i).clone());
	Sort newproj = new Sort(newbase, newattr, operatorType);
	newproj.setSchema(newbase.getSchema());
	return newproj;
    }

}
