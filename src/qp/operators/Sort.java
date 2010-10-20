/**
 * 
 */
package qp.operators;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
    Map<String, Batch> pagesInMem;

    /**
     * The file num of the temp file.
     */
    int filenum = 0;

    Queue<String> tempFiles = new LinkedList<String>();

    TupleComparator tupleComparator;

    /**
     * The current temp file name.
     */
    String tempFile = "";

    boolean isDistinct;

    Batch returned;

    public Sort(Operator base, Vector<AttributeOption> as, boolean isDistinct, OperatorType type) {
	super(type);
	this.attrSet = as;
	this.base = base;
	this.isDistinct = isDistinct;
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
	tupleComparator = new TupleComparator(attrSet);

	base.open();
	
	returned = null;
	while (true) {

	    List<Tuple> sorted = new ArrayList<Tuple>();

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
		    //System.out.println("tuplesInMem and tempFiles are Empty => returned=null");
		    returned = null;
		} else {
		    pagesInMem = new HashMap<String, Batch>(numBuff - 1);
		    Map<String, ObjectInputStream> ins = new HashMap<String, ObjectInputStream>(numBuff - 1);

		    //System.out.println(tempFiles.size() + " " + ins.size()); // XXX

		    while (!tempFiles.isEmpty() && ins.size() < numBuff - 1) {
			tempFile = tempFiles.poll();
			try {
			    ins.put(tempFile, new ObjectInputStream(new FileInputStream(tempFile)));
			} catch (EOFException e) {
			    e.printStackTrace();
			} catch (IOException e) {
			    e.printStackTrace();
			}
			try {
			    pagesInMem.put(tempFile, (Batch) ins.get(tempFile).readObject());
			} catch (IOException e) {
			    e.printStackTrace();
			} catch (ClassNotFoundException e) {
			    e.printStackTrace();
			}
		    }

		    //System.out.println(tempFiles.size() + " " + ins.size()); // XXX

		    Tuple minTuple = null;
		    String minTupleFromBatch = null;
		    while (!pagesInMem.isEmpty()) {
			//System.out.println("P2: used buffer to sort ->" + pagesInMem.size());
			// //XXX
			minTuple = null;
			minTupleFromBatch = null;
			// Looking for the minimum
			for (String filename : pagesInMem.keySet()) {
			    if (minTuple == null) {
				minTuple = pagesInMem.get(filename).elementAt(0);
				minTupleFromBatch = filename;
			    } else {
				if (Tuple.goodOrder(pagesInMem.get(filename).elementAt(0), minTuple, attrSet)) {
				    minTuple = pagesInMem.get(filename).elementAt(0);
				    minTupleFromBatch = filename;
				}
			    }
			}

			// HERE WE CAN ADD A TEST IF DISTINCT OPTION IS ON
			// Inserting the element in the output
			if (!isDistinct || (sorted.isEmpty() || tupleComparator.compare(minTuple, sorted.get(sorted.size() - 1)) != 0)) {
			    sorted.add(minTuple);
			    // System.out.println(sorted.size());
		    	}

			// and removing in from the pile
			pagesInMem.get(minTupleFromBatch).remove(0);
			if (pagesInMem.get(minTupleFromBatch).isEmpty()) {
			    Batch next = null;
			    try {
				next = (Batch) ins.get(minTupleFromBatch).readObject();
			    } catch (EOFException e) {
				try {
				    ins.get(minTupleFromBatch).close();

				} catch (IOException e1) {
				    e1.printStackTrace();
				}
				ins.remove(minTupleFromBatch);
				pagesInMem.remove(minTupleFromBatch);

			    } catch (IOException e) {
			    } catch (ClassNotFoundException e) {
			    }

			    if (next != null && !next.isEmpty()) {
				pagesInMem.put(minTupleFromBatch, next);
			    }

			}
		    }

		    // System.out.println(tempFiles.size() + " " + ins.size()); // XXX
		    if (ins.isEmpty() && tempFiles.isEmpty()) {
			//System.out.println("RETURN"); // XXX
			outbatch = new Batch(batchsize);
			for (int i = 0; i < sorted.size(); i++) {
			    outbatch.add(sorted.get(i));
			}
			returned = outbatch; // XXX THIS BATCH IS OVER SIZED..
			break;
		    } else {
			filenum++;
			tempFile = "SortTemp-" + String.valueOf(filenum);
			try {
			    ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tempFile));
			    int i = 0;
			    while (i < sorted.size()) {
				outbatch = new Batch(batchsize);
				while (!outbatch.isFull() && i < sorted.size()) {
				    outbatch.add(sorted.get(i));
				    i++;
				}
				out.writeObject(outbatch);
			    }
			    out.close();
			    tempFiles.add(tempFile);
			    ///System.out.println("P2 :" + tempFile); // XXX
			} catch (IOException io) {
			    System.out.println("Sort: writing the temporay file error");
			}
			returned = new Batch(batchsize);
		    }
		}
	    } else {
		// Phase ONE
		Collections.sort(tuplesInMem, tupleComparator);

		filenum++;
		tempFile = "SortTemp-" + String.valueOf(filenum);
		try {
		    ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tempFile));

		    tempFiles.add(tempFile);
		    ///System.out.println("P1: " + tempFile); // XXX

		    int i = 0;
		    while (i < tuplesInMem.size()) {
			outbatch = new Batch(batchsize);
			while (!outbatch.isFull() && i < tuplesInMem.size()) {
			    outbatch.add(tuplesInMem.get(i));
			    i++;
			}
			out.writeObject(outbatch);
		    }

		    out.close();
		} catch (IOException io) {
		    System.out.println("Sort:writing the temporay file error");
		}
		returned = new Batch(batchsize);
	    }
	}

	return true;
    }

    /** Read next tuple from operator */

    public Batch next() {

	outbatch = new Batch(batchsize);
	for (int i = 0; i < batchsize; i++) {
	    if(!returned.isEmpty()){
		outbatch.add(returned.elementAt(0));
		returned.remove(0);
	    }
	}	
	if(outbatch.isEmpty()){
	    return null;
	}
	return outbatch;
    }

    /** Close the operator */
    public boolean close() {
	return base.close();
    }

    public Object clone() {
	Operator newbase = (Operator) base.clone();
	Vector<AttributeOption> newattr = new Vector<AttributeOption>();
	for (int i = 0; i < attrSet.size(); i++)
	    newattr.add(attrSet.elementAt(i).clone());
	Sort newproj = new Sort(newbase, newattr, isDistinct, operatorType);
	newproj.setSchema(newbase.getSchema());
	return newproj;
    }

}
