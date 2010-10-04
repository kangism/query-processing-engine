package qp.operators;

import java.util.Hashtable;
import java.util.LinkedList;
import qp.utils.Tuple;

public class InnerHashTable extends Hashtable<Integer, LinkedList<Tuple>> 
{

	public InnerHashTable(int InitialCapacity) 
	{
		super(InitialCapacity);
	}

	public LinkedList<Tuple> put(Integer key, LinkedList<Tuple> list)
	{
		if (containsKey(key)) ///if contains key, add first (actually only one) tuple in list to the existing value list
		{
			LinkedList<Tuple> v = this.get(key);
			v.add(list.getFirst());
			return v;
		} 
		else 
		{
			super.put(key, list);
			return null;
		}
	}
}
