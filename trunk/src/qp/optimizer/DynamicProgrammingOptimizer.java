package qp.optimizer;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import qp.operators.BlockNestedJoin;
import qp.operators.Debug;
import qp.operators.Distinct;
import qp.operators.HashJoin;
import qp.operators.Join;
import qp.operators.JoinType;
import qp.operators.NestedJoin;
import qp.operators.Operator;
import qp.operators.OperatorType;
import qp.operators.Project;
import qp.operators.Scan;
import qp.operators.Select;
import qp.operators.Sort;
import qp.operators.SortMergeJoin;
import qp.utils.Attribute;
import qp.utils.AttributeOption;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.OrderByOption;
import qp.utils.SQLQuery;
import qp.utils.Schema;

public class DynamicProgrammingOptimizer {
	static SQLQuery sqlquery;
	int numJoin;
	int numTable;
	Vector<Map<Set<String>, Operator>> levelSpace;
	
	public DynamicProgrammingOptimizer(SQLQuery sqlquery) {
		this.sqlquery = sqlquery;
		this.numTable = this.sqlquery.getFromList().size();
		levelSpace = new Vector<Map<Set<String>, Operator>>(this.numTable);
		for (int i = 0; i < this.numTable; i++) {
			Map<Set<String>, Operator> m = new HashMap<Set<String>, Operator>();
			levelSpace.add(m);
		}
	}
	
	
	///create optimal plan by dynamic programming
	public Operator getOptimalPlan() {
		Operator root = null;
		Set<String> wholeSet = new HashSet<String>();
	
		///initiate level 0 (cardinality = 1)
		for (int i = 0; i < numTable; i++) {
			Set<String> newset = new HashSet<String>();
			newset.add((String) this.sqlquery.getFromList().elementAt(i));
			root = createInitOperator((String) this.sqlquery.getFromList().elementAt(i));
			levelSpace.elementAt(0).put(newset, root);
		}
		/// start from level one (cardinality=2 for each subset)
		/// to build the optimized plan list
		for (int level = 1; level < numTable; level++) {
			Map<Integer, Set<String>> allSubset = getAllSubsets(level + 1); ///level+1 is the true cardinality
			/// each subset for current level
			for (int pointer = 0; pointer < allSubset.size(); pointer++) {
				Set<String> tmpSubset = allSubset.get(pointer);
				String[] subset = tmpSubset.toArray(new String[0]);
				Operator bestPlan = getBestPlan(subset, level);
				Set<String> tableSet = new HashSet<String>();
				for (int i = 0; i <= level; i++)
					tableSet.add(subset[i]);
				/// map current set to the best plan found
				levelSpace.elementAt(level).put(tableSet, bestPlan);
			}
		}
		
		// final level plan
		for (int i = 0; i < this.sqlquery.getFromList().size(); i++) {
			wholeSet.add((String) this.sqlquery.getFromList().elementAt(i));
		}
		return createFinalPlan(levelSpace.elementAt(numTable - 1).get(wholeSet));
	}


	/// create final optimal plan
	public Operator createFinalPlan(Operator root) {
		Operator base = root;
		Operator newRoot = null;
		
		///create Sort Op , if isDistinct is true, add attr in projectlist to facilitate Distinct Op
		Vector<AttributeOption> orderbylist = sqlquery.getOrderByList();
		if(orderbylist == null)
			orderbylist = new Vector<AttributeOption>();
		Vector<Attribute> projectlist = (Vector<Attribute>) sqlquery.getProjectList();
		if (sqlquery.isDistinct() && projectlist != null && !projectlist.isEmpty()) {
			for (Attribute att : projectlist) {
				if (!orderbylist.contains(new AttributeOption(att))
						&& !orderbylist.contains(new AttributeOption(att,
								OrderByOption.DESC))) {
					orderbylist.add(new AttributeOption(att));
				}
			}
		}
		if (orderbylist != null && !orderbylist.isEmpty()) {
			newRoot = new Sort(base, orderbylist, false, OperatorType.SORT);
			newRoot.setSchema(base.getSchema());
		}
		if (newRoot != null) {
			base = newRoot;
		} else {
			base = root;
		}
	
		///create Project Op
		if (projectlist != null && !projectlist.isEmpty()) {
			newRoot = new Project(base, projectlist, OperatorType.PROJECT);
			Schema newSchema = base.getSchema().subSchema(projectlist);
			newRoot.setSchema(newSchema);
		}
		if (newRoot != null) {
			base = newRoot;
		} else {
			base = root;
		}
		
		///create Distinct Op
		if (sqlquery.isDistinct()) {
			newRoot = new Distinct(base, OperatorType.DISTINCT);
			newRoot.setSchema(base.getSchema());
		}
		PlanCost pc = new PlanCost();
		if (newRoot != null) {
			System.out.println("\n");
			System.out.println("---------------------------Final Plan----------------");
			Debug.PPrint(newRoot);
			System.out.println("  " + pc.getCost(newRoot));
			return newRoot;
		} else {
			System.out.println("\n");
			System.out.println("---------------------------Final Plan----------------");
			Debug.PPrint(root);
			System.out.println("  " + pc.getCost(root));
			return root;
		}
	}


	/**
	 * After finding a choice of method for each operator prepare an execution
	 * plan by replacing the methods with corresponding join operator
	 * implementation
	 **/
	
	public static Operator makeExecPlan(Operator node) {

		if (node.getOperatorType() == OperatorType.JOIN) {
			Operator left = makeExecPlan(((Join) node).getLeft());
			Operator right = makeExecPlan(((Join) node).getRight());
			JoinType joinType = ((Join) node).getJoinType();
			int numbuff = BufferManager.getBuffersPerJoin();
			switch (joinType) {
			case NESTEDJOIN:

				NestedJoin nj = new NestedJoin((Join) node);
				nj.setLeft(left);
				nj.setRight(right);
				nj.setNumBuff(numbuff);
				return nj;

			case BLOCKNESTED:

			    // NestedJoin bj2 = new NestedJoin((Join) node);
			    BlockNestedJoin bj = new BlockNestedJoin((Join) node);
			    bj.setLeft(left);
			    bj.setRight(right);
			    bj.setNumBuff(numbuff);
			    /* + other code */
			    bj.setBlockSize(numbuff - 2);
			    return bj;

			case SORTMERGE:
			    SortMergeJoin sm = new SortMergeJoin((Join) node);
			    // We add a ASC pre sorting
			    sm.setOrderByOption(OrderByOption.ASC);
			    sm.setLeft(addPreSortingForSortMergeJoin(left,OrderByOption.ASC));
			    sm.setRight(addPreSortingForSortMergeJoin(right,OrderByOption.ASC));
			    sm.setNumBuff(numbuff);			    
			    return sm;

			case HASHJOIN:

				HashJoin hj = new HashJoin((Join) node);
				hj.setLeft(left);
				hj.setRight(right);
				hj.setNumBuff(numbuff);
				return hj;
			default:
				return node;
			}
		} else if (node.getOperatorType() == OperatorType.SELECT) {
			Operator base = makeExecPlan(((Select) node).getBase());
			((Select) node).setBase(base);
			return node;
		} else if (node.getOperatorType() == OperatorType.PROJECT) {
			Operator base = makeExecPlan(((Project) node).getBase());
			((Project) node).setBase(base);
			return node;
		} else if (node.getOperatorType() == OperatorType.SORT) {
			Operator base = makeExecPlan(((Sort) node).getBase());
			((Sort) node).setBase(base);
			return node;
		} else if (node.getOperatorType() == OperatorType.DISTINCT) {
			Operator base = makeExecPlan(((Distinct) node).getBase());
			((Distinct) node).setBase(base);
			return node;
		} else {
			return node;
		}
	}
	
	
	private static Operator addPreSortingForSortMergeJoin(Operator node,
			OrderByOption orderByOption) {
		String tabName = null;
		if (node.getOperatorType() == OperatorType.PROJECT) {
			tabName = ((Scan) ((Project) node).getBase()).getTabName();
		} else if (node.getOperatorType() == OperatorType.SCAN) {
			tabName = ((Scan) node).getTabName();
		}
		// XXX maybe there are other case and I should read the whole tree
		// of Operator to get the name of the table

		if (tabName != null) {
			Vector<AttributeOption> orderbylist = new Vector<AttributeOption>();
			for (Condition condition : sqlquery.getJoinList()) {
				if (condition.getLhs().getTabName().equals(tabName)) {
					orderbylist.add(new AttributeOption(condition.getLhs(),
							orderByOption));
				} else if (((Attribute) condition.getRhs()).getTabName()
						.equals(tabName)) {
					// here we assume that the right part of the Condition is a
					// Attribute
					// i.e. we consider only basic join condition
					// but this assumption is made by the framework in
					// RandomInitalPlan
					orderbylist.add(new AttributeOption((Attribute) condition
							.getRhs(), orderByOption));
				}
			}
			Sort presort = new Sort(node, orderbylist, false, OperatorType.SORT);
			presort.setSchema(node.getSchema());
			return presort;
		} else {
			return node;
		}
	}


	public Operator createInitOperator(String tabName) {
		/// scan op
		Scan tempop = null;
		Operator root = null;
		Scan op1 = new Scan(tabName, OperatorType.SCAN);
		tempop = op1;
		String filename = tabName + ".md";
		try {
			ObjectInputStream _if = new ObjectInputStream(new FileInputStream(
					filename));
			Schema schm = (Schema) _if.readObject();
			op1.setSchema(schm);
			_if.close();
		} catch (Exception e) {
			System.err
					.println("createInitOperator(): Error in reading Schema of table "
							+ filename);
			System.exit(1);
		}
		root = op1;
		
		/// select op
		Select op2 = null;	
		for (int j = 0; j < this.sqlquery.getSelectionList().size(); j++) {	
			Condition cn = (Condition) this.sqlquery.getSelectionList().elementAt(j);
			if (cn.getOpType() == Condition.SELECT) {
				if (tabName == cn.getLhs().getTabName()) {
					op2 = new Select(op1, cn, OperatorType.SELECT);
					/** set the schema same as base relation **/
					op2.setSchema(tempop.getSchema());
					root = op2;
				}
			}
		}
		return root;
	}


	/// find the best plan for one subset
	public Operator getBestPlan(String[] subset, int level) {
		int bestPlanCost = Integer.MAX_VALUE;
		Operator newRoot = null;
		/// test each table with last level subset to get the optimized plan
		for (int i = 0; i < level + 1; i++) { ///level+1 is the true cardinality, i is index of table
			String tableName = subset[i];
			Set<String> singleTable = new HashSet<String>();
			singleTable.add(tableName);
			
			Set<String> otherTables = new HashSet<String>();
			Condition condition = null;
			for (int j = 0; j < level + 1; j++) {
				if (j != i)
					otherTables.add(subset[j]);
			}
			Operator lastRoot = levelSpace.elementAt(level - 1).get(otherTables);
			if (lastRoot == null) {
				continue;
			}
			
			condition = getJoinCondition(lastRoot, singleTable, level);
			int plancost = 0;
			PlanCost pc = new PlanCost();
			if (condition != null) {
				Join jn = new Join(lastRoot, levelSpace.elementAt(0).get(singleTable), condition, OperatorType.JOIN);
				Schema newSchema = lastRoot.getSchema().joinWith(levelSpace.elementAt(0).get(singleTable).getSchema());
				jn.setSchema(newSchema);
				JoinType joinType = getJoinType(jn);
				jn.setJoinType(joinType);
				plancost = pc.getCost(jn);
				if (plancost < bestPlanCost) {
					bestPlanCost = plancost;
					newRoot = jn;
				}
			}
		}
		System.out.print("level "+ level +": best join plan cost = " + bestPlanCost + "\t\tfor set { ");
		for(String s : subset)
			System.out.print(s + " ");
		System.out.println("}");
		return newRoot;
	}

	/// return join type due to cost
	public JoinType getJoinType(Join node) {
		JoinType joinType = null;
		PlanCost pc = new PlanCost();
		int leftTupleNum = pc.calculateCost(node.getLeft());
		int rightTupleNum = pc.calculateCost(node.getRight());
		Schema leftSchema = node.getLeft().getSchema();
		Schema rightSchema = node.getRight().getSchema();

		int leftTupleSize = leftSchema.getTupleSize();
		int leftBatchSize = Batch.getPageSize() / leftTupleSize;
		int rightTupleSize = rightSchema.getTupleSize();
		int rightBatchSize = Batch.getPageSize() / rightTupleSize;

		int leftpages = (int) Math.ceil(((double) leftTupleNum) / leftBatchSize);
		int rightpages = (int) Math.ceil(((double) rightTupleNum) / rightBatchSize);
		int numbuff = BufferManager.getBuffersPerJoin();
		
		// 2 *(N1 * K1 + N2 * K2) + N1 + N2
		// K = 1 + Log(N/B)/log(B-1)
		int numPassesLeft= (int) ((Math.log(leftpages/numbuff) / Math.log(numbuff-1)) + 1);
		int numPassesRight= (int) ((Math.log(rightpages/numbuff) / Math.log(numbuff-1)) + 1);
		int presortcost = 2 * (numPassesLeft*leftpages+numPassesRight*rightpages);
		
		int joincost_NJ = leftpages * rightpages;
		int joincost_BNJ = leftpages + leftpages * rightpages / (numbuff - 2);
		int joincost_HJ = 3 * (leftpages + rightpages);
		int joincost_SM = leftpages+rightpages + presortcost;
		
		int minCost = joincost_NJ;
		joinType = JoinType.NESTEDJOIN;
		if (minCost > joincost_BNJ) {
			minCost = joincost_BNJ;
			joinType = JoinType.BLOCKNESTED;
		}
		if (minCost > joincost_HJ) {
			minCost = joincost_HJ;
			joinType = JoinType.HASHJOIN;
		}
		if (minCost > joincost_SM) {
			minCost = joincost_SM;
			joinType = JoinType.SORTMERGE;
		}
		return joinType;
	}

	/// return left deep tree
	public Condition getJoinCondition(Operator lastroot, Set<String> singleTableSet, int level) {
		for (int i = 0; i < this.sqlquery.getJoinList().size(); i++) {
			Condition con = (Condition) this.sqlquery.getJoinList().elementAt(i);
			Attribute leftjoinAttr = con.getLhs();
			Attribute rightjoinAttr = (Attribute) con.getRhs();
			if ((lastroot.getSchema().contains(leftjoinAttr) && (levelSpace.elementAt(0).get(singleTableSet).getSchema().contains(rightjoinAttr)))) {
				return (Condition) con.clone();
			} else if (lastroot.getSchema().contains(rightjoinAttr)	&& levelSpace.elementAt(0).get(singleTableSet).getSchema().contains(leftjoinAttr)) {
				Condition newCon = (Condition) con.clone();
				newCon.setRhs((Attribute) leftjoinAttr.clone());
				newCon.setLhs((Attribute) rightjoinAttr.clone());
				return newCon;
			}
		}
		return null;
	}

	
	/// find all subsets with the specified cardinality
	public Map<Integer, Set<String>> getAllSubsets(int cardinality) {
		Map<Integer, Set<String>> map = new HashMap<Integer, Set<String>>();
		int numOfTable = sqlquery.getFromList().size();
		int i, j, numOfSubset =  (int) Math.pow(2, numOfTable);
		int key = 0;
		for (i = 0; i < numOfSubset; i++) {
			if (cardinality == countNumOfOnes(i)) {
				Set<String> set = new HashSet<String>();
				for (j = 0; j < numOfTable; j++) {
					if ((i & (1 << j)) != 0)
						set.add((String) sqlquery.getFromList().get(j));
				}
				map.put(key, set);
				key++;
			}
		}
		return map;
	}

	///count no. of one's when n is represented in binary
	public int countNumOfOnes(int n) {
		int num = 0;
		while (n != 0) {
			if ((n & 1) != 0)
				num++;
			n >>= 1;
		}
		return num;
	}
}
