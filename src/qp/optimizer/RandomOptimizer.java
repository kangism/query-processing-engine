/** performs randomized optimization, iterative improvement algorithm **/

package qp.optimizer;

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
import qp.utils.Condition;
import qp.utils.OrderByOption;
import qp.utils.RandNumb;
import qp.utils.SQLQuery;

public class RandomOptimizer {

    /** enumeration of different ways to find the neighbor plan **/

    public static final int METHODCHOICE = 0; // selecting neighbor by changing
    // a method for an operator
    public static final int COMMUTATIVE = 1; // by rearranging the operators by
    // commutative rule
    public static final int ASSOCIATIVE = 2; // rearranging the operators by
    // associative rule

    /** Number of altenative methods available for a node as specified above **/

    public static final int NUMCHOICES = 3;

    private static SQLQuery sqlquery; // Vector of Vectors of Select + From + Where + GroupBy
    int numJoin; // Number of joins in this query plan

    /** constructor **/

    public RandomOptimizer(SQLQuery sqlquery) {
	setSqlquery(sqlquery);
    }

    /** Randomly selects a neighbour **/

    protected Operator getNeighbor(Operator root) {
	// Randomly select a node to be altered to get the neighbour
	int nodeNum = RandNumb.randInt(0, numJoin - 1);

	// Randomly select type of alteration: Change
	// Method/Associative/Commutative
	int changeType = RandNumb.randInt(0, NUMCHOICES - 1);
	Operator neighbor = null;

	switch (changeType) {
	    case METHODCHOICE: // Select a neighbour by changing the method type
		neighbor = neighborMeth(root, nodeNum);
		break;
	    case COMMUTATIVE:
		neighbor = neighborCommut(root, nodeNum);
		break;
	    case ASSOCIATIVE:
		neighbor = neighborAssoc(root, nodeNum);
		break;
	}
	return neighbor;
    }

    /**
     * implementation of Iterative Improvement Algorithm for Randomized optimization of Query Plan
     **/

    public Operator getOptimizedPlan() {

	/** get an initial plan for the given sql query **/

	RandomInitialPlan rip = new RandomInitialPlan(getSqlquery());
	numJoin = rip.getNumJoins();

	int MINCOST = Integer.MAX_VALUE;
	Operator finalPlan = null;

	/** NUMTER is number of times random restart **/

	int NUMITER;
	if (numJoin != 0) {
	    NUMITER = 2 * numJoin;
	} else {
	    NUMITER = 1;
	}

	/**
	 * Randomly restart the gradient descent until the maximum specified number of random
	 * restarts (NUMITER) has satisfied
	 **/

	for (int j = 0; j < NUMITER; j++) {
	    Operator initPlan = rip.prepareInitialPlan();

	    modifySchema(initPlan);
	    System.out.println("-----------initial Plan-------------");
	    Debug.PPrint(initPlan);
	    PlanCost pc = new PlanCost();
	    int initCost = pc.getCost(initPlan);
	    System.out.println(initCost);

	    boolean flag = true;
	    int minNeighborCost = initCost; // just initialization purpose;
	    Operator minNeighbor = initPlan; // just initialization purpose;
	    if (numJoin != 0) {

		while (flag) { // flag = false when local minimum is reached
		    System.out.println("---------------while--------");
		    Operator initPlanCopy = (Operator) initPlan.clone();
		    minNeighbor = getNeighbor(initPlanCopy);

		    System.out.println("--------------------------neighbor---------------");
		    Debug.PPrint(minNeighbor);
		    pc = new PlanCost();
		    minNeighborCost = pc.getCost(minNeighbor);
		    System.out.println("  " + minNeighborCost);

		    /**
		     * In this loop we consider from the possible neighbors (randomly selected) and
		     * take the minimum among for next step
		     **/

		    for (int i = 1; i < 2 * numJoin; i++) {
			initPlanCopy = (Operator) initPlan.clone();
			Operator neighbor = getNeighbor(initPlanCopy);
			System.out.println("------------------neighbor--------------");
			Debug.PPrint(neighbor);
			pc = new PlanCost();
			int neighborCost = pc.getCost(neighbor);
			System.out.println(neighborCost);

			if (neighborCost < minNeighborCost) {
			    minNeighbor = neighbor;
			    minNeighborCost = neighborCost;
			}
			// System.out.println("-----------------for-------------");
		    }
		    if (minNeighborCost < initCost) {
			initPlan = minNeighbor;
			initCost = minNeighborCost;
		    } else {
			minNeighbor = initPlan;
			minNeighborCost = initCost;

			flag = false; // local minimum reached
		    }
		}
		System.out.println("------------------local minimum--------------");
		Debug.PPrint(minNeighbor);
		System.out.println(" " + minNeighborCost);

	    }
	    if (minNeighborCost < MINCOST) {
		MINCOST = minNeighborCost;
		finalPlan = minNeighbor;

	    }
	}
	System.out.println("\n\n\n");
	System.out.println("---------------------------Final Plan----------------");
	Debug.PPrint(finalPlan);
	System.out.println("  " + MINCOST);
	return finalPlan;
    }

    /**
     * Selects a random method choice for join wiht number joinNum e.g., Nested loop join,
     * Sort-Merge Join, Hash Join etc.., returns the modified plan
     **/

    protected Operator neighborMeth(Operator root, int joinNum) {
	System.out.println("------------------neighbor by method change----------------");
	int numJMeth = JoinType.numJoinTypes();
	if (numJMeth > 1) {
	    /** find the node that is to be altered **/
	    Join node = (Join) findNodeAt(root, joinNum);
	    int prevJoinMeth = node.getJoinType().getJoinId();
	    int joinMeth = RandNumb.randInt(0, numJMeth - 1);
	    while (joinMeth == prevJoinMeth) {
		joinMeth = RandNumb.randInt(0, numJMeth - 1);
	    }
	    node.setJoinType(JoinType.getJoinTypeById(joinMeth));
	}
	return root;
    }

    /**
     * Applies join Commutativity for the join numbered with joinNum e.g., A X B is changed as B X A
     * returns the modifies plan
     **/

    protected Operator neighborCommut(Operator root, int joinNum) {
	System.out.println("------------------neighbor by commutative---------------");
	/** find the node to be altered **/
	Join node = (Join) findNodeAt(root, joinNum);
	Operator left = node.getLeft();
	Operator right = node.getRight();
	node.setLeft(right);
	node.setRight(left);
	/*** also flip the condition i.e., A X a1b1 B = B X b1a1 A **/
	node.getCondition().flip();
	// Schema newschem = left.getSchema().joinWith(right.getSchema());
	// node.setSchema(newschem);

	/** modify the schema before returning the root **/
	modifySchema(root);
	return root;
    }

    /**
     * Applies join Associativity for the join numbered with joinNum e.g., (A X B) X C is changed to
     * A X (B X C) returns the modifies plan
     **/

    protected Operator neighborAssoc(Operator root, int joinNum) {
	System.out.println("------------------neighbor by associative---------------");
	/** find the node to be altered **/
	Join op = (Join) findNodeAt(root, joinNum);
	// Join op = (Join) joinOpList.elementAt(joinNum);
	Operator left = op.getLeft();
	Operator right = op.getRight();

	if (left.getOperatorType() == OperatorType.JOIN && right.getOperatorType() != OperatorType.JOIN) {
	    transformLefttoRight(op, (Join) left);

	} else if (left.getOperatorType() != OperatorType.JOIN && right.getOperatorType() == OperatorType.JOIN) {
	    transformRighttoLeft(op, (Join) right);
	} else if (left.getOperatorType() == OperatorType.JOIN && right.getOperatorType() == OperatorType.JOIN) {
	    if (RandNumb.flipCoin())
		transformLefttoRight(op, (Join) left);
	    else
		transformRighttoLeft(op, (Join) right);
	} else {
	    // The join is just A X B, therefore Association rule is not
	    // applicable
	}

	/** modify the schema before returning the root **/
	modifySchema(root);
	return root;
    }

    /** This is given plan (A X B) X C **/

    protected void transformLefttoRight(Join op, Join left) {
	System.out.println("------------------Left to Right neighbor--------------");
	Operator right = op.getRight();
	Operator leftleft = left.getLeft();
	Operator leftright = left.getRight();
	Attribute leftAttr = op.getCondition().getLhs();
	Join temp;

	/**
	 * CASE 1 : ( A X a1b1 B) X b4c4 C = A X a1b1 (B X b4c4 C) a1b1, b4c4 are the join
	 * conditions at that join operator
	 **/

	if (leftright.getSchema().contains(leftAttr)) {
	    System.out.println("----------------CASE 1-----------------");

	    temp = new Join(leftright, right, op.getCondition(), OperatorType.JOIN);
	    temp.setJoinType(op.getJoinType());
	    temp.setNodeIndex(op.getNodeIndex());
	    op.setLeft(leftleft);
	    op.setJoinType(left.getJoinType());
	    op.setNodeIndex(left.getNodeIndex());
	    op.setRight(temp);
	    op.setCondition(left.getCondition());

	} else {
	    System.out.println("--------------------CASE 2---------------");
	    /**
	     * CASE 2: ( A X a1b1 B) X a4c4 C = B X b1a1 (A X a4c4 C) a1b1, a4c4 are the join
	     * conditions at that join operator
	     **/
	    temp = new Join(leftleft, right, op.getCondition(), OperatorType.JOIN);
	    temp.setJoinType(op.getJoinType());
	    temp.setNodeIndex(op.getNodeIndex());
	    op.setLeft(leftright);
	    op.setRight(temp);
	    op.setJoinType(left.getJoinType());
	    op.setNodeIndex(left.getNodeIndex());
	    Condition newcond = left.getCondition();
	    newcond.flip();
	    op.setCondition(newcond);
	}
    }

    protected void transformRighttoLeft(Join op, Join right) {

	System.out.println("------------------Right to Left Neighbor------------------");
	Operator left = op.getLeft();
	Operator rightleft = right.getLeft();
	Operator rightright = right.getRight();
	Attribute rightAttr = (Attribute) op.getCondition().getRhs();
	Join temp;
	/**
	 * CASE 3 : A X a1b1 (B X b4c4 C) = (A X a1b1 B ) X b4c4 C a1b1, b4c4 are the join
	 * conditions at that join operator
	 **/
	if (rightleft.getSchema().contains(rightAttr)) {
	    System.out.println("----------------------CASE 3-----------------------");
	    temp = new Join(left, rightleft, op.getCondition(), OperatorType.JOIN);
	    temp.setJoinType(op.getJoinType());
	    temp.setNodeIndex(op.getNodeIndex());
	    op.setLeft(temp);
	    op.setRight(rightright);
	    op.setJoinType(right.getJoinType());
	    op.setNodeIndex(right.getNodeIndex());
	    op.setCondition(right.getCondition());
	} else {
	    /**
	     * CASE 4 : A X a1c1 (B X b4c4 C) = (A X a1c1 C ) X c4b4 B a1b1, b4c4 are the join
	     * conditions at that join operator
	     **/
	    System.out.println("-----------------------------CASE 4-----------------");
	    temp = new Join(left, rightright, op.getCondition(), OperatorType.JOIN);
	    temp.setJoinType(op.getJoinType());
	    temp.setNodeIndex(op.getNodeIndex());

	    op.setLeft(temp);
	    op.setRight(rightleft);
	    op.setJoinType(right.getJoinType());
	    op.setNodeIndex(right.getNodeIndex());
	    Condition newcond = right.getCondition();
	    newcond.flip();
	    op.setCondition(newcond);

	}

    }

    /**
     * This method traverses through the query plan and returns the node mentioned by joinNum
     **/

    protected Operator findNodeAt(Operator node, int joinNum) {
	if (node.getOperatorType() == OperatorType.JOIN) {
	    if (((Join) node).getNodeIndex() == joinNum) {
		return node;
	    } else {
		Operator temp;
		temp = findNodeAt(((Join) node).getLeft(), joinNum);
		if (temp == null)
		    temp = findNodeAt(((Join) node).getRight(), joinNum);
		return temp;
	    }
	} else if (node.getOperatorType() == OperatorType.SCAN) {
	    return null;
	} else if (node.getOperatorType() == OperatorType.SELECT) {
	    // if sort/project/select operator
	    return findNodeAt(((Select) node).getBase(), joinNum);
	} else if (node.getOperatorType() == OperatorType.PROJECT) {
	    return findNodeAt(((Project) node).getBase(), joinNum);
	} else if (node.getOperatorType() == OperatorType.DISTINCT) {
	    return findNodeAt(((Distinct) node).getBase(), joinNum);
	} else if (node.getOperatorType() == OperatorType.SORT) {
	    return findNodeAt(((Sort) node).getBase(), joinNum);
	} else {
	    return null;
	}
    }

    /**
     * modifies the schema of operators which are modified due to selecing an alternative neighbor
     * plan
     **/
    private void modifySchema(Operator node) {

	if (node.getOperatorType() == OperatorType.JOIN) {
	    Operator left = ((Join) node).getLeft();
	    Operator right = ((Join) node).getRight();
	    modifySchema(left);
	    modifySchema(right);
	    node.setSchema(left.getSchema().joinWith(right.getSchema()));
	} else if (node.getOperatorType() == OperatorType.SELECT) {
	    Operator base = ((Select) node).getBase();
	    modifySchema(base);
	    node.setSchema(base.getSchema());
	} else if (node.getOperatorType() == OperatorType.PROJECT) {
	    Operator base = ((Project) node).getBase();
	    modifySchema(base);
	    Vector<Attribute> attrlist = ((Project) node).getProjAttr();
	    node.setSchema(base.getSchema().subSchema(attrlist));
	} else if (node.getOperatorType() == OperatorType.DISTINCT) {
	    Operator base = ((Distinct) node).getBase();
	    modifySchema(base);
	    node.setSchema(base.getSchema());
	} else if (node.getOperatorType() == OperatorType.SORT) {
	    Operator base = ((Sort) node).getBase();
	    modifySchema(base);
	    node.setSchema(base.getSchema());
	}
    }

    /**
     * After finding a choice of method for each operator prepare an execution plan by replacing the
     * methods with corresponding join operator implementation
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

		    /**
		     * Temporarity used simple nested join, replace with hasjoin, if implemented
		     **/

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
		    /* + other code */
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
	} else if (node.getOperatorType() == OperatorType.DISTINCT) {
	    Operator base = makeExecPlan(((Distinct) node).getBase());
	    ((Distinct) node).setBase(base);
	    return node;
	} else if (node.getOperatorType() == OperatorType.SORT) {
	    Operator base = makeExecPlan(((Sort) node).getBase());
	    ((Sort) node).setBase(base);
	    return node;
	} else {
	    return node;
	}
    }

    private static Operator addPreSortingForSortMergeJoin(Operator node, OrderByOption orderByOption) {
	String tabName = null;
	if (node.getOperatorType() == OperatorType.PROJECT) {
	    tabName = ((Scan) ((Project) node).getBase()).getTabName();
	} else if (node.getOperatorType() == OperatorType.SCAN) {
	    tabName = ((Scan) node).getTabName();
	}
	//XXX maybe there are other case and I should read the whole tree 
	// of Operator to get the name of the table

	if (tabName != null) {
	    Vector<AttributeOption> orderbylist = new Vector<AttributeOption>();
	    for (Condition condition : getSqlquery().getJoinList()) {
		if (condition.getLhs().getTabName().equals(tabName)) {
		    orderbylist.add(new AttributeOption(condition.getLhs(),orderByOption));
		} else if (((Attribute) condition.getRhs()).getTabName().equals(tabName)) {
		    // here we assume that the right part of the Condition is a Attribute
		    // i.e. we consider only basic join condition
		    // but this assumption is made by the framework in RandomInitalPlan
		    orderbylist.add(new AttributeOption((Attribute) condition.getRhs(),orderByOption));
		}
	    }
	    Sort presort = new Sort(node, orderbylist, false, OperatorType.SORT);
	    presort.setSchema(node.getSchema());
	    return presort;
	} else {
	    return node;
	}
    }

    public static void setSqlquery(SQLQuery sqlquery) {
	RandomOptimizer.sqlquery = sqlquery;
    }

    public static SQLQuery getSqlquery() {
	return sqlquery;
    }
}
