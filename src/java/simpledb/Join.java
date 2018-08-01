package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

	private JoinPredicate predicate;
	private final OpIterator child1;
	private final OpIterator child2;
	private OpIterator[] children;
	private Tuple tuple1;
	
    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
    	this.child1 = child1;
    	this.child2 = child2;
    	this.predicate = p;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.predicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
    	int i = this.predicate.getField1();
        return child1.getTupleDesc().getFieldName(i);
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
    	int i = this.predicate.getField2();
        return child2.getTupleDesc().getFieldName(i);
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	TupleDesc td1 = child1.getTupleDesc();
    	TupleDesc td2 = child2.getTupleDesc();
        return TupleDesc.merge(td1, td2);
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	child1.open();
    	child2.open();
    	this.tuple1 = null;
    	
    }

    public void close() {
        // some code goes here
    	child1.close();
    	child2.close();
    	this.tuple1 = null;
    	
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	close();
    	open();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	while(true) {
        	if(tuple1==null) {
        		if(child1.hasNext()) {
        			tuple1 = child1.next();
        			child2.rewind();
        		}
        		else {
        			return null;
        		}
        	}
    		
        	while(child2.hasNext()) {
        		Tuple tuple2 = child2.next();
        		if(this.predicate.filter(tuple1, tuple2)) {
        			return Tuple.merge(tuple1, tuple2);
        		}
        	}
        	tuple1 = null;
    	}

    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return this.children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	this.children = children;
    }

}
