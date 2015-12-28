package relop;

import java.util.Comparator;

/**
 * tuple comparator 
 * used for in-memory sort
 */
public class TupleComparator implements Comparator<Tuple> {
	private int compBy;
	
	public TupleComparator(Integer compBy) {
		this.compBy = compBy;
	}
	
	@Override
	public int compare(Tuple t1, Tuple t2) {
		return Tuple.Compare(t1, t2, compBy);
	}
}