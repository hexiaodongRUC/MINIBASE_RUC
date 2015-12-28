package relop;

/**
 * selection operator specifies which tuples to retain under a condition
 */
public class Selection extends Iterator {
	private Iterator childIt;
	private Predicate[] preds;
	private Tuple nextTuple;
	
	public Selection(Iterator iter, Predicate... preds) {
		this.setSchema(iter.getSchema());
		this.childIt = iter;
		this.preds = preds;		
		this.nextTuple = null;
	}

  	public void explain(int depth) {
  		this.indent(depth);
  		System.out.println(String.format("Selection for Iterator %s", childIt.toString()));
  	}

  	public void restart() {
  		nextTuple = null;
  		this.childIt.restart();
  	}

  	public boolean isOpen() {
  		return childIt.isOpen();
  	}

  	public void close() {
  		if(this.isOpen()) {
  			nextTuple = null;
  			childIt.close();
  		}
  	}

  	public boolean hasNext() {
  		if(isOpen()) {
  			while(childIt.hasNext()) {
  				Tuple temp = childIt.getNext();
  				if(Predicate.evaluate(temp, this.preds)) {
  					nextTuple = temp;
  					return true;
  				}
  			}
  			nextTuple = null;
  			return false;
  		}
  		return false;
  	}

  	public Tuple getNext() {
  		if(this.isOpen()) {
  			if(nextTuple != null) return nextTuple;
			if(nextTuple == null && !this.hasNext()) throw new IllegalStateException(String.format("Iterator %s has no next tuple", this.toString()));
			return getNext();
		}
  		throw new IllegalStateException(String.format("Iterator %s not open", this.toString()));
  	}
}