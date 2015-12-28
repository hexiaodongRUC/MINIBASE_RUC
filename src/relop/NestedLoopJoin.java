package relop;

/**
 * nested loops join in tuple-level
 */
public class NestedLoopJoin extends Iterator {
	private Iterator outerIt;
	private Iterator innerIt;
	private Predicate[] preds;
	private Tuple leftTuple;
	private Tuple nextTuple;
	
	public NestedLoopJoin(Iterator outer, Iterator innner, Predicate... preds) {
		this.setSchema(Schema.join(outer.getSchema(), innner.getSchema()));
		this.outerIt = outer;
		this.innerIt = innner;
		this.preds = preds;
	}

	public void explain(int depth) {
		this.indent(depth);
		System.out.println(String.format("NestedLoop Join for %s and %s", this.outerIt.toString(), this.innerIt.toString()));
	}
	
	public void restart() {
		this.nextTuple = null;
		this.outerIt.restart();
	}

	public boolean isOpen() {
		return this.outerIt.isOpen();
	}

	public void close() {
		this.outerIt.close();
		this.innerIt.close();
	}
	
	public boolean hasNext() {
		while(true) {
			while(leftTuple != null && innerIt.hasNext()) {
				Tuple rt = innerIt.getNext();
				Tuple nt = Tuple.join(leftTuple, rt, this.getSchema());
				if(Predicate.evaluate(nt, this.preds)) {
					this.nextTuple = nt;
					return true;
				}
			}
			if(!outerIt.hasNext()) break;
			leftTuple = outerIt.getNext();
			innerIt.restart();
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