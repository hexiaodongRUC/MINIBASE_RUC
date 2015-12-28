package relop;

/**
 * projection operator extracts columns from a relation
 */
public class Projection extends Iterator {
	private Iterator childIt;
	private Integer[] fields;
	
	public Projection(Iterator iter, Integer... fields) {
		this.setSchema(new Schema(iter.getSchema(), fields));
		this.childIt = iter;
		this.fields = fields;
	}

	public void explain(int depth) {
		this.indent(depth);
		System.out.println(String.format("Projection for Iterator %s", this.childIt.toString()));
	}

	public void restart() {
		this.childIt.restart();
	}

  
	public boolean isOpen() {
		return this.childIt.isOpen();
	}

	public void close() {
		this.childIt.close();
	}

	public boolean hasNext() {
		if(this.isOpen()) return this.childIt.hasNext();
		return  false;
	}

	public Tuple getNext() {
		if(this.isOpen()) {
			Tuple get = this.childIt.getNext();
			if(get == null) throw new IllegalStateException(String.format("Iterator %s has no next tuple", this.toString()));
			return Tuple.project(get, this.getSchema(), this.fields);
		}
		throw new IllegalStateException(String.format("Iterator %s not open", this.toString()));
	}
}
