package relop;

import global.AttrType;
import global.Convert;
import global.RID;
import heap.HeapFile;

/**
 * Each tuple in a relation is a collection of bytes that must fit within a
 * single page. The Tuple class provides a logical view of fields, allowing get
 * and set operations while automatically handling offsets and type conversions.
 */
public class Tuple {

	/** Page buffer containing this tuple. */
	protected byte[] data;

	/** Schema information for the fields. */
	protected Schema schema;

	// --------------------------------------------------------------------------

	/**
	 * Creates a new empty tuple, given its schema.
	 * 
	 * @param schema logical information for the fields
	 */
	public Tuple(Schema schema) {
		this.schema = schema;
		data = new byte[schema.getLength()];
	}

	/**
	 * Creates a new tuple, given its schema and values.
	 * 
	 * @param schema logical information for the fields
	 * @param values individual data values of the fields
	 */
	public Tuple(Schema schema, Object... values) {
		this.schema = schema;
		data = new byte[schema.getLength()];
		setAllFields(values);
	}

	/**
	 * Creates a new tuple, given existing values to wrap.
	 * 
	 * @param schema logical information for the fields
	 * @param data record buffer containing the tuple
	 */
	public Tuple(Schema schema, byte[] data) {
		this.schema = schema;
	    this.data = data;
	}
	  	
  	/**
  	 * Builds and returns a new tuple resulting from projection of raw tuple
  	 * 
  	 * @param t raw tuple
  	 * @param schema new schema after projection
  	 * @param fields fields to project
  	 * @return
  	 */
  	public static Tuple project(Tuple t, Schema schema, Integer... fields) {
  		Tuple tuple = new Tuple(schema);
  		for(int i=0; i<fields.length; i++) {
  			tuple.setField(i, t.getField(fields[i]));
  		}
  		return tuple;
  	}
  	
  	/**
  	 * Builds and returns a new tuple resulting from joining two tuples.
  	 * 
  	 * @param t1 the left tuple
  	 * @param t2 the right tuple
  	 * @param schema of the result
  	 */
  	public static Tuple join(Tuple t1, Tuple t2, Schema schema) {

  		// construct the new tuple
  		int t1cnt = t1.schema.getCount();
  		int t2cnt = t2.schema.getCount();
  		Tuple tuple = new Tuple(schema);

  		// copy all fields from t1 and t2
  		int fldno = 0;
  		for (int i = 0; i < t1cnt; i++) {
  			tuple.setField(fldno++, t1.getField(i));
  		}
  		for (int i = 0; i < t2cnt; i++) {
  			tuple.setField(fldno++, t2.getField(i));
  		}

  		// return the resulting tuple
  		return tuple;

  	}
  	
  	/**
  	 * Gets the underlying data buffer.
  	 */
  	public byte[] getData() {
  		return data;
  	}

  	/**
  	 * Inserts the tuple into the given heap file.
  	 */
  	public RID insertIntoFile(HeapFile file) {
  		return file.insertRecord(data);
  	}

  	/**
  	 * Gets a field's value generically.
  	 */
  	public Object getField(int fldno) {
  		
  		// refer to the schema for the appropriate conversion
  		switch (schema.fieldType(fldno)) {

  			case AttrType.INTEGER:
  				return getIntFld(fldno);

  			case AttrType.FLOAT:
  				return getFloatFld(fldno);

  			case AttrType.STRING:
  				return getStringFld(fldno);

  			default:
  				throw new IllegalStateException("invalid attribute type");
  		}

  	}

  	/**
  	 * Gets a field's value generically, by column name.
  	 */
  	public Object getField(String fldName) {
  		return getField(schema.fieldNumber(fldName));
  	}

  	/**
  	 * Sets a field's value generically.
  	 */
  	public void setField(int fldno, Object val) {

  		// refer to the schema for the appropriate conversion
  		switch (schema.fieldType(fldno)) {

  		case AttrType.INTEGER:
  			setIntFld(fldno, (Integer) val);
  			break;

  		case AttrType.FLOAT:
  			setFloatFld(fldno, (Float) val);
  			break;

  		case AttrType.STRING:
  			setStringFld(fldno, (String) val);
  			break;

  		default:
  			throw new IllegalStateException("invalid attribute type");
  		}

  	}

  	/**
  	 * Sets a field's value generically, by column name.
  	 */
  	public void setField(String fldName, Object val) {
  		setField(schema.fieldNumber(fldName), val);
  	}

  	/**
  	 * Returns generic values for all fields.
  	 */
  	public Object[] getAllFields() {

  		Object[] values = new Object[schema.getCount()];
  		for (int i = 0; i < values.length; i++) {
  			values[i] = getField(i);
  		}
  		return values;

  	}
  	
  	/**
  	 * Sets all fields, given generic values.
  	 */
    public void setAllFields(Object... values) {
  		
  		for (int i = 0; i < values.length; i++) {
    	setField(i, values[i]);
  		}

  	}

  	/**
  	 * Gets an integer field.
  	 */
  	public int getIntFld(int fldno) {
  		return Convert.getIntValue(schema.fieldOffset(fldno), data);
  	}

  	/**
  	 * Sets an integer field.
  	 */
  	public void setIntFld(int fldno, int val) {
  		Convert.setIntValue(val, schema.fieldOffset(fldno), data);
  	}

  	/**
  	 * Gets a float field.
  	 */
  	public float getFloatFld(int fldno) {
  		return Convert.getFloatValue(schema.fieldOffset(fldno), data);
  	}

  	/**
  	 * Sets a float field.
  	 */
  	public void setFloatFld(int fldno, float val) {
  		Convert.setFloatValue(val, schema.fieldOffset(fldno), data);
  	}

  	/**
  	 * Gets a string field.
  	 */
  	public String getStringFld(int fldno) {
	  return Convert.getStringValue(schema.fieldOffset(fldno), data, schema
        .fieldLength(fldno));
  	}
  	
  	/**
  	 * Sets a string field (automatically truncates, if necessary).
  	 */
  	public void setStringFld(int fldno, String val) {

  		// truncate the string if too long
  		int len = schema.fieldLength(fldno);
  		if (val.length() > len) {
  			val = val.substring(0, len - 1);
  		}

  		// set the string and zero out the rest
  		int off = schema.fieldOffset(fldno);
  		Convert.setStringValue(val, off, data);
  		for (int i = val.length(); i < len; i++) {
  			data[off + i] = 0;
  		}

  	}
  	

  	/**
  	 * Prints the tuple in a human-readable format.
  	 */
  	public void print() {

  		// print everything as a string, with padding
  		String str = null;
  		int cnt = schema.getCount();
  		for (int i = 0; i < cnt; i++) {
  			switch (schema.fieldType(i)) {

  				case AttrType.INTEGER:
  					str = Integer.toString(getIntFld(i));
  					System.out.print(str);
  					padOutput(0, str.length());
  					break;

  				case AttrType.FLOAT:
  					str = Float.toString(getFloatFld(i));
  					System.out.print(str);
  					padOutput(0, str.length());
  					break;

  				case AttrType.STRING:
  					str = getStringFld(i);
  					System.out.print(str);
  					padOutput(schema.fieldLength(i), str.length());
  					break;

  			}
  		}
  		System.out.println();

  	}

  	/**
  	 * Pads the current output to make columns line up.
  	 * 
  	 * @param fieldLen minimum length of the current field
  	 * @param outputLen length of the current output so far
  	 */
  	protected void padOutput(int fieldLen, int outputLen) {

  		fieldLen = Math.max(Schema.MIN_WIDTH, fieldLen);
  		for (int j = 0; j < fieldLen - outputLen; j++) {
  			System.out.print(' ');
  		}

  	}
  	
  	/**
  	 * get a tuple with minimum value in specific field
  	 * @param schema the tuple schema
  	 * @param field the field
  	 * @return minimum tuple
  	 */
  	public static Tuple getMin(Schema schema, Integer field) {
  		Tuple t = new Tuple(schema);
  		switch(t.schema.fieldType(field)) {
  			case AttrType.INTEGER:
  				t.setIntFld(field, Integer.MIN_VALUE);
  				break;
			case AttrType.FLOAT:
				t.setFloatFld(field, Float.MIN_VALUE);
				break;
			case AttrType.STRING:
				t.setStringFld(field, "");
				break;
			default:
		        throw new IllegalStateException("invalid attribute type");
  		}
  		return t;
  	}
  	
  	/**
  	 * get a tuple with maximum value in specific field
  	 * @param schema the tuple schema
  	 * @param field the field
  	 * @return maximum tuple
  	 */
  	public static Tuple getMax(Schema schema, Integer field) {
  		Tuple t = new Tuple(schema);
  		switch(t.schema.fieldType(field)) {
  			case AttrType.INTEGER:
  				t.setIntFld(field, Integer.MAX_VALUE);
  				break;
			case AttrType.FLOAT:
				t.setFloatFld(field, Float.MAX_VALUE);
				break;
			case AttrType.STRING:
				throw new IllegalStateException("string attribute has no max value");
			default:
		        throw new IllegalStateException("invalid attribute type");
  		}
  		return t;
  	}
  	
  	/**
  	 * compare two tuple in corresponding field
  	 * @param t1 tuple 1
  	 * @param t2 tuple 2
  	 * @param compBy1 tuple 1 field
  	 * @param compBy2 tuple 2 field
  	 * @return comparison result
  	 */
  	public static int Compare(Tuple t1, Tuple t2, Integer compBy1, Integer compBy2) {
  		// TODO t1 t2 have different AttrType on compBy column
  		switch(t1.schema.fieldType(compBy1)) {
  			case AttrType.INTEGER:
	            return t1.getIntFld(compBy1) - t2.getIntFld(compBy2);
  			case AttrType.FLOAT:
  				return t1.getFloatFld(compBy1) > t2.getFloatFld(compBy2) ? 1 : 
  					t1.getFloatFld(compBy1) == t2.getFloatFld(compBy2) ? 0 : -1;
  			case AttrType.STRING:
	            return t1.getStringFld(compBy1).compareTo(t2.getStringFld(compBy2));
  			 default:
  		        throw new IllegalStateException("invalid attribute type");
  		}
	}
  	
  	/**
  	 * compare two tuple with same schema
  	 * @param t1 tuple 1
  	 * @param t2 tuple 2
  	 * @param compBy tuple field
  	 * @return comparison result
  	 */
  	public static int Compare(Tuple t1, Tuple t2, Integer compBy) {
  		return Compare(t1, t2, compBy, compBy);
  	}
}
