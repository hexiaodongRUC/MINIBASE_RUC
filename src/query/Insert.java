package query;

import parser.AST_Insert;
import relop.Schema;
import relop.Tuple;
import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

  protected String fileName;
  protected Schema schema;
  protected Object[] insertObjects;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exists or values are invalid
   */
  public Insert(AST_Insert tree) throws QueryException {
    fileName = tree.getFileName();
    schema = QueryCheck.tableExists(fileName);
    insertObjects = tree.getValues();
    QueryCheck.insertValues(schema, insertObjects);
  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	HeapFile insertFile = new HeapFile(fileName);
	Tuple tuple = new Tuple(schema);
	tuple.setAllFields(insertObjects);
	RID rid = tuple.insertIntoFile(insertFile);
	IndexDesc[] inds = Minibase.SystemCatalog.getIndexes(fileName);
    for (IndexDesc ind : inds) {
      new HashIndex(ind.indexName).insertEntry(new SearchKey(tuple.getField(ind.columnName)), rid);
    }
    // print the output message
    System.out.println("1 rows affected.");

  } // public void execute()

} // class Insert implements Plan
