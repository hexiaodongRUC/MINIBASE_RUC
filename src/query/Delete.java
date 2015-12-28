package query;

import parser.AST_Delete;
import relop.Predicate;
import relop.Schema;
import relop.Selection;
import relop.Iterator;
import relop.FileScan;
import relop.Tuple;
import global.Minibase;
import heap.HeapFile;
/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {
  //table name
  protected String filename;
  
  protected Schema schema;
  
  protected Predicate[][] predicates;
  
  protected FileScan scan;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist or predicates are invalid
   */
  public Delete(AST_Delete tree) throws QueryException {
	  filename = tree.getFileName();
	  schema = QueryCheck.tableExists(filename);
	  
	  predicates = tree.getPredicates();
	  QueryCheck.predicates(schema, predicates);
  } // public Delete(AST_Delete tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	HeapFile hp = new HeapFile(filename);
	scan = new FileScan(schema, hp);
	
	Selection sel = new Selection(scan, predicates[0]);
	
  	for(int pi = 1; pi < predicates.length; pi = pi+1){
		for(int pj = 0; pj < predicates[pi].length; pj = pj+1){
			sel = new Selection(sel, predicates[pi][pj]);
		}
	}
  
	int cnt = 0;
	while(sel.hasNext()){
		hp.deleteRecord(scan.getLastRID());
		cnt++;
	}

	
    // print the output message
	System.out.println(cnt + " rows affected.");
	
  } // public void execute()

} // class Delete implements Plan
