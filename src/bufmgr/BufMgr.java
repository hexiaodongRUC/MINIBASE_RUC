package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;
import java.util.HashMap;

/**
 * <h3>Minibase Buffer Manager</h3> The buffer manager reads disk pages into a
 * main memory page as needed. The collection of main memory pages (called
 * frames) used by the buffer manager for this purpose is called the buffer
 * pool. This is just an array of Page objects. The buffer manager is used by
 * access methods, heap files, and relational operators to read, write,
 * allocate, and de-allocate pages.
 */

public class BufMgr implements GlobalConst {

    private final Replacer replacer;

    //Creates a memory array to simulate the available memory for the DBMS.
    private final Page[] bufferPool;
    //Holds all our FrameDescriptors, with information about the frames.
    private final FrameDesc[] frameDescriptors;
    //Maps relation between PageId and bufferPool location.
    private final HashMap<Integer, Integer> pageMapping;

    //Used for debugging purposes.
    private final Boolean debug = true;

	/**
	 * Constructs a buffer manager with the given settings. Also initiates the
     * local variables, and fills the arrays.
	 *
	 * @param numbufs
	 *            number of bufferPages in the buffer pool
	 */
	public BufMgr(int numbufs) {

        bufferPool = new Page[numbufs];
        frameDescriptors = new FrameDesc[numbufs];
        pageMapping = new HashMap<>(numbufs);
        replacer = new ClockReplacer(this);

        //Fill FrameDesc array, so it is ready for use:
        for (int i=0; i<frameDescriptors.length; i++){
            frameDescriptors[i] = new FrameDesc(i);
        }

        //Fill bufferPool array, so it is ready for use:
        for (int i=0; i<bufferPool.length; i++){
            bufferPool[i] = new Page();
        }
	}

	/**
	 * Allocates a set of new pages, and pins the first one in an appropriate
	 * frame in the buffer pool.
	 * 
	 * @param firstPg
	 *            holds the contents of the first page 
	 * @param run_size
	 *            number of new pages to allocate
	 * @return page id of the first new page
	 * @throws IllegalArgumentException
	 *             if PIN_MEMCPY and the page is pinned
	 * @throws IllegalStateException
	 *             if all pages are pinned (i.e. pool exceeded)
	 */
	public PageId newPage(Page firstPg, int run_size) {
        if(debug)
            System.out.println("Method 'newPage' called.");

        //Error handling:
        if (firstPg == null) {
            throw new UnsupportedOperationException("Invalid page specified in PageId");
        }
        if (bufferFull()) {
            throw new IllegalStateException("All pages are pinned (i.e. pool exceeded)");
        }

        //Allocate pages (often just 1), returns PageId.
        PageId firstPgId = Minibase.DiskManager.allocate_page(run_size);

        //Pins given page, and new pageId, to bufferpool.
        try{
            pinPage(firstPgId, firstPg, PIN_MEMCPY);
        } catch (Exception e){
            Minibase.DiskManager.deallocate_page(firstPgId, run_size);
            return null;
        }

        return firstPgId;
	}

    /**
     * Checks if the buffer is full.
     *
     * @return boolean false/true depending on result
     */
    private boolean bufferFull(){
        for (FrameDesc frameDescriptor : frameDescriptors) {
            if (frameDescriptor.pincnt == 0)
                return false;
        }
        return true;
    }

	/**
	 * De-allocates a single page from disk, freeing it from the pool if needed.
	 * 
	 * @param pageNo
	 *            identifies the page to remove
	 * @throws IllegalArgumentException
	 *            if the page is pinned
	 */
	public void freePage(PageId pageNo) throws IllegalArgumentException {
        if(debug)
            System.out.println("Method 'freePage' called with PID:" + pageNo.getPID());

        //Error handling:
        if (!pageMapping.containsKey(pageNo.getPID())) {
            return; //nothing to do.
        }
        if (frameDescriptors[pageMapping.get(pageNo.getPID())].pincnt!=0){
            throw new IllegalArgumentException("Cannot free page, page is pinned");
        }

        //Flush page if dirty.
        if (frameDescriptors[pageMapping.get(pageNo.getPID())].dirty) {
            flushPage(pageNo);
        }
        //Remove from HashMap.
        pageMapping.remove(pageNo.getPID());

        Minibase.DiskManager.deallocate_page(pageNo);
	}

	/**
	 * Pins a disk page into the buffer pool. If the page is already pinned,
	 * this simply increments the pin count. Otherwise, this selects another
	 * page in the pool to replace, flushing it to disk if dirty.
	 * 
	 * (If one needs to copy the page from the memory instead of reading from the disk, one should set skipRead to PIN_MEMCPY. 
	 * In this case, the page shouldn't be in the buffer pool. Throw an IllegalArgumentException if so. )
	 * 
	 * 
	 * @param pageNo
	 *            identifies the page to pin
	 * @param page
	 *            holds contents of the page, either an input or output param
	 * @param skipRead
	 *            PIN_MEMCPY (replace in pool); PIN_DISKIO (read the page in)
	 * @throws IllegalArgumentException
	 *             if PIN_MEMCPY and the page is pinned
	 * @throws IllegalStateException
	 *             if all pages are pinned (i.e. pool exceeded)
	 */
	public void pinPage(PageId pageNo, Page page, boolean skipRead) {
        if(debug)
            System.out.println("Method 'pinPage' called with PID:" + pageNo.getPID());

        //Error handling:
        if (page == null){
            throw new UnsupportedOperationException("Invalid page specified in pinPage.");
        }

        // Check if page is in pool already using HashMap:
        if (pageMapping.containsKey(pageNo.getPID())){
            // Page found, increment counter, set page, and stop.
            int framePlacement = pageMapping.get(pageNo.getPID());
            frameDescriptors[framePlacement].pincnt++;
            frameDescriptors[framePlacement].state = 1;
            page.setPage(bufferPool[framePlacement]);
            return;
        }

        // Page not in pool, assign frame and update HashMap, read page from disk.
        int framePlacement = replacer.pickVictim();

        if(framePlacement == -1){
            throw new IllegalStateException("All pages are pinned (i.e. pool exceeded)");
        }

        //Update HashMap.
        pageMapping.remove(frameDescriptors[framePlacement].pageno.getPID(), framePlacement);
        pageMapping.put(pageNo.getPID(),framePlacement);

        //Update frameDesc info to match new page.
        frameDescriptors[framePlacement] = new FrameDesc(framePlacement);
        frameDescriptors[framePlacement].pageno.copyPageId(pageNo);
        frameDescriptors[framePlacement].pincnt = 1;
        frameDescriptors[framePlacement].state = 1;

        //Either copy page in, or simply read it.
        if (skipRead) {
            bufferPool[framePlacement].copyPage(page);
            page.setPage(bufferPool[framePlacement]);
        } else {
            page.setPage(bufferPool[framePlacement]);
            Minibase.DiskManager.read_page(pageNo, page);
        }
	}

	/**
	 * Unpins a disk page from the buffer pool, decreasing its pin count.
	 *
	 * @param pageNo
	 *            identifies the page to unpin
	 * @param dirty
	 *            UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
	 * @throws IllegalArgumentException
	 *             if the page is not present or not pinned
	 */
	public void unpinPage(PageId pageNo, boolean dirty) throws IllegalArgumentException {
        if(debug)
            System.out.println("Method 'unpinPage' called with PID:" + pageNo.getPID());

        //Error handling:
        if (pageNo == null) {
            throw new IllegalArgumentException("Invalid pageNo specified in unpinPage.");
        }

        //Get frame location, if not found throw exception:
        int frameDesc;
        if (pageMapping.containsKey(pageNo.getPID()))
            frameDesc = pageMapping.get(pageNo.getPID());
        else
            throw new IllegalArgumentException("Page is not present.");

        if (frameDescriptors[frameDesc].pincnt == 0)
            throw new IllegalArgumentException("Page is not pinned.");

        //Decrement pin_count:
        frameDescriptors[frameDesc].pincnt--;
        //Set dirty status:
        frameDescriptors[frameDesc].dirty=dirty;
	}

	/**
	 * Immediately writes a page in the buffer pool to disk, if dirty.
     * If the page is not found, nothing happens.
	 */
	public void flushPage(PageId pageNo) {
        if(debug)
            System.out.println("Method 'flushPage' called with PID:" + pageNo.getPID());

        //Error handling:
        if (pageNo == null) {
            throw new UnsupportedOperationException("Invalid pageNo specified in flushPage.");
        }

        //See if the PageId is in the bufferpool:
        if (pageMapping.containsKey(pageNo.getPID())){
            //It is found in the HashMap, assume its in the pool:
            int frameLoc = pageMapping.get(pageNo.getPID());
            Minibase.DiskManager.write_page(pageNo, bufferPool[frameLoc]);
            frameDescriptors[frameLoc].dirty=false;
        }
	}

	/**
	 * Immediately writes all dirty pages in the buffer pool to disk.
	 */
	public void flushAllPages() {
        if(debug)
            System.out.println("Method 'flushAllPages' called.");

        //Traverse HashMap (everything in memory is in the HashMap) looking for dirty flag:
        for(int key: pageMapping.keySet()) {
            if (frameDescriptors[key].dirty) {
                PageId pageId = frameDescriptors[key].pageno;
                flushPage(pageId);
            }
        }
	}

	/**
	 * Gets the total number of buffer frames.
	 */
	public int getNumBuffers() {
        if(debug)
            System.out.println("Method 'getNumBuffers' called.");

        return bufferPool.length;
	}

	/**
	 * Gets the total number of unpinned buffer frames.
	 */
	public int getNumUnpinned() {
        if(debug)
            System.out.println("Method 'getNumUnpinned' called.");

        //Iterates trough the frameDescriptors array, counting each pincnt == 0.
        int pinCount = 0;
        for (FrameDesc frameDescriptor : frameDescriptors) {
            if (frameDescriptor.pincnt == 0) {
                pinCount++;
            }
        }

        return pinCount;
	}

    /**
    * Gets the frameDesc array, which holds information about all the frames/pages.
    */
    public FrameDesc[] getFrameDesc(){

        return frameDescriptors;
    }

} // public class BufMgr implements GlobalConst
