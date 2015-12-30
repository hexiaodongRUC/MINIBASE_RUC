package bufmgr;

import global.Minibase;

/**
 * Created by crashh on 9/19/14.
 */
public class ClockReplacer extends Replacer {

    private int numbufs;
    private int currentFrame;

    /**
     * Constructs the replacer, given the buffer manager.
     *
     * @param bufmgr
     */
    protected ClockReplacer(BufMgr bufmgr) {

        super(bufmgr);

        //Reference back to the buffer manager's frame table.
        frametab = bufmgr.getFrameDesc();

        //Need this to make the clock circular.
        numbufs = bufmgr.getNumBuffers()-1;
        currentFrame = numbufs;
    }

    /**
     * Notifies the replacer of a new page.
     *
     * @param fdesc
     */
    @Override
    public void newPage(FrameDesc fdesc) {

        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Notifies the replacer of a free page.
     *
     * @param fdesc
     */
    @Override
    public void freePage(FrameDesc fdesc) {

        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Notifies the replacer of a pined page.
     *
     * @param fdesc
     */
    @Override
    public void pinPage(FrameDesc fdesc) {

        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Notifies the replacer of an unpinned page.
     *
     * @param fdesc
     */
    @Override
    public void unpinPage(FrameDesc fdesc) {

        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Selects the best frame to use for pinning a new page.
     *
     * @return victim frame number, or -1 if none available
     */
    @Override
    public int pickVictim() {

        /* Written using this algorithm:
         * http://courses.cs.washington.edu/courses/csep544/99au/minirel/bufmgr.html
         * */

        //Keeps track of how many frames are pinned.
        int pinCount = 0;

        //Our clock algorithm, keeps going until all pinned or frame found.
        while (true) {

            //Makes the rotation circular.
            if (currentFrame++ >= numbufs){
                currentFrame=0;
                pinCount = 0;
            }

            if(pinCount==numbufs)
                return -1; //No available frame found.

            //Is refBit set? if so clear it and advance pointer..
            if (frametab[currentFrame].state == 1){
                frametab[currentFrame].state = 0;
                continue;
            }
            //Is page pinned? If so advance pointer..
            if (frametab[currentFrame].pincnt>0){
                pinCount++; //Counting if all are pinned to break.
                continue;
            }
            //Is page dirty? If so flush it and continue..
            if (frametab[currentFrame].dirty == true){
                Minibase.BufferManager.flushPage(frametab[currentFrame].pageno);
            }

            return currentFrame; //Use this frame.
        }
    }
}
