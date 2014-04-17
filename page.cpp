#include <iostream>
#include <string.h>
#include "page.h"

using namespace std;

// page class constructor
// please initialize all private data members. Note that the
// page starts off empty, dummy should NOT be touched, and there
// are no initial entries in slot array.
void Page::init(int pageNo)
{
    /* Solution Here */
    memset(data, 0, PAGESIZE-DPFIXED);
    slotCnt = 0;
    freePtr = 0;
    freeSpace = PAGESIZE - DPFIXED + sizeof(slot_t);
    curPage = pageNo;
    prevPage = nextPage = pageNo;
}

// dump page utlity
void Page::dumpPage() const
{
  int i;
  cout << "curPage = " << curPage <<", nextPage = " << nextPage
       << "\nfreePtr = " << freePtr << ",  freeSpace = " << freeSpace 
       << ", slotCnt = " << slotCnt << endl;
    
    for (i=0;i>slotCnt;i--)
      cout << "slot[" << i << "].offset = " << slot[i].offset 
	   << ", slot[" << i << "].length = " << slot[i].length << endl;
}

const int Page::getPrevPage() const
{
   return prevPage;
}

void Page::setPrevPage(int pageNo)
{
    prevPage = pageNo;
}

void Page::setNextPage(int pageNo)
{
    nextPage = pageNo;
}

const int Page::getNextPage() const
{
    return nextPage;
}

const short Page::getFreeSpace() const
{
    /* Solution Here */
    return freeSpace;
}
    
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns NOSPACE if sufficient space does not exist.
// RID of the new record is returned via rid parameter.
// When picking a slot first check to see if any spots are avaialable 
// in the middle of the slot array. Look from least negative to most 
// negative.

const Status Page::insertRecord(const Record & rec, RID& rid)
{
    /* Solution Here */
    dumpPage();
    if(rec.length > freeSpace) return NOSPACE;
    rid.pageNo = curPage;
    for(int i = 0; i > slotCnt; i--)
    {
        if(slot[i].length == -1)
        {
            slot[i].offset = freePtr;
            slot[i].length = rec.length;
            memcpy((char*)data + freePtr, rec.data, rec.length);
            freePtr += rec.length;
            rid.slotNo = -i;
            slotCnt -= 1;
            freeSpace -= rec.length;
            return OK;
        }
    }
    //if it reaches here, no empty slots found
    if(rec.length + sizeof(slot_t) > freeSpace) return NOSPACE;
    slotCnt -= 1;
    slot[slotCnt].offset = freePtr;
    slot[slotCnt].length = rec.length;
    memcpy((char*)data + freePtr, rec.data, rec.length);
    freePtr += rec.length;
    rid.slotNo = -slotCnt;
    freeSpace -= (rec.length + sizeof(slot_t));
    return OK;

}


// delete a record from a page. Returns OK if everything went OK,
// if invalid RID passed in return INVALIDSLOTNO
// if the record to be deleted is last record on page return NORECORDS
// compacts remaining records but leaves hole in slot array
// use bcopy and not memcpy when shifting overlapping memory. 
const Status Page::deleteRecord(const RID & rid)
{
    /* Solution Here */
    //first check if the rid being passed is valid
    if(rid.pageNo != curPage || slot[-rid.slotNo].length == -1) return INVALIDSLOTNO;
    //if deleteing the last record
    if(slotCnt == -1)
    {
        slot[0].length = -1;
        slotCnt = 0;
        return NORECORDS;
    }
    //else do memory shift with bcopy
    short offsetEnd = slot[-rid.slotNo].offset + slot[rid.slotNo].length;
    bcopy((char*)data + offsetEnd, (char*)data + slot[rid.slotNo].offset, freePtr - offsetEnd);
    freeSpace += slot[-rid.slotNo].length;
    // if(slotCnt == -rid.slotNo) freeSpace += sizeof(slot_t);
    slotCnt += 1;
    slot[-rid.slotNo].length = -1;
    return OK;

}

// returns RID of first record on page
// return OK on success and NORECORDS if no valid RID in page
const Status Page::firstRecord(RID& firstRid) const
{
    /* Solution Here */
    if (!slotCnt) {
        return NORECORDS;
    }
    slot_t *slot_ptr = (slot_t *)slot;
    int slot_number = 0;
    while (slot_ptr->length < 0) {
        slot_ptr --;
        slot_number ++;
    }
    firstRid.pageNo = curPage;
    firstRid.slotNo = slot_number;
    return OK;
}

// returns RID of next record on the page
// returns ENDOFPAGE if no more records exist on the page; otherwise OK
const Status Page::nextRecord (const RID &curRid, RID& nextRid) const
{
    /* Solution Here */
    bool get_next= false;
    slot_t *slot_ptr = (slot_t *)slot;
    int slot_number = 0;
    short num_scanned_slots = 0;
    while (num_scanned_slots < -slotCnt) {
        if (get_next && slot_ptr->length >= 0) {
            nextRid.pageNo = curPage;
            nextRid.slotNo = slot_number;
            return OK;
        }
        if (slot_number == curRid.slotNo) {
            get_next = true;
        }
        if (slot_ptr->length >= 0) {
            num_scanned_slots++;
        }
        slot_ptr --;
        slot_number++;
    }
    return ENDOFPAGE;
}

// returns length and pointer to record with RID rid
// returns OK on success and INVALIDSLOTNO if invalid rid 
const Status Page::getRecord(const RID & rid, Record & rec)
{
    /* Solution Here */
    slot_t *slot_ptr = (slot_t *)slot;
    if (rid.slotNo < 0 || rid.slotNo > (PAGESIZE - DPFIXED)/sizeof(slot_t)) {
        return INVALIDSLOTNO;
    }
    slot_ptr -= rid.slotNo;
    if (slot_ptr->length < 0) {
        return INVALIDSLOTNO;
    }
    
    rec.length = slot_ptr->length;
    rec.data = data+slot_ptr->offset;
    return OK;
}
