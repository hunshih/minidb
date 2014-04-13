#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>
#include "utility.h"
/*
 * Inserts a record into the specified relation
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{
    /* Your solution goes here */

    //DECLARE A MAP THAT KEEP TRACKS OF OFFSET AND ATTRINFO
    map<int,attrInfo> indexMap;

    //CREATE RECORD OBJECT
    Record newTuple;

    //USE TO CHECK THE RESULT OF EACH OPERATION
    Status returnStatus;

    //LOOK UP SYSTEM CATALOG FOR RELATION AND ATTR
    RelDesc schema;
    returnStatus = relCat->getInfo(relation, schema);
    if(returnStatus != OK) return returnStatus;

    //ERROR CHECK
    if(attrCnt != schema.attrCnt) return ATTRTYPEMISMATCH; //NUMBER OF ATTRIBUTE MISMATCH
    for(int i = 0; i < attrCnt; i++)
    {
        if(attrList[i].attrValue == NULL) return ATTRTYPEMISMATCH; //CAN'T HAVE NULL VALUE FOR ATTR
    }

    int schemaAttrCnt = 0;
    AttrDesc* allAttr = new AttrDesc;
    returnStatus = attrCat->getRelInfo(relation, schemaAttrCnt, allAttr);
    if(returnStatus != OK) return returnStatus;
    
    //ALLOCATE SPACE FOR DATA
    AttrDesc* getSize = allAttr;
    int realDataSize = 0;
    for(int i = 0; i < schemaAttrCnt; i++, getSize++)
    {
        realDataSize += getSize->attrLen;
    }
    
    newTuple.data = new char[realDataSize];

    //USE MEMCPY TO COPY DATA
    int offset = 0;
    for(int aryCount = 0; aryCount < schemaAttrCnt; aryCount++, allAttr++)
    {
        for(int i = 0; i < attrCnt; i++)
        {
            if(strcmp(allAttr->attrName,attrList[i].attrName) == 0)
            {
                memcpy ( ((char*)newTuple.data) + offset, attrList[i].attrValue, allAttr->attrLen);
                if(allAttr->indexed) //IF INDEXED
                {
                    attrInfo correctLen = attrList[i];
                    correctLen.attrLen = allAttr->attrLen;
                    indexMap.insert(pair<int,attrInfo>(offset ,correctLen));
                }
                offset += allAttr->attrLen;
            }
            if(attrList[i].attrValue == NULL) //NO VALUE SPECIFIED FOR ATTR
            {
                return ATTRNOTFOUND;
            }
        }
    }
    newTuple.length = offset;
    
    //CREATE HEAPFILE AND INSERT DATA
    RID tupleID;
    HeapFile insertedTuple(relation,returnStatus);
    if(returnStatus != OK) return returnStatus;
    
    returnStatus = insertedTuple.insertRecord(newTuple, tupleID);
    if(returnStatus != OK) return returnStatus;
    delete[] newTuple.data;

    //UPDATE INDEX (FOR ALL ATTR)
    for(map<int,attrInfo>::iterator it = indexMap.begin() ; it != indexMap.end(); ++it)
    {
        Index updateIndex(relation, it->first, it->second.attrLen, (Datatype)it->second.attrType, 0, returnStatus); 
        if(returnStatus != OK) return returnStatus;
        returnStatus = updateIndex.insertEntry(it->second.attrValue, tupleID);
        if(returnStatus != OK) return returnStatus;
    }
    //Utilities u;
    //u.Print(relation);
    return OK;
}
