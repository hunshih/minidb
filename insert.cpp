#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>
#include <assert.h>
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

    //LOOK UP SYSTEM CATALOG FOR RELATION AND ATTR
    RelDesc schema;
    relCat->getInfo(relation, schema);
    if(attrCnt != schema.attrCnt) return ATTRTYPEMISMATCH; //NUMBER OF ATTRIBUTE MISMATCH
    int schemaAttrCnt = 0;
    AttrDesc* allAttr = new AttrDesc;
    attrCat->getRelInfo(relation, schemaAttrCnt, allAttr);
    
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
                cerr<<"Attr Name: "<<allAttr->attrName<<", ";
                if(attrList[i].attrType == INTEGER)
                {
                    cerr<<"Value: "<<*((int*)attrList[i].attrValue);
                }
                else if(attrList[i].attrType == DOUBLE)
                {
                    cerr<<"Value: "<<*((double*)attrList[i].attrValue);
                }
                if(attrList[i].attrType == STRING)
                {
                    cerr<<"Value: "<<(char*)attrList[i].attrValue;
                }
                cerr<<", offset: "<<offset<<", attr length:  "<<allAttr->attrLen<<endl;
                memcpy ( ((char*)newTuple.data) + offset, attrList[i].attrValue, allAttr->attrLen);
                if(allAttr->indexed) //IF INDEXED
                {
                    indexMap.insert(pair<int,attrInfo>(offset ,attrList[i]));
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
    Status returnStatus;
    RID tupleID;
    HeapFile insertedTuple(relation,returnStatus);
    assert(returnStatus == OK);
    
    returnStatus = insertedTuple.insertRecord(newTuple, tupleID);
    assert(returnStatus == OK);
    Utilities u;
    u.Print(relation);
    delete[] newTuple.data;


    //UPDATE INDEX (FOR ALL ATTR)
    for(map<int,attrInfo>::iterator it = indexMap.begin() ; it != indexMap.end(); ++it)
    {
        Index updateIndex(relation, it->first, it->second.attrLen, (Datatype)it->second.attrType, 0, returnStatus); 
        updateIndex.insertEntry(it->second.attrValue, tupleID);
    }
    
    return OK;
}
