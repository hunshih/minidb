#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>

/* 
 * A simple scan select using a heap file scan
 */

Status Operators::ScanSelect(const string& result,       // Name of the output relation
                             const int projCnt,          // Number of attributes in the projection
                             const AttrDesc projNames[], // Projection list (as AttrDesc)
                             const AttrDesc* attrDesc,   // Attribute in the selection predicate
                             const Operator op,          // Predicate operator
                             const void* attrValue,      // Pointer to the literal value in the predicate
                             const int reclen)           // Length of a tuple in the result relation
{
    cout << "Algorithm: File Scan" << endl;

    /* Your solution goes here */
    Status returnStatus;

    //CREATE A RESULT HEAPFILE
    HeapFile resultFile(result, returnStatus);
    if(returnStatus != OK) return returnStatus;
    HeapFileScan scanFile(attrDesc->relName, attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, (char*)attrValue, op, returnStatus);
    if(returnStatus != OK) return returnStatus;

    RID resultID;
    Record resultRecord;
    
    //FINALTUPLE IS THE RECORD BEING INSERTED TO THE RESULT RELATION
    Record finalTuple;
    int projLength = 0;
    for(int j = 0; j < projCnt; j++)
    {
        projLength += projNames[j].attrLen;
    }
    finalTuple.data = new char[projLength];
    finalTuple.length = projLength;
    cerr<<"length: "<<projLength<<endl;

    while(returnStatus == OK)
    {
    
        returnStatus = scanFile.scanNext(resultID);
        if(returnStatus != OK) break;

        returnStatus = scanFile.getRecord(resultID, resultRecord);
        if(returnStatus != OK) break;

        //COPY THE DESIRE ATTRS FROM RECORD
        for(int i = 0, offset = 0; i < projCnt; i++)
        {
            memcpy((char*)finalTuple.data + offset, (char*)resultRecord.data + projNames[i].attrOffset, projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }
        returnStatus = resultFile.insertRecord(finalTuple, resultID);
        
    }
    delete [] finalTuple.data;
    returnStatus = scanFile.endScan();
    return OK;
}
