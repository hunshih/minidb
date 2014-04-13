#include "catalog.h"
#include "query.h"
#include "index.h"

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
Status Operators::Select(const string & result,      // name of the output relation
	                 const int projCnt,          // number of attributes in the projection
		         const attrInfo projNames[], // the list of projection attributes
		         const attrInfo *attr,       // attribute used inthe selection predicate 
		         const Operator op,         // predicate operation
		         const void *attrValue)     // literal value in the predicate
{
    //FIRST CHECK IF INDEX EXISTS ON SELECTION PREDICATE
    Status returnStatus;

    //LOOK UP SYSTEM CATALOG FOR RELATION AND ATTR
    AttrDesc schema;
    
    if(op != NOTSET)
    {
        returnStatus = attrCat->getInfo(attr->relName, attr->attrName, schema);
        if(returnStatus != OK) return returnStatus;
    }

    //CREATE A LIST OF PROJECTION IN ATTRDESC
    AttrDesc projList[projCnt];
    int resultLen = 0;
    for(int i = 0; i < projCnt; i++)
    {
        AttrDesc projAttr;
        returnStatus = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projAttr);
        if(returnStatus != OK) return returnStatus;
        projList[i] = projAttr;
        resultLen += projAttr.attrLen;
    }

    //IF INDEX EXISTS ON THIS ATTR
    if(schema.indexed && (op == EQ || op == NE))
    {   
        returnStatus = IndexSelect(result, projCnt, projList, &schema, op, attrValue, resultLen);
        if(returnStatus != OK) return returnStatus;
    }
    else
    {
        returnStatus = ScanSelect(result, projCnt, projList, &schema, op, attrValue, resultLen);
        if(returnStatus != OK) return returnStatus;
    }
    
    return OK;
}

