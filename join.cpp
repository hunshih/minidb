#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cmath>
#include <cstring>

#define MAX(a,b) ((a) > (b) ? (a) : (b))
#define DOUBLEERROR 1e-07

/*
 * Joins two relations
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Operators::Join(const string& result,           // Name of the output relation 
                       const int projCnt,              // Number of attributes in the projection
    	               const attrInfo projNames[],     // List of projection attributes
    	               const attrInfo* attr1,          // Left attr in the join predicate
    	               const Operator op,              // Predicate operator
    	               const attrInfo* attr2)          // Right attr in the join predicate
{
    /* Your solution goes here */
    Status prep_status;
    int rec_len = 0;
    AttrDesc output_attrs[projCnt];
    for (int i = 0; i < projCnt; i++) {
        prep_status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, output_attrs[i]);
        if (prep_status != OK) {
            return prep_status;
        }
        rec_len += output_attrs[i].attrLen;
    }
    AttrDesc attrdesc1, attrdesc2;
    prep_status = attrCat->getInfo(attr1->relName, attr1->attrName, attrdesc1);
    if (prep_status != OK) {
        return prep_status;
    }
    prep_status = attrCat->getInfo(attr2->relName, attr2->attrName, attrdesc2);
    if (prep_status != OK) {
        return prep_status;
    }

    Status status;
    if (op != EQ) { // non-equi join
        status = SNL(result, projCnt, output_attrs, attrdesc1, op, attrdesc2, rec_len);
    }
    else {
        if (attrdesc1.indexed) {
            status = INL(result, projCnt, output_attrs, attrdesc2, op, attrdesc1, rec_len);
        }
        else if (attrdesc2.indexed) {
            status = INL(result, projCnt, output_attrs, attrdesc1, op, attrdesc2, rec_len);
        }
        else {
            status = SMJ(result, projCnt, output_attrs, attrdesc1, op, attrdesc2, rec_len);
        }
        
    }
    if (status != OK) {
        return status;
    }

	return OK;
}

// Function to compare two record based on the predicate. Returns 0 if the two attributes 
// are equal, a negative number if the left (attrDesc1) attribute is less that the right 
// attribute, otherwise this function returns a positive number.
int Operators::matchRec(const Record& outerRec,     // Left record
                        const Record& innerRec,     // Right record
                        const AttrDesc& attrDesc1,  // Left attribute in the predicate
                        const AttrDesc& attrDesc2)  // Right attribute in the predicate
{
    int tmpInt1, tmpInt2;
    double tmpFloat1, tmpFloat2, floatDiff;

    // Compare the attribute values using memcpy to avoid byte alignment issues
    switch(attrDesc1.attrType)
    {
        case INTEGER:
            memcpy(&tmpInt1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(int));
            memcpy(&tmpInt2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(int));
            return tmpInt1 - tmpInt2;

        case DOUBLE:
            memcpy(&tmpFloat1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(double));
            memcpy(&tmpFloat2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(double));
            floatDiff = tmpFloat1 - tmpFloat2;
            return (fabs(floatDiff) < DOUBLEERROR) ? 0 : floatDiff;

        case STRING:
            return strncmp(
                (char *) outerRec.data + attrDesc1.attrOffset, 
                (char *) innerRec.data + attrDesc2.attrOffset, 
                MAX(attrDesc1.attrLen, attrDesc2.attrLen));
    }

    return 0;

}

Status Operators::join_project(const int proj_count, const AttrDesc attr_descs[], const int rec_len,
                               const string &rel1_name, const string &rel2_name,
                               const Record &rec1_in, const Record &rec2_in, Record *rec_out)
{
    char *data = new char[rec_len];
    int offset = 0;
    for (int i = 0; i < proj_count; i++) {
        int attr_len = attr_descs[i].attrLen;
        if (!rel1_name.compare(attr_descs[i].relName)) { // rel1 attr
            memcpy(data+offset, (char *)rec1_in.data+attr_descs[i].attrOffset, attr_len);
        }
        else if (!rel2_name.compare(attr_descs[i].relName)) { // rel2 attr
            memcpy(data+offset, (char *)rec2_in.data+attr_descs[i].attrOffset, attr_len);
        }
        
        offset += attr_len;
    }
    rec_out->data = data;
    rec_out->length = rec_len;
    
    return OK;
}
