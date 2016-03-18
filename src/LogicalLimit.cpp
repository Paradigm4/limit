/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* limit is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* limit is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* limit is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with limit.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/Operator.h>
#include <log4cxx/logger.h>

using std::shared_ptr;
using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;

using namespace std;

namespace scidb {

class LogicalLimit: public LogicalOperator {
public:
    LogicalLimit(const std::string& logicalName, const std::string& alias) :
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_CONSTANT("uint64");
    }

    /**
     * output schema of limit is same as the input schema
     */
    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas,
            shared_ptr<Query> query)
    {
        ArrayDesc res(
                schemas[0].getName(),
                schemas[0].getAttributes(),
                schemas[0].getDimensions(),
                defaultPartitioning());
        return res;
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalLimit, "limit");

}
 // emd namespace scidb
