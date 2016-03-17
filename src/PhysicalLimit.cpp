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

#include <limits>
#include <string>
#include <vector>

#include <system/Exceptions.h>
#include <query/TypeSystem.h>
#include <query/Operator.h>
#include <util/Platform.h>
#include <util/Network.h>
#include <boost/scope_exit.hpp>

#include <log4cxx/logger.h>

using std::shared_ptr;
using std::make_shared;

using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.limit"));

using namespace scidb;

template <class T>
inline void resetIterators(vector<std::shared_ptr<T> > & iterators)
{
    for (size_t i=0; i<iterators.size(); i++)
    {
        if(iterators[i].get())
        {
            iterators[i]->flush();
            iterators[i].reset();
        }
    }
}


class PhysicalLimit: public PhysicalOperator
{
public:
    PhysicalLimit(std::string const& logicalName, std::string const& physicalName,
            Parameters const& parameters, ArrayDesc const& schema) :
                PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    size_t peekCount(shared_ptr<Array>& input, size_t const limit)
    {
        size_t count = 0;
        shared_ptr<ConstArrayIterator> saiter = input->getConstIterator(input->getArrayDesc().getAttributes().size()-1);
        while(!saiter->end() && count < limit)
        {
            count+=saiter->getChunk().count();
            ++(*saiter);
        }
        if(count > limit)
        {
            return limit;
        }
        return count;
    }

    shared_ptr<Array> makeLimitedArray(shared_ptr<Array>& input, shared_ptr<Query>& query, size_t const limit, size_t& actualCount)
    {
        shared_ptr<Array> result = make_shared<MemArray>(_schema,query);
        size_t const nSrcAttrs = _schema.getAttributes(true).size();
        vector<shared_ptr<ConstArrayIterator> > saiters (nSrcAttrs); //source array and chunk iters
        vector<shared_ptr<ConstChunkIterator> > sciters (nSrcAttrs);
        vector<shared_ptr<ArrayIterator> > daiters (nSrcAttrs);      //destination array and chunk iters
        vector<shared_ptr<ChunkIterator> > dciters (nSrcAttrs);
        BOOST_SCOPE_EXIT((&dciters))
        {
            resetIterators(dciters);
        } BOOST_SCOPE_EXIT_END
        for(AttributeID i =0; i<nSrcAttrs; i++)
        {
            saiters[i] = input->getConstIterator(i);
            daiters[i] = result->getIterator(i);
        }
        actualCount = 0;
        while ( !saiters[0]->end() && actualCount < limit)
        {
            Coordinates const& chunkPos = saiters[0]->getPosition();
            for (AttributeID i = 0; i < nSrcAttrs; i++)
            {
                sciters[i] = saiters[i]->getChunk().getConstIterator();
                dciters[i] = daiters[i]->newChunk(chunkPos).getIterator(query, i == 0 ? ChunkIterator::SEQUENTIAL_WRITE : //populate empty tag from attr 0 implicitly
                                                                        ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
            }
            while(!sciters[0]->end() && actualCount < limit)
            {
                Coordinates const& inputCellPos = sciters[0]->getPosition();
                for(size_t i=0; i<nSrcAttrs; i++)
                {
                    dciters[i]->setPosition(inputCellPos);
                    dciters[i]->writeItem(sciters[i]->getItem());
                    ++(*sciters[i]);
                }
                actualCount++;
            }
            for (AttributeID i = 0; i < nSrcAttrs; i++)
            {
                dciters[i]->flush();
                dciters[i].reset();
                ++(*saiters[i]);
            }
        }
        resetIterators(dciters);
        return result;
    }

    shared_ptr<Array> execute(vector<std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        size_t limit = std::numeric_limits<ssize_t>::max();
        Value const& limVal = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate();
        if(!limVal.isNull() && limVal.getUint64() < static_cast<size_t>(std::numeric_limits<ssize_t>::max()))
        {
            limit= limVal.getUint64();
        }
        size_t availableCount = peekCount(inputArrays[0], limit);
        InstanceID const myId    = query->getInstanceID();
        InstanceID const coordId = query->getCoordinatorID() == INVALID_INSTANCE ? myId : query->getCoordinatorID();
        size_t const numInstances = query->getInstancesCount();
        size_t requiredCount = 0;
        if(myId != coordId) //send my count to the coordinator
        {
            shared_ptr<SharedBuffer> buffer(new MemoryBuffer(&availableCount, sizeof(size_t)));
            BufSend(coordId, buffer, query);
            buffer = BufReceive(coordId, query);
            requiredCount = *(static_cast<size_t const*>(buffer->getData()));
        }
        else //on coordinator, lower everyone's count as needed
        {
            vector<size_t> requiredCounts(numInstances);
            for(size_t i =0; i<numInstances; ++i)
            {
                if(i==myId)
                {
                    requiredCounts[myId] = availableCount;
                    continue;
                }
                shared_ptr<SharedBuffer> buffer = BufReceive(i, query);
                requiredCounts[i] = *(static_cast<size_t const*>(buffer->getData()));
            }
            size_t totalCount =0;
            for(size_t i=0; i<numInstances; ++i)
            {
                if(requiredCounts[i] > limit)
                {
                    requiredCounts[i] = limit;
                }
                else if(totalCount + requiredCounts[i] > limit)
                {
                    requiredCounts[i] = static_cast<ssize_t>(limit) - static_cast<ssize_t>(totalCount) > 0 ? limit - totalCount : 0;
                }
                totalCount += requiredCounts[i];
            }
            for(size_t i =0; i<numInstances; ++i)
            {
                if(i==myId)
                {
                    requiredCount = requiredCounts[myId];
                    continue;
                }
                shared_ptr<SharedBuffer> buffer(new MemoryBuffer(&(requiredCounts[i]), sizeof(size_t)));
                BufSend(i, buffer, query);
            }
        }
        if(requiredCount == 0)
        {
            return shared_ptr<Array>(new MemArray(_schema, query));
        }
        else
        {
            return makeLimitedArray(inputArrays[0], query, requiredCount, availableCount);
        }
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalLimit, "limit", "PhysicalLimit");

}
 // end namespace scidb
