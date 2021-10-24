#ifndef MULTITHREADING_HPP_
#define MULTITHREADING_HPP_

// ----------------------------------------------------
// Dependencies

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <execution>
#include <mutex>
#include <thread>
#include <vector>

#include "debug/debug.hpp"

// ----------------------------------------------------

#ifndef __APPLE__
#include "multithreading/parallel_for_each.hpp"
#endif

#include "multithreading/Barrier.hpp"

#include "multithreading/ReadWriteLock.hpp"

#include "multithreading/ReadLock.hpp"
#include "multithreading/WeakWriteLock.hpp"
#include "multithreading/WriteLock.hpp"

#include "multithreading/Spinlock.hpp"

#include "multithreading/Communicator.hpp"

#include "multithreading/all_reduce.hpp"
#include "multithreading/broadcast.hpp"

#include "multithreading/maximum.hpp"
#include "multithreading/minimum.hpp"

#include "multithreading/Reducer.hpp"

// ----------------------------------------------------

#endif  // MULTITHREADING_HPP_
