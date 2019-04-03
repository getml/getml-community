#ifndef MULTITHREADING_HPP_
#define MULTITHREADING_HPP_

// ----------------------------------------------------
// Dependencies

#include <algorithm>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

// ----------------------------------------------------

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

// ----------------------------------------------------

#endif  // MULTITHREADING_HPP_
