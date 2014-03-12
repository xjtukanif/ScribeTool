/**
 *    \file   SafeSharedPtr.hpp
 *    \brief  
 *    \date   2011Äê05ÔÂ10ÈÕ
 *    \author guoshiwei (guoshiwei@gmail.com)
 */

#ifndef  SAFESHAREDPTR_INC
#define  SAFESHAREDPTR_INC

#include <stdexcept>
#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <pthread.h>

template <typename T>
class SafeSharedPtr: public boost::noncopyable
{
    public:
	SafeSharedPtr()
	{
	    if(pthread_spin_init(&lock_, 0) != 0){
		throw std::runtime_error("pthread_spin_init failed");
	    }
	}

	~SafeSharedPtr()
	{
	    pthread_spin_unlock(&lock_);
	}

	boost::shared_ptr<T> copy_shared_ptr()
	{
	    pthread_spin_lock(&lock_);
	    boost::shared_ptr<T> ptr_copy(ptr_);
	    pthread_spin_unlock(&lock_);
	    return ptr_copy;
	}

	void reset_shared_ptr(boost::shared_ptr<T> p)
	{
	    pthread_spin_lock(&lock_);
	    ptr_ = p;
	    pthread_spin_unlock(&lock_);
	}

    private:
	boost::shared_ptr<T> ptr_;
	pthread_spinlock_t lock_;
};

#endif   /* ----- #ifndef SAFESHAREDPTR_INC  ----- */
