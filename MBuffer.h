/*! \file MBuffer.h
    \brief  Buffer for holding messages.

	Synchronised between multiple producer and consumer threads.
*/


#include <atomic>
#include <cassert>

namespace Messenger {


	
//! Ring buffer management class with number of rows and number of columns per row.

//! Buffer has a total of TRows x TColumns elements. 
// The ring buffer size is TRows with
// each row now representing TColumns.
// The same buffer can be reconfigured as different number of rows and 
// columns as long as their product equals the original number of elements.
// A producer acquires an entire row synchronously and writes all values in
// the row in one go. A consumer in turn acquires an entire row synchronously and
// reads all the values in one go. This reduces synchronization costs, significantly 
// increasing throughput.
template<size_t TRows, size_t TColumns, typename T>
class MBuffer {
public:
	//! raw buffer size
	static const size_t m_rawBufSize = TRows*TColumns;
	typedef T ValueType;
private:
	//! number of rows; invariant m_rows x m_columns = m_rawBufSize	
	//! Number of rows also constitues ring buffer size. The synchronization
	// is done for an entire row.
	size_t    m_rows;
	//! number of columns; invariant m_rows x m_columns = m_rawBufSize
	size_t    m_columns;
	//! if 'true', producers and consumers are expected to stop.
	bool	  m_stop;
	//! raw buffer
	T         m_buf[m_rawBufSize];
	//! Highest absolute consumer loc where a thread is attempting to read from.
	// All the previous locations have been read.
	std::atomic<long>	m_consLoc;
	//! highest absolute producer loc where a thread is attempting to write into.
	// All the previous locations have been written.
	std::atomic<long>   m_prodLoc;

	/*! \enum location status

	    READY_FOR_WRITE: available to write
	    WRITING:			being written
	    READY_FOR_READ:	available to read
	    READING:			being read
	*/
	enum class	Status: long {READY_FOR_WRITE = 0, WRITING=1, READY_FOR_READ=2, 
		                      READING=3};
	//! buffer to store per location status.

	//! strictly speaking the array need be no greater than m_rows,
	// but unless we do dynamic allocation when m_rows, m_columns change
	// we stick to static m_rows x m_columns size.
	std::atomic<Status>	m_locStatus[m_rawBufSize];

	//! Ring buffer location to abs location map.

	// This is a ring buffer.
	// A 'location' corresponds to an entire row of m_columns.
	// So, a location x can refer to x, x + m_rows, x + 2*m_rows..and so on.
	// A ring buffer location can be associated with only one absolute location
	// at any point.
	// For example, consider ring buffer of 5 (locations 0,1,2,3,4)
	// absolute location, ever increasing  0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 ...
	// ring buffer loc wraps around        0 1 2 3 4 <-- mapped to 5 abs values starting at 0
	//                                               0 1 2 3 4 mapped to 5 abs values starting at 5
	//
	//
	// Once elements in ring buffer row x which is mapped to x + m_rows are produced and consumed,
	// the same x is mapped to x + 2*m_rows for subsequent use by producer.
	// Thus the mapping is to chronologically increasing values.

	// When a producer or consumer is processing a ring buffer location, 
	// its corresponding  map location should not change.
	// Otherwise a producer and a consumer can see different values in the same
	// location they are accessing.
	// This map is written to by a producer:
	// For eg. a producer writes a value in ring buffer x and specifies in the map
	// what absoute location that the value is for.
	// It is consumer responsibility (via GetNextLocForCons) to ensure that
	// the buf loc it is seeking to consume refers to the same absolute
	// location producer wrote, and did not change absolute location
	// by the time they return the location to the caller. This map is used for that.
	// Strictly speaking the array need be no greater than m_rows,
	// but unless we do dynamic allocation, we stick to static m_rows x m_columns size.
	std::atomic<int64_t> m_locToAbsLocMap[m_rawBufSize];

public:
	//! ctor
	MBuffer() : 
		m_rows(TRows),
		m_columns(TColumns),
		m_stop(false)
	{
		m_consLoc.store(0);
		m_prodLoc.store(0);
		ReleaseAllLocks();
	}
	//! set rows and columns.
	/*! rows x columns must equal TRows x TColumns.
	    This method would resize number of elements in each rows for 
	    thread-free buffer.
	    This is cheaper because, one could re-use same buf_
	    with different row/column config, rather than creating a new type
	    using TRows, TColumns template params.
		
		\param rows_               number of rows
		\param columns_            number of columns
	*/
	void SetRowsColumns(size_t rows_, size_t columns_)
	{
		if (rows_*columns_ != TRows*TColumns)
		{
			throw std::runtime_error("rows x columns != buffer size");
		}
		m_rows = rows_;
		m_columns = columns_;
	}
	//! get next free loc in m_buf to produce.
	/*!
	   This is m_prodLoc: one past the last produced location.
	   a  loc will give access to m_columns elements to write which
	   need not be synchronised.
	   A producer can write to a loc only if status is READY_FOR_WRITE
	   which is the starting state of the buffer. 
	   A consumer also sets this state
	   once it has consumed and location is ready for writing.
	   Once a producer thread acquires a loc, its status is set to WRITING to prevent
	   consumers and other producers accessing it.
	   absLoc_ is the absolute location, without modulus, as if the buffer were 
	   infinite.
	  
	   \param  [out]   absLoc_  next absolute location for the prodcuer 
	   \return         ring buffer location = absLoc_ % m_rows.
	                   size_t(-1), illegal value, returned when buffer is stopped.
	*/
	size_t GetNextLocForProd(size_t& absLoc_)
	{

		// wait as long as m_prodLoc status is not READY_FOR_WRITE;
		// and then set status to WRITING.
		// When status is WRITING, no other producer can write, 
		// and no consumer can read.
		auto absLoc = m_prodLoc.load();
		auto loc = absLoc % m_rows;
		std::atomic<Status>* status{ &m_locStatus[loc] };
		auto statusReadyForWrite = Status::READY_FOR_WRITE;
		auto statusWriting = Status::WRITING;
		while ( (!status->compare_exchange_strong (statusReadyForWrite, statusWriting))
			&& (!m_stop) )
		{
			std::this_thread::sleep_for(std::chrono::microseconds(1)); 
			// restore statusReadyForWrite as this can be overwritten
			// by compare_exchange_strong
			statusReadyForWrite = Status::READY_FOR_WRITE;
			// update status in case m_prodLoc is changed by another 
			// thread meanwhile
			absLoc = m_prodLoc.load();
			loc = absLoc % m_rows;
			status = &m_locStatus[loc];
		}
		absLoc_ = absLoc;
		// when stopped, return val is invalid for caller
		if (m_stop) return (size_t)(-1); 
		// loc is now assocaited with absLoc : loc = absLoc % m_rows 
		m_locToAbsLocMap[loc].store(absLoc);
		// before returning, increment m_prodLoc for next pos
		++absLoc;
		m_prodLoc.store(absLoc);
		// all elements at this loc can be written to lock-free
		return loc; 
	}

	//! get next free loc in m_buf to consume.
	/*!
	This is m_consLoc: one past the last consumed absolute location.
	a  loc will give access to m_columns elements to read which
	need not be synchronised.
	A consumer can read from a loc only if status is READY_FOR_READ.
	A producer  sets this state
	once it has produced and location is ready for reading.
	Once a consumer thread acquires a loc, its status is set to READING to prevent
	consumers and other producers accessing it.
	absLoc_ is the absolute location, without modulus, as if the buffer were
	infinite.

	\param  [out]   absLoc_  next absolute location for the consumer
	\return         ring buffer location = absLoc_ % m_rows.
	                size_t(-1), illegal value, returned when buffer is stopped.
	*/
	size_t	GetNextLocForCons(size_t& absLoc_)
	{
		// wait as long as m_consLoc status is not READY_FOR_READ;
		// and then set status to READING.
		// When status is READING, no producer can write, and no other consumer can read.
		auto absLoc = m_consLoc.load();
		auto loc = absLoc % m_rows;
		std::atomic<Status>* status{ &m_locStatus[loc] };
		auto statusReadyForRead = Status::READY_FOR_READ;
		auto statusReading = Status::READING;
		while (!m_stop)
		{
			while ((!status->compare_exchange_strong(statusReadyForRead, statusReading))
				&& (!m_stop))
				// ------- (1)
			{
				std::this_thread::sleep_for(std::chrono::microseconds(1)); 
				// restore statusReadyForRead as this is overwritten
				statusReadyForRead = Status::READY_FOR_READ;
				// update status in case m_consLoc is changed by 
				// another thread meanwhile
				absLoc = m_consLoc.load(); // ------------- (2)
				loc = absLoc % m_rows;
				status = &m_locStatus[loc];
				// --------- (3)
				// In the sequence(2)--> (3)--- next iteration --->  (1) following may happen:
				// Let this thread be A, another thread, say B, would be executing 
				// the same code at (1).
				// Thus, A and B are competing for the same m_consLoc.
				// Let B wins and gets access to m_consLoc, sets status to READING
				// comes out of loop, reaches (5), and returns. Afer it is consumed, its
				// status is set to READY_FOR_WRITE for a producer to pick up.
				// Let a producer thread C, meanwhile puts a value in this location
				// and sets status to READY_FOR_READ.
				// Note that when the producer writes here, it will be equivalent to
				// absolute location m_consLoc + m_rows.
				// m_consLoc  and m_consLoc + m_rows are mapped to the same ring buffer loc.
				// m_consLoc % m_rows =  (m_consLoc + m_roww) % m_rows

				// A then finally gets this loc and reads value from here.
				// The problem here is, A consumed a value which was the latest produced
				// by producer, and not the previous value (m_rows back) it expected to read.
				// The original value in that loc was overwritten by producer
				// after B read it. This happens because we use ring buffer.
				// A location x in ring buffer can refer to all absolute values
				// x + m_rows, x + 2*m_rows, ..x + n*m_rows.
				// Thus while a consumer is waiting to read at x, a producer writing
				// at x + m_rows might overwrite ring buffer status.
				// Thus a consumer reading at absolute loc x + n*m_rows should get
				// value written by producer at x + n*m_rows only. This
				// sanity check is done at (4) using the
				// ring buffer location to absolute location map.
			}
			// check if loc has same absLoc mapping and no producer changed it meanwhile.
			if (m_locToAbsLocMap[loc].load() == absLoc) // ------ (4)
				break;
			// the absLoc has changed by producer. This consumer is interested 
			// in the old value which no longer exists as it was consumed allready.
			// release from READING to READY_FOR_READ so that a consumer thread (this or
			// another thread)
			// that wants to read from new abs loc can take it.
			status->store(statusReadyForRead); 
		}
		absLoc_ = absLoc;
		if (m_stop) return (size_t)(-1); // when stopped, return val is invalid for caller
		// before returning, increment m_consLoc for next pos
		++absLoc;
		m_consLoc.store(absLoc); //-------------- (5)

		return loc; // all elements at this loc can be read lock-free
	}

	//! set given loc ready to consume.
	/*!
	   Status must be set to READY_FOR_READ.
	   this is called by a producer after writing all elements at loc.
	   \param  [in ]   absloc_  asbolute location to be marked for read
	*/
	void	SetLocReadyForCons(size_t absloc_)
	{
		const auto loc = absloc_ % m_rows;
		std::atomic<Status>& status{ m_locStatus[loc] };
		status.store(Status::READY_FOR_READ);
	}

	/*!
	Status must be set to READY_FOR_WRITE.
	this is called by a consumer after reading all elements at loc.
	\param  [in ]   absloc_  asbolute location to be marked for write
	*/
	void	SetLocReadyForProd(size_t absloc_)
	{
		const auto  loc = absloc_ % m_rows;
		std::atomic<Status>& status{ m_locStatus[loc] };
		status.store(Status::READY_FOR_WRITE);
	}

	//! Release all locks. 
	/*! Typically called from a different thread
	    from the ones prod and cons are waiting in, 
	    when 'stop' is issued
	*/
	void ReleaseAllLocks()
	{
		// release all locations
		for (auto i = 0u; i < m_rows; ++i) {
			std::atomic<Status>& status{ m_locStatus[i] };
			status.store(Status::READY_FOR_WRITE);
			m_locToAbsLocMap[i].store(-1); // loc -> abs location is not set to start woth
		}
	}

	//! Stop producer-consumer
	void Stop()
	{
		m_stop = true;
		ReleaseAllLocks();
	}

	//! reset as if this object is yet to be used.
	/*! 
	   typically called before/after setting new rows and column values
	   using SetRowsColumns.
	   This would enable the caller to reuse the same object with
	   a different row/column combination.
	*/
	void Reset()
	{
		m_consLoc.store(0);
		m_prodLoc.store(0);
		ReleaseAllLocks();
		m_stop = false;
	}

	//! Access a location
	/*!
	    Return address to the first element of a given location.
		No checking is performed on the index.
	*/
	T* operator[](size_t loc_)  { return &m_buf[loc_*m_columns]; }
	//! Return number of buffers.
	size_t	BufSize() const { return m_rows; }
	//! Return number of elements in a buffer.
	size_t	BufElemSize() const { return m_columns; }
};


}

