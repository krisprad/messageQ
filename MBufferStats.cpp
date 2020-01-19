/*! \file MBuffer.cpp
\brief  Client program to use multi buffer Q.

Synchronised between multiple producer and consumer threads.
*/
#include "MBuffer.h"
#include <iostream>
#include <string>
#include <vector>
#include <exception>      // std::exception
#include <thread>         // std::thread, std::this_thread::sleep_for
#include <chrono>



// uncomment this for debug output (very long for large buffers)
// DEBUG_MSG

// default number of producers, consumers
static const auto g_NumProd = 2;
static const auto g_NumCons = 2;


//! Dbg: forwards requests to ostream
/*! This prints output messages if ostream is supplied,
    otherwise the print calls are nop-s.
	Works similar to cout.
*/
class Dbg // debug output printed, if _os is not NULL
{
	std::ostream*_os;
public:
	Dbg(std::ostream* os_ = NULL) : _os(os_) {}

	template<typename T>
	Dbg& operator<<(const T& t_)
	{
		if (_os)
			*_os << t_;
		return *this;
	}
	// support endl
	Dbg& operator<<(std::ostream& (*f)(std::ostream&))
	{
		if (_os)
			*_os << f;
		return *this;
	}
};

// enable/suppress debug output
#ifdef DEBUG_MSG
Dbg	_dbg_(&std::cout);
#else
Dbg	_dbg_;
#endif

//! timer class to measure duration.
class TimeKeeper
{
	std::chrono::steady_clock::time_point m_start;
	std::chrono::steady_clock::time_point m_end;
	std::string m_name;
public:
	TimeKeeper(const std::string& n_) : m_name(n_)
	{
		startTimer();
	}
	~TimeKeeper()
	{
		stopTimer();
		PrintElasped();
	}
	void startTimer()
	{
		m_start = std::chrono::steady_clock::now();
	}
	void stopTimer()
	{
		m_end = std::chrono::steady_clock::now();
	}
	double getElapsedTime()
	{
		using namespace std::chrono;
		auto dur = m_end - m_start;
		auto ticks = dur.count(); // ticks of nanoseconds
		auto seconds = double(ticks)*steady_clock::period::num / steady_clock::period::den;
		return seconds;
	}
	void PrintElasped()
	{
		auto seconds = getElapsedTime();
		_dbg_ << "Elapsed time for " << m_name << ": " << seconds << " secs\n";
	}

};

//! type of message stored in buffer.
// This class is used as a wrapper for underlying
// type specified as template param.
// This is used for sanity checks
// in verifying whether producders/consumers
// are working correctly, by checking associated indices.
template<typename T> class MsgType;

//! Object type of message: int specialization
template<> class MsgType<int64_t>
{
	int64_t m_obj;
public:
	explicit MsgType(int64_t obj_ = 0) : m_obj(obj_) {}
	//!Get object index for int val: same as value
	/*! index is used to do sanity check.
	    An object produced at absolute location
		x must have index x.
		Absolute location in the buffer[row][col] =
		row*number-of-columns-per-row + col
	*/
	int64_t GetIndex() const { return m_obj; }
	bool operator<(const MsgType& obj_) { return m_obj < obj_.m_obj; }
	bool operator!=(const MsgType& obj_) { return m_obj != obj_.m_obj; }
	int64_t Value() const { return m_obj; }
	void Value(const int64_t obj_) { m_obj = obj_;  }
};

//! Object type of message: string specialization
/*! In this implementation this is just a string
    representation of an intergal value.
	For example, the object "255903" represent 255903.
	This simplified version used to easily obtain
	index for the object.
*/
template<> class MsgType<std::string>
{
	std::string m_obj;
	int64_t m_objIndex;
public:
	explicit MsgType(int64_t obj_) : m_obj(std::to_string(obj_)), m_objIndex(obj_) {}
	MsgType(std::string obj_="0") : m_obj(obj_), m_objIndex(stoll(obj_)) {}
	//!Get object index
	/*! index is used to do sanity check.
	An object produced at absolute location
	x must have index x.
	Absolute location in the buffer[row][col] =
	row*number-of-columns-per-row + col
	*/
	int64_t GetIndex() const { return m_objIndex; }
	bool operator<(const MsgType& obj_) { return m_objIndex < obj_.m_objIndex; }
	bool operator!=(const MsgType& obj_) { return m_obj != obj_.m_obj; }
	std::string Value() const { return m_obj; }
	void Value(const int64_t obj_) { m_obj = std::to_string(obj_); }
};

//! output a message element
template<typename T>
std::ostream& operator<<(std::ostream& os_, const MsgType<T>& obj_)
{
	os_ << obj_.Value();
	return os_;
}


// IndexToObject: called by producer with unique index. 
// Return an object appropriate for the  index.
// By default, integral type is assumed and the object is same as
// the index. For strings, it is string equivalent. 
template<typename T>
T IndexToObject(size_t index_) { return T{ index_ }; }

template<>
std::string IndexToObject<std::string>(size_t index_) { return std::to_string(index_); }


//! Producer thread
template<typename TBuffer>
class Producer
{
	typedef typename TBuffer::ValueType ObjType;
	std::string m_name; // name of the producer
	bool   m_stop; // when true, producer stops
	size_t m_numObjs; // number of objects produced
	double	m_timeElapsed; // total time elapsed
	ObjType		m_lastObj; // value of the last object
	TBuffer&     m_buffer; // buffer to write to
	std::thread m_thread; // default constructed thread

public:
	Producer(TBuffer& buf_, const char* s_ = "") :
		m_name(s_), m_stop(false), m_numObjs(0), 
		m_lastObj(-1), m_buffer(buf_)
	{
		m_thread = std::thread(ThreadFuncForProducer, this);
		_dbg_ << m_name << " started\n"; // thread starts
	}
	~Producer()
	{
	}
	void SetName(const std::string & s_) { m_name = s_; }
	const std::string& GetName() const { return m_name; }

	// launch thread
	void Start()
	{
	}
	// implement thread func code here
	void Run()
	{
		auto	lastLoc = -1; // last location produced as if array were infinite
		auto	lastAbsRow = -1; // last row as if array were infinite 
		auto	lastCol = -1; // last column within a row
		// Thus lastLoc = lastAbsRow*columns-per-row + lastCol 
		// when the values are not -1
	
		TimeKeeper sw("Producer Timekeeper");
		sw.startTimer();
		while (!m_stop)
		{
			// produce: get next row to produce and fill object values
			size_t absRow;
			_dbg_ << "prod: " << m_name << " get next loc - ";
			auto row = m_buffer.GetNextLocForProd(absRow);
			if (row >= m_buffer.BufSize() )
			{
				_dbg_ << m_name << " : Illegal row " << row << ". Buffer probably stopped\n";
				break;
			}
			if (m_stop) break;
			auto* arr =  &m_buffer[row][0];
			auto col = 0u;
			for (; (col < m_buffer.BufElemSize() ) && (!m_stop) ; ++col)
			{
				lastLoc = absRow*m_buffer.BufElemSize() + col; // produced until this loc
				auto nextProdVal = IndexToObject<ObjType>(lastLoc);
				arr[col] = nextProdVal;
				_dbg_ << "----nextProdVal " << nextProdVal << ", col " << col
					   << ", arr[col] " << arr[col] << std::endl;
				_dbg_ << m_name << ": absRow " << absRow << ", row " << row
					<< ", col " << col << ", lastLoc " << lastLoc  
					<< ", nextProdVal " << nextProdVal
					<< ", arr[col] " << arr[col]
					<< std::endl;
				_dbg_ << "Wrote " << m_buffer[row][col] << " at [" << row << "]["
					  << col << "], absRow " << absRow << std::endl;
				m_lastObj = arr[col];
				++m_numObjs;
				lastCol = col;
			}
			lastAbsRow = absRow;
			m_buffer.SetLocReadyForCons(row); // all elements in row written. release this row to consumer
		}
		sw.stopTimer();
		m_timeElapsed = sw.getElapsedTime();
		_dbg_ << m_name << " stopped. Produced " << m_numObjs 
			 << ". Last loc " << lastLoc 
			 << " ( " << lastAbsRow << "*" <<  m_buffer.BufElemSize() 
			 << " + " << lastCol << " )"
			 <<  " Last produced " << m_lastObj << "\n"; // thread stops
	}

	double GetElapsedTime() const { return m_timeElapsed; }
	std::thread&	GetThread()  { return m_thread; }

	// flag to stop : called from some other thread
	void Stop() {
		m_stop = true;
		m_buffer.Stop();
	}
	size_t      GetTotal() const { return m_numObjs; }
	ObjType		GetLastObj() const { return m_lastObj; }

	// thread function: transfers control back to Producer by calling Run method
	static void ThreadFuncForProducer(Producer* p)
	{
		p->Run();
	}
};



//! Consumer thread
template<typename TBuffer>
class Consumer
{
	typedef typename TBuffer::ValueType ObjType;
	std::string m_name; // name of the consumer
	bool   m_stop; // when true, consumer stops
	size_t m_numObjs; // number of objects consumed
	double	m_timeElapsed; // total time elapsed
	ObjType		m_lastObj; // value of the last object
	TBuffer&    m_buffer; // buffer to read from
	std::thread m_thread; // default constructed thread

public:
	Consumer(TBuffer& buffer_, const char* s_ = "") : 
		m_name(s_), m_stop(false), m_numObjs(0), 
		 m_lastObj(-1), m_buffer(buffer_)
	{
		m_thread = std::thread(ThreadFuncForConsumer, this);
		_dbg_ << m_name << " started\n"; // thread starts
	}
	~Consumer()
	{
	}
	void SetName(const std::string & s_) { m_name = s_; }
	const std::string& GetName() const { return m_name; }

	// launch thread
	void Start()
	{
	}
	// implement thread func code here
	void Run()
	{
		TimeKeeper sw("Consumer Timekeeper");
		ObjType	prevObj{ -1 };
		ObjType	curObj{ 0 };
		auto	lastLoc = -1; // last location consumed as if array were infinite
		auto	lastAbsRow = -1; // last row as if array were infinite 
		auto	lastCol = -1; // last column within a row
							  // Thus lastLoc = lastAbsRow*columns-per-row + lastCol 
							  // when the values are not -1

		sw.startTimer();
		while (!m_stop)
		{
			// consumer
			_dbg_ << "Get loc for " << m_name << std::endl;
			size_t absRow;
			_dbg_ << "cons: " << m_name << " get next consloc " << std::endl;
			size_t row = m_buffer.GetNextLocForCons(absRow);
			_dbg_ << "cons: " << m_name << " got next consloc, absRow " 
				<< absRow << ", row " << row << std::endl;
			if (row >= m_buffer.BufSize() )
			{
				_dbg_ << m_name << " : Illegal row " << row << ". Buffer probably stopped\n";
				break;
			}
			if (m_stop) break;
			auto* arr = (ObjType*) &m_buffer[row][0];
			auto col = 0u;
			for (; (col < m_buffer.BufElemSize() ) && (!m_stop) ; ++col)
			{
				curObj = arr[col];
				_dbg_ << "Read " << curObj << " at [" << row << "]["
					  << col << "], absRow " << absRow << std::endl;
				if (curObj < prevObj)
				{
					std::cout << "Error: at [" << row << "][" << col << "] absRow " << absRow

						<< " Cur obj " << curObj << " < prev obj " << prevObj << ". '" << m_name << "' Consumed in wrong sequence\n";
					exit(0);  
				}
				++m_numObjs;
				prevObj = curObj;
				m_lastObj = curObj;
				lastLoc = absRow*m_buffer.BufElemSize() + col; // consumed until this loc, if buffer is infinite 1-d
				// sanity check. The loc of the obj and obj val must be identical
			
				if (lastLoc != curObj.GetIndex())
				{
					std::cout << "Error: at [" << row << "][" << col << "] last loc " << lastLoc << " not same as cur obj " << curObj << ". Consumed wrong obj\n";
					exit(0);  
				}
			    arr[col].Value(0); // reset consumed val at this location
				lastCol = col;

			}
			lastAbsRow = absRow;
			m_buffer.SetLocReadyForProd(row); // all elements in row read. release this row to producer
		}
		sw.stopTimer();
		m_timeElapsed = sw.getElapsedTime();
		_dbg_ << m_name << " stopped. Consumed " << m_numObjs 
			 << ". Last loc " << lastLoc 
			 << " ( " << lastAbsRow << "*" <<  m_buffer.BufElemSize() 
			 << " + " << lastCol << " )"
			 << ". Last consumed " << curObj << "\n"; // thread stops
	}

	double GetElapsedTime() const { return m_timeElapsed; }
	std::thread& GetThread()  { return m_thread; }

	// flag to stop : called from some other thread
	void Stop() {
		m_stop = true;
		m_buffer.Stop();
	}
	size_t GetTotal() const { return m_numObjs; }
	ObjType		GetLastObj() const { return m_lastObj; }

	// thread function: transfers control back to Consumer by calling Run method
	static void ThreadFuncForConsumer(Consumer* c)
	{
		c->Run();
	}
};



template<typename TBuffer>
void RunProducersConsumers(size_t numProd_, size_t numCons_, TBuffer& buffer_)
{
	_dbg_ << " Number of producers " << numProd_ << std::endl;
	_dbg_ << " Number of consumers " << numCons_ << std::endl;
	std::vector<std::unique_ptr<Producer<TBuffer>>> prods;
	std::vector<std::unique_ptr<Consumer<TBuffer>>> cons;

	decltype(((typename TBuffer::ValueType*) nullptr)->GetIndex()) lastProduced = -1;
	decltype(((typename TBuffer::ValueType*) nullptr)->GetIndex()) lastConsumed = -1;

	for (auto i = 0u; i < numProd_; ++i)
	{
		auto p = std::make_unique<Producer<TBuffer>>(buffer_);
		auto s = "prod " + std::to_string(i);
		p->SetName(s);
		prods.push_back(std::move(p));
	}
	for (auto i = 0u; i < numCons_; ++i)
	{
		auto c = std::make_unique<Consumer<TBuffer>>(buffer_);
		auto s = "cons " + std::to_string(i);
		c->SetName(s);
		cons.push_back(std::move(c));
	}

	{
		TimeKeeper tk("All prod-cons");

		for (auto i = 0u; i < prods.size(); ++i)
		{
			prods[i]->Start();
			_dbg_ << prods[i]->GetName() << " Handle " << prods[i]->GetThread().get_id() << std::endl;
		}
		for (auto i = 0u; i < cons.size(); ++i)
		{
			cons[i]->Start();
			_dbg_ << cons[i]->GetName() << " Handle " << cons[i]->GetThread().get_id() << std::endl;
		}
		const auto numSecs = 5;
		_dbg_ << "Sleep for " << numSecs << " seconds\n";
		std::this_thread::sleep_for(std::chrono::seconds(numSecs));
		_dbg_ << "Stopping producers and consumers\n";
		for (auto i = 0u; i < prods.size(); ++i)
		{
			prods[i]->Stop();
		}
		for (auto i = 0u; i < cons.size(); ++i)
		{
			cons[i]->Stop();
		}

		// wait for all  threads to complete
		_dbg_ << "Waiting for producers and consumers to complete\n";
		for (auto i = 0u; i < cons.size(); ++i)
		{
			cons[i]->GetThread().join();
		}
		for (auto i = 0u; i < prods.size(); ++i)
		{
			prods[i]->GetThread().join();
		}
	}
	auto totalProduced = 0, totalConsumed = 0;
	auto	totalElapsedProd = 0.0, totalElapsedCons = 0.0;
	for (auto i = 0u; i < prods.size(); ++i)
	{
		auto num = prods[i]->GetTotal();
		totalProduced += num;
		_dbg_ << prods[i]->GetName() << " : " 
			 << prods[i]->GetElapsedTime() << " secs. " << num << " produced\n";
		totalElapsedProd += prods[i]->GetElapsedTime();
		auto lastp = prods[i]->GetLastObj().GetIndex();
		if (lastp > lastProduced) lastProduced = lastp;
	}
	prods.clear();

	for (auto i = 0u; i < cons.size(); ++i)
	{
		auto num = cons[i]->GetTotal();
		totalConsumed += num;
		_dbg_ << cons[i]->GetName() << " : " 
			 << cons[i]->GetElapsedTime() << " secs. " << num << " consumed\n";
		totalElapsedCons += cons[i]->GetElapsedTime();
		auto lastc = cons[i]->GetLastObj().GetIndex();
		if (lastc > lastConsumed) lastConsumed = lastc;
	}
	cons.clear();

	// print timings
	auto totalMsgsProd = totalProduced;
	auto totalMsgsCons = totalConsumed;
	auto msecPerProd = 1000.0f*totalElapsedProd/totalMsgsProd;
	auto msecPerCons = 1000.0f*totalElapsedCons/totalMsgsProd;
	auto usecPerProd = 1000.0f*msecPerProd;
	auto usecPerCons = 1000.0f*msecPerCons;
	// producer performance stat.
	//std::cout << buffer_.BufElemSize()
	//	<< " ----------- " << usecPerProd * 100
	//	<< "( " << totalMsgsProd << " messages produced )" << std::endl;

	std::cout << "------Buffer : " << buffer_.BufSize() << "x"
		 << buffer_.BufElemSize() << " = " << buffer_.BufSize()*buffer_.BufElemSize()
		 << std::endl;
	std::cout << "------Number of producers : " << numProd_ << ", Total produced "
		 << totalMsgsProd << " (" << totalElapsedProd << "s -- "
		 << usecPerProd << " usec/msg)" << std::endl;
	std::cout << "------Number of consumers : " << numCons_ << ", Total consumed "
		 << totalMsgsCons << " (" << totalElapsedCons << "s -- "
		 << usecPerCons << " usec/msg)" << std::endl;
	_dbg_ << "Last produced " << lastProduced << ", last consumed " << lastConsumed << std::endl;
	if (numProd_ <= 1 && numCons_ <= 1) // this sanity test valid only for single prod and single cons
	{
		if ( (lastProduced != (totalMsgsProd-1) ) || (lastConsumed != (totalMsgsCons-1) ) )
		{
			std::cout << "ERROR: mismatch between produced and consumed\n";
		}
		else
			_dbg_ << "Produced and consumed match numbers\n";
	}
}


int main(int argc, char** argv)
{
	int numProd = g_NumProd, numCons = g_NumCons;
	_dbg_ << "Num args :  " << argc << std::endl;
	if (argc == 3)
	{
		sscanf_s(argv[1], "%d", &numProd);
		sscanf_s(argv[2], "%d", &numCons);
	}
	else
	{
		std::cout << "Usage: Messenger <num prod> <num cons>\n";
		std::cout << "No args provided. Taking defaults: "
			<< numProd << " producer(s), "
			<< numCons << " consumer(s)\n" << std::endl;
	}
	// buffer rows x columns = 10 million
	static const auto BufSize = 10'000'000;
	static const auto NumColumns = 1;
	typedef Messenger::MBuffer<BufSize, NumColumns, MsgType<int64_t>> BufType;
	auto buffer = std::make_unique<BufType>();

	// vary number of columns from 1 (min) to BufSize (max)
	// and verify the performance.
	std::cout << "Buffer row x column size  vs usec/message\n";
	std::cout << "------------------------------------------------------\n";
	for (auto numCols = 1u; numCols <= BufSize; numCols *= 10)
	{
		if (numCols >= 10) {
			// consider half of the column value as well.
			// Thus we will have number of columns: 1,5,10,50,100,500,1000...
			auto numColsTmp = numCols/2;
			auto numRows = BufSize / numColsTmp;
			buffer->Reset();
			buffer->SetRowsColumns(numRows, numColsTmp);
			RunProducersConsumers(numProd, numCons, *buffer);
		}
		size_t numRows = BufSize / numCols;
		buffer->Reset();
		buffer->SetRowsColumns(numRows, numCols);
		RunProducersConsumers(numProd, numCons, *buffer);
	}
	_dbg_ << ">>>>>>>> DEBUG print ON\n";
	_dbg_ << "End of simulation\n";
}
