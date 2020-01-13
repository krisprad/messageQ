/*! \file MBuffer.cpp
\brief  Client program to use multi buffer Q.

Synchronised between multiple producer and consumer threads.
*/
#include "MBuffer.h"
#include <iostream>
#include <string>
#include <vector>
#include <exception>      // std::exception
#include <cstdio>
#include <thread>         // std::thread, std::this_thread::sleep_for
#include <ctime>
#include <ratio>
#include <chrono>


using namespace Messenger;
using namespace std;

// uncomment this for debug output (very long for large buffers)
// #define DEBUG_MSG

// default number of producers, consumers
static const int g_NumProd = 2;
static const int g_NumCons = 2;


//! Dbg: forwards requests to ostream
/*! This prints output messages if ostream is supplied,
    otherwise the print calls are nop-s.
	Works similar to cout.
*/
class Dbg // debug output printed, if _os is not NULL
{
	ostream*_os;
public:
	Dbg(ostream* os_ = NULL) : _os(os_) {}

	template<typename T>
	Dbg& operator<<(const T& t_)
	{
		if (_os)
			*_os << t_;
		return *this;
	}
	// support endl
	Dbg& operator<<(ostream& (*f)(ostream&))
	{
		if (_os)
			*_os << f;
		return *this;
	}
};

// enable/suppress debug output
#ifdef DEBUG_MSG
Dbg	_dbg_(&cout);
#else
Dbg	_dbg_;
#endif

//! timer class to measure duration.
class TimeKeeper
{
	std::chrono::steady_clock::time_point m_start;
	std::chrono::steady_clock::time_point m_end;
	string m_name;
public:
	TimeKeeper(const string& n_) : m_name(n_)
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
		steady_clock::duration dur = m_end - m_start;
		long long ticks = dur.count(); // ticks of nanoseconds
		double seconds = double(ticks)*steady_clock::period::num / steady_clock::period::den;
		return seconds;
	}
	void PrintElasped()
	{
		double seconds = getElapsedTime();
		_dbg_ << "Elapsed time for " << m_name << ": " << seconds << " secs\n";
	}

};

//! Object type of message
template<typename T> class ObjType;
//! Object type of message: int specialization
template<> class ObjType<int64_t>
{
	int64_t m_obj;
public:
	ObjType(int64_t obj_ = 0) : m_obj(obj_) {}
	//!Get object index for int val: same as value
	/*! index is used to do sanity check.
	    An object produced at absolute location
		x must have index x.
		Absolute location in the buffer[row][col] =
		row*number-of-columns-per-row + col
	*/
	int64_t GetIndex() { return m_obj; }
	bool operator<(const ObjType& obj_) { return m_obj < obj_.m_obj; }
	bool operator!=(const ObjType& obj_) { return m_obj != obj_.m_obj; }
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
template<> class ObjType<std::string>
{
	std::string m_obj;
	int64_t m_objIndex;
public:
	ObjType(int64_t obj_) : m_obj(to_string(obj_)), m_objIndex(obj_) {}
	ObjType(std::string obj_="0") : m_obj(obj_), m_objIndex(stoll(obj_)) {}
	//!Get object index
	/*! index is used to do sanity check.
	An object produced at absolute location
	x must have index x.
	Absolute location in the buffer[row][col] =
	row*number-of-columns-per-row + col
	*/
	int64_t GetIndex() { return m_objIndex; }
	bool operator<(const ObjType& obj_) { return m_objIndex < obj_.m_objIndex; }
	bool operator!=(const ObjType& obj_) { return m_obj != obj_.m_obj; }
	std::string Value() const { return m_obj; }
	void Value(const int64_t obj_) { m_obj = to_string(obj_); }
};

//! output a message element
template<typename T>
std::ostream& operator<<(std::ostream& os_, const ObjType<T>& obj_)
{
	os_ << obj_.Value();
	return os_;
}

//! object generator
/*!
   T is expected to be an integral type.
   specialize for non-integral ones.
*/
template<typename T>
struct ObjectGenerator
{
	// called by producer with unique index. 
	// Return an object appropriate for the  index.
	// In this example, our object is same as the index. 
	// The index can be used by obj generator to 'stamp' its objects
	// with unique values.
	T	GetNext(size_t index_) { return index_; }
};

template<>
struct ObjectGenerator<std::string>
{
	// called by producer with unique index. 
	// Return an object appropriate for the  index.
	// In this example, our object is same as the index. 
	// The index can be used by obj generator to 'stamp' its objects
	// with unique values.
	std::string	GetNext(size_t index_) { 
		return to_string(index_);
	}
};

//! Producer thread
template<typename TBuffer>
class Producer
{
	typedef typename TBuffer::ValueType ObjType;
	string m_name; // name of the producer
	bool   m_stop; // when true, producer stops
	size_t m_numObjs; // number of objects produced
	double	m_timeElapsed; // total time elapsed
	ObjType		m_lastObj; // value of the last object
	ObjectGenerator<ObjType>	m_objGenerator; // generator to get objects produced 
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
	void SetName(const char* s_) { m_name = s_; }
	const string& GetName() const { return m_name; }

	// launch thread
	void Start()
	{
	}
	// implement thread func code here
	void Run()
	{
		size_t	lastLoc = -1; // last location produced as if array were infinite
		size_t	lastAbsRow = -1; // last row as if array were infinite 
		size_t	lastCol = -1; // last column within a row
		// Thus lastLoc = lastAbsRow*columns-per-row + lastCol 
		// when the values are not -1
	
		TimeKeeper sw("Producer Timekeeper");
		sw.startTimer();
		while (!m_stop)
		{
			// produce: get next row to produce and fill object values
			size_t absRow;
			_dbg_ << "prod: " << m_name << " get next loc - ";
			size_t row = m_buffer.GetNextLocForProd(absRow);
			if (row >= m_buffer.BufSize() )
			{
				_dbg_ << m_name << " : Illegal row " << row << ". Buffer probably stopped\n";
				break;
			}
			if (m_stop) break;
			ObjType* arr = (ObjType*) &m_buffer[row][0];
			size_t col = 0;
			for (; (col < m_buffer.BufElemSize() ) && (!m_stop) ; ++col)
			{
				lastLoc = absRow*m_buffer.BufElemSize() + col; // produced until this loc
				ObjType nextProdVal = m_objGenerator.GetNext(lastLoc);
				arr[col] = nextProdVal;
				_dbg_ << "----nextProdVal " << nextProdVal << ", col " << col
					   << ", arr[col] " << arr[col] << endl;
				_dbg_ << m_name << ": absRow " << absRow << ", row " << row
					<< ", col " << col << ", lastLoc " << lastLoc  
					<< ", nextProdVal " << nextProdVal
					<< ", arr[col] " << arr[col]
					<< endl;
				_dbg_ << "Wrote " << m_buffer[row][col] << " at [" << row << "][" << col << "], absRow " << absRow << endl; 
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
	string m_name; // name of the consumer
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
	void SetName(const char* s_) { m_name = s_; }
	const string& GetName() const { return m_name; }

	// launch thread
	void Start()
	{
	}
	// implement thread func code here
	void Run()
	{
		TimeKeeper sw("Consumer Timekeeper");
		ObjType	prevObj = -1;
		ObjType	curObj = 0;
		size_t	lastLoc = -1; // last location consumed as if array were infinite
		size_t	lastAbsRow = -1; // last row as if array were infinite 
		size_t	lastCol = -1; // last column within a row
							  // Thus lastLoc = lastAbsRow*columns-per-row + lastCol 
							  // when the values are not -1

		sw.startTimer();
		while (!m_stop)
		{
			// consumer
			_dbg_ << "Get loc for " << m_name << endl;
			size_t absRow;
			_dbg_ << "cons: " << m_name << " get next consloc " << endl;
			size_t row = m_buffer.GetNextLocForCons(absRow);
			_dbg_ << "cons: " << m_name << " got next consloc, absRow " 
				<< absRow << ", row " << row << endl;
			if (row >= m_buffer.BufSize() )
			{
				_dbg_ << m_name << " : Illegal row " << row << ". Buffer probably stopped\n";
				break;
			}
			if (m_stop) break;
			ObjType* arr = (ObjType*) &m_buffer[row][0];
			size_t col = 0;
			for (; (col < m_buffer.BufElemSize() ) && (!m_stop) ; ++col)
			{
				curObj = arr[col];
				_dbg_ << "Read " << curObj << " at [" << row << "][" << col << "], absRow " << absRow << endl;
				if (curObj < prevObj)
				{
					cout << "Error: at [" << row << "][" << col << "] absRow " << absRow 

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
					cout << "Error: at [" << row << "][" << col << "] last loc " << lastLoc << " not same as cur obj " << curObj << ". Consumed wrong obj\n";
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
void RunProducersConsumers(int numProd_, int numCons_, TBuffer& buffer_)
{
	typedef TBuffer::ValueType ObjType;
	_dbg_ << " Number of producers " << numProd_ << endl;
	vector<Producer<TBuffer>*> prods(numProd_);
	_dbg_ << " Number of consumers " << numCons_ << endl;
	vector<Consumer<TBuffer>*> cons(numCons_);
	int64_t lastProduced = -1;
	int64_t lastConsumed = -1;

	for (size_t i = 0; i < prods.size(); ++i)
	{
		prods[i] = new Producer<TBuffer>(buffer_);
		char s[10];
		sprintf_s(s, sizeof(s), "prod %03d", i);
		prods[i]->SetName(s);
	}
	for (size_t i = 0; i < cons.size(); ++i)
	{
		cons[i] = new Consumer<TBuffer>(buffer_);
		char s[10];
		sprintf_s(s, sizeof(s), "cons %03d", i);
		cons[i]->SetName(s);
	}

	{
		TimeKeeper tk("All prod-cons");

		for (size_t i = 0; i < prods.size(); ++i)
		{
			//prods[i]->Start();
			_dbg_ << prods[i]->GetName() << " Handle " << prods[i]->GetThread().get_id() << endl;
		}
		for (size_t i = 0; i < cons.size(); ++i)
		{
			//cons[i]->Start();
			_dbg_ << cons[i]->GetName() << " Handle " << cons[i]->GetThread().get_id() << endl;
		}
		const int numSecs = 5;
		_dbg_ << "Sleep for " << numSecs << " seconds\n";
		std::this_thread::sleep_for(std::chrono::seconds(numSecs));
		_dbg_ << "Stopping producers and consumers\n";
		for (size_t i = 0; i < prods.size(); ++i)
		{
			prods[i]->Stop();
		}
		for (size_t i = 0; i < cons.size(); ++i)
		{
			cons[i]->Stop();
		}

		// wait for all  threads to complete
		_dbg_ << "Waiting for producers and consumers to complete\n";
		for (size_t i = 0; i < cons.size(); ++i)
		{
			cons[i]->GetThread().join();
		}
		for (size_t i = 0; i < prods.size(); ++i)
		{
			prods[i]->GetThread().join();
		}
	}
	size_t totalProduced = 0, totalConsumed = 0;
	double	totalElapsedProd = 0, totalElapsedCons = 0;
	for (size_t i = 0; i < prods.size(); ++i)
	{
		size_t num = prods[i]->GetTotal();
		totalProduced += num;
		_dbg_ << prods[i]->GetName() << " : " 
			 << prods[i]->GetElapsedTime() << " secs. " << num << " produced\n";
		totalElapsedProd += prods[i]->GetElapsedTime();
		int64_t lp = prods[i]->GetLastObj().GetIndex();
		if (lp > lastProduced) lastProduced = lp;

		delete prods[i];
		prods[i] = NULL;
	}
	for (size_t i = 0; i < cons.size(); ++i)
	{
		size_t num = cons[i]->GetTotal();
		totalConsumed += num;
		_dbg_ << cons[i]->GetName() << " : " 
			 << cons[i]->GetElapsedTime() << " secs. " << num << " consumed\n";
		totalElapsedCons += cons[i]->GetElapsedTime();
		int64_t lc = cons[i]->GetLastObj().GetIndex();
		if (lc > lastConsumed) lastConsumed = lc;

		delete cons[i];
		cons[i] = NULL;
	}
	// print timings
	size_t totalMsgsProd = totalProduced;
	size_t totalMsgsCons = totalConsumed;
	double msecPerProd = 1000.0f*totalElapsedProd/totalMsgsProd;
	double msecPerCons = 1000.0f*totalElapsedCons/totalMsgsProd;
	double usecPerProd = 1000.0f*msecPerProd;
	double usecPerCons = 1000.0f*msecPerCons;
	// producer performance stat.
	// Log of number of elem per buffer * 10.
	// production time per elem (in microsec) * 100
	//cout << 10 * log(buffer_.BufElemSize()) + 1
	//	<< " ----------- " << usecPerProd * 100 << endl;
	cout << buffer_.BufElemSize()
		<< " ----------- " << usecPerProd * 100 << endl;

	//cout << "------Buffer : " << buffer_.BufSize() << "x"
	//	 << buffer_.BufElemSize() << " = " << buffer_.BufSize()*buffer_.BufElemSize()
	//	 << endl;
	//cout << "------Number of producers : " << numProd_ << ", Total produced "
	//	 << totalMsgsProd << " (" << totalElapsedProd << "s -- "
	//	 << usecPerProd << " usec/msg)" << endl;
	//cout << "------Number of consumers : " << numCons_ << ", Total consumed "
	//	 << totalMsgsCons << " (" << totalElapsedCons << "s -- "
	//	 << usecPerCons << " usec/msg)" << endl;
	_dbg_ << "Last produced " << lastProduced << ", last consumed " << lastConsumed << endl;
	if (numProd_ <= 1 && numCons_ <= 1) // this sanity test valid only for single prod and single cons
	{
		if ( (lastProduced != (totalMsgsProd-1) ) || (lastConsumed != (totalMsgsCons-1) ) )
		{
			cout << "ERROR: mismatch between produced and consumed\n";
		}
		else
			_dbg_ << "Produced and consumed match numbers\n";
	}
}


int main(int argc, char** argv)
{
	int numProd = g_NumProd, numCons = g_NumCons;
	_dbg_ << "Num args :  " << argc << endl;
	if (argc == 3)
	{
		sscanf_s(argv[1], "%d", &numProd);
		sscanf_s(argv[2], "%d", &numCons);
	}
	else
	{
		cout << "Usage: Messenger <num prod> <num cons>\n";
		cout << "Taking defaults: Messenger 1 1\n";
	}
	// buffer rows x columns = 10 million
	static const size_t BufSize = 10000000;
	static const size_t NumColumns = 1;
	typedef MBuffer<BufSize, NumColumns, ObjType<int64_t>> BufType;
	std::unique_ptr<BufType> buffer(new BufType());

	// vary number of columns from 1 (min) to BufSize (max)
	// and verify the performance.
	//cout << "Buffer row size [10*log(row size) + 1] vs usec/message\n";
	cout << "Buffer row size  vs 100*usec/message\n";
	cout << "------------------------------------------------------\n";
	for (size_t numCols = 1; numCols <= BufSize; numCols *= 10)
	{
		if (numCols >= 10) {
			// consider half of the column value as well.
			// Thus we will have number of columns: 1,5,10,50,100,500,1000...
			size_t numColsTmp = numCols/2;
			size_t numRows = BufSize / numColsTmp;
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
