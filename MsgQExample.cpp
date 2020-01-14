/*! \file MsgQExample.cpp
\brief  Client program to use multi buffer Q.

Synchronised between multiple producer and consumer threads.
*/
#include "MBuffer.h"
#include <iostream>
#include <string>
#include <vector>
#include <exception>      // std::exception
#include <cstdio>
#include <future>


// default number of producers, consumers
static const int g_NumProd = 2;
static const int g_NumCons = 2;
typedef int64_t ObjectType;

//! Producer thread
template<typename TBuffer>
class Producer
{
	typedef typename TBuffer::ValueType ObjType;
	bool   m_stop; // when true, producer stops
	TBuffer&     m_buffer; // buffer to write to
	size_t      m_numObjs;
	std::thread m_thread; // default constructed thread

public:
	Producer(TBuffer& buf_) :
		m_stop(false), m_buffer(buf_), m_numObjs(0)
	{
		m_thread = std::thread(ThreadFuncForProducer, this);
	}
	~Producer()
	{
		std::cout << m_numObjs << " values produced in this thread\n";
	}

	// launch thread
	void Start()
	{
	}
	// implement thread func code here
	void Run()
	{
		while (!m_stop)
		{
			// produce: get next row to produce and fill object values
			size_t absRow;
			size_t row = m_buffer.GetNextLocForProd(absRow);
			if (m_stop || row == -1) break;
			ObjType* arr = &m_buffer[row][0];
			for (size_t col = 0; (col < m_buffer.BufElemSize()) && (!m_stop); ++col)
			{
				// value to produce is the next sequential element
				arr[col] = absRow*m_buffer.BufElemSize() + col;
				//cout << "Wrote " << m_buffer[row][col] << " at [" << row << "][" << col << "], absRow " << absRow << endl;
				++m_numObjs;
			}
			m_buffer.SetLocReadyForCons(row); // all elements in row written. release this row to consumer
		}
	}

	std::thread&	GetThread() { return m_thread; }

	// flag to stop : called from some other thread
	void Stop() {
		m_stop = true;
		m_buffer.Stop();
	}

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
	bool   m_stop; // when true, consumer stops
	TBuffer&    m_buffer; // buffer to read from
	size_t      m_numObjs;
	std::thread m_thread; // default constructed thread

public:
	Consumer(TBuffer& buffer_) :
		m_stop(false), m_buffer(buffer_), m_numObjs(0)
	{
		//m_thread = std::thread(ThreadFuncForConsumer, this);
		std::async(std::launch::async, ThreadFuncForConsumer, this);
	}
	~Consumer()
	{
		std::cout << m_numObjs << " vaues consumed in this thread\n";
	}
	// launch thread
	void Start()
	{
	}
	// implement thread func code here
	void Run()
	{
		while (!m_stop)
		{
			size_t absRow;
			size_t row = m_buffer.GetNextLocForCons(absRow);
			if (m_stop || row == -1) break;
			ObjectType* arr = &m_buffer[row][0];
			for (size_t col = 0; (col < m_buffer.BufElemSize()) && (!m_stop); ++col)
			{
				ObjectType curObj = arr[col];
				//cout << "Read " << curObj << " at [" << absRow << "][" << col << "] "  << endl;
				++m_numObjs;
			}
			m_buffer.SetLocReadyForProd(row); // all elements in row read. release this row to producer
		}
	}

	std::thread& GetThread() { return m_thread; }

	// flag to stop : called from some other thread
	void Stop() {
		m_stop = true;
		m_buffer.Stop();
	}

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
	std::vector<Producer<TBuffer>*> prods(numProd_);
	std::vector<Consumer<TBuffer>*> cons(numCons_);

	for (size_t i = 0; i < prods.size(); ++i)
	{
		prods[i] = new Producer<TBuffer>(buffer_);
	}
	for (size_t i = 0; i < cons.size(); ++i)
	{
		cons[i] = new Consumer<TBuffer>(buffer_);
	}

	{
		for (size_t i = 0; i < prods.size(); ++i)
		{
			//prods[i]->Start();
		}
		for (size_t i = 0; i < cons.size(); ++i)
		{
			//cons[i]->Start();
		}
		const int numSecs = 5;
		std::cout << "Sleep for " << numSecs << " seconds\n";
		std::this_thread::sleep_for(std::chrono::seconds(numSecs));
		std::cout << "Stopping producers and consumers\n";
		for (size_t i = 0; i < prods.size(); ++i)
		{
			prods[i]->Stop();
		}
		for (size_t i = 0; i < cons.size(); ++i)
		{
			cons[i]->Stop();
		}

		// wait for all  threads to complete
		std::cout << "Waiting for producers and consumers to complete\n";
		for (size_t i = 0; i < cons.size(); ++i)
		{
			cons[i]->GetThread().join();
			delete cons[i];
		}
		for (size_t i = 0; i < prods.size(); ++i)
		{
			prods[i]->GetThread().join();
			delete prods[i];
		}
	}
}


int main(int argc, char** argv)
{
	int numProd = g_NumProd, numCons = g_NumCons;
	// buffer rows x columns = 1 million
	static const size_t BufSize = 1000000;
	static const size_t NumColumns = 100;
	static const size_t NumRows = BufSize / NumColumns;
	typedef Messenger::MBuffer<NumRows, NumColumns, int64_t> BufType;
	std::unique_ptr<BufType> buffer(new BufType());

	RunProducersConsumers(numProd, numCons, *buffer);
	std::cout << "End of simulation\n";
}
