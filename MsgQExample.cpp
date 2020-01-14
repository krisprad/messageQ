/*! \file MsgQExample.cpp
\brief  Client program to use multi buffer Q.

Synchronised between multiple producer and consumer threads.
*/
#include "MBuffer.h"
#include <iostream>
#include <string>
#include <vector>
#include <exception>      // std::exception
#include <thread>         // std::thread, std::this_thread::sleep_for


// default number of producers, consumers
static const auto g_NumProd = 2;
static const auto g_NumCons = 2;
typedef int64_t ObjectType;

//! Producer thread
template<typename TBuffer>
class Producer
{
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
			auto* arr = &m_buffer[row][0];
			for (size_t col = 0; (col < m_buffer.BufElemSize()) && (!m_stop); ++col)
			{
				// value to produce is the next sequential element
				arr[col] = absRow*m_buffer.BufElemSize() + col;
				//cout << "Wrote " << m_buffer[row][col] << " at ["
				//     << row << "][" << col << "], absRow " << absRow << endl;
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
	bool   m_stop; // when true, consumer stops
	TBuffer&    m_buffer; // buffer to read from
	size_t      m_numObjs;
	std::thread m_thread; // default constructed thread

public:
	Consumer(TBuffer& buffer_) :
		m_stop(false), m_buffer(buffer_), m_numObjs(0)
	{
		m_thread = std::thread(ThreadFuncForConsumer, this);
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
			auto row = m_buffer.GetNextLocForCons(absRow);
			if (m_stop || row == -1) break;
			auto* arr = &m_buffer[row][0];
			for (auto col = 0u; (col < m_buffer.BufElemSize()) && (!m_stop); ++col)
			{
				auto curObj = arr[col];
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
void RunProducersConsumers(size_t numProd_, size_t numCons_, TBuffer& buffer_)
{
	std::vector<std::unique_ptr<Producer<TBuffer>>> prods;
	std::vector<std::unique_ptr<Consumer<TBuffer>>> cons;

	for (auto i = 0u; i < numProd_; ++i)
	{
		auto p = std::make_unique<Producer<TBuffer>>(buffer_);
		prods.push_back(std::move(p));
	}
	for (auto i = 0u; i < numCons_; ++i)
	{
		auto c = std::make_unique<Consumer<TBuffer>>(buffer_);
		cons.push_back(std::move(c));
	}

	{
		for (auto i = 0u; i < numProd_; ++i)
		{
			prods[i]->Start();
		}
		for (auto i = 0u; i < numCons_; ++i)
		{
			cons[i]->Start();
		}
		const auto numSecs = 5;
		std::cout << "Sleep for " << numSecs << " seconds\n";
		std::this_thread::sleep_for(std::chrono::seconds(numSecs));
		std::cout << "Stopping producers and consumers\n";
		for (auto i = 0u; i < prods.size(); ++i)
		{
			prods[i]->Stop();
		}
		for (auto i = 0u; i < numCons_; ++i)
		{
			cons[i]->Stop();
		}

		// wait for all  threads to complete
		std::cout << "Waiting for producers and consumers to complete\n";
		for (auto i = 0u; i < numCons_; ++i)
		{
			cons[i]->GetThread().join();
		}
		for (auto i = 0u; i < numProd_; ++i)
		{
			prods[i]->GetThread().join();
		}
	}
}


int main(int argc, char** argv)
{
	auto  numProd = g_NumProd, numCons = g_NumCons;
	// buffer rows x columns = 1 million
	static const auto BufSize = 1000000;
	static const auto  NumColumns = 100;
	static const auto  NumRows = BufSize / NumColumns;
	typedef Messenger::MBuffer<NumRows, NumColumns, int64_t> BufType;
	auto buffer = std::make_unique<BufType>();

	RunProducersConsumers(numProd, numCons, *buffer);
	std::cout << "End of simulation\n";
}
