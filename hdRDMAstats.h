

#include <thread>
#include <vector>
#include <string>

#if HAVE_ZEROMQ
#include <zmq.h>
#endif // HAVE_ZEROMQ

class hdRDMAstats{

	public:
		hdRDMAstats(int port);
		~hdRDMAstats(void);
		
		void Publish(void);
		void HostStatusPROC(std::map<std::string,float> &vals);
		void GetDiskSpace(std::string dirname, std::map<std::string,float> &vals);

		bool done = false;
		int port = 0;
		std::thread *thr = nullptr;
};




