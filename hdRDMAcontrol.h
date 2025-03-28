//
// Created by hdsys on 11/21/19.
//

#ifndef PACKAGES_HDRDMACONTROL_H
#define PACKAGES_HDRDMACONTROL_H

#include <thread>
#include <vector>
#include <string>

#if HAVE_ZEROMQ
#include <zmq.h>
#endif // HAVE_ZEROMQ

class hdRDMAcontrol{
	public:

		hdRDMAcontrol(int port);
		~hdRDMAcontrol(void);

		void ServerLoop(void);

		bool done = false;
		int port = 0;
		std::thread *thr = nullptr;


};

#endif //PACKAGES_HDRDMACONTROL_H
