//
// Created by hdsys on 11/21/19.
//

#include <unistd.h>
#include <string.h>
#include <sys/statvfs.h>
#include <sys/stat.h>

#include <iostream>
#include <map>
#include <string>
#include <sstream>
#include <fstream>
#include <atomic>
#include <cmath>
#include <iterator>
#include <set>
#include <mutex>
using namespace std;

#include "hdRDMAcontrol.h"

extern bool QUIT;
extern void *HDRDMA_ZEROMQ_CONTEXT;
extern int HDRDMA_RET_VAL;

extern std::mutex HDRDMA_RECV_FNAMES_MUTEX;
extern std::set<string> HDRDMA_RECV_FNAMES;

extern atomic<uint64_t> BYTES_RECEIVED_TOT;
extern atomic<uint64_t> NFILES_RECEIVED_TOT;
extern double HDRDMA_LAST_RATE_10sec;
extern double HDRDMA_LAST_RATE_1min;
extern double HDRDMA_LAST_RATE_5min;

//-------------------------------------------------------------
// hdRDMAcontrol
//-------------------------------------------------------------
hdRDMAcontrol::hdRDMAcontrol(int port):port(port)
{

	cout << "Launching hdRDMAcontrol thread ..." << endl;
	thr = new std::thread( &hdRDMAcontrol::ServerLoop, this );

}

//-------------------------------------------------------------
// ~hdRDMAcontrol
//-------------------------------------------------------------
hdRDMAcontrol::~hdRDMAcontrol(void)
{
	done = true;
	if(thr != nullptr) {
		cout << "Joining hdRDMAcontrol thread ..." << endl;
		thr->join();
		cout << "hdRDMAcontrol thread joined." << endl;
	}
}

//-------------------------------------------------------------
// ServerLoop
//-------------------------------------------------------------
void hdRDMAcontrol::ServerLoop(void)
{
	cout << "hdRDMAcontrol::Publish called" << endl;

	pthread_setname_np( pthread_self(), "hdRDMAcontrol" );

#if HAVE_ZEROMQ
	char bind_str[256];
	sprintf( bind_str, "tcp://*:%d", port );

	void *responder = zmq_socket( HDRDMA_ZEROMQ_CONTEXT, ZMQ_REP );
	int rc = zmq_bind( responder, bind_str);
	if( rc != 0 ){
		cout << "Unable to bind zeroMQ control socket " << port << "!" << endl;
		perror("zmq_bind");
		return;
	}

	// All messages will include host
	char host[256];
	gethostname( host, 256 );

	// Loop until told to quit
	while( !done && ! QUIT){

		// Listen for request (non-blocking)
		char buff[2048];
		auto rc = zmq_recv( responder, buff, 2048, ZMQ_DONTWAIT);
		if( rc< 0 ){
			if( (errno==EAGAIN) || (errno==EINTR) ){
				std::this_thread::sleep_for(std::chrono::milliseconds(250));
				continue;
			}else{
				cerr << "ERROR listening on control socket: errno=" << errno << endl;
				done = true;
				continue;
			}
		}

		// Split request into tokens
		std::vector<string> vals;
		istringstream iss( std::string(buff, rc) );
		copy( istream_iterator<string>(iss), istream_iterator<string>(), back_inserter(vals) );

		// Response
		stringstream ss;

		if( vals.empty()){
			ss << "<Empty Message>";
		}else if( vals[0] == "quit" ){
			QUIT = true;
			done = true;
			HDRDMA_RET_VAL = vals.size()>1 ? atoi(vals[1].c_str()):-1; // allow remote user to set return value. default to -1 so system service will restart
			ss << "OK";
		}else if( vals[0] == "reset_counters" ){
			BYTES_RECEIVED_TOT     = 0;
			NFILES_RECEIVED_TOT    = 0;
			HDRDMA_LAST_RATE_10sec = 0.0;
			HDRDMA_LAST_RATE_1min  = 0.0;
			HDRDMA_LAST_RATE_5min  = 0.0;
			ss << "OK";
		}else if( vals[0] == "get_file_size" ){ // mulitple files may be specified
			if( vals.size()<2){
				ss << "<NO FILE GIVEN>";
			}else{
				for( uint32_t i=1; i<vals.size(); i++){

					auto &fname = vals[i];

					// Wait up to 10 secs for file to finish writing if it is currently being written to
					bool timedout = true;
					for( int i=0; i<20; i++ ){
						HDRDMA_RECV_FNAMES_MUTEX.lock();
						bool is_writing = (HDRDMA_RECV_FNAMES.count(fname) != 0);
						HDRDMA_RECV_FNAMES_MUTEX.unlock();
//						cout << "is_writing: " << is_writing << endl;
						if( !is_writing ) {
							timedout = false;
							break;
						}
						std::this_thread::sleep_for( std::chrono::milliseconds(500) );
					}

					struct stat st;
					int64_t fsize = -1;
					if( stat(fname.c_str(), &st) == 0) fsize = (int64_t)st.st_size;
					if( timedout ) ss << "!"; // prefix filename with "!" if we timed out waiting for transfer to complete
					ss << fname << " " << fsize << "\n";
				}
			}
		}else{
			ss << "Bad command: " << vals[0] << "\n";
			ss << "valid commands are:\n";
			ss << "   quit\n";
			ss << "   reset_counters\n";
			ss << "   get_file_size file1 [file2 ...]\n";
		}

		zmq_send( responder, ss.str().data(), ss.str().length(), 0);
	}
//
//	if(publisher != nullptr) zmq_close(publisher);
//	if(context   != nullptr) zmq_ctx_destroy(context);
#endif // HAVE_ZEROMQ

}
