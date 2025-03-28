//
// This is used to periodically publish zeroMQ pub/sub messages 
// with JSON formatted data regarding status of the process.
// It is only used if the hdrdmacp command was invoced with the
// -zp option to specify the tcp port on which to publish.
//
//

#include <unistd.h>
#include <string.h>
#include <sys/statvfs.h>

#include <iostream>
#include <map>
#include <string>
#include <sstream>
#include <fstream>
#include <atomic>
#include <cmath>
using namespace std;

#include "hdRDMAstats.h"

// The following are convenience macros that make
// the code below a little more succinct and easier
// to read. It changes this:
//
//  JSONADD(key) << val;
//
//   to
//
//  ss<<"\n,\""<<key<<"\":" << val;
//
// The JSONADS version takes a second argument and
// encapsulates it in double quotes in the JSON output.
#define JSONADD(K) ss<<"\n,\""<<K<<"\":"
#define JSONADS(K,V) ss<<"\n,\""<<K<<"\":\""<<V<<"\""

extern const char *HDRDMA_VERSION;
extern void *HDRDMA_ZEROMQ_CONTEXT;
extern int HDRDMA_ZEROMQ_STATS_PORT;
extern atomic<uint64_t> BYTES_RECEIVED_TOT;
extern atomic<uint64_t> NFILES_RECEIVED_TOT;
extern double HDRDMA_LAST_RATE_10sec;
extern double HDRDMA_LAST_RATE_1min;
extern double HDRDMA_LAST_RATE_5min;
extern uint64_t HDRDMA_BUFF_LEN_GB;
extern uint64_t HDRDMA_NUM_BUFF_SECTIONS;


//-------------------------------------------------------------
// hdRDMAstats
//-------------------------------------------------------------
hdRDMAstats::hdRDMAstats(int port):port(port)
{
	cout << "Launching hdRDMAstats thread ..." << endl;
	thr = new std::thread( &hdRDMAstats::Publish, this );
	
}

//-------------------------------------------------------------
// ~hdRDMAstats
//-------------------------------------------------------------
hdRDMAstats::~hdRDMAstats(void)
{
	done =true;
	if(thr != nullptr) {
		cout << "Joining hdRDMAstats thread ..." << endl;
		thr->join();
		cout << "hdRDMAstats thread joined." << endl;
	}

}

//-------------------------------------------------------------
// Publish
//-------------------------------------------------------------
void hdRDMAstats::Publish(void)
{
	cout << "hdRDMAstats::Publish called" << endl;

	pthread_setname_np( pthread_self(), "hdRDMAstats" );

#if HAVE_ZEROMQ
	char bind_str[256];
	sprintf( bind_str, "tcp://*:%d", HDRDMA_ZEROMQ_STATS_PORT );

	if( HDRDMA_ZEROMQ_CONTEXT == nullptr ) HDRDMA_ZEROMQ_CONTEXT = zmq_ctx_new();
	void *publisher = zmq_socket( HDRDMA_ZEROMQ_CONTEXT, ZMQ_PUB );
	int rc = zmq_bind( publisher, bind_str);
	if( rc != 0 ){
		cout << "Unable to bind zeroMQ stats socket " << HDRDMA_ZEROMQ_STATS_PORT << "!" << endl;
		perror("zmq_bind");
		return;
	}

	// All messages will include host
	char host[256];
	gethostname( host, 256 );

	// Loop until told to quit
	while( !done ){
	
		// Create JSON string
		stringstream ss;
		ss << "{\n";
		ss << "\"program\":\"hdrdmacp\"";

		JSONADS( "version", HDRDMA_VERSION);
		JSONADS( "host" ,host );
		JSONADD( "Nfiles"      ) << NFILES_RECEIVED_TOT;
		JSONADD( "TB_received" ) << BYTES_RECEIVED_TOT*1.0E-12;
		JSONADD( "avg10sec"    ) << HDRDMA_LAST_RATE_10sec; // GB/s (calculated in hdrdmacp.cc)
		JSONADD( "avg1min"     ) << HDRDMA_LAST_RATE_1min;  // GB/s (calculated in hdrdmacp.cc)
		JSONADD( "avg5min"     ) << HDRDMA_LAST_RATE_5min;  // GB/s (calculated in hdrdmacp.cc)
		
		// Get current system info from /proc
		map<string,float> vals;
		HostStatusPROC(vals);
		
		// Get disk info
		GetDiskSpace("/media/ramdisk", vals);
		GetDiskSpace("/data1", vals);
		GetDiskSpace("/data2", vals);
		GetDiskSpace("/data3", vals);
		GetDiskSpace("/data4", vals);

		for( auto p : vals ) JSONADD(p.first) << p.second;

		ss << "\n}\n";		
		
		// Publish record
		zmq_send( publisher, ss.str().c_str(), ss.str().length(), 0 );
		
		// Only send updates once every 3 seconds. According to
		// zeroMQ documentation, this doesn't send anything out
		// if there are no current subscribers.
		sleep(3);		
	}

	if(publisher != nullptr) zmq_close(publisher);
#else
	cout << "No ZMQ support. Stats will not be published." << endl;
#endif // HAVE_ZEROMQ
}

//---------------------------------
// HostStatusPROC  (Linux)
//---------------------------------
void hdRDMAstats::HostStatusPROC(std::map<std::string,float> &vals)
{
	/// Get host info using the /proc mechanism on Linux machines.
	/// This returns the CPU usage/idle time. In order to work, it
	/// it needs to take two measurements separated in time and
	/// calculate the difference. So that we don't linger here
	/// too long, we maintain static members to keep track of the
	/// previous reading and take the delta with that.
	
	//------------------ CPU Usage ----------------------
	static time_t last_time = 0;
	static double last_user = 0.0;
	static double last_nice = 0.0;
	static double last_sys  = 0.0;
	static double last_idle = 0.0;
	static double delta_user = 0.0;
	static double delta_nice = 0.0;
	static double delta_sys  = 0.0;
	static double delta_idle = 1.0;

	time_t now = time(NULL);
	if(now > last_time){
		ifstream ifs("/proc/stat");
		if( ifs.is_open() ){
			string cpu;
			double user, nice, sys, idle;
	
			ifs >> cpu >> user >> nice >> sys >> idle;
			ifs.close();
			
			delta_user = user - last_user;
			delta_nice = nice - last_nice;
			delta_sys  = sys  - last_sys;
			delta_idle = idle - last_idle;
			last_user = user;
			last_nice = nice;
			last_sys  = sys;
			last_idle = idle;

			last_time = now;
		}
	}
	
	double norm = delta_user + delta_nice + delta_sys + delta_idle;
	double user_percent = 100.0*delta_user/norm;
	double nice_percent = 100.0*delta_nice/norm;
	double sys_percent  = 100.0*delta_sys /norm;
	double idle_percent = 100.0*delta_idle/norm;
	double cpu_usage    = 100.0 - idle_percent;

	vals["cpu_user" ] = user_percent;
	vals["cpu_nice" ] = nice_percent;
	vals["cpu_sys"  ] = sys_percent;
	vals["cpu_idle" ] = idle_percent;
	vals["cpu_total"] = cpu_usage;

	//------------------ Memory Usage ----------------------

	// Read memory from /proc/meminfo
	ifstream ifs("/proc/meminfo");
	int mem_tot_kB = 0;
	int mem_free_kB = 0;
	int mem_avail_kB = 0;
	if(ifs.is_open()){
		char buff[4096];
		bzero(buff, 4096);
		ifs.read(buff, 4095);
		ifs.close();

		string sbuff(buff);

		size_t pos = sbuff.find("MemTotal:");
		if(pos != string::npos) mem_tot_kB = atoi(&buff[pos+10+1]);

		pos = sbuff.find("MemFree:");
		if(pos != string::npos) mem_free_kB = atoi(&buff[pos+9+1]);

		pos = sbuff.find("MemAvailable:");
		if(pos != string::npos) mem_avail_kB = atoi(&buff[pos+14+1]);
	}

	// RAM
	// reported RAM from /proc/memInfo apparently excludes some amount
	// claimed by the kernel. To get the correct amount in GB, I did a
	// linear fit to the values I "knew" were correct and the reported
	// values in kB for several machines.
	int mem_tot_GB = (int)round(0.531161471 + (double)mem_tot_kB*9.65808E-7);
	vals["ram_tot_GB"] = mem_tot_GB;
	vals["ram_free_GB"] = mem_free_kB*1.0E-6;
	vals["ram_avail_GB"] = mem_avail_kB*1.0E-6;
	vals["HDRDMA_BUFF_LEN_GB"] = HDRDMA_BUFF_LEN_GB;
	vals["HDRDMA_NUM_BUFF_SECTIONS"] = HDRDMA_NUM_BUFF_SECTIONS;
}

//---------------------------------
// GetDiskSpace
//---------------------------------
void hdRDMAstats::GetDiskSpace(std::string dirname, std::map<std::string,float> &vals)
{
	// Attempt to get stats on the disk specified by dirname.
	// If found, they are added to vals. If no directory by
	// that name is found then nothing is added to vals and
	// this returns quietly.

	struct statvfs vfs;
	int err = statvfs(dirname.c_str(), &vfs);
	if( err != 0 ) return;

	double total_GB = vfs.f_bsize * vfs.f_blocks * 1.0E-9;
	double avail_GB = vfs.f_bsize * vfs.f_bavail * 1.0E-9;
	double used_GB  = total_GB-avail_GB;
	double used_percent = 100.0*used_GB/total_GB;

	vals[dirname+"_tot"] = total_GB;
	vals[dirname+"_avail"] = avail_GB;
	vals[dirname+"_used"] = used_GB;
	vals[dirname+"_percent_used"] = used_percent;
}

