
#include <unistd.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>
#include <sys/fsuid.h>

//#include <infiniband/verbs.h>
#include <hdRDMA.h>
#include <hdRDMAstats.h>
#include <hdRDMAcontrol.h>

#include <iostream>
#include <vector>
#include <mutex>
#include <set>
using namespace std;
using std::chrono::steady_clock;
using std::chrono::duration;
using std::chrono::duration_cast;

int VERBOSE=1;
bool QUIT = false;
const char *HDRDMA_VERSION = "1.0.2";

int HDRDMA_RET_VAL = 0;
bool HDRDMA_IS_SERVER = false;
bool HDRDMA_IS_CLIENT = false;

int HDRDMA_LOCAL_PORT  = 10470;
int HDRDMA_REMOTE_PORT = 10470;
std::string HDRDMA_REMOTE_ADDR = "gluon47.jlab.org";
int HDRDMA_CONNECTION_TIMEOUT = 10; // seconds
string HDRDMA_SRCFILENAME = "";
std::string HDRDMA_DSTFILENAME = "";
bool HDRDMA_DELETE_AFTER_SEND  = false;
bool HDRDMA_CALCULATE_CHECKSUM = false;
bool HDRDMA_MAKE_PARENT_DIRS   = false;
string HDRDMA_USERNAME         = "";     
string HDRDMA_GROUPNAME        = "";
int HDRDMA_ZEROMQ_STATS_PORT   = 10471;   // port to publish zeroMQ messages about our stats to. 0=don't publish
int HDRDMA_ZEROMQ_CONTROL_PORT = 10472;   // port to publish zeroMQ messages about our stats to. 0=don't publish
string HDRDMA_COMMAND_HOST;
string HDRDMA_COMMAND;
void *HDRDMA_ZEROMQ_CONTEXT    = nullptr; // will be set by hdRDMAstats or hdRDMAcontrol (whichever is called first)
uint64_t HDRDMA_BUFF_LEN_GB = 0;          // defaults differ for server and client modes
uint64_t HDRDMA_NUM_BUFF_SECTIONS = 0;    // so these are set in ParseCommandLineArguments
std::mutex HDRDMA_RECV_FNAMES_MUTEX;
std::set<string> HDRDMA_RECV_FNAMES;

atomic<uint64_t> BYTES_RECEIVED_TOT(0);
atomic<uint64_t> NFILES_RECEIVED_TOT(0);
double HDRDMA_LAST_RATE_10sec=0.0;
double HDRDMA_LAST_RATE_1min=0.0;
double HDRDMA_LAST_RATE_5min=0.0;

string SendControlCommand(string host, string command);
int  GetTotalSystemRAMGB(void);
void ParseCommandLineArguments( int narg, char *argv[] );
void Usage(bool show_extended=false);


//-------------------------------------------------------------
// main
//-------------------------------------------------------------
int main(int narg, char *argv[])
{
	ParseCommandLineArguments( narg, argv );

	// Listen for remote peers if we are in server mode
	// This will launch a thread and listen for any remote connections.
	// Any that are made will have their RDMA transfer information setup
	// and stored in the hdRDMA object and be made available for transfers.
	// This will return right away so one must check the GetNpeers() method
	// to see when a connection is made.
	if( HDRDMA_IS_SERVER ){

		// Create an hdRDMA object
		hdRDMA hdrdma;

		// Setup network connections for external control and periodic
		// publication of stats (if ZMQ support compiled in)
		hdRDMAstats *hdrdmastats = nullptr;
		hdRDMAcontrol *hdrdmacontrol = nullptr;
#ifdef HAVE_ZEROMQ
		HDRDMA_ZEROMQ_CONTEXT = zmq_ctx_new();
		if( HDRDMA_ZEROMQ_STATS_PORT   ) hdrdmastats   = new hdRDMAstats(   HDRDMA_ZEROMQ_STATS_PORT   );
		if( HDRDMA_ZEROMQ_CONTROL_PORT ) hdrdmacontrol = new hdRDMAcontrol( HDRDMA_ZEROMQ_CONTROL_PORT );
#endif //	HAVE_ZEROMQ

		hdrdma.Listen( HDRDMA_LOCAL_PORT );

		// We want to report 10sec, 1min, and 5min averages
		auto t_last_10sec = steady_clock::now();
		auto t_last_1min = t_last_10sec;
		auto t_last_5min = t_last_10sec;
		uint64_t last_bytes_received_10sec = BYTES_RECEIVED_TOT;
		uint64_t last_bytes_received_1min  = BYTES_RECEIVED_TOT;
		uint64_t last_bytes_received_5min  = BYTES_RECEIVED_TOT;

		pthread_setname_np( pthread_self(), "main" );

		while( !QUIT ){
			
			hdrdma.Poll();
			
			auto now = steady_clock::now();
			auto duration_10sec = duration_cast<std::chrono::seconds>(now - t_last_10sec);
			auto duration_1min  = duration_cast<std::chrono::seconds>(now - t_last_1min);
			auto duration_5min  = duration_cast<std::chrono::seconds>(now - t_last_5min);
			auto delta_t_10sec  = duration_10sec.count();
			auto delta_t_1min   = duration_1min.count();
			auto delta_t_5min   = duration_5min.count();

			if( delta_t_10sec >= 10.0 ){
				double GB_received = (BYTES_RECEIVED_TOT - last_bytes_received_10sec)/1000000000;
				double rate = GB_received/delta_t_10sec;
				if( VERBOSE>2 )cout << "=== [10 sec avg.] " << rate << " GB/s  --  " << (double)BYTES_RECEIVED_TOT/1.0E12 << " TB total received" << endl;
				t_last_10sec = now;
				last_bytes_received_10sec = BYTES_RECEIVED_TOT;
				HDRDMA_LAST_RATE_10sec= rate;
			}

			if( delta_t_1min >= 60.0 ){
				double GB_received = (BYTES_RECEIVED_TOT - last_bytes_received_1min)/1000000000;
				double rate = GB_received/delta_t_1min;
				if( VERBOSE>1 )cout << "=== [ 1 min avg.] " << rate << " GB/s" << endl;
				t_last_1min = now;
				last_bytes_received_1min = BYTES_RECEIVED_TOT;
				HDRDMA_LAST_RATE_1min= rate;
			}

			if( delta_t_5min >= 300.0 ){
				double GB_received = (BYTES_RECEIVED_TOT - last_bytes_received_5min)/1000000000;
				double rate = GB_received/delta_t_5min;
				if( VERBOSE>0 )cout << "=== [ 5 min avg.] " << rate << " GB/s" << endl;
				t_last_5min = now;
				last_bytes_received_5min = BYTES_RECEIVED_TOT;
				HDRDMA_LAST_RATE_5min= rate;
			}

			std::this_thread::sleep_for( std::chrono::milliseconds( 100 ) );
		}

		// Stop server from listening (if one is)
		hdrdma.StopListening();
		
		// Stop stats reporting 
		if( hdrdmastats   != nullptr ) delete hdrdmastats;
		if( hdrdmacontrol != nullptr ) delete hdrdmacontrol;

		// At this point we have shut most things down but there are still a few
		// threads running that are stuck in blocking calls that are just not
		// easy to kill. Thus, we harshly kill the entire process to force the
		// issue.
		// n.b. we set an exit value of -1 so the system service will re-start
		// automatically (if we're being run that way).
		_exit(HDRDMA_RET_VAL);

#ifdef HAVE_ZEROMQ
		zmq_ctx_destroy( HDRDMA_ZEROMQ_CONTEXT ); // we'll never get here due to above call. This would block forever anyway.
#endif // HAVE_ZEROMQ
	}

	// Connect to remote peer if we are in client mode.
	// This will attempt to connect to an hdRDMA object listening on the
	// specified host/port. If a connection is made then the RDMA transfer
	// information will be exchanged and stored in the hdRDMA object and
	// be made available for transfers. If the connection cannot be made 
	// then it will exit the program with an error message.
	if( HDRDMA_IS_CLIENT ){
		// Create an hdRDMA object
		hdRDMA hdrdma;

		hdrdma.Connect( HDRDMA_REMOTE_ADDR, HDRDMA_REMOTE_PORT );
		hdrdma.SendFile( HDRDMA_SRCFILENAME, HDRDMA_DSTFILENAME, HDRDMA_DELETE_AFTER_SEND, HDRDMA_CALCULATE_CHECKSUM, HDRDMA_MAKE_PARENT_DIRS );
	}

	// Send control command to remote hdrdmacp process being run in server mode
	if( !HDRDMA_COMMAND_HOST.empty() ) cout << SendControlCommand(HDRDMA_COMMAND_HOST, HDRDMA_COMMAND) << endl;
	
	return HDRDMA_RET_VAL;
}

//-------------------------------------------------------------
// SendControlCommand
//
// Send a control command to a remote hdrdmacp process running in
// server mode. The response is returned in the form of a string.
//-------------------------------------------------------------
string SendControlCommand(string host, string command)
{

#ifndef HAVE_ZEROMQ
	return string("Unable to send control command due to zeroMQ support not being compiled in.");
#else
	try{
		char conn_str[256];
		sprintf( conn_str, "tcp://%s:%d", host.c_str(), HDRDMA_ZEROMQ_CONTROL_PORT );

		HDRDMA_ZEROMQ_CONTEXT = zmq_ctx_new();
		void *requester = zmq_socket( HDRDMA_ZEROMQ_CONTEXT, ZMQ_REQ );
		zmq_connect( requester, conn_str);

		vector<char> buff( 20480, 0);
		zmq_send( requester, command.c_str(), command.size(), 0 );
		zmq_recv( requester, buff.data(), buff.size(), 0);

		zmq_close(requester);
		zmq_ctx_destroy( HDRDMA_ZEROMQ_CONTEXT );

		return string( buff.data() );
	}catch(...){
		return string("Error sending zmq control message");
	}
#endif // HAVE_ZEROMQ

}

//-------------------------------------------------------------
// GetTotalSystemRAMGB
//
// Returns the total system memory in GB. This is only used
// when automatically determining amount to allocate when in
// server mode.
//-------------------------------------------------------------
int GetTotalSystemRAMGB(void)
{
	// Read memory from /proc/meminfo
	ifstream ifs("/proc/meminfo");
	if(ifs.is_open()){
		char buff[4096];
		bzero(buff, 4096);
		ifs.read(buff, 4095);
		ifs.close();

		string sbuff(buff);

		size_t pos = sbuff.find("MemTotal:");
		if(pos != string::npos) return atoi(&buff[pos+10+1])/1E6;
	}

	return 0;
}

//-------------------------------------------------------------
// ParseCommandLineArguments
//-------------------------------------------------------------
void ParseCommandLineArguments( int narg, char *argv[] )
{

	if( narg ==1 ) { Usage(); exit(0); }

	std::vector<std::string> vfnames;

	for( int i=1; i<narg; i++){
		string arg( argv[i] );
		string next = (i+1)<narg ? argv[i+1]:"";
		string nextnext = (i+2)<narg ? argv[i+2]:"";

		if( arg=="-h" || arg=="--help" ){
			Usage(true);
			exit(0);
		}else if( arg=="-s" || arg=="--server" ){
			HDRDMA_IS_SERVER = true;
		}else if( arg=="-p"){
			HDRDMA_REMOTE_PORT = atoi( next.c_str() );
			i++;
		}else if( arg=="-sp"){
			HDRDMA_LOCAL_PORT = atoi( next.c_str() );
			i++;
		}else if( arg=="-d"){
			HDRDMA_DELETE_AFTER_SEND = true;
		}else if( arg=="-c"){
			HDRDMA_CALCULATE_CHECKSUM = true;
		}else if( arg=="-P"){
			HDRDMA_MAKE_PARENT_DIRS = true;
		}else if( arg=="-m"){
			HDRDMA_BUFF_LEN_GB = atoi( next.c_str() );
			i++;
		}else if( arg=="-n"){
			HDRDMA_NUM_BUFF_SECTIONS = atoi( next.c_str() );
			i++;
		}else if( arg=="-u"){
			HDRDMA_USERNAME = next;
			i++;
		}else if( arg=="-g"){
			HDRDMA_GROUPNAME = next;
			i++;
		}else if( arg=="-v"){
			VERBOSE++;
		}else if( arg=="-q"){
			VERBOSE = 0;
		}else if( arg=="-zp"){
			HDRDMA_ZEROMQ_STATS_PORT = atoi( next.c_str() );
			i++;
		}else if( arg=="-cp"){
			HDRDMA_ZEROMQ_CONTROL_PORT = atoi( next.c_str() );
			i++;
		}else if( arg=="-cmd"){
			HDRDMA_COMMAND_HOST = next;
			HDRDMA_COMMAND = nextnext;
			i+=2;
		}else if( arg[0]!='-'){
			vfnames.push_back( arg.c_str() );
			HDRDMA_IS_CLIENT = true;
		}
	}

	if( HDRDMA_IS_CLIENT ){
		// We are the client (i.e. we are sending the file)
		if( vfnames.size() != 2 ){
			cout << "ERROR: exactly 2 arguments needed: src dst" << endl;
			Usage();
			exit( -50 );
		}
		
		HDRDMA_SRCFILENAME = vfnames[0];
		
		// Extract destination host, port, and filename from dest
		auto pos = vfnames[1].find(":");
		if( pos == string::npos ){
			cout << "ERROR: destination must be in the form host:[port:]dest_filename (you entered " << vfnames[1] << ")" << endl;
			exit(-51);
		}
		HDRDMA_REMOTE_ADDR = vfnames[1].substr(0, pos);
		
		auto pos2 = vfnames[1].find(":", pos+1);
		if( pos2 == string::npos ){
			// port not specfied in dest string
			HDRDMA_DSTFILENAME = vfnames[1].substr(pos+1);
		}else{
			// port is specfied in dest string
			auto portstr = vfnames[1].substr(pos+1, pos2-pos-1);
			HDRDMA_REMOTE_PORT = atoi( portstr.c_str() );
			HDRDMA_DSTFILENAME = vfnames[1].substr(pos2+1);
		}

		// Set memory region size for client mode. Use default if user didn't specify
		if( HDRDMA_BUFF_LEN_GB == 0 ) HDRDMA_BUFF_LEN_GB = 1;
		if( HDRDMA_NUM_BUFF_SECTIONS == 0 ) HDRDMA_NUM_BUFF_SECTIONS = 4;

	}

	if( HDRDMA_IS_SERVER ){
		// Set memory region size for server mode. Use default if user didn't specify
		if( HDRDMA_BUFF_LEN_GB == 0 ){
			HDRDMA_BUFF_LEN_GB = GetTotalSystemRAMGB()/16;     // Use 1/16 of total system memory ....
			if( HDRDMA_BUFF_LEN_GB<1 ) HDRDMA_BUFF_LEN_GB = 1; // ... but limit to reasonable range ...
			if( HDRDMA_BUFF_LEN_GB>8 ) HDRDMA_BUFF_LEN_GB = 8;
		}
		if( HDRDMA_NUM_BUFF_SECTIONS == 0 ){
			HDRDMA_NUM_BUFF_SECTIONS = 4*HDRDMA_BUFF_LEN_GB;
			if( HDRDMA_NUM_BUFF_SECTIONS<8 ) HDRDMA_NUM_BUFF_SECTIONS = 8;
		}
	}
}

//-------------------------------------------------------------
// Usage
//-------------------------------------------------------------
void Usage(bool show_extended)
{
	cout << endl;
	cout << "Hall-D RDMA file copy server/client" <<endl;
	cout << endl;
	cout << "Usage:" <<endl;
	cout << endl;
	cout << "   hdrdmacp [options] srcfile host:[port:]destfile" << endl;
	cout << "   hdrdmacp -s" << endl;
	cout << endl;
	cout << "This program can be used as both the server and client to copy a" << endl;
	cout << "file from the local host to a remote host using RDMA over IB." << endl;
	cout << "This currently does not support copying files from the remote" << endl;
	cout << "server back to the client. It also only supports copying a single" << endl;
	cout << "file per connection at the moment. In server mode it can accept" << endl;
	cout << "multiple simultaneous connections and so can receive any number" << endl;
	cout << "of files. In client mode however, only a single file can be" << endl;
	cout << "tranferred. Run multiple clients to transfer multiple files." << endl;
	cout << endl;
	cout << "Note: In the options below: " << endl;
	cout << "    CMO=Client Mode Only" << endl;
	cout << "    SMO=Server Mode Only" << endl;
	cout << endl;
	cout << " options:" <<endl;
	cout << "    -c         calculate checksum (adler32 currently only prints) (CMO)" << endl;
	cout << "    -d         delete source file upon successful transfer (CMO)" << endl;
	cout << "    -g  group  set effective group (useful if run as a system service) (SMO)" << endl;
	cout << "    -h         print this usage statement." << endl;
	cout << "    -m  GB     total memory to allocate (def. 8GB for server, 1GB for client)" << endl;
	cout << "    -n  Nbuffs number of buffers to break the allocated memory into. This" << endl;
	cout << "               will determine the size of RDMA transfer requests." << endl;
	cout << "    -P         make parent directory path on remote host if needed (CMO)" << endl;
	cout << "    -p  port   set remote port to connect to (can also be given in dest name) (CMO)" << endl;
	cout << "    -s         server mode (SMO)" << endl;
	cout << "    -sp port   server port to listen on (default is 10470) (SMO)" << endl;
	cout << "    -u  user   set effective user (useful if run as a system service) (SMO)" << endl;
	cout << "    -v         increase verbosity level" << endl;
	cout << "    -q         quiet mode (set verbosity level to 0)" << endl;
	cout << "    -zp port   port to publish stats as zeroMQ messages to" << endl;
	cout << "    -cp port   port to listen for zeroMQ control messages on" << endl;
	cout << "    -cmd host \"command arg ...\" send command to specified host" << endl;
	cout << endl;

	if( ! show_extended ){
		cout << " (run with --help for extended help)" << endl << endl;
		return;
	}


	cout << "NOTES:" << endl;
	cout << "  1. The full filename on the destination must be specfied, not just" << endl;
	cout << "     a directory. This is not checked for automatically so the user" <<endl;
	cout << "     must take care." << endl;
	cout << endl;
	cout << "  2. The remote host and port refer to a TCP connection that is" << endl;
	cout << "     first made to exchange the RDMA connection info. The file is" << endl;
	cout << "     then transferred via RDMA." << endl;
	cout << endl;
	cout << "  3. The destination port may be speficied either via the -p option" << endl;
	cout << "     or as part of the destination argument. e.g." << endl;
	cout << "        my.remote.host:12345:/path/to/my/destfilename" << endl;
	cout << "     if both are given then the one given in the destination argument" << endl;
	cout << "     is used." << endl;
	cout << endl;
	cout << "  4. If you see an error about \"Unable to register memory region!\" then" << endl;
	cout << "     this may be due to the maximum locked memory size. Check this by" <<endl;
	cout << "     running \"limit\" if using tcsh and looking for \"memorylocked\". If" << endl;
	cout << "     using bash, then run \"ulimit -a\" and look for \"max locked memory\"." << endl;
	cout << "     These should be set to \"unlimited\". On some of our systems this defaults" << endl;
	cout << "     to 64kB and would not honor global settings. A wierd work around was to" << endl;
	cout << "     do a \"su $USER\" which set it to \"unlimited\". (I do not understand why.)" << endl;
	cout << endl;
	cout << "  5. Two zeroMQ servers are started by default. One for periodicaly" << endl;
	cout << "     publishing statistics (PUB-SUB) and the other for accepting" << endl;
	cout << "     control commands and sending a response (REQ_REP). The port" << endl;
	cout << "     numbers used for these can be specified with the -zp and -cp" << endl;
	cout << "     options respectively. Either of the ports may ve specified as" << endl;
	cout << "     \"0\" in which case that server is not started." << endl;
	cout << endl;
	cout << "  6. The -cmd option can be used to send arbitrary control commands to" << endl;
	cout << "     a server. Commands are sent via the zeroMQ control port specified" << endl;
	cout << "     using the -cp option or " << HDRDMA_ZEROMQ_CONTROL_PORT << " by default." << endl;
	cout << "     Control commands use a zeroMQ REQ-REP socket pattern so every command" << endl;
	cout << "     sent will receive a reply." << endl;
	cout << "     A few available commands are:" << endl;
	cout << "       quit                            - Tell sever to quit. If the server is" << endl;
	cout << "                                         being run as a service it will likely be" << endl;
	cout << "                                         restarted automatically. This is primaily " << endl;
	cout << "                                         useful after installing a new hdrdmacp" << endl;
	cout << "                                         binary and you want the server to restart" << endl;
	cout << "                                         without having to use an admin account." << endl;
	cout << "       get_file_size  file1 file2 ...  - Get the size of the specified file(s)" << endl;
	cout << "                                         Files sizes are in bytes. If the file" << endl;
	cout << "                                         does not exist on the server then -1" << endl;
	cout << "                                         is returned." << endl;
	cout << "       reset_counters                  - Reset internal counters used when publishing" << endl;
	cout << "                                         statistics." << endl;
 	cout << endl;
	cout << "Example:" << endl;
	cout << "  On destination host run:  hdrdmacp -s" << endl;
	cout << endl;
	cout << "  On source host run:  hdrdmacp /path/to/my/srcfile my.remote.host:/path/to/my/destfile" << endl;
	cout << endl;
	cout << "Note that the above will fail if /path/to/my does not already exist on" << endl;
	cout << "my.remote.host. If you add the -P argument then /path/to/my will be " << endl;
	cout << "automatically created (if it doesn't already exist)." << endl;
	cout << endl;
	cout << "Example:" << endl;
	cout << "  To get size of file on server:  hdrdmacp -cmd server.domain.org \"get_file_size /path/to/file\"" << endl;
	cout << "Note that -cmd always takes 2 arguments with the second being a string" << endl;
	cout << "containing the entire command being sent. This likely will need to be enclosed" << endl;
	cout << "in quotes as shown if it contains multiple arguments itself." << endl;
	cout << endl;
 }
