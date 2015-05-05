#pragma once

#include "StorageEngine.h"
#include "LogRecord.h"

using std::map;
using std::string;
using std::vector;

const int NULL_LSN = -1;
const int NULL_TX = -1;

///////////////////  LogMgr  ///////////////////

class LogMgr {
private:
	//internal data structure for 
	map <int, txTableEntry> tx_table;
	map <int, int> dirty_page_table;
	vector <LogRecord*> logtail;

	StorageEngine* se;

	/*
	* Find the LSN of the most recent log record for this TX.
	* If there is no previous log record for this TX, return 
	* the null LSN.
	*/
	int getLastLSN(int txnum);

	/*
	* Update the TX table to reflect the LSN of the most recent
	* log entry for this transaction.
	*/
	void setLastLSN(int txnum, int lsn);

	/*
	* Force log records up to and including the one with the
	* maxLSN to disk. Don't forget to remove them from the
	* logtail once they're written!
	*/
	void flushLogTail(int maxLSN);


	/* 
	* Run the analysis phase of ARIES.
	*/
	void analyze(vector <LogRecord*> log);

	/*
	* Run the redo phase of ARIES.
	* If the StorageEngine stops responding, return false.
	* Else when redo phase is complete, return true. 
	*/
	bool redo(vector <LogRecord*> log);

	/*
	* If no txnum is specified, run the undo phase of ARIES.
	* If a txnum is provided, abort that transaction.
	* Hint: the logic is very similar for these two tasks!
	*/
	void undo(vector <LogRecord*> log, int txnum = NULL_TX);

	/*
	* Parse a log string into log records
	*/
	vector<LogRecord*> stringToLRVector(string logstring);

public:
	/*
	* Abort the specified transaction.
	* Hint: you can use your undo function
	*/
	void abort(int txid);

	/*
	* Write the begin checkpoint and end checkpoint
	*/
	void checkpoint();

	/*
	* Commit the specified transaction.
	*/
	void commit(int txid);

	/*
	* A function that StorageEngine will call when it's about to 
	* write a page to disk. 
	* Remember, you need to implement write-ahead logging
	*/
	void pageFlushed(int page_id);

	/*
	* Recover from a crash, given the log from the disk.
	*/
	void recover(string log);

	/*
	* Logs an update to the database and updates tables if needed.
	*/
	int write(int txid, int page_id, int offset, string input, string oldtext);

	/*
	* Sets this.se to engine. 
	*/
	void setStorageEngine(StorageEngine* engine);

	~LogMgr();

	//copy constructor omitted

	//Overloaded assignment operator
	LogMgr &operator= (const LogMgr &rhs);

};
