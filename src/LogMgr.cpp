#include <cassert>
#include <algorithm>
#include <set>
#include <sstream>
#include <climits>
#include <functional>
#include "LogMgr.h"

using namespace std;

int LogMgr::getLastLSN(int txnum)
{

	for (auto it = tx_table.begin(); it != tx_table.end(); ++it)
	{
		if (it->first == txnum)
		{
			return it->second.lastLSN;
		}
	}

	return NULL_LSN;
}

void LogMgr::setLastLSN(int txnum, int lsn)
{
	if (tx_table.find(txnum) != tx_table.end())
	{
		tx_table[txnum].lastLSN = lsn;
	}
}

void LogMgr::flushLogTail(int maxLSN)
{
	if (logtail.size())
	{
		int count = 0;
		for (auto it = logtail.begin(); it != logtail.end() && (*it)->getLSN() <= maxLSN; ++it)
		{
			se->updateLog((*it)->toString());
			++count;
		}
		logtail.erase(logtail.begin(), logtail.begin() + count);
	}
}

int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext)
{
	int prevLSN = NULL_LSN;
	int currLSN = se->nextLSN();
	//find prevLSN
	if (tx_table.find(txid) != tx_table.end())
	{
		prevLSN = tx_table[txid].lastLSN;
	}
	else
	{
		tx_table[txid].status = U;
	}
	//update tx_table
	tx_table[txid].lastLSN = currLSN;
	//update dirty_page_table
	if (dirty_page_table.find(page_id) == dirty_page_table.end())
	{
		dirty_page_table[page_id] = currLSN;
	}

	logtail.push_back(new UpdateLogRecord(currLSN, prevLSN, txid, page_id, offset, oldtext, input));

	return currLSN;
}

void LogMgr::commit(int txid)
{
	int prevLSN = NULL_LSN;
	int currLSN = se->nextLSN();
	//find prevLSN
	if (tx_table.find(txid) != tx_table.end() && tx_table[txid].status == U)
	{
		prevLSN = tx_table[txid].lastLSN;
		//update tx_table
		//tx_table[txid].lastLSN = currLSN;
		logtail.push_back(new LogRecord(currLSN, prevLSN, txid, COMMIT));
		//change status in tx table
		tx_table[txid].lastLSN = currLSN;
		tx_table[txid].status = C;
		//flush
		flushLogTail(currLSN);
		//create end log
		prevLSN = currLSN;
		currLSN = se->nextLSN();
		logtail.push_back(new LogRecord(currLSN, prevLSN, txid, END));
		//delete the ended tx from tx_table
		tx_table.erase(txid);
	}
}

void LogMgr::checkpoint()
{
	//set up begin check point
	int prevLSN = NULL_LSN;
	int currLSN = se->nextLSN();
	logtail.push_back(new LogRecord(currLSN, prevLSN, NULL_TX, BEGIN_CKPT));
	//set up end check point
	prevLSN = currLSN;
	currLSN = se->nextLSN();
	logtail.push_back(new ChkptLogRecord(currLSN, prevLSN, NULL_TX, tx_table, dirty_page_table));
	//flush logtail
	flushLogTail(currLSN);
	//store begin check point at master
	se->store_master(prevLSN);
}

void LogMgr::pageFlushed(int page_id)
{
	flushLogTail(se->getLSN(page_id));
	if (dirty_page_table.find(page_id) != dirty_page_table.end())
	{
		dirty_page_table.erase(page_id);
	}
}

void LogMgr::setStorageEngine(StorageEngine* engine)
{
	se = engine;
}

/*
* 1.Analysis: reconstructs tables at time of crash
* Jump to most-recent checkpoint, scan forward in log
* If we find end-tx in log, remove it from tx table
* If we find log entry for tx not in table, add it to table, lastLSN is this log LSN
* Else, The lastLSN field is set to the LSN of this log record
*  If the log record is a commit record, the status is set to C, otherwise it is set to U
* If we find log entry that impacts page P, and P is not in dirty page table, add it to table, recLSN is this log LSN
*/
void LogMgr::analyze(vector <LogRecord*> log)
{
	int beginLSN = se->get_master();

	auto it = log.begin();

	if (beginLSN != NULL_LSN)
	{
		while ((*it)->getLSN() != beginLSN)
		{
			++it;
		}

		++it;
		ChkptLogRecord* tmp = dynamic_cast<ChkptLogRecord*>(*it);
		tx_table = tmp->getTxTable();
		dirty_page_table = tmp->getDirtyPageTable();
		++it;
	}

	int page_id;
	for (; it != log.end(); ++it)
	{
		switch ((*it)->getType())
		{
			case END:
				if (tx_table.find((*it)->getTxID()) != tx_table.end())
				{
					tx_table.erase((*it)->getTxID());
				}
				break;
			case UPDATE:
				//update tx_table
				tx_table[(*it)->getTxID()].lastLSN = (*it)->getLSN();
				tx_table[(*it)->getTxID()].status = U;
				//update dirty_page_table
				page_id = dynamic_cast<UpdateLogRecord*>(*it)->getPageID();
				if (dirty_page_table.find(page_id) == dirty_page_table.end())
				{
					dirty_page_table[page_id] = (*it)->getLSN();
				}
				break;
			case CLR:
				//update tx_table
				tx_table[(*it)->getTxID()].lastLSN = (*it)->getLSN();
				tx_table[(*it)->getTxID()].status = U;
				//update dirty_page_table
				page_id = dynamic_cast<CompensationLogRecord*>(*it)->getPageID();
				if (dirty_page_table.find(page_id) == dirty_page_table.end())
				{
					dirty_page_table[page_id] = (*it)->getLSN();
				}
				break;
			case ABORT:
				//update tx_table
				tx_table[(*it)->getTxID()].lastLSN = (*it)->getLSN();
				tx_table[(*it)->getTxID()].status = U;
				break;
			case COMMIT:
				//update tx_table
				tx_table[(*it)->getTxID()].lastLSN = (*it)->getLSN();
				tx_table[(*it)->getTxID()].status = C;
				break;
			case BEGIN_CKPT:
			case END_CKPT:
				break;
			default:
				break;
		}
	}
}

/*
* 2.Redo: applies all updates in log
* Start with log record of smallest recLSN of any page in dirty page table; scan forward
* For each update/CLR (redoable log) encountered, check whether the update has to be applied:
* 1.  Is this page in the dirty page table?
* 2.  If yes, is the dirty page entry’s recLSN ?current log LSN?
* 3.  If yes, read the actual page from disk. Is the, LSN recorded on page (called PageLSN) smaller current log LSN?
* If yes, apply the update/CLR log to this page and set its PageLSN to the current log’s LSN
* If the answer to any of the questions above is no, move on!
*/
bool LogMgr::redo(vector <LogRecord*> log)
{
	//find min recLSN from dirty_page_table
	int min_recLSN = INT_MAX;
	for (auto& i: dirty_page_table)
	{
		min_recLSN = min(min_recLSN, i.second);
	}

	auto it = log.begin();
	while ((*it)->getLSN() != min_recLSN)
	{
		++it;
	}

	int page_id = -1;
	int offset = -1;
	string input;
	for (; it != log.end(); ++it)
	{
		switch ((*it)->getType())
		{
			case CLR:
				page_id = dynamic_cast<CompensationLogRecord*>(*it)->getPageID();
				offset = dynamic_cast<CompensationLogRecord*>(*it)->getOffset();
				input = dynamic_cast<CompensationLogRecord*>(*it)->getAfterImage();
				break;
			case UPDATE:
				page_id = dynamic_cast<UpdateLogRecord*>(*it)->getPageID();
				offset = dynamic_cast<UpdateLogRecord*>(*it)->getOffset();
				input = dynamic_cast<UpdateLogRecord*>(*it)->getAfterImage();
				break;
			case COMMIT:
				if (tx_table.find((*it)->getTxID()) != tx_table.end())
				{
					logtail.push_back(new LogRecord(se->nextLSN(), (*it)->getLSN(), (*it)->getTxID(), END));
					tx_table.erase((*it)->getTxID());
				}
				continue;
			default:
				continue;
		}

		if (dirty_page_table.find(page_id) != dirty_page_table.end()
			&& dirty_page_table[page_id] <= (*it)->getLSN()
			&& se->getLSN(page_id) < (*it)->getLSN())
		{
			//execute update
			if (!se->pageWrite(page_id, offset, input, (*it)->getLSN()))
			{
				return false;
			}
		}
	}

	return true;
}

/*
* 3.Undo: Scan log backwards
* Identify all live txs at time of crash
* ToUndo = {the LastLSN of all uncommitted live txs}
* While ToUndo is not empty
* L <- PickthemaxLSNinToUndo
* If L is an update record
* Undo the action of L, write an CLR record to the log with undonextLSN=L.PrevLSN
* ToUndo <- ToUndo U {L.prevLSN }
* IfLisaCLRrecord
* ToUndo <- ToUndo U {L.undonextLSN}
* Remove L from ToUndo
* write end record for Xact, remove it from Xacts table
*/
void LogMgr::undo(vector <LogRecord*> log, int txnum)
{
	//find all active transaction that has not been committed
	set<int, greater<int> > toUndo;

	if (txnum == NULL_TX)
	{
		for (auto &i: tx_table)
		{
			if (i.second.status == U)
			{
				toUndo.insert(i.second.lastLSN);
			}
		}
	}
	else
	{
		toUndo.insert(tx_table[txnum].lastLSN);
	}

	int undoLSN = NULL_LSN;
	auto it = log.begin();
	while (toUndo.size())
	{
		undoLSN = *(toUndo.begin());
		toUndo.erase(toUndo.begin());
		//traverse logtail to find logrecord corresponding to undoLSN
		it = log.begin();
		while ((*it)->getLSN() != undoLSN)
		{
			++it;
		}

		int page_id, offset, prevLSN, currLSN, nextUndoLSN;
		string input;
		switch ((*it)->getType())
		{
			case UPDATE:
				page_id = dynamic_cast<UpdateLogRecord*>(*it)->getPageID();
				offset = dynamic_cast<UpdateLogRecord*>(*it)->getOffset();
				input = dynamic_cast<UpdateLogRecord*>(*it)->getBeforeImage();
				//update tx_table
				currLSN = se->nextLSN();
				prevLSN = tx_table[(*it)->getTxID()].lastLSN;
				tx_table[(*it)->getTxID()].lastLSN = currLSN;
				logtail.push_back(new CompensationLogRecord(currLSN, prevLSN, (*it)->getTxID(), page_id, offset, input, (*it)->getprevLSN()));

				if (dirty_page_table.find(page_id) == dirty_page_table.end() ||
					dirty_page_table[page_id] > (*it)->getLSN())
				{
					dirty_page_table[page_id] = (*it)->getLSN();
				}

				if (!se->pageWrite(page_id, offset, input, currLSN)) {
					return;
				}

				if ((*it)->getprevLSN() != NULL_LSN)
				{
					toUndo.insert((*it)->getprevLSN());
				}
				else
				{
					prevLSN = currLSN;
					currLSN = se->nextLSN();
					logtail.push_back(new LogRecord(currLSN, prevLSN, (*it)->getTxID(), END));
					//delete the ended tx from tx_table
					tx_table.erase((*it)->getTxID());
				}
				break;
			case CLR:
				nextUndoLSN = dynamic_cast<CompensationLogRecord*>(*it)->getUndoNextLSN();
				if (nextUndoLSN != NULL_LSN)
				{
					toUndo.insert(nextUndoLSN);
				}
				else
				{
					prevLSN = tx_table[(*it)->getTxID()].lastLSN;
					currLSN = se->nextLSN();
					logtail.push_back(new LogRecord(currLSN, prevLSN, (*it)->getTxID(), END));
					//delete the ended tx from tx_table
					tx_table.erase((*it)->getTxID());
				}
				break;
			default:
				break;
		}
	}
}

vector<LogRecord*> LogMgr::stringToLRVector(string logstring)
{
	stringstream ss(logstring);
	string line;
	vector<LogRecord*> recovery_log;
	while (getline(ss, line))
	{
		recovery_log.push_back(LogRecord::stringToRecordPtr(line));
		line.clear();
	}
	return recovery_log;
}

/*
* Abort the specified transaction.
* Hint: you can use your undo function
*/
void LogMgr::abort(int txid)
{
	//read log from disk
	vector<LogRecord*> recovery_log = stringToLRVector(se->getLog());
	//write abort log
	int prevLSN = tx_table[txid].lastLSN;
	int currLSN = se->nextLSN();
	logtail.push_back(new LogRecord(currLSN, prevLSN, txid, ABORT));
	recovery_log.insert(recovery_log.end(), logtail.begin(), logtail.end());
	//update tx_table
	tx_table[txid].lastLSN = currLSN;
	//undo
	undo(recovery_log, txid);
	//prevLSN = currLSN;
	//currLSN = se->nextLSN();
	//logtail.push_back(new LogRecord(currLSN, prevLSN, txid, END));
	//update tx_table
	//tx_table.erase(txid);
}

/*
* Recover from a crash, given the log from the disk.
*/
void LogMgr::recover(string log)
{
	vector<LogRecord*> recovery_log = stringToLRVector(log);
	analyze(recovery_log);
	if (redo(recovery_log))
	{
		undo(recovery_log);
	}

}

LogMgr::~LogMgr()
{
	while (!logtail.empty()) {
		delete logtail[0];
		logtail.erase(logtail.begin());
	}
}

LogMgr& LogMgr::operator=(const LogMgr &rhs)
{
	if (this == &rhs) return *this;
	//delete anything in the logtail vector
	while (!logtail.empty()) {
		delete logtail[0];
		logtail.erase(logtail.begin());
	}
	for (vector<LogRecord*>::const_iterator it = rhs.logtail.begin(); it != rhs.logtail.end(); ++it) {
		LogRecord * lr = *it;
		int lsn = lr->getLSN();
		int prevLSN = lr->getprevLSN();
		int txid = lr->getTxID();
		TxType type = lr->getType();
		if (type == UPDATE) {
			UpdateLogRecord* ulr = dynamic_cast<UpdateLogRecord *>(lr);
			int page_id = ulr->getPageID();
			int offset = ulr->getOffset();
			string before = ulr->getBeforeImage();
			string after = ulr->getAfterImage();
			UpdateLogRecord* cpy_lr = new UpdateLogRecord(lsn, prevLSN, txid, page_id, offset,
				before, after);
			logtail.push_back(cpy_lr);
		}
		else if (type == CLR) {
			CompensationLogRecord* clr = dynamic_cast<CompensationLogRecord *>(lr);
			int page_id = clr->getPageID();
			int offset = clr->getOffset();
			string after = clr->getAfterImage();
			int nextLSN = clr->getUndoNextLSN();
			CompensationLogRecord* cpy_lr = new CompensationLogRecord(lsn, prevLSN, txid, page_id, offset,
				after, nextLSN);
			logtail.push_back(cpy_lr);
		}
		else if (type == END_CKPT) {
			ChkptLogRecord * chk_ptr = dynamic_cast<ChkptLogRecord *>(lr);
			map <int, txTableEntry> tx_table = chk_ptr->getTxTable();
			map <int, int> dp_table = chk_ptr->getDirtyPageTable();
			ChkptLogRecord * cpy_lr = new ChkptLogRecord(lsn, prevLSN, txid, tx_table, dp_table);
			logtail.push_back(cpy_lr);
		}
		else { //type is ordinary log record
			LogRecord * cpy_lr = new LogRecord(lsn, prevLSN, txid, type);
			logtail.push_back(cpy_lr);
		}
	}
	se = rhs.se;
	tx_table = rhs.tx_table;
	dirty_page_table = rhs.dirty_page_table;
	return *this;
}
