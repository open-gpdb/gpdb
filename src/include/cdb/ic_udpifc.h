/*-------------------------------------------------------------------------
 * ic_udpifc.h
 *	  Motion Layer IPC Layer.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-2025 Pivotal Software, Inc.
 * Portions Copyright (c) 2025- Present open-gpdb
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/ic_udpifc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IC_UDPIFC_H
#define IC_UDPIFC_H

#include "postgres.h"
#include "fmgr.h"


#include <unistd.h>

/*
 * ICStatistics
 *
 * A structure keeping various statistics about interconnect internal.
 *
 * Note that the statistics for ic are not accurate for multiple cursor case on QD.
 *
 * totalRecvQueueSize        - receive queue size sum when main thread is trying to get a packet.
 * recvQueueSizeCountingTime - counting times when computing totalRecvQueueSize.
 * totalCapacity             - the capacity sum when packets are tried to be sent.
 * capacityCountingTime      - counting times used to compute totalCapacity.
 * totalBuffers              - total buffers available when sending packets.
 * bufferCountingTime        - counting times when compute totalBuffers.
 * activeConnectionsNum      - the number of active connections.
 * retransmits               - the number of packet retransmits.
 * mismatchNum               - the number of mismatched packets received.
 * crcErrors                 - the number of crc errors.
 * sndPktNum                 - the number of packets sent by sender.
 * recvPktNum                - the number of packets received by receiver.
 * disorderedPktNum          - disordered packet number.
 * duplicatedPktNum          - duplicate packet number.
 * recvAckNum                - the number of Acks received.
 * statusQueryMsgNum         - the number of status query messages sent.
 *
 */
typedef struct ICStatistics
{
	uint64		totalRecvQueueSize;
	uint64		recvQueueSizeCountingTime;
	uint64		totalCapacity;
	uint64		capacityCountingTime;
	uint64		totalBuffers;
	uint64		bufferCountingTime;
	uint32		activeConnectionsNum;
	int32		retransmits;
	int32		startupCachedPktNum;
	int32		mismatchNum;
	int32		crcErrors;
	int32		sndPktNum;
	int32		recvPktNum;
	int32		disorderedPktNum;
	int32		duplicatedPktNum;
	int32		recvAckNum;
	int32		statusQueryMsgNum;
} ICStatistics;

extern void InterconnectShmemInitUDPIFC(void);
extern Size InterconnectShmemSizeUDPIFC(void);
extern void WaitInterconnectQuitUDPIFC(void);

/* Get interconnect statistics local to this slice */
extern ICStatistics UDPIFCGetICStats(void);

/* Get global cummulative IC stats */
extern Datum GpInterconnectGetStatsUDPIFC(PG_FUNCTION_ARGS);

#endif