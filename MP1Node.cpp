/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <random>

MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

ProtocolMessage::ProtocolMessage(Member *sendingNode, vector<MemberListEntry> *membershipListToSend, MsgTypes messageType) {
    MessageHdr *msg;
    if (messageType == GOSSIP || messageType == JOINREP)
    {
        int sizeOfMemberEntry = sizeof(int) + sizeof(short) + sizeof(long);
        messageSize = (membershipListToSend->size() * sizeOfMemberEntry) + sizeof(MessageHdr);

        msg = (MessageHdr *) malloc(messageSize * sizeof(char));

        msg->msgType = messageType;

        vector<MemberListEntry>::iterator iter = membershipListToSend->begin();

        char* ptr = (char*)msg + sizeof(MessageHdr);

        while (iter != membershipListToSend->end()) {
            long heartbeat = iter->heartbeat;
            int id = iter->id;
            short port = iter->port;

            memcpy((char*) ptr, &id, sizeof(int));
            ptr += sizeof(int);
            memcpy((char*) ptr, &port, sizeof(short));
            ptr += sizeof(short);
            memcpy((char*) ptr, &heartbeat, sizeof(long));
            ptr += sizeof(long);
            ++iter;
        }

        message = (char*) msg;
    }
}

char* ProtocolMessage::getSerializedMessage() {
    return message;
}

size_t ProtocolMessage::getMessageSize() {
    return messageSize;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {

	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    memberNode->heartbeat = 1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(int) + sizeof(short) + sizeof(long) + sizeof(MessageHdr);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;

        int id = getId(&memberNode->addr);
        short port = getPort(&memberNode->addr);

        char* ptr = (char*)msg + sizeof(MessageHdr);

        memcpy((char *)ptr, &id, sizeof(int));
        memcpy((char *)ptr + sizeof(int), &port, sizeof(short));

        memcpy((char *)ptr + sizeof(int) + sizeof(short), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
        // send JOINREQ message to introducer member
        string x = (char*) msg;
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

int MP1Node::getId(Address * address) {
    int id = 0;
    memcpy(&id, &address->addr[0], sizeof(int));
    return id;
}

short MP1Node::getPort(Address * address) {
    short port = 0;
    memcpy(&port, &address->addr[4], sizeof(short));
    return port;
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){

    memberNode->inited = false;
    memberNode->memberList.clear();
    memberNode->inGroup = false;

    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {

    vector<MemberListEntry> memberList;
    MessageHdr *msgHdr = (MessageHdr *)(data);


    if (msgHdr->msgType == JOINREQ) {

        int *id = (int *) (data + sizeof(MessageHdr));
        short *port = (short *)(data + sizeof(MessageHdr) + sizeof(int));
        long *heartbeat = (long *)(data + sizeof(MessageHdr) + sizeof(int) + sizeof(short) + 1);

        Address a = getAddress(*id, *port);

        addOrUpdateMember(*id, *port, *heartbeat);

        vector<MemberListEntry> *membershipListToSend = new vector<MemberListEntry>();
        for (vector<MemberListEntry>::iterator iter = memberNode->memberList.begin();
             iter != memberNode->memberList.end(); ++ iter) {
            if (iter->timestamp + TFAIL > par->getcurrtime()){
                     membershipListToSend->push_back(*iter);
            }
        }

        ProtocolMessage *message = new ProtocolMessage(memberNode, membershipListToSend, JOINREP);

        char *msgString = message->getSerializedMessage();
        size_t msgSize = message->getMessageSize();

        emulNet->ENsend(&memberNode->addr, &a, msgString, msgSize);
        delete message;
    }

    if (msgHdr->msgType == JOINREP) {
        memberNode->inGroup = true;
    }

    // In both GOSSIP and JOINREP messages, the receiving node would receive a cluster membership list that it needs to update
    if (msgHdr->msgType == GOSSIP || msgHdr->msgType == JOINREP) {
        int totalMembers = (size - sizeof(MessageHdr)) / (sizeof(int) + sizeof(short) + sizeof(long));
        char * ptr;
        ptr = data + sizeof(MessageHdr);
        for (int i = 0; i < totalMembers; i++) {
            int *id = (int *) (ptr);
            short *port = (short *)(ptr + sizeof(int));
            long *heartbeat = (long *)(ptr + sizeof(int) + sizeof(short));

            addOrUpdateMember(*id, *port, *heartbeat);
            ptr = ptr + sizeof(int) + sizeof(short) + sizeof(long);
        }
    }
}

bool MP1Node::addOrUpdateMember(int id, short port, long heartbeat) {
    if (!hasMember(id, port, heartbeat)) {
        MemberListEntry member = MemberListEntry(id, port, heartbeat, par->getcurrtime());
        memberNode->memberList.push_back(member);

        Address addr = getAddress(id, port);
        log->logNodeAdd(&memberNode->addr, &addr);
    }
}

bool MP1Node::hasMember(int id, short port, long heartbeat) {
    for (int i=0; i < memberNode->memberList.size(); i++) {
        if (memberNode->memberList[i].getid() == id && memberNode->memberList[i].getport() == port) {
            if (memberNode->memberList[i].heartbeat < heartbeat) {
                memberNode->memberList[i].setheartbeat(heartbeat);
                memberNode->memberList[i].settimestamp(par->getcurrtime());
            }
            return true;
        }
    }
    return false;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    memberNode->heartbeat++;
    auto aliveMembers = getAliveMemberList();
    memberNode->memberList = *aliveMembers;
    sendMembershipList();

    return;
}

/**
 * FUNCTION NAME: removeTimedOutNodes
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 */

vector<MemberListEntry> * MP1Node::getAliveMemberList() {
    vector<MemberListEntry> *aliveMembers = new vector<MemberListEntry>();

    for(
        vector<MemberListEntry>::iterator iter = memberNode->memberList.begin();
        iter != memberNode->memberList.end();
        ++iter
    ) {
        if (iter->timestamp + TREMOVE < par->getcurrtime()) {
            Address address = getAddress(iter->id, iter->port);

            log->logNodeRemove(&memberNode->addr, &address);
        }
        else {
            if (iter->id == getId(&memberNode->addr)) {
                iter->heartbeat = memberNode->heartbeat;
                iter->timestamp = par->getcurrtime();
            }

            aliveMembers->push_back(*iter);
        }
    }

    return aliveMembers;
}


void MP1Node::sendMembershipList() {
    vector<MemberListEntry> *membershipListToSend = new vector<MemberListEntry>();
    for (vector<MemberListEntry>::iterator iter = memberNode->memberList.begin();
         iter != memberNode->memberList.end(); ++ iter) {
        if (iter->timestamp + TFAIL > par->getcurrtime()){
            membershipListToSend->push_back(*iter);
        }
    }

    unsigned seed = par->getcurrtime();
    auto rng = std::default_random_engine(seed);
    std::shuffle(std::begin(*membershipListToSend), std::end(*membershipListToSend), rng);

    int numMembers = membershipListToSend->size();
    int selectedRecipients = (long) (SENDRATIO * numMembers);

    if (numMembers > 0 && selectedRecipients == 0)
    {
        selectedRecipients = (int) SENDTHRESHOLD > numMembers ? numMembers : (int) SENDTHRESHOLD;
    }

    std::vector<MemberListEntry>::iterator iter = membershipListToSend->begin();
    for (int i = 0; i < selectedRecipients; i++) {

        Address selectedRecipientAddress = getAddress(iter->id, iter->port);
        ProtocolMessage *message = new ProtocolMessage(memberNode, membershipListToSend, GOSSIP);

        emulNet->ENsend(&memberNode->addr, &selectedRecipientAddress, message->getSerializedMessage(), message->getMessageSize());
        iter++;
    }
}

Address MP1Node::getAddress(int id, short port) {

    Address addr;
    memset(&addr, 0, sizeof(id));
    *(int *)(&addr.addr) = id;
    *(short *)(&addr.addr[4]) = port;

    return addr;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();

    Address joinaddr = getJoinAddress();
    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr.addr), sizeof(memberNode->addr.addr))) {
        addOrUpdateMember(getId(&memberNode->addr), getPort(&memberNode->addr), memberNode->heartbeat);
    }
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}
