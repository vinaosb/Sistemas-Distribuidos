#include <mpi.h>
#include <set>
#include <string>
#include <thread>
#include <atomic>
#include <random>
#include <array>
#include <map>


#define MAX_Learners 1
#define beginLearners 0
#define endLearners beginLearners+MAX_Learners

#define MAX_Acceptors 3
#define beginAcceptors endLearners
#define endAcceptors beginAcceptors+MAX_Acceptors

#define MAX_Proposers 3
#define beginProposers endAcceptors
#define endProposers beginProposers+MAX_Proposers


using namespace std;
namespace praxos {

set<int> acceptor;
set<int> proposer;
set<int> learner;
array<int, MAX_Proposers> generated;


typedef struct {
	int message_code;
	string message;
} message_t;

enum MessTags {
	prep_req1,
	prep_req2,
	prep_req3,
	prep_res1,
	prep_res2,
	prep_res3,
	accp_req1,
	accp_req2,
	accp_req3,
	accepted1,
	accepted2,
	accepted3
};



int main(int argc, char** argv) {
	seed_seq seed = {9999,1111,191919};
	seed.generate(generated.begin(), generated.end());

	// Define a lista inicial de Learners, Proposers e Acceptors
	for (int i = beginLearners; i < endLearners; i++)
		learner.insert(i);
	for (int i = beginAcceptors; i < endAcceptors; i++)
		acceptor.insert(i);
	for (int i = beginProposers; i < endProposers; i++)
		proposer.insert(i);

	MPI_Init(NULL,NULL);
	int world_rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
	int world_size;
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);

	if (world_size < 3) {
		fprintf(stderr, "World size must be greater than 3 for %s\n", argv[0]);
		MPI_Abort(MPI_COMM_WORLD, 1);
	}

	if (world_rank == 0) {
		Learner lear = Learner();
		message_t ret = lear.start();
		printf(ret.message.c_str());
	} else if (world_rank < 3) {
		Acceptor accp = Acceptor(world_rank);
		accp.start();
	} else {
		Proposer prop = Proposer(world_rank);
		prop.start();
	}

	MPI_Finalize();

	return 0;
}

void sendMessage(message_t msg, int destination, MessTags tag) {
		int size;
		MPI_Send(&msg.message_code, 1, MPI_INT, destination, tag, MPI_COMM_WORLD);
		size = msg.message.size();
		MPI_Send(&size, 1, MPI_INT, destination, tag+1, MPI_COMM_WORLD);
		MPI_Send(&msg.message, sizeof(msg.message), MPI_BYTE, destination, tag+2, MPI_COMM_WORLD);
}

message_t receiveMessage(int source, MessTags tag) {
		int code, size;
		char *msg;
		MPI_Recv(&code, 1, MPI_INT, source, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		MPI_Recv(&size, 1, MPI_INT, source, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		MPI_Recv(&msg, size, MPI_BYTE, source, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		message_t ret;
		ret.message = msg;
		ret.message_code = code;

		return ret;
}

class Proposer {
	public:
	message_t MessageToPropose;
	atomic<int> responses = 0;

	Proposer(int pid) {
		MessageToPropose.message_code = proposal_number(pid);
		MessageToPropose.message = to_string(proposal_number(pid));
	}

	void start() {
		thread prep_req_threads[acceptor.size()];
		thread prep_res_threads[acceptor.size()];
		thread accp_req_threads[acceptor.size()];
		// Envia request e em seguida um prepara pra receber response
		for(set<int>::iterator acc = acceptor.begin(); acc != acceptor.end(); ++acc)
		{
			prep_req_threads[*acc] = thread(prepare_request,MessageToPropose,*acc);
			prep_res_threads[*acc] = thread(prepare_response,*acc);
		}

		// Enquanto não tiver metade dos acceptors, aguarda
		while (responses < acceptor.size/2);

		// Envia mensagem quando tiver metade dos acceptors
		for(set<int>::iterator acc = acceptor.begin(); acc != acceptor.end(); ++acc)
		{
			accp_req_threads[*acc] = thread(accept_request,MessageToPropose,*acc);
		}
	}

	// Pega numero da proposta
	int proposal_number(int id) {
		return generated[id];
	}

	// Envia msg para um acceptor
	void prepare_request(message_t msg, int destination) {
		sendMessage(msg,destination, MessTags::prep_req1);
	}

	// Recebe response de um acceptor
	void prepare_response(int accep) {
		message_t r = receiveMessage(accep, MessTags::prep_res1);

		// Se mensagem recebida for de valor menor,
		// anota que possui mais um acceptor
		if(r.message_code < MessageToPropose.message_code)
			responses++;
	}

	// Envia pedido de aceitacao para um acceptor
	void accept_request(message_t msg, int destination) {
		sendMessage(msg,destination, MessTags::accp_req1);
	}
};

class Acceptor {
	public:
	atomic_flag flag = ATOMIC_FLAG_INIT;
	message_t MessageAcceptedToDate;
	int acceptedProposer;
	int proposersTested = 0;

	Acceptor(int rid) {
	}

	void start() {
		thread prep_req_threads[proposer.size()];
		// Cria threads para prepare_request e accept_request
		// Lembrando que prepare_response é criado por prepare_request
		// e accepted por accept_response
		for(set<int>::iterator prop = proposer.begin(); prop != proposer.end(); ++prop)
		{
			prep_req_threads[*prop] = thread(prepare_request,*prop);
			prep_req_threads[*prop] = thread(accept_request,*prop);
		}


		// Enquanto não tiver metade dos proposers, aguarda
		while (proposersTested < proposer.size/2);

		for(set<int>::iterator acc = proposer.begin(); acc != proposer.end(); ++acc)
		{
			accp_req_threads[*acc] = thread(accept_request,MessageAcceptedToDate,*acc);
		}
	}

	// Recebe o request do proposer
	void prepare_request(int proposer) {
		message_t r = receiveMessage(proposer, MessTags::prep_req1);
		// Garante que só uma thread pode editar a mensagem aceita
		while(flag.test_and_set()) {}

		// Testa mensagem do proposer
		if (r.message_code > MessageAcceptedToDate.message_code) {
			// Se mensagem do proposer for a aceita,
			// envia pedido pra preparar resposta;
			// envia resposta que tinha ate o momento
			prepare_response(proposer, MessageAcceptedToDate);
			// Seta a mensagem atual
			MessageAcceptedToDate = r;
			acceptedProposer = proposer;
		}

		flag.clear();
	}
	
	// Envia mensagem pra preparar resposta ao Proposer
	void prepare_response(int proposer, message_t msg) {
		sendMessage(msg, proposer, MessTags::prep_res1);
	}

	// Recebe accepted request do Proposer
	message_t accept_request(int proposer) {
		message_t ret = receiveMessage(proposer,MessTags::accp_req1);
		// Garante que threads não alterem a MessageAcceptedToDate até o fim da operação
		while(flag.test_and_set());
		if (ret.message_code == MessageAcceptedToDate.message_code)
		{
			flag.clear();
			for (set<int>::iterator it = learner.begin(); it != learner.end(); ++it)
				accepted(*it, ret);
		}
		else
			flag.clear();
	}

	// Envia mensagem aceita para os Learners
	void accepted(int learner, message_t msg) {
		sendMessage(msg, learner, MessTags::accepted1);
	}
};
	
class Learner {
	public:
	map<int, atomic_int> messages_count;
	map<int, message_t> messages;
	atomic_int responses;

	Learner() {
	}

	message_t start() {
		// Cria threads de aceitação
		thread accept_threads[acceptor.size()];
		for(set<int>::iterator it = acceptor.begin(); it != acceptor.end(); ++it) {
			accept_threads[*it] = thread(accept, *it);
		}
		// Espera join
		for(set<int>::iterator it = acceptor.begin(); it != acceptor.end(); ++it) {
			accept_threads[*it].join();
		}

		return counter();
	}

	// Conta qual a resposta mais votada e retorna a mesma
	message_t counter() {
		int count = -1;
		message_t ret;
		for (set<int>::iterator it = acceptor.begin(); it != acceptor.end(); ++it)
		{
			if (messages_count[*it] > count) {
				count = messages_count[*it];
				ret = messages[*it];
			}
		}

		return ret;
	}

	void accept(int accep) {
		message_t ret = receiveMessage(accep, MessTags::accepted1);

		messages_count[ret.message_code]++;
		messages[ret.message_code] = ret;
		responses++;
	}
};
}