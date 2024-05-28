#include "ClientWithProducer.h"
#include <iostream>  // Include iostream for cout and cerr

void ClientWithProducer::streamMessagesToKafka(const std::string& message) {
    RdKafka::ErrorCode resp = producer->produce(
        /* Topic name */ KAFKA_TOPIC,
        /* Any Partition */ RdKafka::Topic::PARTITION_UA,
        /* Message payload and length */ RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
        const_cast<char *>(message.c_str()), message.size(),
        /* Optional key and its length */ NULL, 0,
        /* Message timestamp (defaults to current time) */ 0,
        /* Message headers, if any */ NULL,
        /* Per-message opaque value passed to delivery report */ NULL);

    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "Failed to produce message: " << RdKafka::err2str(resp) << std::endl;
    } else {
        std::cout << "Message produced successfully" << std::endl;
    }

    // Poll to handle delivery reports
    producer->poll(0);
}

void ClientWithProducer::processMessages() {
    // Call the base class method
    TestCppClient::processMessages();

    // Your additional logic for Kafka streaming
    std::string message = "example message"; // Replace with your actual message
    streamMessagesToKafka(message);

    // Optionally, handle any additional logic required
    std::cout << "Kafka Producer Out Queue Length: " << producer->outq_len() << std::endl;
    if (producer->outq_len() > 0) {
        producer->poll(1000);
    }
}
