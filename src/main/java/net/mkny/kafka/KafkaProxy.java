package net.mkny.kafka;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import kafka.api.ApiVersion;
import kafka.controller.KafkaController;
import kafka.coordinator.group.GroupCoordinator;
import kafka.coordinator.transaction.TransactionCoordinator;
import kafka.network.RequestChannel;
import kafka.network.SocketServer;
import kafka.security.CredentialProvider;
import kafka.security.auth.Authorizer;
import kafka.server.AdminManager;
import kafka.server.BrokerTopicStats;
import kafka.server.DelegationTokenManager;
import kafka.server.FetchManager;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRequestHandlerPool;
import kafka.server.MetadataCache;
import kafka.server.QuotaFactory;
import kafka.server.ReplicaManager;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import scala.None$;
import scala.Option;
import scala.reflect.ClassTag;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class KafkaProxy {

    public static void main(String[] args) {

        /*
         * Hostname and port of the proxy that clients connect to
         */
        String proxyHostname = System.getenv("PROXY_HOSTNAME");
        int proxyPort = Integer.parseInt(System.getenv("PROXY_PORT"));
        InetSocketAddress proxyAddress = new InetSocketAddress(proxyHostname, proxyPort);
        System.out.println("Proxy Address: " + proxyAddress.toString());
        /*
         * Hostname and port of the upstream Kafka broker to which the proxy forwards requests
         */
        String upstreamHostname = System.getenv("UPSTREAM_HOSTNAME");
        int upstreamPort = Integer.parseInt(System.getenv("UPSTREAM_PORT"));
        InetSocketAddress upstreamBrokerAddress = new InetSocketAddress(upstreamHostname, upstreamPort);
        System.out.println("Proxy Address: " + upstreamBrokerAddress.toString());

        /*
         * Topic name aliasing configuration. The key is the topic name alias, known to the client, which the proxy
         * converts on the fly to the key value, which is the real name of the topic known to the broker.
         */
        Map<String, String> aliasToTopicName = Map.ofEntries(
                Map.entry("monolog", "monolog-aggregate-2018-11-24")
        );

        /*
         * The proxy is effectively the shell of a Kafka broker. This is the proxy's Kafka broker config. The values
         * initialized below must match the upstream Broker config.
         */
        HashMap<Object, Object> proxyServerConfigProps = new HashMap<>();
        proxyServerConfigProps.put("zookeeper.connect", ""); // Required
        proxyServerConfigProps.put("security.protocol", "PLAINTEXT");
        proxyServerConfigProps.put("broker.id", "1");
        proxyServerConfigProps.put("listeners", "PLAINTEXT://" + proxyAddress.getHostName() + ":" + proxyAddress.getPort());

        KafkaConfig proxyServerConfig = new KafkaConfig(proxyServerConfigProps);

        /*
         * An instance of the same Socket Server class used in Kafka brokers
         */
        SocketServer server = new SocketServer(proxyServerConfig, new Metrics(), Time.SYSTEM,
                new CredentialProvider(ScramMechanism.mechanismNames(),
                        new DelegationTokenCache(ScramMechanism.mechanismNames())));

        server.startup(true);
        server.startProcessors();

        /*
         * The KafkaApiProxy intercepts Kafka API requests, performs some conversions, and forwards them to the upstream
         * Kafka broker.
         */
        KafkaApiProxy kafkaApi = new KafkaApiProxy(server.requestChannel(),
                null,
                null,
                null,
                null,
                null,
                null,
                proxyServerConfig.brokerId(),
                proxyServerConfig,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                Time.SYSTEM,
                null,
                proxyAddress,
                upstreamBrokerAddress,
                aliasToTopicName
        );

        new KafkaRequestHandlerPool(proxyServerConfig.brokerId(), server.requestChannel(), kafkaApi, Time.SYSTEM, 1);

        System.out.println("Starting proxy...");
        kafkaApi.run();
    }

    private static class KafkaApiProxy extends kafka.server.KafkaApis {

        private final RequestChannel requestChannel;
        private final Map<String, NetworkClient> clients = new HashMap<>();
        private final ConcurrentLinkedQueue<KafkaApiProxy.NetClientRequest> upstreamRequestQueue = new ConcurrentLinkedQueue<>();
        private final InetSocketAddress upstreamBrokerAddress;
        private final InetSocketAddress proxyAddress;
        private final Map<String, String> aliasToTopicName;

        KafkaApiProxy(RequestChannel requestChannel, ReplicaManager replicaManager, AdminManager adminManager,
                      GroupCoordinator groupCoordinator, TransactionCoordinator txnCoordinator,
                      KafkaController controller, KafkaZkClient zkClient, int brokerId, KafkaConfig config,
                      MetadataCache metadataCache, Metrics metrics, Option<Authorizer> authorizer,
                      QuotaFactory.QuotaManagers quotas, FetchManager fetchManager, BrokerTopicStats brokerTopicStats,
                      String clusterId, Time time, DelegationTokenManager tokenManager,
                      InetSocketAddress proxyAddress, InetSocketAddress upstreamBrokerAddress,
                      Map<String, String> aliasToTopicName) {
            super(requestChannel, replicaManager, adminManager, groupCoordinator, txnCoordinator, controller, zkClient,
                    brokerId, config, metadataCache, metrics, authorizer, quotas, fetchManager, brokerTopicStats, clusterId,
                    time, tokenManager);
            this.requestChannel = requestChannel;
            this.proxyAddress = proxyAddress;
            this.upstreamBrokerAddress = upstreamBrokerAddress;
            this.aliasToTopicName = aliasToTopicName;
        }

        private List<Node> copyAndReplaceNodeList(Collection<Node> isrOld) {
            List<Node> isrNew = new ArrayList<>(isrOld.size());

            isrOld.forEach(nodeOld -> {
                Node nodeNew = replaceNodeHostname(nodeOld);
                isrNew.add(nodeNew);
            });

            return isrNew;
        }

        private Node replaceNodeHostname(Node nodeOld) {
            return new Node(nodeOld.id(), proxyAddress.getHostName(), proxyAddress.getPort(), nodeOld.rack());
        }

        /**
         * Converts InetSocketAddress to Kafka hostname/port string
         *
         * @param address
         * @return String in hostname:port format
         */
        String socketAddressToBrokerAddressString(InetSocketAddress address) {
            return address.getHostName() + ":" + address.getPort();
        }

        NetworkClient makeClient(String connectionId) {

            Metadata metadata = new Metadata(100, 100, true);
            metadata.update(Cluster.bootstrap(Collections.singletonList(upstreamBrokerAddress)), Collections.emptySet(), 0);

            LogContext logContext = new LogContext("Network client");
            Metrics metrics = new Metrics();

            Properties props = new Properties();
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, socketAddressToBrokerAddressString(upstreamBrokerAddress));

            ConsumerConfig c = new ConsumerConfig(props);

            ChannelBuilder chan = ClientUtils.createChannelBuilder(c);

            Selector selector = new Selector(c.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, Time.SYSTEM, "consumer", chan, logContext);

            NetworkClient netClient = new NetworkClient(
                    selector,
                    metadata,
                    connectionId,
                    100, // a fixed large enough value will suffice for max in-flight requests
                    c.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    c.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    c.getInt(ConsumerConfig.SEND_BUFFER_CONFIG),
                    c.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
                    c.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                    Time.SYSTEM,
                    true,
                    new ApiVersions(),
                    logContext
            );

            while (!netClient.ready(new Node(1, upstreamBrokerAddress.getHostName(), upstreamBrokerAddress.getPort()), 1000)) {
                netClient.poll(100, System.currentTimeMillis());
            }

            return netClient;
        }

        @Override
        public void handle(RequestChannel.Request request) {

            try {
                System.out.println(request.context().header.apiKey());
                super.handle(request);
            } catch (Throwable t) {
                System.out.println("Request error: " + request.header().apiKey());
                t.printStackTrace();
            }
        }

        @Override
        public void handleProduceRequest(RequestChannel.Request request) {

            ClassTag<ProduceRequest> tag = scala.reflect.ClassTag$.MODULE$.apply(ProduceRequest.class);
            ProduceRequest req = request.body(tag, null);

            final Map<TopicPartition, MemoryRecords> partitionRecords = new HashMap<>(req.partitionRecordsOrFail().size());
            final Set<String> aliasedTopics = new HashSet<>();

            for (Map.Entry<TopicPartition, MemoryRecords> partitionRecord : req.partitionRecordsOrFail().entrySet()) {

                final TopicPartition downstreamTopicPartition = partitionRecord.getKey();
                final TopicPartition upstreamTopicPartition;

                if (isAlias(partitionRecord.getKey().topic())) {
                    final String aliasTopicName = aliasToTopicName(downstreamTopicPartition.topic());
                    aliasedTopics.add(aliasTopicName);
                    upstreamTopicPartition = new TopicPartition(aliasTopicName, downstreamTopicPartition.partition());
                } else {
                    upstreamTopicPartition = downstreamTopicPartition;
                }

                partitionRecords.put(upstreamTopicPartition, partitionRecord.getValue());
            }

            upstreamRequestQueue.offer(NetClientRequest.build(
                    request.context().connectionId,
                    ProduceRequest.Builder.forCurrentMagic(
                            req.acks(),
                            req.timeout(),
                            partitionRecords
                    ),
                    clientResponse -> {
                        ProduceResponse responseFromBroker = (ProduceResponse) clientResponse.responseBody();

                        Map<TopicPartition, ProduceResponse.PartitionResponse> downstreamResponse =
                                new HashMap<>(responseFromBroker.responses().size());

                        responseFromBroker.responses().forEach(((upstreamTopicPartition, partitionResponse) -> {

                            final TopicPartition downstreamTopicPartition;

                            if (aliasedTopics.contains(upstreamTopicPartition.topic())) {
                                downstreamTopicPartition = new TopicPartition(topicNameToAlias(upstreamTopicPartition.topic()),
                                        upstreamTopicPartition.partition());
                            } else {
                                downstreamTopicPartition = upstreamTopicPartition;
                            }

                            downstreamResponse.put(downstreamTopicPartition, partitionResponse);
                        }));

                        ProduceResponse responseToClient = new ProduceResponse(
                                downstreamResponse, responseFromBroker.throttleTimeMs());

                        Send response = request.context().buildResponse(responseToClient);

                        requestChannel.sendResponse(new RequestChannel.SendResponse(request, response, Option.apply(response.toString()), ScalaLang.none()));
                    }
            ));
        }

        @Override
        public void handleLeaderAndIsrRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleLeaderAndIsrRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleStopReplicaRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleStopReplicaRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleUpdateMetadataRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleUpdateMetadataRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleControlledShutdownRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleControlledShutdownRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleOffsetCommitRequest(RequestChannel.Request request) {

            ClassTag<OffsetCommitRequest> tag = scala.reflect.ClassTag$.MODULE$.apply(OffsetCommitRequest.class);
            OffsetCommitRequest req = request.body(tag, null);

            Map<TopicPartition, OffsetCommitRequest.PartitionData> upstreamOffsetData = new HashMap<>(req.offsetData().size());

            final Set<String> aliasedTopics = new HashSet<>();
            req.offsetData().forEach(((downstreamTopicPartition, partitionData) -> {

                final TopicPartition upstreamTopicPartition;

                if (isAlias(downstreamTopicPartition.topic())) {
                    String aliasTopicName = aliasToTopicName(downstreamTopicPartition.topic());
                    aliasedTopics.add(aliasTopicName);
                    upstreamTopicPartition = new TopicPartition(aliasToTopicName(downstreamTopicPartition.topic()),
                            downstreamTopicPartition.partition());
                } else {
                    upstreamTopicPartition = downstreamTopicPartition;
                }

                upstreamOffsetData.put(upstreamTopicPartition, partitionData);
            }));

            upstreamRequestQueue.offer(NetClientRequest.build(
                    request.context().connectionId,
                    new OffsetCommitRequest.Builder(req.groupId(), upstreamOffsetData)
                            .setGenerationId(req.generationId())
                            .setMemberId(req.memberId())
                            .setRetentionTime(req.retentionTime()),
                    clientResponse -> {

                        OffsetCommitResponse responseFromBroker = (OffsetCommitResponse) clientResponse.responseBody();

                        Map<TopicPartition, Errors> downstreamResponse = new HashMap<>(responseFromBroker.responseData().size());

                        responseFromBroker.responseData().forEach(((upstreamTopicPartition, errors) -> {

                            final TopicPartition downstreamTopicPartition;

                            if (aliasedTopics.contains(upstreamTopicPartition.topic())) {
                                downstreamTopicPartition = new TopicPartition(topicNameToAlias(upstreamTopicPartition.topic()),
                                        upstreamTopicPartition.partition());
                            } else {
                                downstreamTopicPartition = upstreamTopicPartition;
                            }

                            downstreamResponse.put(downstreamTopicPartition, errors);
                        }));

                        OffsetCommitResponse responseToClient = new OffsetCommitResponse(
                                responseFromBroker.throttleTimeMs(), downstreamResponse);

                        Send response = request.context().buildResponse(responseToClient);

                        requestChannel.sendResponse(new RequestChannel.SendResponse(request, response, Option.apply(response.toString()), ScalaLang.none()));
                    }
            ));
        }

        @Override
        public void handleFetchRequest(RequestChannel.Request request) {

            ClassTag<FetchRequest> tag = scala.reflect.ClassTag$.MODULE$.apply(FetchRequest.class);
            FetchRequest req = request.body(tag, null);

            final Map<TopicPartition, FetchRequest.PartitionData> upstreamFetchData = new HashMap<>(req.fetchData().size());
            final Set<String> aliasedTopics = new HashSet<>();

            req.fetchData().forEach((topicPartition, partitionData) -> {
                if (isAlias(topicPartition.topic())) {
                    String aliasTopicName = aliasToTopicName(topicPartition.topic());
                    aliasedTopics.add(aliasTopicName);
                    upstreamFetchData.put(new TopicPartition(aliasTopicName, topicPartition.partition()), partitionData);
                } else {
                    upstreamFetchData.put(topicPartition, partitionData);
                }
            });

            upstreamRequestQueue.offer(NetClientRequest.build(
                    request.context().connectionId,
                    FetchRequest.Builder.forConsumer(
                            req.maxWait(),
                            req.minBytes(),
                            upstreamFetchData
                    ).isolationLevel(req.isolationLevel())
                            .metadata(req.metadata())
                            .setMaxBytes(req.maxBytes())
                            .toForget(req.toForget()),
                    clientResponse -> {

                        FetchResponse responseFromBroker = (FetchResponse) clientResponse.responseBody();

                        LinkedHashMap<TopicPartition, FetchResponse.PartitionData> downstreamResponseData =
                                new LinkedHashMap<>(responseFromBroker.responseData().size());

                        for (Object entry : responseFromBroker.responseData().entrySet()) {

                            Map.Entry<TopicPartition, FetchResponse.PartitionData> responseData = (Map.Entry<TopicPartition, FetchResponse.PartitionData>) entry;

                            final TopicPartition upstreamTopicPartition = responseData.getKey();
                            final TopicPartition downstreamTopicPartition;

                            if (aliasedTopics.contains(upstreamTopicPartition.topic())) {
                                downstreamTopicPartition = new TopicPartition(topicNameToAlias(upstreamTopicPartition.topic()),
                                        upstreamTopicPartition.partition());
                            } else {
                                downstreamTopicPartition = upstreamTopicPartition;
                            }

                            downstreamResponseData.put(downstreamTopicPartition, responseData.getValue());

                        }

                        FetchResponse responseToClient = new FetchResponse(
                                responseFromBroker.error(), downstreamResponseData, responseFromBroker.throttleTimeMs(), responseFromBroker.sessionId()
                        );

                        Send response = request.context().buildResponse(responseToClient);

                        requestChannel.sendResponse(new RequestChannel.SendResponse(request, response, Option.apply(response.toString()), ScalaLang.none()));
                    }
            ));
        }

        private void sendDownstreamResponse(RequestChannel.Request request, ClientResponse clientResponse) {
            Send response = request.context().buildResponse(clientResponse.responseBody());
            requestChannel.sendResponse(new RequestChannel.SendResponse(request, response, Option.apply(clientResponse.toString()), ScalaLang.none()));
        }

        @Override
        public void handleListOffsetRequest(RequestChannel.Request request) {

            ClassTag<ListOffsetRequest> tag = scala.reflect.ClassTag$.MODULE$.apply(ListOffsetRequest.class);
            ListOffsetRequest req = request.body(tag, null);

            final Set<String> aliasedTopics = new HashSet<>();
            final Map<TopicPartition, Long> upstreamPartitionTimestamps = new HashMap<>();

            req.partitionTimestamps().forEach((topicPartition, timestamp) -> {
                if (isAlias(topicPartition.topic())) {
                    String aliasTopicName = aliasToTopicName(topicPartition.topic());
                    aliasedTopics.add(aliasTopicName);
                    upstreamPartitionTimestamps.put(new TopicPartition(aliasTopicName, topicPartition.partition()), timestamp);
                } else {
                    upstreamPartitionTimestamps.put(topicPartition, timestamp);
                }
            });


            upstreamRequestQueue.offer(NetClientRequest.build(
                    request.context().connectionId,
                    ListOffsetRequest.Builder.forConsumer(true, req.isolationLevel())
                            .setTargetTimes(upstreamPartitionTimestamps),
                    clientResponse -> {

                        ListOffsetResponse responseFromBroker = (ListOffsetResponse) clientResponse.responseBody();

                        Map<TopicPartition, ListOffsetResponse.PartitionData> downstreamResponse = new HashMap<>(responseFromBroker.responseData().size());

                        responseFromBroker.responseData().forEach(((upstreamTopicPartition, partitionData) -> {

                            final TopicPartition downstreamTopicPartition;

                            if (aliasedTopics.contains(upstreamTopicPartition.topic())) {
                                downstreamTopicPartition = new TopicPartition(topicNameToAlias(upstreamTopicPartition.topic()),
                                        upstreamTopicPartition.partition());
                            } else {
                                downstreamTopicPartition = upstreamTopicPartition;
                            }

                            downstreamResponse.put(downstreamTopicPartition, partitionData);
                        }));

                        ListOffsetResponse responseToClient =
                                new ListOffsetResponse(responseFromBroker.throttleTimeMs(), downstreamResponse);

                        Send response = request.context().buildResponse(responseToClient);

                        requestChannel.sendResponse(new RequestChannel.SendResponse(request, response, Option.apply(response.toString()), ScalaLang.none()));
                    }
            ));
        }

        @Override
        public void handleTopicMetadataRequest(RequestChannel.Request request) {

            ClassTag<MetadataRequest> tag = scala.reflect.ClassTag$.MODULE$.apply(MetadataRequest.class);
            MetadataRequest req = request.body(tag, null);

            final List<String> upstreamTopics = new ArrayList<>(req.topics().size());
            final Set<String> aliasedTopics = new HashSet<>();

            req.topics().forEach((topicName) -> {
                if (isAlias(topicName)) {
                    final String aliasTopicName = aliasToTopicName(topicName);
                    aliasedTopics.add(aliasTopicName);
                    upstreamTopics.add(aliasTopicName);
                } else {
                    upstreamTopics.add(topicName);
                }
            });

            upstreamRequestQueue.offer(NetClientRequest.build(
                    request.context().connectionId,
                    new MetadataRequest.Builder(upstreamTopics, req.allowAutoTopicCreation()),
                    clientResponse -> {

                        MetadataResponse responseFromBroker = (MetadataResponse) clientResponse.responseBody();

                        // Rewrite topic metadata
                        List<MetadataResponse.TopicMetadata> downstreamTopicMetadata = new ArrayList<>(responseFromBroker.topicMetadata().size());

                        responseFromBroker.topicMetadata().forEach((upstreamTopicMetadata -> {
                            List<MetadataResponse.PartitionMetadata> partitionMetadataOld = upstreamTopicMetadata.partitionMetadata();
                            List<MetadataResponse.PartitionMetadata> partitionMetadataNew = new ArrayList<>(partitionMetadataOld.size());

                            partitionMetadataOld.forEach((partitionMetadata -> partitionMetadataNew.add(new MetadataResponse.PartitionMetadata(
                                    partitionMetadata.error(),
                                    partitionMetadata.partition(),
                                    replaceNodeHostname(partitionMetadata.leader()),
                                    copyAndReplaceNodeList(partitionMetadata.replicas()),
                                    copyAndReplaceNodeList(partitionMetadata.isr()),
                                    copyAndReplaceNodeList(partitionMetadata.offlineReplicas())
                            ))));

                            final String downstreamTopicName;

                            if (aliasedTopics.contains(upstreamTopicMetadata.topic())) {
                                downstreamTopicName = topicNameToAlias(upstreamTopicMetadata.topic());
                            } else {
                                downstreamTopicName = upstreamTopicMetadata.topic();
                            }

                            downstreamTopicMetadata.add(new MetadataResponse.TopicMetadata(
                                    upstreamTopicMetadata.error(),
                                    downstreamTopicName,
                                    upstreamTopicMetadata.isInternal(),
                                    partitionMetadataNew
                            ));
                        }));

                        MetadataResponse newRes = new MetadataResponse(
                                responseFromBroker.throttleTimeMs(),
                                copyAndReplaceNodeList(responseFromBroker.brokers()),
                                responseFromBroker.clusterId(),
                                responseFromBroker.controller().id(),
                                downstreamTopicMetadata
                        );

                        Send response = request.context().buildResponse(newRes);

                        requestChannel.sendResponse(new RequestChannel.SendResponse(request, response, Option.apply(response.toString()), ScalaLang.none()));
                    }
            ));
        }

        private boolean isAlias(String topicName) {
            return aliasToTopicName.containsKey(topicName);
        }

        private String aliasToTopicName(String alias) {
            return aliasToTopicName.getOrDefault(alias, alias);
        }

        private String topicNameToAlias(String topicName) {

            for (Map.Entry<String, String> entry : aliasToTopicName.entrySet()) {
                if (topicName.equals(entry.getValue())) {
                    return entry.getKey();
                }
            }

            return topicName;
        }

        @Override
        public void handleOffsetFetchRequest(RequestChannel.Request request) {

            ClassTag<OffsetFetchRequest> tag = scala.reflect.ClassTag$.MODULE$.apply(OffsetFetchRequest.class);
            OffsetFetchRequest req = request.body(tag, null);

            final Set<String> aliasedTopics = new HashSet<>();
            final List<TopicPartition> upstreamTopicPartitions = new ArrayList<>(req.partitions().size());

            req.partitions().forEach((topicPartition) -> {
                if (isAlias(topicPartition.topic())) {
                    String aliasTopicName = aliasToTopicName(topicPartition.topic());
                    aliasedTopics.add(aliasTopicName);
                    upstreamTopicPartitions.add(new TopicPartition(aliasTopicName, topicPartition.partition()));
                } else {
                    upstreamTopicPartitions.add(topicPartition);
                }
            });

            upstreamRequestQueue.offer(NetClientRequest.build(
                    request.context().connectionId,
                    new OffsetFetchRequest.Builder(req.groupId(), upstreamTopicPartitions),
                    clientResponse -> {

                        OffsetFetchResponse responseFromBroker = (OffsetFetchResponse) clientResponse.responseBody();

                        Map<TopicPartition, OffsetFetchResponse.PartitionData> downstreamResponseData =
                                new HashMap<>(responseFromBroker.responseData().size());

                        responseFromBroker.responseData().forEach(((upstreamTopicPartition, partitionData) -> {

                            final TopicPartition downstreamTopicPartition;

                            if (aliasedTopics.contains(upstreamTopicPartition.topic())) {
                                downstreamTopicPartition = new TopicPartition(topicNameToAlias(upstreamTopicPartition.topic()),
                                        upstreamTopicPartition.partition());
                            } else {
                                downstreamTopicPartition = upstreamTopicPartition;
                            }

                            downstreamResponseData.put(downstreamTopicPartition, partitionData);
                        }));

                        OffsetFetchResponse responseToClient = new OffsetFetchResponse(
                                responseFromBroker.throttleTimeMs(),
                                responseFromBroker.error(),
                                downstreamResponseData
                        );

                        Send response = request.context().buildResponse(responseToClient);

                        requestChannel.sendResponse(new RequestChannel.SendResponse(request, response, Option.apply(response.toString()), ScalaLang.none()));
                    }
            ));
        }

        @Override
        public void handleFindCoordinatorRequest(RequestChannel.Request request) {

            ClassTag<FindCoordinatorRequest> tag = scala.reflect.ClassTag$.MODULE$.apply(FindCoordinatorRequest.class);
            FindCoordinatorRequest req = request.body(tag, null);

            upstreamRequestQueue.offer(NetClientRequest.build(
                    request.context().connectionId,
                    new FindCoordinatorRequest.Builder(req.coordinatorType(), req.coordinatorKey()),
                    clientResponse -> {
                        FindCoordinatorResponse coordinatorResponse = (FindCoordinatorResponse) clientResponse.responseBody();

                        FindCoordinatorResponse coord = new FindCoordinatorResponse(coordinatorResponse.error(), replaceNodeHostname(coordinatorResponse.node()));
                        Send response = request.context().buildResponse(coord);

                        requestChannel.sendResponse(new RequestChannel.SendResponse(request, response, Option.apply(response.toString()), ScalaLang.none()));
                    }
            ));
        }

        @Override
        public void handleDescribeGroupRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleDescribeGroupRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleListGroupsRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleListGroupsRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleJoinGroupRequest(RequestChannel.Request request) {

            ClassTag<JoinGroupRequest> tag = scala.reflect.ClassTag$.MODULE$.apply(JoinGroupRequest.class);
            JoinGroupRequest req = request.body(tag, null);

            upstreamRequestQueue.offer(NetClientRequest.build(
                    request.context().connectionId,
                    new JoinGroupRequest.Builder(req.groupId(), req.sessionTimeout(), req.memberId(), req.protocolType(), req.groupProtocols()),
                    clientResponse -> sendDownstreamResponse(request, clientResponse)
            ));
        }

        @Override
        public void handleSyncGroupRequest(RequestChannel.Request request) {

            ClassTag<SyncGroupRequest> tag = scala.reflect.ClassTag$.MODULE$.apply(SyncGroupRequest.class);
            SyncGroupRequest req = request.body(tag, null);

            upstreamRequestQueue.offer(NetClientRequest.build(
                    request.context().connectionId,
                    new SyncGroupRequest.Builder(req.groupId(), req.generationId(), req.memberId(), req.groupAssignment()),
                    clientResponse -> sendDownstreamResponse(request, clientResponse)
            ));
        }

        @Override
        public void handleDeleteGroupsRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleDeleteGroupsRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleHeartbeatRequest(RequestChannel.Request request) {

            ClassTag<HeartbeatRequest> tag = scala.reflect.ClassTag$.MODULE$.apply(HeartbeatRequest.class);
            HeartbeatRequest req = request.body(tag, null);

            upstreamRequestQueue.offer(NetClientRequest.build(
                    request.context().connectionId,
                    new HeartbeatRequest.Builder(req.groupId(), req.groupGenerationId(), req.memberId()),
                    clientResponse -> sendDownstreamResponse(request, clientResponse)
            ));
        }

        @Override
        public void handleLeaveGroupRequest(RequestChannel.Request request) {

            ClassTag<LeaveGroupRequest> tag = scala.reflect.ClassTag$.MODULE$.apply(LeaveGroupRequest.class);
            LeaveGroupRequest req = request.body(tag, null);

            upstreamRequestQueue.offer(NetClientRequest.build(
                    request.context().connectionId,
                    new LeaveGroupRequest.Builder(req.groupId(), req.memberId()),
                    clientResponse -> sendDownstreamResponse(request, clientResponse)
            ));
        }

        @Override
        public void handleSaslHandshakeRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleSaslHandshakeRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleSaslAuthenticateRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleSaslAuthenticateRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleApiVersionsRequest(RequestChannel.Request request) {

            upstreamRequestQueue.offer(NetClientRequest.build(
                    request.context().connectionId,
                    new ApiVersionsRequest.Builder(),
                    (ClientResponse clientResponse) -> sendDownstreamResponse(request, clientResponse)
            ));
        }

        @Override
        public void handleCreateTopicsRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleCreateTopicsRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleCreatePartitionsRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleCreatePartitionsRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleDeleteTopicsRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleDeleteTopicsRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleDeleteRecordsRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleDeleteRecordsRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleInitProducerIdRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleInitProducerIdRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleEndTxnRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleEndTxnRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleWriteTxnMarkersRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleWriteTxnMarkersRequest proxy support is not yet implemented.");
        }

        @Override
        public void ensureInterBrokerVersion(ApiVersion version) {
            throw new UnsupportedOperationException("ensureInterBrokerVersion proxy support is not yet implemented.");
        }

        @Override
        public void handleAddPartitionToTxnRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleAddPartitionToTxnRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleAddOffsetsToTxnRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleAddOffsetsToTxnRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleTxnOffsetCommitRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleTxnOffsetCommitRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleDescribeAcls(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleDescribeAcls proxy support is not yet implemented.");
        }

        @Override
        public void handleCreateAcls(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleCreateAcls proxy support is not yet implemented.");
        }

        @Override
        public void handleDeleteAcls(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleDeleteAcls proxy support is not yet implemented.");
        }

        @Override
        public void handleOffsetForLeaderEpochRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleOffsetForLeaderEpochRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleAlterConfigsRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleAlterConfigsRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleDescribeConfigsRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleDescribeConfigsRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleAlterReplicaLogDirsRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleAlterReplicaLogDirsRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleDescribeLogDirsRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleLeaderAndIsrRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleCreateTokenRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleDescribeLogDirsRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleRenewTokenRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleRenewTokenRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleExpireTokenRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleExpireTokenRequest proxy support is not yet implemented.");
        }

        @Override
        public void handleDescribeTokensRequest(RequestChannel.Request request) {
            throw new UnsupportedOperationException("handleDescribeTokensRequest proxy support is not yet implemented.");
        }

        void run() {

            while (!Thread.currentThread().isInterrupted()) {

                while (true) {

                    NetClientRequest upstreamRequest = upstreamRequestQueue.poll();

                    if (upstreamRequest == null) {
                        break;
                    }

                    NetworkClient netClient = clients.computeIfAbsent(upstreamRequest.connectionId, this::makeClient);

                    ClientRequest clientRequest = netClient.newClientRequest(
                            brokerId() + "",
                            upstreamRequest.requestBuilder,
                            System.currentTimeMillis(),
                            true,
                            2000,
                            upstreamRequest.requestCompletionHandler
                    );

                    netClient.send(clientRequest, System.currentTimeMillis());
                }

                for (Map.Entry<String, NetworkClient> client : clients.entrySet()) {
                    try {
                        client.getValue().poll(100, System.currentTimeMillis());
                    } catch (Throwable t) {
                        System.out.println("Got error: " + t.getMessage());
                        t.printStackTrace();
                    }

                }
            }
        }

        private static class ScalaLang {
            static <T> Option<T> none() {
                return (Option<T>) None$.MODULE$;
            }
        }

        private static class NetClientRequest {

            final String connectionId;
            final AbstractRequest.Builder requestBuilder;
            final RequestCompletionHandler requestCompletionHandler;

            NetClientRequest(String connectionId, AbstractRequest.Builder requestBuilder, RequestCompletionHandler requestCompletionHandler) {
                this.connectionId = connectionId;
                this.requestBuilder = requestBuilder;
                this.requestCompletionHandler = requestCompletionHandler;
            }

            static NetClientRequest build(String connectionId, AbstractRequest.Builder requestBuilder, RequestCompletionHandler requestCompletionHandler) {
                return new NetClientRequest(connectionId, requestBuilder, requestCompletionHandler);
            }
        }

    }

}